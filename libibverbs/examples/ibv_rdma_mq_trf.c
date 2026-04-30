/* SPDX-License-Identifier: Marvell-MIT
 * Copyright (c) 2025 Marvell.
 */

#define _GNU_SOURCE
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <infiniband/verbs.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <endian.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sched.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <getopt.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include "ibv_rdma_mq_trf.h"
#include <net/if.h>
#include <ifaddrs.h>
#include <errno.h>
#include <limits.h>
#include <rdma/rdma_cma.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <time.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CMD_BUF_SIZE  256
#define LINE_BUF_SIZE 512

#define MAX_IB_DEVICES 8
static struct device_ctx g_devices[MAX_IB_DEVICES];
static int g_num_devices;
struct app_ctx g_ctx = {0};
static struct cm_test cm_test = {0};
static struct rdma_addrinfo hints = {0};
static struct qp_stats_record g_saved_stats[MAX_QUEUES];
static void save_qp_stats(int slot, struct qp_data *qdata);

/* Bitmap helpers forward declarations */
void atomic_bitmap_set(uint16_t count_id);
void atomic_bitmap_clear(uint16_t count_id);
int atomic_bitmap_is_set(uint16_t count_id);

/* TCP functions forward declarations */
static int recv_remote_info(int rsock, struct qp_info *local, struct qp_info *remote);
static int send_local_info(int rsock, struct qp_info *local);

/* CM functions forward declarations */
static void cm_connect_error(void);
static int cm_alloc_nodes(void);
static void cm_destroy_nodes(void);

/* Init functions forward declarations */
int rdma_mq_init(int csock, struct device_ctx *dev);
int rdma_mq_init_unified(struct conn_ctx *conn);
void *rdma_mq_thread(void *arg);

/* QP division among threads */
struct qp_range *divide_qps_among_threads(int total_qps, int nthreads);

static void
add_ip_to_device(struct device_ctx *dev, const char *ip)
{
	dev->ip_list = realloc(dev->ip_list, sizeof(char *) * (dev->ip_count + 1));
	dev->ip_list[dev->ip_count] = strdup(ip);
	dev->ip_count++;
	printf("[INFO] Added IP %s to device %s\n", ip, dev->ib_devname);
}

static struct device_ctx *
find_device_by_ip(const char *ip)
{
	for (int i = 0; i < g_num_devices; ++i) {
		for (int j = 0; j < g_devices[i].ip_count; ++j) {
			if (strcmp(g_devices[i].ip_list[j], ip) == 0)
				return &g_devices[i];
		}
	}
	return NULL;
}

static int
get_netdev_from_rdma(const char *rdma_dev, char *netdev, size_t netdev_size)
{
	char cmd[CMD_BUF_SIZE];

	snprintf(cmd, sizeof(cmd), "rdma link show %s/1 2>/dev/null", rdma_dev);

	FILE *fp = popen(cmd, "r");

	if (!fp) {
		perror("popen failed");
		return -1;
	}

	char line[LINE_BUF_SIZE];

	while (fgets(line, sizeof(line), fp)) {
		char *netdev_ptr = strstr(line, "netdev ");

		if (netdev_ptr) {
			netdev_ptr += strlen("netdev ");
			if (sscanf(netdev_ptr, "%s", netdev) == 1) {
				pclose(fp);
				return 0;
			}
		}
	}

	pclose(fp);
	return -1; // Not found
}

static int
enumerate_ib_devices_and_ips(void)
{
	int ret = 0;
	struct ibv_device **dev_list = ibv_get_device_list(NULL);
	char netdev[IFNAMSIZ] = {0};

	if (!dev_list) {
		printf("Dev list get failed\n");
		exit(1);
	}
	struct ifaddrs *ifaddr, *ifa;

	if (getifaddrs(&ifaddr) == -1) {
		perror("getifaddrs");
		exit(1);
	}
	for (int i = 0; dev_list[i] && g_num_devices < MAX_IB_DEVICES; ++i) {
		struct ibv_device *ib_dev = dev_list[i];
		struct device_ctx *dev = &g_devices[g_num_devices];

		dev->ib_devname = strdup(ibv_get_device_name(ib_dev));
		if (!g_ctx.is_server) {
			if (g_ctx.ib_devname && strcmp(g_ctx.ib_devname, dev->ib_devname) != 0) {
				printf("Skipping device %s as it does not match specified device %s\n",
				       dev->ib_devname, g_ctx.ib_devname);
				free(dev->ib_devname);
				continue;
			}
		}
		dev->dev_ctx = ibv_open_device(ib_dev);
		if (!dev->dev_ctx) {
			printf("Couldn't get context for %s\n", dev->ib_devname);
			continue;
		}
		// No per-device PD allocation; PD is per-connection now
		dev->ip_list = NULL;
		dev->ip_count = 0;
		/* Clear per-client maps */
		for (int c = 0; c < MAX_CLIENTS; ++c) {
			dev->client_pds[c] = NULL;
			dev->fd_to_client_idx[c] = -1;
		}
		if (get_netdev_from_rdma(dev->ib_devname, netdev, sizeof(netdev)) < 0) {
			printf("Failed to get netdev for RDMA device %s\n", dev->ib_devname);
			ret = -1;
			break;
		}

		for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
			if (ifa->ifa_addr == NULL || ifa->ifa_addr->sa_family != AF_INET)
				continue;
			if (!strcmp(ifa->ifa_name, netdev)) {
				char ip[INET_ADDRSTRLEN];
				struct sockaddr_in *sa = (struct sockaddr_in *)ifa->ifa_addr;

				inet_ntop(AF_INET, &sa->sin_addr, ip, sizeof(ip));
				add_ip_to_device(dev, ip);
			}
		}
		printf("[INFO] IB device %s has %d IP(s)\n", dev->ib_devname, dev->ip_count);
		g_num_devices++;
		if (!g_ctx.is_server)
			break;
	}
	freeifaddrs(ifaddr);
	ibv_free_device_list(dev_list);

	return ret;
}

static int
count_free_slots(void)
{
	int freec = 0;

	for (int i = 0; i < g_ctx.total_slots; ++i) {
		if (!atomic_bitmap_is_set(i) && g_ctx.qp_data[i] == NULL)
			freec++;
	}
	return freec;
}

static int
find_free_slot(void)
{
	int limit = g_ctx.total_slots > 0 ? g_ctx.total_slots : MAX_QUEUES;

	for (int i = 0; i < limit; ++i) {
		if (!atomic_bitmap_is_set(i) && g_ctx.qp_data[i] == NULL)
			return i;
	}
	return -1;
}

static void
signal_handler(int signum)
{
	if (signum == SIGINT || signum == SIGTERM) {
		uint64_t val = 1;

		printf("Closing poll FD\n");
		if (write(g_ctx.shutdown_fd, &val, sizeof(val)) != sizeof(val))
			printf("Writing to g_ctx.shutdown_fd failed\n");
		g_ctx.force_quit = true;
	}
}

static void
usage(const char *argv0)
{
	printf("Usage:\n");
	printf("  %s            start a server and wait for connection (no args)\n", argv0);
	printf("  %s <host>     connect to server at <host>\n", argv0);
	printf("\n");
	printf("Options:\n");
	printf("  -g, --gid-idx=<gid index> local port gid index\n");
	printf("  -i, --interval=<us>  Inter-packet interval in microseconds\n");
	printf("  -q, --num-qp=<num QP> Number of QP's\n");
	printf("  -c, --num-client=<num client> Number of clients that can connect. Note: Option only for server\n");
	printf("  -t, --num-thread=<num thread> Number of thread's\n");
	printf("  -n, --num-packet=<num packets> Number of packets to be sent per QP\n");
	printf("  -s, --send All QP's send RoCE packets. Default is send and receive per QP\n");
	printf("  -r, --recv All QP's receive RoCE packets. Default is send and receive per QP\n");
	printf("  -d, --ib-dev=<dev> use IB device <dev> (default first device found)\n");
	printf("  -h, --help Command Help\n");
	printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
	printf("  -z, --size=<size>      message size in bytes (default 1024)\n");
	printf("  --nb-sge=<num>         number of SGEs per WR (default 1)\n");
	printf("  --rdma-cm           Use RDMA CM instead of TCP for connections\n");
	printf("  --src-addr=<addr>   Source address for RDMA CM\n");
	printf("  --cm-port=<port>    Port for RDMA CM (default 7174)\n");
	printf("  --qp-type=<UD|RC> QP type (default: UD)\n");
	printf("  --op-type=<SEND|WRITE|WRITE_IMM|READ> Operation type (default: SEND)\n");
	printf("  --max-send-wr=<N>   SQ depth per QP (default 2)\n");
	printf("  --signal-every=<N>  Signal every N sends (default 1 = signal all)\n");
	printf("  --inline=<bytes>    Use inline for SEND up to this size (0=disable)\n");
	printf("  --pingpong          For SEND: only SEND after a RECV (default on)\n");
	printf("  --no-pingpong       Disable ping-pong; allow SEND when idle\n");
	printf("  --stats             Enable per-QP statistics; dump to file on exit\n");
	printf("  --separate-cq       Use separate CQs for SQ and RQ (default: shared CQ)\n");
}

static inline void
rdma_init_default(void)
{
	g_ctx.qp_type = IBV_QPT_UD;
	g_ctx.force_quit = false;
	g_ctx.max_num_threads = MAX_NUM_THREADS;
	g_ctx.max_cpu_cores = MAX_CPU_CORE;
	g_ctx.min_cpu_cores = MIN_CPU_CORE;
	g_ctx.op_type = IBV_WR_SEND;
	g_ctx.rx_depth = 512;
	g_ctx.rx_thold = 128;
	g_ctx.servername = NULL;
	g_ctx.ib_devname = NULL;
	g_ctx.num_pkt_set = false;
	g_ctx.msg_size = 1024;
	g_ctx.num_threads = -1;
	g_ctx.num_client = 1;
	g_ctx.sockfd = -1;
	g_ctx.numqp = -1;
	/* Let runtime choose a valid GID index if not provided via -g */
	g_ctx.gidx = -1;
	g_ctx.dir = 0;
	g_ctx.qpcount = 0;
	g_ctx.path_mtu = 1024; // Default MTU
	g_ctx.nb_sge = 1;      // Default SGE count
	memset(&g_ctx.qbmap, 0, sizeof(g_ctx.qbmap));
	g_ctx.max_send_wr = 2;
	g_ctx.debug = false;    // Debug disabled by default, use -D to enable
	g_ctx.signal_every = 1; // signal every send by default
	g_ctx.inline_thresh = 0;
	g_ctx.pingpong = true;
	g_ctx.use_rdma_cm = false; // default to TCP mode
	g_ctx.src_addr = NULL;
	g_ctx.port = strdup("7174"); // default port for RDMA CM

	/* Initialize RDMA CM hints - will be updated based on QP type */
	memset(&hints, 0, sizeof(hints));
	hints.ai_port_space = RDMA_PS_UDP; // Default for UD, updated based on QP type
	hints.ai_flags = RAI_PASSIVE;      // For server binding (updated later for client)
	hints.ai_family = AF_INET;         // Use IPv4 addresses
	g_ctx.stats_enabled = false;
	g_ctx.separate_cq = false;
}

/* Pick a valid (non-zero) GID index for the given device/port if none was provided. */
/* Return a GID index for the given device/port that matches the netdev and/or IPv4 local IP. */
static int
choose_gid_index_for_local(struct ibv_context *ctx, const char *ibdevname, uint8_t port,
			   const char *local_ip, int preferred_idx)
{
	struct ibv_port_attr p = {0};
	char netdev[IFNAMSIZ] = {0};
	char path[PATH_MAX];
	int fallback = -1;

	if (ibv_query_port(ctx, port, &p))
		p.gid_tbl_len = 32; /* best effort */

	if (preferred_idx >= 0) {
		union ibv_gid g;

		if (ibv_query_gid(ctx, port, preferred_idx, &g) == 0 &&
		    (g.global.subnet_prefix || g.global.interface_id))
			return preferred_idx;
	}

	/* Determine netdev name for this RDMA device/port */
	if (get_netdev_from_rdma(ibdevname, netdev, sizeof(netdev)) < 0)
		netdev[0] = '\0';

	/* Parse local IPv4 bytes for IPv4-mapped GID matching */
	struct in_addr lip = {0};
	bool have_ip = local_ip && inet_pton(AF_INET, local_ip, &lip) == 1;

	/* 1) Prefer IPv4-mapped GID that matches local_ip */
	if (have_ip) {
		for (int idx = 0; idx < (int)p.gid_tbl_len && idx < 64; ++idx) {
			union ibv_gid g;

			memset(&g, 0, sizeof(g));
			if (ibv_query_gid(ctx, port, idx, &g))
				continue;
			if (!(g.global.subnet_prefix || g.global.interface_id))
				continue;
			const uint8_t *b = (const uint8_t *)&g;
			bool mapped = true;

			for (int k = 0; k < 10; ++k)
				if (b[k] != 0x00) {
					mapped = false;
					break;
				}
			if (mapped && b[10] == 0xff && b[11] == 0xff) {
				if (memcmp(&b[12], &lip, 4) == 0)
					return idx;
			}
		}
	}

	/* 2) Prefer matching sysfs ndevs entry if available */
	for (int idx = 0; idx < (int)p.gid_tbl_len && idx < 64; ++idx) {
		union ibv_gid g;

		memset(&g, 0, sizeof(g));
		if (ibv_query_gid(ctx, port, idx, &g))
			continue;
		if (!(g.global.subnet_prefix || g.global.interface_id))
			continue; /* all zero */

		if (fallback < 0)
			fallback = idx; /* remember first non-zero */

		if (netdev[0]) {
			snprintf(path, sizeof(path),
				 "/sys/class/infiniband/%s/ports/%u/gid_attrs/ndevs/%d", ibdevname,
				 port, idx);
			FILE *f = fopen(path, "r");

			if (f) {
				char buf[IFNAMSIZ + 8] = {0};

				if (fgets(buf, sizeof(buf), f)) {
					buf[strcspn(buf, "\r\n")] = 0;
					if (strcmp(buf, netdev) == 0) {
						fclose(f);
						return idx;
					}
				}
				fclose(f);
			}
		}
	}
	return fallback >= 0 ? fallback : 0;
}

static int
make_socket_non_blocking(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);

	if (flags == -1)
		return -1;

	if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
		perror("fcntl F_SETFL O_NONBLOCK");
		return -1;
	}

	return 0;
}

static int
tcp_server_listen(int port)
{
	struct sockaddr_in addr = {0};
	struct epoll_event event;
	int epoll_fd, val = 1;

	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(port);

	g_ctx.sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (g_ctx.sockfd == -1) {
		printf("Error creating server socket\n");
		return -1;
	}

	setsockopt(g_ctx.sockfd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
	if (bind(g_ctx.sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		printf("Error binding the server socket\n");
		return -1;
	}

	if (listen(g_ctx.sockfd, SOMAXCONN) < 0) {
		printf("Error listening on server socket\n");
		return -1;
	}

	/* Make server socket non-blocking */
	if (make_socket_non_blocking(g_ctx.sockfd) == -1) {
		printf("Error making socket non blocking\n");
		return -1;
	}

	/* Create epoll instance */
	epoll_fd = epoll_create1(0);
	if (epoll_fd == -1) {
		printf("epoll create failed\n");
		return -1;
	}

	/* Add server socket to epoll */
	event.data.fd = g_ctx.sockfd;
	event.events = EPOLLIN;
	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_ctx.sockfd, &event) == -1) {
		printf("epoll_ctl: server g_ctx.sockfd\n");
		close(epoll_fd);
		close(g_ctx.sockfd);
		return -1;
	}

	g_ctx.shutdown_fd = eventfd(0, 0);
	event.data.fd = g_ctx.shutdown_fd;
	event.events = EPOLLIN;

	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_ctx.shutdown_fd, &event) == -1) {
		printf("epoll_ctl: server g_ctx.sockfd\n");
		close(g_ctx.shutdown_fd);
		close(epoll_fd);
		close(g_ctx.sockfd);
		return -1;
	}

	return epoll_fd;
}

static int
tcp_connect_to_server(const char *ip, int port)
{
	int csockfd = -1;
	struct sockaddr_in addr = {.sin_family = AF_INET, .sin_port = htons(port)};

	csockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (csockfd == -1) {
		printf("Error creating server socket\n");
		return -1;
	}

	inet_pton(AF_INET, ip, &addr.sin_addr);
	connect(csockfd, (struct sockaddr *)&addr, sizeof(addr));

	return csockfd;
}

/* Exchange QP info as before */
static int
tcp_exchange_info(int rsock, struct qp_info *local, struct qp_info *remote)
{
	if (write(rsock, local, sizeof(*local)) != sizeof(*local))
		return -1;
	if (read(rsock, remote, sizeof(*remote)) != sizeof(*remote))
		return -1;
	return 0;
}

/* Send connection params (client) and receive (server)*/
static int
send_conn_params(int sock, struct conn_params *params)
{
	return write(sock, params, sizeof(*params)) == sizeof(*params) ? 0 : -1;
}

static int
recv_conn_params(int sock, struct conn_params *params)
{
	return read(sock, params, sizeof(*params)) == sizeof(*params) ? 0 : -1;
}

/* Unified parameter exchange for both TCP and RDMA CM */
static int
unified_send_conn_params(struct conn_ctx *conn, struct conn_params *params)
{
	switch (conn->type) {
	case CONN_TYPE_TCP:
		return send_conn_params(conn->u.tcp.csock, params);
	case CONN_TYPE_RDMA_CM:
		// For RDMA CM, we can use private data or a separate mechanism
		// For now, store parameters in the node for later use
		if (conn->u.cm.node) {
			memcpy(&conn->u.cm.node->conn_params, params, sizeof(*params));
			return 0;
		}
		return -1;
	default:
		return -1;
	}
}

static int
unified_recv_conn_params(struct conn_ctx *conn, struct conn_params *params)
{
	switch (conn->type) {
	case CONN_TYPE_TCP:
		return recv_conn_params(conn->u.tcp.csock, params);
	case CONN_TYPE_RDMA_CM:
		// For RDMA CM, retrieve from node or private data
		if (conn->u.cm.node) {
			memcpy(params, &conn->u.cm.node->conn_params, sizeof(*params));
			return 0;
		}
		return -1;
	default:
		return -1;
	}
}

/* Unified QP info exchange */
static int
unified_exchange_qp_info(struct conn_ctx *conn, struct qp_info *local, struct qp_info *remote)
{
	switch (conn->type) {
	case CONN_TYPE_TCP:
		if (g_ctx.is_server)
			return recv_remote_info(conn->u.tcp.csock, local, remote);
		else
			return tcp_exchange_info(conn->u.tcp.csock, local, remote);
	case CONN_TYPE_RDMA_CM:
		// For RDMA CM, QP info is available through the connection event
		if (conn->u.cm.node && conn->u.cm.node->connected) {
			// Extract QP info from RDMA CM node
			remote->qp_num = conn->u.cm.node->remote_qpn;
			remote->lid = 0;  // Not used for RDMA CM UD
			remote->psn = 0;  // Not used for UD
			remote->rkey = 0; // Will be set if needed for RC
			// For UD, GID info is in the AH, for RC it's in the route
			memset(&remote->gid, 0, sizeof(remote->gid));
			return 0;
		}
		return -1;
	default:
		return -1;
	}
}

static int
unified_send_qp_info(struct conn_ctx *conn, struct qp_info *local)
{
	switch (conn->type) {
	case CONN_TYPE_TCP:
		return send_local_info(conn->u.tcp.csock, local);
	case CONN_TYPE_RDMA_CM:
		// For RDMA CM, QP info is sent through connection parameters
		return 0; // Already handled during connection establishment
	default:
		return -1;
	}
}

static int
recv_remote_info(int rsock, struct qp_info *local, struct qp_info *remote)
{
	int retry = g_ctx.is_server ? 10 : 1;

	while (retry--) {
		if (read(rsock, remote, sizeof(*remote)) == sizeof(*remote))
			return 0;
		usleep(5000);
	}

	return -1;
}

/* Structure for MR info exchange via CM private_data */
struct cm_private_exchange {
	uint64_t addr;
	uint32_t rkey;
	uint32_t magic;
	uint32_t op_type;
	uint32_t num_pkts;
	uint32_t msg_size;
};

#define CM_EXCH_MAGIC 0x52444D41

static int
get_rdma_addr(const char *src, const char *dst, const char *port, struct rdma_addrinfo *hints_param,
	      struct rdma_addrinfo **rai)
{
	int ret;

	ret = rdma_getaddrinfo(dst, port, hints_param, rai);
	if (ret) {
		printf("rdma_getaddrinfo: %s\n", gai_strerror(ret));
		return ret;
	}

	return 0;
}

static struct rdma_event_channel *
create_event_channel(void)
{
	struct rdma_event_channel *channel;

	channel = rdma_create_event_channel();
	if (!channel) {
		printf("Failed to create event channel\n");
		return NULL;
	}

	return channel;
}

static int
cm_create_message(struct cm_node *node)
{
	if (!g_ctx.msg_size)
		return 0;

	node->mem = malloc(g_ctx.msg_size + sizeof(struct ibv_grh));
	if (!node->mem) {
		printf("failed message allocation\n");
		return -1;
	}

	node->mr = ibv_reg_mr(node->pd, node->mem, g_ctx.msg_size + sizeof(struct ibv_grh),
			      IBV_ACCESS_LOCAL_WRITE);
	if (!node->mr) {
		printf("failed to reg MR\n");
		goto err;
	}
	return 0;
err:
	free(node->mem);
	return -1;
}

static int
cm_verify_test_params(struct cm_node *node)
{
	struct ibv_port_attr port_attr;
	int ret;

	ret = ibv_query_port(node->cma_id->verbs, node->cma_id->port_num, &port_attr);
	if (ret)
		return ret;

	if (g_ctx.msg_size && g_ctx.msg_size > (1 << (port_attr.active_mtu + 7))) {
		printf("rdma_cm: message_size %d is larger than active mtu %d\n", g_ctx.msg_size,
		       1 << (port_attr.active_mtu + 7));
		return -EINVAL;
	}

	return 0;
}

static int
cm_init_node(struct cm_node *node)
{
	struct ibv_qp_init_attr init_qp_attr;
	int cqe, ret;

	node->pd = ibv_alloc_pd(node->cma_id->verbs);
	if (!node->pd) {
		ret = -ENOMEM;
		printf("rdma_cm: unable to allocate PD\n");
		goto out;
	}

	/* CQ must hold: rx_depth recv + max_send_wr send + 4 spare */
	cqe = g_ctx.rx_depth + g_ctx.max_send_wr + 4;
	if (g_ctx.num_pkts > 0 && g_ctx.num_pkts * 2 > cqe)
		cqe = g_ctx.num_pkts * 2;
	node->cq = ibv_create_cq(node->cma_id->verbs, cqe, node, NULL, 0);
	if (!node->cq) {
		ret = -ENOMEM;
		printf("rdma_cm: unable to create CQ (cqe=%d)\n", cqe);
		goto out;
	}

	memset(&init_qp_attr, 0, sizeof(init_qp_attr));
	init_qp_attr.cap.max_send_wr = g_ctx.max_send_wr;
	init_qp_attr.cap.max_recv_wr = g_ctx.rx_depth;
	init_qp_attr.cap.max_send_sge = 1;
	init_qp_attr.cap.max_recv_sge = 1;
	init_qp_attr.qp_context = node;
	init_qp_attr.sq_sig_all = 0;
	init_qp_attr.qp_type = g_ctx.qp_type;
	init_qp_attr.send_cq = node->cq;
	init_qp_attr.recv_cq = node->cq;
	ret = rdma_create_qp(node->cma_id, node->pd, &init_qp_attr);
	if (ret) {
		perror("rdma_cm: unable to create QP");
		goto out;
	}

	/* INIT->INIT re-modify: rdma_create_qp sets access_flags=0.
	 * We must add REMOTE_WRITE + REMOTE_READ for RDMA ops to work. */
	{
		struct ibv_qp_attr qpa_init;

		memset(&qpa_init, 0, sizeof(qpa_init));
		qpa_init.qp_state = IBV_QPS_INIT;
		qpa_init.qp_access_flags =
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
		qpa_init.pkey_index = 0;
		qpa_init.port_num = node->cma_id->port_num;
		ret = ibv_modify_qp(node->cma_id->qp, &qpa_init,
				    IBV_QP_STATE | IBV_QP_ACCESS_FLAGS | IBV_QP_PKEY_INDEX |
					    IBV_QP_PORT);
	}
	ret = cm_create_message(node);
	if (ret) {
		printf("rdma_cm: failed to create messages: %d\n", ret);
		goto out;
	}

	/* Register data buffer MR with remote write for RDMA WRITE */
	if (g_ctx.qp_type == IBV_QPT_RC) {
		size_t buf_sz = g_ctx.msg_size + 40;

		node->data_buf = calloc(1, buf_sz);
		if (!node->data_buf) {
			ret = -ENOMEM;
			goto out;
		}
		node->data_mr = ibv_reg_mr(node->pd, node->data_buf, buf_sz,
					   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
						   IBV_ACCESS_REMOTE_READ);
		if (!node->data_mr) {
			free(node->data_buf);
			node->data_buf = NULL;
			ret = -ENOMEM;
			goto out;
		}
		node->exchange_rkey = node->data_mr->rkey;
		node->exchange_addr = (uintptr_t)node->data_buf + 40;
	}
out:
	return ret;
}

static void
cm_connect_error(void)
{
	cm_test.connects_left--;
}

static int
cm_addr_handler(struct cm_node *node)
{
	int ret;

	ret = rdma_resolve_route(node->cma_id, 2000);
	if (ret) {
		perror("rdma_cm: resolve route failed");
		cm_connect_error();
	}
	return ret;
}

static int
cm_route_handler(struct cm_node *node)
{
	struct rdma_conn_param conn_param;
	int ret;

	ret = cm_verify_test_params(node);
	if (ret)
		goto err;

	ret = cm_init_node(node);
	if (ret)
		goto err;

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.rnr_retry_count = 7;
	if (g_ctx.qp_type == IBV_QPT_RC && node->data_mr) {
		static struct cm_private_exchange client_exch;

		client_exch.addr = node->exchange_addr;
		client_exch.rkey = node->exchange_rkey;
		client_exch.magic = CM_EXCH_MAGIC;
		client_exch.op_type = g_ctx.op_type;
		client_exch.num_pkts = g_ctx.num_pkts;
		client_exch.msg_size = g_ctx.msg_size;
		conn_param.private_data = &client_exch;
		conn_param.private_data_len = sizeof(client_exch);
	} else if (g_ctx.qp_type == IBV_QPT_UD) {
		/* UD/SIDR: pack client QPN + test params in SIDR_REQ private_data.
		 * The server never gets ESTABLISHED for SIDR, so it reads these
		 * at CONNECT_REQUEST to learn the client's QPN and test params. */
		static struct cm_private_exchange ud_client_exch;

		ud_client_exch.magic = CM_EXCH_MAGIC;
		ud_client_exch.addr = (uint64_t)node->cma_id->qp->qp_num;
		ud_client_exch.rkey = 0;
		ud_client_exch.op_type = g_ctx.op_type;
		ud_client_exch.num_pkts = g_ctx.num_pkts;
		ud_client_exch.msg_size = g_ctx.msg_size;
		conn_param.private_data = &ud_client_exch;
		conn_param.private_data_len = sizeof(ud_client_exch);
	} else {
		conn_param.private_data = cm_test.rai->ai_connect;
		conn_param.private_data_len = cm_test.rai->ai_connect_len;
	}
	ret = rdma_connect(node->cma_id, &conn_param);
	if (ret) {
		perror("rdma_cm: failure connecting");
		goto err;
	}
	return 0;
err:
	cm_connect_error();
	return ret;
}

static int cm_integrate_connection(struct cm_node *node);

static int
cm_connect_handler(struct rdma_cm_id *cma_id, struct rdma_cm_event *event)
{
	struct cm_node *node;
	struct rdma_conn_param conn_param;
	int ret;

	if (cm_test.conn_index == g_ctx.num_client) {
		ret = -ENOMEM;
		goto err1;
	}
	node = &cm_test.nodes[cm_test.conn_index++];

	node->cma_id = cma_id;
	cma_id->context = node;

	// Check if this connection has already been processed
	if (node->initialized) {
		printf("RDMA CM connection already initialized, skipping\n");
		return 0;
	}

	ret = cm_verify_test_params(node);
	if (ret)
		goto err2;

	ret = cm_init_node(node);
	if (ret)
		goto err2;

	/* Read client MR info from REQ private_data */
	if (g_ctx.qp_type == IBV_QPT_RC &&
	    event->param.conn.private_data_len >= sizeof(struct cm_private_exchange)) {
		const struct cm_private_exchange *peer =
			(const struct cm_private_exchange *)event->param.conn.private_data;
		if (peer->magic == CM_EXCH_MAGIC) {
			node->remote_rkey_cm = peer->rkey;
			node->remote_addr_cm = peer->addr;
			node->mr_info_valid = 1;
			/* Apply client conn params so server matches */
			/* Always apply - IBV_WR_RDMA_WRITE is 0, cannot use truthiness check */
			g_ctx.op_type = peer->op_type;
			if (peer->num_pkts) {
				g_ctx.num_pkts = peer->num_pkts;
				/* Only set num_pkt_set for SEND mode where server actively sends.
				 * For WRITE/READ the server is passive - dont trigger exit
				 * condition */
				if (peer->op_type == IBV_WR_SEND)
					g_ctx.num_pkt_set = 1;
			}
			if (peer->msg_size)
				g_ctx.msg_size = peer->msg_size;
		}
	}

	/* For UD: read client QPN and test params from SIDR_REQ private_data.
	 * Also create AH for the client so the server can send replies.
	 * The UD server never receives ESTABLISHED in SIDR, so this is the
	 * only chance to learn the client's identity. */
	if (g_ctx.qp_type == IBV_QPT_UD &&
	    event->param.ud.private_data_len >= sizeof(struct cm_private_exchange)) {
		const struct cm_private_exchange *ud_peer =
			(const struct cm_private_exchange *)event->param.ud.private_data;
		if (ud_peer->magic == CM_EXCH_MAGIC) {
			node->remote_qpn = (uint32_t)ud_peer->addr;
			/* Always apply - IBV_WR_RDMA_WRITE is 0 */
			g_ctx.op_type = ud_peer->op_type;
			if (ud_peer->num_pkts) {
				g_ctx.num_pkts = ud_peer->num_pkts;
				if (ud_peer->op_type == IBV_WR_SEND)
					g_ctx.num_pkt_set = 1;
			}
			if (ud_peer->msg_size)
				g_ctx.msg_size = ud_peer->msg_size;
		}
		/* Create AH from the requester's address info in the SIDR_REQ */
		node->ah = ibv_create_ah(node->pd, &event->param.ud.ah_attr);
		node->remote_qkey = event->param.ud.qkey;
		if (!node->remote_qkey)
			node->remote_qkey = 0x01234567; /* RDMA_UDP_QKEY default */
	}

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.rnr_retry_count = 7;
	if (g_ctx.qp_type == IBV_QPT_UD)
		conn_param.qp_num = node->cma_id->qp->qp_num;

	/* For RC: put server MR in REP private_data, detach QP from rdma_accept
	 * to skip broken ucma_modify_qp_rtr/rts on octep_rdma driver. */
	struct ibv_qp *saved_qp = NULL;

	if (g_ctx.qp_type == IBV_QPT_RC && node->data_mr) {
		static struct cm_private_exchange srv_exch;

		srv_exch.addr = node->exchange_addr;
		srv_exch.rkey = node->exchange_rkey;
		srv_exch.magic = CM_EXCH_MAGIC;
		conn_param.private_data = &srv_exch;
		conn_param.private_data_len = sizeof(srv_exch);
		saved_qp = node->cma_id->qp;
		conn_param.qp_num = saved_qp->qp_num;
		node->cma_id->qp = NULL;
	}

	ret = rdma_accept(node->cma_id, &conn_param);
	if (saved_qp)
		node->cma_id->qp = saved_qp;
	if (ret) {
		perror("rdma_cm: failure accepting");
		goto err2;
	}

	printf("RDMA CM connection request accepted (QP %u)\n",
	       node->cma_id->qp ? node->cma_id->qp->qp_num : 0);

	if (g_ctx.qp_type == IBV_QPT_UD) {
		node->connected = 1;
		cm_test.connects_left--;
		ret = cm_integrate_connection(node);
		if (ret < 0)
			goto err2;
	}

	return 0;

err2:
	node->cma_id = NULL;
	cm_connect_error();
err1:
	printf("rdma_cm: failing connection request\n");
	rdma_reject(cma_id, NULL, 0);
	return ret;
}

static int
cm_integrate_connection(struct cm_node *node)
{
	struct conn_ctx conn;
	int ret;

	// Check if already initialized to prevent double integration
	if (node->initialized) {
		if (g_ctx.debug)
			printf("RDMA CM connection already integrated with threading system\n");
		return 0;
	}

	// Find or create device context
	struct device_ctx *dev = NULL;

	if (g_num_devices > 0) {
		// Use existing device context if available (server mode)
		dev = &g_devices[0];
	} else {
		// For RDMA CM client, create a minimal device context from the CM connection
		if (g_num_devices >= MAX_IB_DEVICES) {
			printf("Too many devices, cannot add RDMA CM device\n");
			goto err;
		}
		dev = &g_devices[g_num_devices];
		dev->dev_ctx = node->cma_id->verbs;
		dev->ib_devname = strdup(ibv_get_device_name(node->cma_id->verbs->device));
		dev->ip_list = NULL;
		dev->ip_count = 0;
		// Initialize client PDs array
		for (int k = 0; k < MAX_CLIENTS; ++k) {
			dev->client_pds[k] = NULL;
			dev->fd_to_client_idx[k] = -1;
		}
		g_num_devices++;
		printf("RDMA CM: Created device context for %s port %d\n", dev->ib_devname,
		       node->cma_id->port_num);
	}

	// Prepare connection context
	conn.type = CONN_TYPE_RDMA_CM;
	conn.u.cm.node = node;
	conn.dev = dev;
	conn.client_idx = -1;

	// Find available client index
	for (int i = 0; i < MAX_CLIENTS; ++i) {
		if (dev->fd_to_client_idx[i] == -1) {
			conn.client_idx = i;
			dev->fd_to_client_idx[i] = i; // Mark as used
			break;
		}
	}
	if (conn.client_idx < 0) {
		printf("No free client index slots available for RDMA CM\n");
		goto err;
	}

	// Allocate PD if needed
	if (!dev->client_pds[conn.client_idx]) {
		dev->client_pds[conn.client_idx] = ibv_alloc_pd(dev->dev_ctx);
		if (!dev->client_pds[conn.client_idx]) {
			printf("Couldn't allocate PD for RDMA CM connection\n");
			goto err;
		}
	}

	// Initialize QPs using the unified function
	ret = rdma_mq_init_unified(&conn);
	if (ret < 0) {
		printf("Failed to initialize QPs for RDMA CM connection\n");
		goto err;
	}

	// Mark node as initialized to prevent re-processing
	node->initialized = 1;

	printf("RDMA CM connection established and integrated with threading system\n");
	return 0;

err:
	cm_connect_error();
	return -1;
}

static int
cm_resolved_handler(struct cm_node *node, struct rdma_cm_event *event)
{
	int ret;

	if (g_ctx.qp_type == IBV_QPT_UD) {
		node->remote_qpn = event->param.ud.qp_num;
		node->remote_qkey = event->param.ud.qkey;
		node->ah = ibv_create_ah(node->pd, &event->param.ud.ah_attr);
		if (!node->ah) {
			printf("rdma_cm: failure creating address handle\n");
			goto err;
		}
	}

	node->connected = 1;
	cm_test.connects_left--;

	/* For RC client: read server MR from REP private_data */
	if (!g_ctx.is_server && g_ctx.qp_type == IBV_QPT_RC &&
	    event->param.conn.private_data_len >= sizeof(struct cm_private_exchange)) {
		const struct cm_private_exchange *peer =
			(const struct cm_private_exchange *)event->param.conn.private_data;
		if (peer->magic == CM_EXCH_MAGIC) {
			node->remote_rkey_cm = peer->rkey;
			node->remote_addr_cm = peer->addr;
			node->mr_info_valid = 1;
		}
	}

	/* For RC server: manual QP INIT->RTR->RTS */
	if (g_ctx.is_server && g_ctx.qp_type == IBV_QPT_RC && node->cma_id->qp) {
		struct ibv_qp_attr qpa;
		int qpm;

		memset(&qpa, 0, sizeof(qpa));
		qpa.qp_state = IBV_QPS_RTR;
		ret = rdma_init_qp_attr(node->cma_id, &qpa, &qpm);
		/* octep_rdma ibv_query_qp returns zeros -- override with sane defaults */
		if (qpa.max_dest_rd_atomic == 0)
			qpa.max_dest_rd_atomic = 1;
		if (qpa.min_rnr_timer == 0)
			qpa.min_rnr_timer = 12;
		if (ret == 0)
			ret = ibv_modify_qp(node->cma_id->qp, &qpa, qpm);

		memset(&qpa, 0, sizeof(qpa));
		qpa.qp_state = IBV_QPS_RTS;
		ret = rdma_init_qp_attr(node->cma_id, &qpa, &qpm);
		/* octep_rdma ibv_query_qp returns zeros -- override with sane defaults */
		if (qpa.timeout == 0)
			qpa.timeout = 14;
		if (qpa.retry_cnt == 0)
			qpa.retry_cnt = 7;
		if (qpa.rnr_retry == 0)
			qpa.rnr_retry = 7;
		if (qpa.max_rd_atomic == 0)
			qpa.max_rd_atomic = 1;
		if (ret == 0)
			ret = ibv_modify_qp(node->cma_id->qp, &qpa, qpm);
	}

	ret = cm_integrate_connection(node);
	if (ret < 0)
		goto err;

	return 0;

err:
	cm_connect_error();
	return -1;
}

static int
cm_handler(struct rdma_cm_id *cma_id, struct rdma_cm_event *event)
{
	int ret = 0;

	printf("[DEBUG] RDMA CM event: %s\n", rdma_event_str(event->event));

	switch (event->event) {
	case RDMA_CM_EVENT_ADDR_RESOLVED:
		ret = cm_addr_handler(cma_id->context);
		break;
	case RDMA_CM_EVENT_ROUTE_RESOLVED:
		ret = cm_route_handler(cma_id->context);
		break;
	case RDMA_CM_EVENT_CONNECT_REQUEST:
		ret = cm_connect_handler(cma_id, event);
		break;
	case RDMA_CM_EVENT_ESTABLISHED:
		ret = cm_resolved_handler(cma_id->context, event);
		break;
	case RDMA_CM_EVENT_ADDR_ERROR:
	case RDMA_CM_EVENT_ROUTE_ERROR:
	case RDMA_CM_EVENT_CONNECT_ERROR:
	case RDMA_CM_EVENT_UNREACHABLE:
	case RDMA_CM_EVENT_REJECTED:
		printf("rdma_cm: event: %s, error: %d\n", rdma_event_str(event->event),
		       event->status);
		cm_connect_error();
		ret = event->status;
		break;
	case RDMA_CM_EVENT_DEVICE_REMOVAL:
		/* Cleanup will occur after test completes. */
		break;
	default:
		break;
	}
	return ret;
}

static void
cm_destroy_node(struct cm_node *node)
{
	if (!node->cma_id)
		return;

	if (node->ah)
		ibv_destroy_ah(node->ah);

	if (node->cma_id->qp)
		rdma_destroy_qp(node->cma_id);

	if (node->cq)
		ibv_destroy_cq(node->cq);

	if (node->data_mr) {
		ibv_dereg_mr(node->data_mr);
		node->data_mr = NULL;
	}
	if (node->data_buf) {
		free(node->data_buf);
		node->data_buf = NULL;
	}

	if (node->mem) {
		ibv_dereg_mr(node->mr);
		free(node->mem);
	}

	if (node->pd)
		ibv_dealloc_pd(node->pd);

	/* Destroy the RDMA ID after all device resources */
	rdma_destroy_id(node->cma_id);
}

static int
cm_alloc_nodes(void)
{
	int ret, i;

	cm_test.nodes = malloc(sizeof(*cm_test.nodes) * g_ctx.num_client);
	if (!cm_test.nodes) {
		printf("rdma_cm: unable to allocate memory for test nodes\n");
		return -ENOMEM;
	}
	memset(cm_test.nodes, 0, sizeof(*cm_test.nodes) * g_ctx.num_client);

	for (i = 0; i < g_ctx.num_client; i++) {
		cm_test.nodes[i].id = i;
		if (g_ctx.servername) {
			ret = rdma_create_id(cm_test.channel, &cm_test.nodes[i].cma_id,
					     &cm_test.nodes[i], hints.ai_port_space);
			if (ret)
				goto err;
		}
	}
	return 0;
err:
	while (--i >= 0)
		rdma_destroy_id(cm_test.nodes[i].cma_id);
	free(cm_test.nodes);
	return ret;
}

static void
cm_destroy_nodes(void)
{
	int i;

	for (i = 0; i < g_ctx.num_client; i++)
		cm_destroy_node(&cm_test.nodes[i]);
	free(cm_test.nodes);
}

/* Background thread to watch for CM disconnect events after connection setup.
 * For server in WRITE/READ mode, the server is passive and needs to know
 * when the client disconnects so it can exit cleanly. */
static void *
cm_disconnect_watcher(void *arg)
{
	struct rdma_cm_event *event;
	(void)arg;

	while (1) {
		if (rdma_get_cm_event(cm_test.channel, &event))
			break;
		printf("[CM-WATCHER] Event: %s\n", rdma_event_str(event->event));
		if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
			rdma_ack_cm_event(event);
			printf("[CM-WATCHER] Client disconnected, setting force_quit\n");
			g_ctx.force_quit = true;
			break;
		}
		rdma_ack_cm_event(event);
	}
	return NULL;
}

static int
cm_connect_events(void)
{
	struct rdma_cm_event *event;
	int ret = 0;

	printf("[DEBUG] Starting event loop, connects_left=%d\n", cm_test.connects_left);
	while (cm_test.connects_left && !ret) {
		printf("[DEBUG] Waiting for RDMA CM event (connects_left=%d)\n",
		       cm_test.connects_left);
		ret = rdma_get_cm_event(cm_test.channel, &event);
		if (!ret) {
			ret = cm_handler(event->id, event);
			rdma_ack_cm_event(event);
			printf("[DEBUG] Event processed, connects_left=%d, ret=%d\n",
			       cm_test.connects_left, ret);
		} else {
			printf("[DEBUG] rdma_get_cm_event failed: ret=%d\n", ret);
		}
	}
	printf("[DEBUG] Event loop exited, connects_left=%d, ret=%d\n", cm_test.connects_left, ret);
	return ret;
}

static int
cm_run_server(void)
{
	struct rdma_cm_id *listen_id;
	int ret;

	printf("rdma_cm: starting server\n");
	ret = rdma_create_id(cm_test.channel, &listen_id, &cm_test, hints.ai_port_space);
	if (ret) {
		perror("rdma_cm: listen request failed");
		return ret;
	}

	/* For server mode, use NULL as destination and let RDMA CM handle the address */
	printf("rdma_cm: getting address for port %s\n", g_ctx.port ? g_ctx.port : "NULL");
	ret = get_rdma_addr(g_ctx.src_addr, NULL, g_ctx.port, &hints, &cm_test.rai);
	if (ret) {
		printf("rdma_cm: get_rdma_addr failed with ret=%d\n", ret);
		goto out;
	}

	printf("rdma_cm: binding to address\n");
	ret = rdma_bind_addr(listen_id, cm_test.rai->ai_src_addr);
	if (ret) {
		perror("rdma_cm: bind address failed");
		printf("rdma_cm: bind failed with ret=%d\n", ret);
		goto out;
	}

	ret = rdma_listen(listen_id, 0);
	if (ret) {
		perror("rdma_cm: failure trying to listen");
		goto out;
	}

	printf("rdma_cm: waiting for connections...\n");
	ret = cm_connect_events();
	if (ret)
		goto out;

	printf("rdma_cm: server connected successfully\n");

	/* Start background thread to watch for CM disconnect events */
	{
		pthread_t watcher_tid;

		if (pthread_create(&watcher_tid, NULL, cm_disconnect_watcher, NULL) == 0)
			pthread_detach(watcher_tid);
	}

out:
	rdma_destroy_id(listen_id);
	return ret;
}

static int
cm_run_client(void)
{
	int i, ret;

	printf("rdma_cm: starting client\n");
	printf("rdma_cm: resolving address for server=%s, port=%s\n",
	       g_ctx.servername ? g_ctx.servername : "NULL", g_ctx.port ? g_ctx.port : "NULL");

	ret = get_rdma_addr(g_ctx.src_addr, g_ctx.servername, g_ctx.port, &hints, &cm_test.rai);
	if (ret) {
		printf("rdma_cm: get_rdma_addr failed with ret=%d\n", ret);
		return ret;
	}

	printf("rdma_cm: connecting\n");
	for (i = 0; i < 1; i++) { /* Connect one connection for now */
		ret = rdma_resolve_addr(cm_test.nodes[i].cma_id, cm_test.rai->ai_src_addr,
					cm_test.rai->ai_dst_addr, 2000);
		if (ret) {
			perror("rdma_cm: failure getting addr");
			cm_connect_error();
			return ret;
		}
	}

	ret = cm_connect_events();
	if (ret)
		goto out;

	printf("rdma_cm: client connected successfully\n");

	/* Start background thread to watch for CM disconnect events */
	{
		pthread_t watcher_tid;

		if (pthread_create(&watcher_tid, NULL, cm_disconnect_watcher, NULL) == 0)
			pthread_detach(watcher_tid);
	}

out:
	return ret;
}

static int
send_local_info(int rsock, struct qp_info *local)
{
	if (write(rsock, local, sizeof(*local)) != sizeof(*local))
		return -1;

	return 0;
}

void
atomic_bitmap_set(uint16_t count_id)
{
	if (count_id >= MAX_QUEUES)
		return;
	uint16_t word = count_id / 64;
	uint16_t bit = count_id % 64;

	atomic_fetch_or(&g_ctx.qbmap.bits[word], 1ULL << bit);
}

void
atomic_bitmap_clear(uint16_t count_id)
{
	if (count_id >= MAX_QUEUES)
		return;
	uint16_t word = count_id / 64;
	uint16_t bit = count_id % 64;

	if (!atomic_bitmap_is_set(count_id))
		return;
	atomic_fetch_and(&g_ctx.qbmap.bits[word], ~(1ULL << bit));
}

int
atomic_bitmap_is_set(uint16_t count_id)
{
	if (count_id >= MAX_QUEUES)
		return 0;
	uint16_t word = count_id / 64;
	uint16_t bit = count_id % 64;

	return (atomic_load(&g_ctx.qbmap.bits[word]) >> bit) & 1;
}

static void
rdma_cleanup_client_pd(struct device_ctx *dev, int client_idx)
{
	if (!dev)
		return;
	if (client_idx < 0 || client_idx >= MAX_CLIENTS)
		return;
	if (dev->client_pds[client_idx]) {
		if (ibv_dealloc_pd(dev->client_pds[client_idx])) {
			printf("Couldn't deallocate PD for %s (client %d)\n", dev->ib_devname,
			       client_idx);
		} else {
			printf("Deallocated PD for %s (client %d)\n", dev->ib_devname, client_idx);
		}
		dev->client_pds[client_idx] = NULL;
	}
}

static int
rdma_cleanup(struct qp_data *qdata)
{
	// Check if already cleaned up
	if (qdata->cleaned_up) {
		printf("QP %u already cleaned up, skipping\n", qdata->local_info.qp_num);
		return 0;
	}

	printf("Cleaning up QP %u device cleanup (qdata=%p, qp=%p, ctx=%p)\n",
	       qdata->local_info.qp_num, qdata, qdata->qp, qdata->qp ? qdata->qp->context : NULL);

	// Mark as being cleaned up to prevent concurrent cleanup
	qdata->cleaned_up = 1;

	qdata->armed = 0;

	struct ibv_qp *qp = qdata->qp;
	struct ibv_cq *cq = qdata->cq;
	struct ibv_cq *scq = qdata->send_cq;

	qdata->qp = NULL;
	qdata->cq = NULL;
	qdata->send_cq = NULL;

	/* For RDMA CM QPs, cm_destroy_nodes() handles QP/CQ destruction.
	 * The QP/CQ may already be freed, so skip modify and drain. */
	if (qp && !qdata->is_rdma_cm) {
		struct ibv_qp_attr attr = {.qp_state = IBV_QPS_ERR};

		if (ibv_modify_qp(qp, &attr, IBV_QP_STATE))
			printf("Warning: failed to move QP %u to ERROR state\n",
			       qdata->local_info.qp_num);
	}

	if (cq && !qdata->is_rdma_cm) {
		struct ibv_wc wc;

		while (ibv_poll_cq(cq, 1, &wc) > 0)
			;
	}
	if (scq && !qdata->is_rdma_cm) {
		struct ibv_wc wc;

		while (ibv_poll_cq(scq, 1, &wc) > 0)
			;
	}

	// Only destroy manually created QPs, not RDMA CM QPs
	if (qp && !qdata->is_rdma_cm) {
		// This is a manually created QP (TCP mode), safe to destroy
		if (ibv_destroy_qp(qp)) {
			printf("Couldn't destroy QP\n");
			return -1;
		}
		if (g_ctx.debug)
			printf("[DEBUG] Destroyed manual QP %u\n", qdata->local_info.qp_num);
	} else if (qp) {
		// This is an RDMA CM QP, don't destroy it manually
		printf("[DEBUG] Skipping QP destroy for RDMA CM QP %u (managed by librdmacm)\n",
		       qdata->local_info.qp_num);
	}

	if (cq) {
		if (qdata->is_cq_rdma_cm) {
			// This is an RDMA CM CQ, don't destroy it manually
			printf("[DEBUG] Skipping CQ destroy for RDMA CM CQ %p (managed by librdmacm)\n",
			       (void *)cq);
		} else {
			// This is a manually created CQ, destroy it
			if (ibv_destroy_cq(cq)) {
				printf("Couldn't destroy CQ\n");
				return -1;
			}
			if (g_ctx.debug) {
				printf("[DEBUG] Destroyed manual CQ %p\n", (void *)cq);
			}
		}
	}
	if (scq && ibv_destroy_cq(scq)) {
		printf("Couldn't destroy send CQ\n");
		return -1;
	}

	if (qdata->mr_arr) {
		for (int sge_idx = 0; sge_idx < g_ctx.nb_sge; ++sge_idx) {
			if (qdata->mr_arr[sge_idx])
				ibv_dereg_mr(qdata->mr_arr[sge_idx]);
		}
		free(qdata->mr_arr);
	}
	if (qdata->buf_arr) {
		for (int sge_idx = 0; sge_idx < g_ctx.nb_sge; ++sge_idx) {
			if (qdata->buf_arr[sge_idx])
				free(qdata->buf_arr[sge_idx]);
		}
		free(qdata->buf_arr);
	}

	// Only destroy manually created AHs, not RDMA CM AHs
	if (qdata->ah && !qdata->is_ah_rdma_cm) {
		if (ibv_destroy_ah(qdata->ah)) {
			printf("Couldn't destroy AH\n");
			return -1;
		}
		if (g_ctx.debug)
			printf("[DEBUG] Destroyed manual AH\n");
	} else if (qdata->ah) {
		// RDMA CM AH is managed by librdmacm, don't destroy manually
		if (g_ctx.debug)
			printf("[DEBUG] Skipping AH destroy for RDMA CM AH (managed by librdmacm)\n");
	}

	/* Per-client PD is cleaned up on disconnect or at program exit. */

	return 0;
}

static inline void
get_qp_modify_attr(struct ibv_qp_attr *attr_mod, enum ibv_qp_attr_mask *flags,
		   enum ibv_qp_state state, struct qp_data *data)
{
	memset(attr_mod, 0, sizeof(struct ibv_qp_attr));

	switch (state) {
	case IBV_QPS_INIT:
		attr_mod->qp_state = IBV_QPS_INIT;
		attr_mod->port_num = 1;
		attr_mod->pkey_index = 0;
		*flags = IBV_QP_STATE | IBV_QP_PORT | IBV_QP_PKEY_INDEX;
		if (g_ctx.qp_type == IBV_QPT_UD) {
			attr_mod->qkey = 0x11111111;
			*flags |= IBV_QP_QKEY;
		} else if (g_ctx.qp_type == IBV_QPT_RC) {
			attr_mod->qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
						    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ;
			*flags |= IBV_QP_ACCESS_FLAGS;
		} else {
			printf("Invalid QP type %d\n", g_ctx.qp_type);
			exit(EXIT_FAILURE);
		}
		break;
	case IBV_QPS_RTR:
		attr_mod->qp_state = IBV_QPS_RTR;
		*flags = IBV_QP_STATE;
		if (g_ctx.qp_type == IBV_QPT_RC) {
			attr_mod->max_dest_rd_atomic = 1;
			attr_mod->min_rnr_timer = 12;
			attr_mod->ah_attr.is_global = 0;
			attr_mod->ah_attr.port_num = 1;
			attr_mod->ah_attr.dlid = data->remote_info.lid;
			attr_mod->ah_attr.sl = 0;
			attr_mod->ah_attr.src_path_bits = 0;
			attr_mod->dest_qp_num = data->remote_info.qp_num;
			attr_mod->rq_psn = data->remote_info.psn;
			attr_mod->path_mtu = path_mtu_to_enum(g_ctx.path_mtu);
			*flags |= IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER |
				  IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_PATH_MTU;

			if (data->remote_info.gid.global.interface_id) {
				attr_mod->ah_attr.is_global = 1;
				attr_mod->ah_attr.grh.hop_limit = 1;
				attr_mod->ah_attr.grh.dgid = data->remote_info.gid;
				attr_mod->ah_attr.grh.sgid_index = g_ctx.gidx;
			}
		}
		break;

	case IBV_QPS_RTS:
		attr_mod->qp_state = IBV_QPS_RTS;
		attr_mod->sq_psn = data->local_info.psn;
		*flags = IBV_QP_STATE | IBV_QP_SQ_PSN;
		if (g_ctx.qp_type == IBV_QPT_RC) {
			attr_mod->max_rd_atomic = 1;
			attr_mod->timeout = 11;
			attr_mod->retry_cnt = 9;
			attr_mod->rnr_retry = 9;
			*flags |= IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
				  IBV_QP_RNR_RETRY;
		}
		break;
	default:
		printf("Invalid QP state %d\n", state);
		exit(EXIT_FAILURE);
	}
}

static inline int
post_recv(struct qp_data *qdata, int rxdepth, int wr_id)
{
	struct ibv_sge recv_sge_arr[g_ctx.nb_sge];
	struct ibv_recv_wr recv_wr = {0};
	struct ibv_recv_wr *bad_recv;
	int j;

	{
		uint32_t grh_len = (g_ctx.qp_type == IBV_QPT_UD) ? 40 : 0;
		uint32_t total = g_ctx.msg_size + grh_len;
		uint32_t per_sge = total / g_ctx.nb_sge;
		uint32_t remainder = total % g_ctx.nb_sge;

		for (int sge_idx = 0; sge_idx < g_ctx.nb_sge; ++sge_idx) {
			recv_sge_arr[sge_idx].addr = (uintptr_t)qdata->buf_arr[sge_idx];
			recv_sge_arr[sge_idx].length = per_sge + (sge_idx < (int)remainder ? 1 : 0);
			recv_sge_arr[sge_idx].lkey = qdata->mr_arr[sge_idx]->lkey;
		}
	}
	recv_wr.wr_id = wr_id;
	recv_wr.sg_list = &recv_sge_arr[0];
	recv_wr.num_sge = g_ctx.nb_sge;

	// Debug validation before posting
	if (!qdata->qp) {
		printf("ERROR: QP is NULL in post recv\n");
		return 0;
	}
	if (!qdata->qp->context) {
		printf("ERROR: QP context is NULL in post recv\n");
		return 0;
	}

	// Check memory regions
	for (int sge_idx = 0; sge_idx < g_ctx.nb_sge; ++sge_idx) {
		if (!qdata->mr_arr[sge_idx]) {
			printf("ERROR: Memory region %d is NULL in post recv\n", sge_idx);
			return 0;
		}
		if (!qdata->buf_arr[sge_idx]) {
			printf("ERROR: Buffer %d is NULL in post recv\n", sge_idx);
			return 0;
		}
	}

	// For manually created UD QPs in RESET state, transition to INIT if needed
	if (!qdata->is_rdma_cm && g_ctx.qp_type == IBV_QPT_UD) {
		struct ibv_qp_attr qp_attr;
		struct ibv_qp_init_attr qp_init_attr;

		if (ibv_query_qp(qdata->qp, &qp_attr, IBV_QP_STATE, &qp_init_attr) == 0) {
			if (qp_attr.qp_state == IBV_QPS_RESET) {
				struct ibv_qp_attr init_attr = {0};

				init_attr.qp_state = IBV_QPS_INIT;
				init_attr.port_num = 1;
				init_attr.pkey_index = 0;
				init_attr.qkey = 0x11111111;

				int init_mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
						IBV_QP_QKEY;

				if (ibv_modify_qp(qdata->qp, &init_attr, init_mask)) {
					if (g_ctx.debug)
						printf("[WARNING] Failed to transition UD QP %u to INIT state\n",
						       qdata->qp->qp_num);
				} else if (g_ctx.debug) {
					printf("[DEBUG] Successfully transitioned UD QP %u to INIT state\n",
					       qdata->qp->qp_num);
				}
			}
		}
	}

	for (j = 0; j < rxdepth; j++) {
		int ret = ibv_post_recv(qdata->qp, &recv_wr, &bad_recv);

		if (ret) {
			printf("Error posting receive buffer for QP %d: %s (errno=%d)\n",
			       qdata->local_info.qp_num, strerror(ret), ret);
			if (bad_recv) {
				printf("  Bad WR: wr_id=%lu, num_sge=%d, sg_list=%p\n",
				       bad_recv->wr_id, bad_recv->num_sge, bad_recv->sg_list);
			}
			qdata->stats.recv_wr_failed++;
			break;
		}
		qdata->stats.recv_wr_posted++;
	}
	qdata->rcnt += j;
	return j;
}

int
rdma_mq_init(int csock, struct device_ctx *dev)
{
	struct conn_ctx conn;
	int client_idx = -1;

	/* Retrieve or assign a compact client index for this fd */
	if (csock >= 0 && csock < MAX_CLIENTS)
		client_idx = dev->fd_to_client_idx[csock];
	if (client_idx < 0) {
		/* Find a free slot */
		for (int k = 0; k < MAX_CLIENTS; ++k) {
			if (!dev->client_pds[k]) {
				client_idx = k;
				break;
			}
		}
		if (client_idx < 0) {
			printf("No free client index slots available for device %s\n",
			       dev->ib_devname);
			return -1;
		}
		if (csock >= 0 && csock < MAX_CLIENTS)
			dev->fd_to_client_idx[csock] = client_idx;
	}

	if (!dev->client_pds[client_idx]) {
		dev->client_pds[client_idx] = ibv_alloc_pd(dev->dev_ctx);
		if (!dev->client_pds[client_idx]) {
			printf("Couldn't allocate PD for %s (client %d)\n", dev->ib_devname,
			       client_idx);
			return -1;
		}
	}

	/* Setup connection context for TCP */
	conn.type = CONN_TYPE_TCP;
	conn.u.tcp.csock = csock;
	conn.dev = dev;
	conn.client_idx = client_idx;

	/* Use the unified initialization function */
	return rdma_mq_init_unified(&conn);
}

/* Unified initialization function that works with both TCP and RDMA CM connections */
int
rdma_mq_init_unified(struct conn_ctx *conn)
{
	struct conn_params params;
	struct ibv_port_attr port_attr = {0};
	struct ibv_qp_init_attr attr = {0};
	struct ibv_qp_attr attr_mod = {0};
	struct ibv_ah_attr ah_attr = {0};
	enum ibv_qp_attr_mask flags = 0;
	struct qp_data *data;
	int wr_id = 0;
	char lgid[INET6_ADDRSTRLEN];
	char rgid[INET6_ADDRSTRLEN];
	int i, j, n;
	struct device_ctx *dev = conn->dev;
	int client_idx = conn->client_idx;

	if (g_ctx.debug) {
		static int call_count;

		call_count++;
		printf("DEBUG: #%d for %s connection\n", call_count,
		       conn->type == CONN_TYPE_RDMA_CM ? "RDMA_CM" : "TCP");
	}

	// Handle connection parameter exchange (skip for RDMA CM)
	if (conn->type == CONN_TYPE_RDMA_CM) {
		printf("[DEBUG] RDMA CM: Skipping parameter exchange, using local g_ctx values\n");
		params.qp_type = g_ctx.qp_type;
		params.op_type = g_ctx.op_type;
		params.num_pkts = g_ctx.num_pkts;
		params.msg_size = g_ctx.msg_size;
		params.numqp = g_ctx.numqp;
	} else {
		// TCP parameter exchange
		if (g_ctx.is_server) {
			if (unified_recv_conn_params(conn, &params) < 0) {
				printf("[ERROR] Failed to receive conn_params\n");
				return -1;
			}
			if (g_ctx.debug) {
				printf("[DEBUG] Server received params: qp_type=%d, op_type=%d, num_pkts=%d, msg_size=%d, numqp=%d\n",
				       params.qp_type, params.op_type, params.num_pkts,
				       params.msg_size, params.numqp);
			}
			g_ctx.qp_type = params.qp_type;
			g_ctx.op_type = params.op_type;
			g_ctx.num_pkts = params.num_pkts;
			g_ctx.msg_size = params.msg_size;
			g_ctx.numqp = params.numqp;
			g_ctx.num_pkt_set = 1;
		} else {
			params.qp_type = g_ctx.qp_type;
			params.op_type = g_ctx.op_type;
			params.num_pkts = g_ctx.num_pkts;
			params.msg_size = g_ctx.msg_size;
			params.numqp = g_ctx.numqp;
			if (unified_send_conn_params(conn, &params) < 0) {
				printf("[ERROR] Failed to send conn_params\n");
				return -1;
			}
		}
	}

	// Ensure we have enough free slots
	if (count_free_slots() < g_ctx.numqp) {
		printf("Not enough free QP slots available (need %d, have %d)\n", g_ctx.numqp,
		       count_free_slots());
		return -1;
	}

	int qp_count = (conn->type == CONN_TYPE_RDMA_CM) ? 1 : g_ctx.numqp;

	if (g_ctx.debug) {
		printf("[DEBUG] Creating %d QP data structures for %s connection\n", qp_count,
		       (conn->type == CONN_TYPE_RDMA_CM ? "RDMA_CM" : "TCP"));
	}

	for (i = 0; i < qp_count; i++) {
		int slot = find_free_slot();

		if (slot < 0) {
			printf("No free QP slot found\n");
			return 0;
		}

		data = (struct qp_data *)calloc(1, sizeof(struct qp_data));
		if (data == NULL) {
			printf("Failed to allocate memory for qp_data\n");
			return -1;
		}

		data->dev = dev;

		// Set connection info based on type
		switch (conn->type) {
		case CONN_TYPE_TCP:
			data->csock = conn->u.tcp.csock;
			break;
		case CONN_TYPE_RDMA_CM:
			data->csock = -1; // No socket for RDMA CM
			break;
		}

		data->dir = g_ctx.dir;

		// Create or use CQ based on connection type
		if (conn->type == CONN_TYPE_RDMA_CM && conn->u.cm.node && conn->u.cm.node->cq) {
			// For RDMA CM, use the existing CQ from CM node
			data->cq = conn->u.cm.node->cq;
			data->is_cq_rdma_cm = 1; // Mark CQ as RDMA CM managed
			if (g_ctx.debug)
				printf("[DEBUG] Using RDMA CM CQ %p from CM node\n",
				       (void *)data->cq);
		} else {
			// For TCP connections create new CQ(s)
			data->cq = ibv_create_cq(dev->dev_ctx, g_ctx.rx_depth + 1, NULL, NULL, 0);
			data->is_cq_rdma_cm = 0;
			if (!data->cq) {
				printf("CQ create failed\n");
				goto cleanup;
			}
			if (g_ctx.separate_cq) {
				data->send_cq = ibv_create_cq(dev->dev_ctx, g_ctx.max_send_wr + 1,
							      NULL, NULL, 0);
				if (!data->send_cq) {
					printf("Send CQ create failed\n");
					goto cleanup;
				}
			}
			if (g_ctx.debug)
				printf("[DEBUG] Created %s CQ %p (rx_depth %u) on dev %s\n",
				       g_ctx.separate_cq ? "separate send+recv" : "shared",
				       (void *)data->cq, g_ctx.rx_depth + 1, dev->ib_devname);
		}

		// Allocate and register memory
		data->buf_arr = calloc(g_ctx.nb_sge, sizeof(void *));
		data->mr_arr = calloc(g_ctx.nb_sge, sizeof(struct ibv_mr *));
		if (!data->buf_arr || !data->mr_arr) {
			printf("Failed to allocate buffer/mr arrays for multi-SGE\n");
			goto cleanup;
		}

		for (int sge_idx = 0; sge_idx < g_ctx.nb_sge; ++sge_idx) {
			if (posix_memalign(&data->buf_arr[sge_idx], sysconf(_SC_PAGESIZE),
					   g_ctx.msg_size + 40)) {
				printf("Page-aligned buffer allocation failed for SGE %d\n",
				       sge_idx);
				goto cleanup;
			}
			memset((char *)data->buf_arr[sge_idx] + 40,
			       g_ctx.op_type != IBV_WR_RDMA_READ ? 0x66 : 0x77, g_ctx.msg_size);

			// Use the correct PD based on connection type
			struct ibv_pd *pd_to_use;

			if (conn->type == CONN_TYPE_RDMA_CM && conn->u.cm.node &&
			    conn->u.cm.node->pd)
				pd_to_use = conn->u.cm.node->pd;
			else
				pd_to_use = dev->client_pds[client_idx];

			data->mr_arr[sge_idx] =
				ibv_reg_mr(pd_to_use, data->buf_arr[sge_idx], g_ctx.msg_size + 40,
					   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
						   IBV_ACCESS_REMOTE_READ);
			if (!data->mr_arr[sge_idx]) {
				printf("Memory registration failed for SGE %d\n", sge_idx);
				goto cleanup;
			}
		}

		data->mr = data->mr_arr[0];

		// Create QP - use RDMA CM QP if available, otherwise create manually
		if (conn->type == CONN_TYPE_RDMA_CM && conn->u.cm.node && conn->u.cm.node->cma_id) {
			// For RDMA CM, check if QP already exists
			if (conn->u.cm.node->cma_id->qp) {
				data->qp = conn->u.cm.node->cma_id->qp;
				data->is_rdma_cm = 1; // Mark as RDMA CM QP
				printf("[INFO] Using existing RDMA CM QP %u\n", data->qp->qp_num);
			} else {
				printf("[ERROR] RDMA CM QP not found\n");
				goto cleanup;
			}
		} else {
			// Create QP manually for TCP connections
			attr.send_cq = data->send_cq ? data->send_cq : data->cq;
			attr.recv_cq = data->cq;
			attr.cap.max_send_wr = g_ctx.max_send_wr;
			attr.cap.max_recv_wr = g_ctx.rx_depth;
			attr.cap.max_send_sge = g_ctx.nb_sge;
			attr.cap.max_recv_sge = g_ctx.nb_sge;
			attr.qp_type = g_ctx.qp_type;
			if (g_ctx.inline_thresh > 0)
				attr.cap.max_inline_data = g_ctx.inline_thresh;

			data->qp = ibv_create_qp(dev->client_pds[client_idx], &attr);
			if (!data->qp) {
				printf("Error creating QP for queue: %d\n", i);
				goto cleanup;
			}

			data->is_rdma_cm = 0; // Mark as manually created QP
			printf("[INFO] Created manual QP %u\n", data->qp->qp_num);

			// Initialize QP state for manually created QPs
			get_qp_modify_attr(&attr_mod, &flags, IBV_QPS_INIT, data);
			if (ibv_modify_qp(data->qp, &attr_mod, flags)) {
				printf("Error modifying QP %u to INIT state\n", data->qp->qp_num);
				goto cleanup;
			}
		}

		if (g_ctx.debug)
			printf("[DEBUG] QP %u initialized\n", data->qp->qp_num);

		// Set up local QP info
		data->local_info.qp_num = data->qp->qp_num;
		data->local_info.rkey = data->mr->rkey;
		data->local_info.remote_addr = (uintptr_t)data->buf_arr[0] + 40;

		// Handle GID and LID setup
		if (conn->type == CONN_TYPE_TCP) {
			// For TCP connections, get local IP from socket
			char local_ip_sel[INET_ADDRSTRLEN] = {0};
			struct sockaddr_in laddr = {0};
			socklen_t laddrlen = sizeof(laddr);

			if (getsockname(conn->u.tcp.csock, (struct sockaddr *)&laddr, &laddrlen) ==
			    0)
				inet_ntop(AF_INET, &laddr.sin_addr, local_ip_sel,
					  sizeof(local_ip_sel));

			int sgid_index =
				choose_gid_index_for_local(dev->dev_ctx, dev->ib_devname, 1,
							   local_ip_sel[0] ? local_ip_sel : NULL,
							   g_ctx.gidx);
			g_ctx.gidx = sgid_index;

			if (ibv_query_gid(dev->dev_ctx, 1, sgid_index, &data->local_info.gid)) {
				printf("query gid failed\n");
				goto cleanup;
			}

			ibv_query_port(dev->dev_ctx, 1, &port_attr);
			data->local_info.lid = port_attr.lid;
			data->local_info.psn = rand() & 0xffffff;
		} else {
			// For RDMA CM, GID info is handled by CM
			memset(&data->local_info.gid, 0, sizeof(data->local_info.gid));
			data->local_info.lid = 0;
			data->local_info.psn = 0;
		}

		// Exchange QP information
		if (unified_exchange_qp_info(conn, &data->local_info, &data->remote_info) < 0) {
			printf("Error exchanging QP information for QP %u\n",
			       data->local_info.qp_num);
			goto cleanup;
		}

		// QP state transitions and AH setup
		if (conn->type == CONN_TYPE_TCP) {
			inet_ntop(AF_INET6, &data->local_info.gid, lgid, sizeof(lgid));
			inet_ntop(AF_INET6, &data->remote_info.gid, rgid, sizeof(rgid));
			printf("  local address: QPN 0x%06x, GID %s -- remote address: QPN 0x%06x, GID %s\n",
			       data->local_info.qp_num, lgid, data->remote_info.qp_num, rgid);

			get_qp_modify_attr(&attr_mod, &flags, IBV_QPS_RTR, data);
			if (ibv_modify_qp(data->qp, &attr_mod, flags)) {
				printf("Error modifying QP %u to RTR state\n", data->qp->qp_num);
				goto cleanup;
			}
			if (g_ctx.debug)
				printf("[DEBUG] QP %u moved to RTR\n", data->qp->qp_num);

			// Send local info for server
			if (g_ctx.is_server && unified_send_qp_info(conn, &data->local_info) < 0) {
				printf("Error sending QP info for QP %u\n",
				       data->local_info.qp_num);
				goto cleanup;
			}

			get_qp_modify_attr(&attr_mod, &flags, IBV_QPS_RTS, data);
			if (ibv_modify_qp(data->qp, &attr_mod, flags)) {
				printf("Error modifying QP %u to RTS state\n", data->qp->qp_num);
				goto cleanup;
			}
			if (g_ctx.debug)
				printf("[DEBUG] QP %u moved to RTS\n", data->qp->qp_num);

			// Create AH for UD
			if (g_ctx.qp_type == IBV_QPT_UD) {
				ah_attr.is_global = 1;
				ah_attr.port_num = 1;
				ah_attr.grh.dgid = data->remote_info.gid;
				ah_attr.grh.sgid_index = g_ctx.gidx;
				ah_attr.grh.hop_limit = 8;
				data->ah = ibv_create_ah(dev->client_pds[client_idx], &ah_attr);
				if (!data->ah) {
					printf("AH create failed\n");
					goto cleanup;
				}
				data->is_ah_rdma_cm = 0; // Mark as manually created AH

				// Set remote qkey for TCP UD connections
				data->remote_qkey = 0x11111111;
				if (g_ctx.debug) {
					printf("[DEBUG] TCP UD QP %u: AH=%p, remote QPN=%u, remote_qkey=0x%x\n",
					       data->qp->qp_num, data->ah, data->remote_info.qp_num,
					       data->remote_qkey);
				}
			}
		} else if (conn->type == CONN_TYPE_RDMA_CM) {
			// For RDMA CM, check connection is established
			if (!conn->u.cm.node || !conn->u.cm.node->connected) {
				printf("ERROR: RDMA CM connection not yet established\n");
				goto cleanup;
			}

			// For RDMA CM, use the AH from the CM node
			if (g_ctx.qp_type == IBV_QPT_UD && conn->u.cm.node && conn->u.cm.node->ah) {
				data->ah = conn->u.cm.node->ah;
				data->is_ah_rdma_cm = 1; // Mark as RDMA CM managed AH
				data->remote_info.qp_num = conn->u.cm.node->remote_qpn;
				data->remote_qkey = conn->u.cm.node->remote_qkey;
				printf("[DEBUG] UD QP %u: AH=%p, remote QPN=%u, remote_qkey=0x%x\n",
				       data->qp->qp_num, data->ah, data->remote_info.qp_num,
				       conn->u.cm.node->remote_qkey);
			} else if (g_ctx.qp_type == IBV_QPT_RC && conn->u.cm.node->mr_info_valid) {
				/* Use MR info from CM private_data */
				data->remote_info.rkey = conn->u.cm.node->remote_rkey_cm;
				data->remote_info.remote_addr = conn->u.cm.node->remote_addr_cm;
				if (conn->u.cm.node->data_mr) {
					data->local_info.rkey = conn->u.cm.node->data_mr->rkey;
					data->local_info.remote_addr =
						conn->u.cm.node->exchange_addr;
				}
				printf("[INFO] RC CM: MR via private_data:"
				       " local rkey=0x%x addr=0x%lx"
				       " remote rkey=0x%x addr=0x%lx\n",
				       data->local_info.rkey, data->local_info.remote_addr,
				       data->remote_info.rkey, data->remote_info.remote_addr);
			}
			printf("  RDMA CM QP %u connected (remote QPN %u)\n",
			       data->local_info.qp_num, data->remote_info.qp_num);
		}

		// Post receives if needed
		wr_id = g_ctx.op_type == IBV_WR_SEND      ? RDMA_UD_RECV :
			g_ctx.op_type == IBV_WR_RDMA_READ ? RDMA_READ_REQ :
							    RDMA_WRITE_REQ;

		bool should_post_recv =
			(g_ctx.qp_type == IBV_QPT_UD ||
			 (g_ctx.qp_type == IBV_QPT_RC && g_ctx.op_type == IBV_WR_SEND));

		if (should_post_recv && conn->type == CONN_TYPE_RDMA_CM) {
			// For RDMA CM, check if receives have already been posted for this QP
			bool recv_already_posted = false;

			for (int check_slot = 0; check_slot < slot; check_slot++) {
				if (g_ctx.qp_data[check_slot] && g_ctx.qp_data[check_slot]->qp &&
				    g_ctx.qp_data[check_slot]->qp->qp_num == data->qp->qp_num &&
				    g_ctx.qp_data[check_slot]->rcnt > 0) {
					recv_already_posted = true;
					break;
				}
			}

			if (recv_already_posted) {
				printf("[INFO] Receives already posted for RDMA CM QP %u, skipping\n",
				       data->qp->qp_num);
				j = g_ctx.rx_depth - 1;
			} else {
				j = post_recv(data, g_ctx.rx_depth - 1, wr_id);
				if (j > 0) {
					printf("[INFO] Posted %d receive buffers for RDMA CM QP %u\n",
					       j, data->qp->qp_num);
				}
			}
		} else if (should_post_recv) {
			j = post_recv(data, g_ctx.rx_depth - 1, wr_id);
		} else {
			j = 0;
		}

		if (j == 0 && should_post_recv) {
			printf("Error posting receive buffer. Cleaning up..\n");
			goto cleanup;
		}

		// Initialize remaining fields
		data->pending = (g_ctx.op_type == IBV_WR_SEND && data->dir != RDMA_UD_SEND) ?
					RDMA_UD_RECV :
					0;
		data->rcnt = j;
		data->init = 0;
		data->num_pkt = g_ctx.num_pkts;
		data->armed = 1;
		data->send_posted_count = 0;
		data->pending_echo_count = 0;
		data->deferred_echo = 0;

		g_ctx.qp_data[slot] = data;

		// Immediately mark this slot as active in the bitmap
		if (!atomic_bitmap_is_set(slot)) {
			atomic_bitmap_set(slot);
			printf("[INFO] Immediately marked QP slot %d as active for QP %u\n", slot,
			       data->qp->qp_num);
		}
		continue;

	cleanup:
		rdma_cleanup(data);
		free(data);
		data = NULL;
	}

	n = i;
	// Mark the slots we just filled as active in the bitmap
	int limit = g_ctx.total_slots > 0 ? g_ctx.total_slots : MAX_QUEUES;

	for (int idx = 0, marked = 0; idx < limit && marked < n; ++idx) {
		if (g_ctx.qp_data[idx] && !atomic_bitmap_is_set(idx)) {
			atomic_bitmap_set(idx);
			marked++;
			printf("[INFO] Marked QP slot %d as active in bitmap for QP %u\n", idx,
			       g_ctx.qp_data[idx]->qp->qp_num);
		}
	}

	return 0;
}

static inline int
post_send(struct qp_data *qdata, int wr_id, int opcode)
{
	struct ibv_sge send_sge_arr[g_ctx.nb_sge];
	struct ibv_send_wr send_wr = {0};
	struct ibv_send_wr *bad_send;

	{
		uint32_t per_sge = g_ctx.msg_size / g_ctx.nb_sge;
		uint32_t remainder = g_ctx.msg_size % g_ctx.nb_sge;

		for (int sge_idx = 0; sge_idx < g_ctx.nb_sge; ++sge_idx) {
			send_sge_arr[sge_idx].addr = (uintptr_t)qdata->buf_arr[sge_idx] + 40;
			send_sge_arr[sge_idx].length = per_sge + (sge_idx < (int)remainder ? 1 : 0);
			send_sge_arr[sge_idx].lkey = qdata->mr_arr[sge_idx]->lkey;
		}
	}
	send_wr.wr_id = wr_id;
	send_wr.sg_list = &send_sge_arr[0];
	send_wr.num_sge = g_ctx.nb_sge;
	// For RDMA CM UD, use SEND_WITH_IMM to send our QP number in immediate data
	if (qdata->is_rdma_cm && g_ctx.qp_type == IBV_QPT_UD && opcode == IBV_WR_SEND) {
		send_wr.opcode = IBV_WR_SEND_WITH_IMM;
		send_wr.imm_data = htobe32(qdata->local_info.qp_num);
	} else {
		send_wr.opcode = opcode;
		send_wr.imm_data = 0x44333377;
	}

	// Signal rate limiting
	qdata->send_posted_count++;
	if (g_ctx.signal_every <= 1 || (qdata->send_posted_count % g_ctx.signal_every) == 0)
		send_wr.send_flags |= IBV_SEND_SIGNALED;

	// Inline when small and supported
	if (g_ctx.inline_thresh > 0 && (int)g_ctx.msg_size <= g_ctx.inline_thresh)
		send_wr.send_flags |= IBV_SEND_INLINE;

	if (g_ctx.qp_type == IBV_QPT_UD) {
		send_wr.wr.ud.ah = qdata->ah;
		send_wr.wr.ud.remote_qpn = qdata->remote_info.qp_num;
		send_wr.wr.ud.remote_qkey = qdata->remote_qkey;
	} else {
		if (opcode == IBV_WR_RDMA_READ || opcode == IBV_WR_RDMA_WRITE ||
		    opcode == IBV_WR_RDMA_WRITE_WITH_IMM) {
			send_wr.wr.rdma.remote_addr = qdata->remote_info.remote_addr;
			send_wr.wr.rdma.rkey = qdata->remote_info.rkey;
		}
	}
	int ret = ibv_post_send(qdata->qp, &send_wr, &bad_send);

	if (ret) {
		qdata->stats.send_wr_failed++;
		return -1;
	}
	qdata->stats.send_wr_posted++;
	if (g_ctx.num_pkt_set && qdata->num_pkt > 0)
		--qdata->num_pkt;
	if (g_ctx.debug)
		printf("[DEBUG] Posted %s on QP %u (wr_id=%d)\n",
		       (opcode == IBV_WR_SEND      ? "SEND" :
			opcode == IBV_WR_RDMA_READ ? "READ" :
						     "WRITE"),
		       qdata->local_info.qp_num, wr_id);

	return 0;
}

static void
rdma_post_echo_send(struct qp_data *qdata, int tindex)
{
	if (g_ctx.debug)
		printf("[DEBUG][T:%d] RC RECV done, posting SEND on QP %u\n", tindex,
		       qdata->local_info.qp_num);
	if (post_send(qdata, RDMA_UD_SEND, IBV_WR_SEND) == 0) {
		qdata->pending |= RDMA_UD_SEND;
		if (g_ctx.servername)
			qdata->pending_echo_count++;
	} else {
		printf("[ERROR][T:%d] Failed to post RC SEND on QP %u after RECV\n", tindex,
		       qdata->local_info.qp_num);
	}
}

static inline int
rdma_handle_rdma_send(struct qp_data *qdata, int tindex)
{
	struct ibv_wc wc[16];
	int ne, i;

	static __thread uint64_t dbg_ticks;

	if (qdata->armed == 0 || qdata->cq == NULL || qdata->qp == NULL ||
	    (g_ctx.separate_cq && qdata->send_cq == NULL)) {
		if (g_ctx.debug)
			printf("[DEBUG][T:%d] Skipping QP with null cq/qp (qid unknown)\n", tindex);
		return 0;
	}

	if (g_ctx.servername && qdata->init == 0 && qdata->dir == RDMA_UD_SEND_RECV) {
		if (post_send(qdata, RDMA_UD_SEND, IBV_WR_SEND)) {
			printf("Error posting send for QP %u\n", qdata->local_info.qp_num);
			return -1;
		}
		qdata->init = 1;
		qdata->pending |= RDMA_UD_SEND;
		if (g_ctx.pingpong && g_ctx.qp_type == IBV_QPT_RC && g_ctx.op_type == IBV_WR_SEND)
			qdata->pending_echo_count++;
	} else if (qdata->dir == RDMA_UD_SEND && qdata->init == 0) {
		if (post_send(qdata, RDMA_UD_SEND, IBV_WR_SEND) == 0) {
			qdata->init = 1;
			qdata->pending |= RDMA_UD_SEND;
			if (g_ctx.pingpong && g_ctx.qp_type == IBV_QPT_RC &&
			    g_ctx.op_type == IBV_WR_SEND)
				qdata->pending_echo_count++;
		}
	}

	/* Poll send CQ first to clear pending before processing recv CQEs */
	if (qdata->send_cq) {
		int sne = ibv_poll_cq(qdata->send_cq, 16, wc);

		if (sne < 0) {
			printf("poll send CQ failed %d\n", sne);
			return -1;
		}
		for (i = 0; i < sne; i++) {
			if (wc[i].status == IBV_WC_SUCCESS)
				qdata->stats.send_cqe_ok++;
			else
				qdata->stats.cqe_err++;
			qdata->pending &= ~(int)wc[i].wr_id;
			/* SEND CQE cleared the pending flag; if an echo
			 * was deferred (RECV arrived while SEND was still
			 * in flight), post it now.
			 */
			if (g_ctx.pingpong && g_ctx.qp_type == IBV_QPT_RC &&
			    g_ctx.op_type == IBV_WR_SEND && !(qdata->pending & RDMA_UD_SEND) &&
			    qdata->deferred_echo > 0 && (!g_ctx.num_pkt_set || qdata->num_pkt)) {
				qdata->deferred_echo--;
				rdma_post_echo_send(qdata, tindex);
			}
		}
	}

	ne = ibv_poll_cq(qdata->cq, 16, wc);
	if (ne < 0) {
		printf("poll CQ failed %d\n", ne);
		return -1;
	}
	if (g_ctx.debug && ne == 0) {
		if ((dbg_ticks++ & 0x3fff) == 0)
			printf("[DEBUG][T:%d] CQ poll returned 0 for QP %u (pending=%x rcnt=%d)\n",
			       tindex, qdata->local_info.qp_num, qdata->pending, qdata->rcnt);
	}

	/*
	 * Two-pass CQE processing: first handle SEND completions to clear
	 * the pending flag, then handle RECV completions which may need to
	 * post echo sends. This avoids lost echoes when a RECV CQE appears
	 * before a SEND CQE in the same poll batch.
	 */
	for (i = 0; i < ne; i++) {
		if (wc[i].wr_id == RDMA_UD_RECV)
			continue;
		if (wc[i].status == IBV_WC_SUCCESS &&
		    (wc[i].wr_id == RDMA_UD_SEND || wc[i].opcode == IBV_WC_SEND)) {
			qdata->stats.send_cqe_ok++;
		} else {
			qdata->stats.cqe_err++;
		}
		qdata->pending &= ~(int)wc[i].wr_id;
		if ((!g_ctx.pingpong || g_ctx.qp_type != IBV_QPT_RC ||
		     g_ctx.op_type != IBV_WR_SEND) &&
		    qdata->dir != RDMA_UD_RECV && !(qdata->pending & RDMA_UD_SEND) &&
		    (!g_ctx.num_pkt_set || qdata->num_pkt)) {
			if (post_send(qdata, RDMA_UD_SEND, IBV_WR_SEND))
				return -1;
			qdata->pending |= RDMA_UD_SEND;
		}
		/* RC SEND pingpong: a RECV CQE may have arrived while a
		 * previous echo SEND was still pending in the shared CQ.
		 * Now that the SEND CQE has cleared pending, post the
		 * deferred echo.
		 */
		if (g_ctx.pingpong && g_ctx.qp_type == IBV_QPT_RC && g_ctx.op_type == IBV_WR_SEND &&
		    !(qdata->pending & RDMA_UD_SEND) && qdata->deferred_echo > 0 &&
		    (!g_ctx.num_pkt_set || qdata->num_pkt)) {
			qdata->deferred_echo--;
			rdma_post_echo_send(qdata, tindex);
		}
	}

	/* Pass 2: RECV completions — pending is now clear for echo posting */
	for (i = 0; i < ne; i++) {
		if (wc[i].wr_id != RDMA_UD_RECV)
			continue;

		qdata->pending &= ~(int)RDMA_UD_RECV;

		if (wc[i].status != IBV_WC_SUCCESS) {
			qdata->stats.cqe_err++;
			continue;
		}
		qdata->stats.recv_cqe_ok++;
		qdata->rcnt--;

		if (g_ctx.servername && g_ctx.pingpong && g_ctx.qp_type == IBV_QPT_RC &&
		    g_ctx.op_type == IBV_WR_SEND && qdata->pending_echo_count > 0)
			qdata->pending_echo_count--;

		if (qdata->rcnt < g_ctx.rx_thold) {
			int n = g_ctx.rx_depth - qdata->rcnt - 1;

			/*
			 * Do not return early on post_recv failure; the
			 * RECV CQE has already been consumed from the CQ
			 * and the received data must still be processed.
			 */
			post_recv(qdata, n, RDMA_UD_RECV);
		}

		if (g_ctx.pingpong && g_ctx.qp_type == IBV_QPT_RC && g_ctx.op_type == IBV_WR_SEND &&
		    (qdata->dir != RDMA_UD_RECV) && (!g_ctx.num_pkt_set || qdata->num_pkt)) {
			if (!(qdata->pending & RDMA_UD_SEND))
				rdma_post_echo_send(qdata, tindex);
			else
				/* A SEND is already in flight on this QP;
				 * defer the echo until its CQE clears the
				 * pending flag (handled in Pass 1).
				 */
				qdata->deferred_echo++;
		}

		if ((!g_ctx.pingpong || g_ctx.qp_type != IBV_QPT_RC ||
		     g_ctx.op_type != IBV_WR_SEND) &&
		    qdata->dir != RDMA_UD_RECV && !(qdata->pending & RDMA_UD_SEND) &&
		    (!g_ctx.num_pkt_set || qdata->num_pkt)) {
			if (post_send(qdata, RDMA_UD_SEND, IBV_WR_SEND))
				return -1;
			qdata->pending |= RDMA_UD_SEND;
		}
	}

	return 0;
}

static inline int
rdma_handle_rdma_op(struct qp_data *qdata, int tindex, enum ibv_wr_opcode opcode, uint64_t wr_id)
{
	struct ibv_wc wc[2];
	int ne, i;

	static __thread uint64_t dbg_ticks;

	if (qdata->armed == 0 || qdata->cq == NULL || qdata->qp == NULL ||
	    (g_ctx.separate_cq && qdata->send_cq == NULL)) {
		if (g_ctx.debug)
			printf("[DEBUG][T:%d] Skipping RC op: null cq/qp on QP %u\n", tindex,
			       qdata->local_info.qp_num);
		return 0;
	}

	if (g_ctx.servername && !qdata->pending && (!g_ctx.num_pkt_set || qdata->num_pkt)) {
		if (g_ctx.debug)
			printf("[DEBUG][T:%d] Posting %s on QP %u\n", tindex,
			       (opcode == IBV_WR_RDMA_READ ? "READ" : "WRITE"),
			       qdata->local_info.qp_num);
		if (post_send(qdata, wr_id, opcode) == 0)
			qdata->pending = wr_id;
	}

	ne = ibv_poll_cq(qdata->send_cq ? qdata->send_cq : qdata->cq, 1, wc);
	if (ne < 0) {
		printf("poll CQ failed %d\n", ne);
		return -1;
	}
	if (g_ctx.debug && ne == 0) {
		if ((dbg_ticks++ & 0x3fff) == 0)
			printf("[DEBUG][T:%d] CQ poll returned 0 for RC QP %u (pending=%" PRIu64
			       ")\n",
			       tindex, qdata->local_info.qp_num, (uint64_t)qdata->pending);
	}

	for (i = 0; i < ne; i++) {
		if (wc[i].status == IBV_WC_SUCCESS && wc[i].wr_id == wr_id) {
			qdata->stats.send_cqe_ok++;
			qdata->pending = 0;
		} else if (wc[i].status != IBV_WC_SUCCESS) {
			qdata->stats.cqe_err++;
		}
	}

	return 0;
}

static inline int
rdma_check_all_qp_num_pkt_count(struct qp_range *tq_range)
{
	uint32_t i;

	if (!g_ctx.init_done)
		return 0;

	for (i = tq_range->start_qp; i <= tq_range->end_qp; i++) {
		if (g_ctx.qp_data[i] != NULL)
			return 0;
	}

	return 1;
}

void *
rdma_mq_thread(void *arg)
{
	struct qp_range *tq_range = (struct qp_range *)arg;
	struct qp_data *qdata;
	cpu_set_t cpuset;
	uint32_t qid, i;
	int ret;

	CPU_ZERO(&cpuset);
	CPU_SET(tq_range->coreid, &cpuset);

	pthread_t thread = pthread_self();

	if (pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset) != 0) {
		perror("pthread_setaffinity_np");
		pthread_exit(NULL);
	}

	/* Confirm affinity */
	CPU_ZERO(&cpuset);
	if (pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset) == 0) {
		printf("Thread %lu pinned to CPU(s): ", thread);
		for (i = 0; i < CPU_SETSIZE; i++) {
			if (CPU_ISSET(i, &cpuset))
				printf("%d ", i);
		}
		printf("\n");
	} else {
		printf("pthread_getaffinity_np error\n");
	}

	printf("[%d]tq_range->start_qp: %d\n", tq_range->tindex, tq_range->start_qp);
	printf("[%d]tq_range->end_qp: %d\n", tq_range->tindex, tq_range->end_qp);

	qid = tq_range->start_qp;
	while (!g_ctx.force_quit) {
		if (tq_range->count == 0 || tq_range->start_qp > tq_range->end_qp) {
			usleep(1000);
			continue;
		}

		if (qid > tq_range->end_qp)
			qid = tq_range->start_qp;

		if (qid >= MAX_QUEUES || !atomic_bitmap_is_set(qid)) {
			qid++;
			continue;
		}

		qdata = g_ctx.qp_data[qid];
		if (qdata == NULL || qdata->cq == NULL || qdata->qp == NULL) {
			qid++;
			continue;
		}

		if (g_ctx.op_type == IBV_WR_SEND) {
			ret = rdma_handle_rdma_send(qdata, tq_range->tindex);
		} else if (g_ctx.op_type == IBV_WR_RDMA_WRITE ||
			   g_ctx.op_type == IBV_WR_RDMA_WRITE_WITH_IMM) {
			ret = rdma_handle_rdma_op(qdata, tq_range->tindex, g_ctx.op_type,
						  RDMA_WRITE_REQ);
		} else if (g_ctx.op_type == IBV_WR_RDMA_READ) {
			ret = rdma_handle_rdma_op(qdata, tq_range->tindex, g_ctx.op_type,
						  RDMA_READ_REQ);
		} else {
			printf("Invalid operation type %d\n", g_ctx.op_type);
			goto done;
		}
		if (ret < 0) {
			printf("Error handling RDMA operation for QP %u\n",
			       qdata->local_info.qp_num);
		}

		if ((g_ctx.num_pkt_set && qdata->num_pkt == 0 && qdata->pending_echo_count <= 0 &&
		     qdata->pending == 0 &&
		     (qdata->dir == RDMA_UD_SEND || g_ctx.op_type != IBV_WR_SEND ||
		      g_ctx.qp_type == IBV_QPT_UD || qdata->stats.recv_cqe_ok >= g_ctx.num_pkts)) ||
		    qdata->delete_me) {
			qdata->armed = 0;
			atomic_bitmap_clear(qid);
			save_qp_stats(qid, qdata);
			/*
			 * Destroy QP/CQ/MR before clearing the slot.
			 * cleanup_client_fd_resources polls
			 * g_ctx.qp_data[qid] == NULL to decide when
			 * it is safe to call ibv_dealloc_pd; if the
			 * slot is NULLed first, the PD dealloc races
			 * with still-live resources and fails.
			 */
			rdma_cleanup(qdata);
			g_ctx.qp_data[qid] = NULL;
			free(qdata);
		}

		if (!g_ctx.is_server && g_ctx.num_pkt_set &&
		    rdma_check_all_qp_num_pkt_count(tq_range)) {
			printf("   All QP's in Thread %d have completed sending packets successfully!\n",
			       tq_range->tindex);
			printf("   QP Range: %d-%d | Total Messages Sent: %d per QP\n",
			       tq_range->start_qp, tq_range->end_qp, g_ctx.num_pkts);
			goto done;
		}

		if (g_ctx.interval)
			usleep(g_ctx.interval);

		qid++;
	}

done:
	printf("Worker Thread %d completed successfully!\n", tq_range->tindex);
	printf("  - Processed QP range: %d-%d\n", tq_range->start_qp, tq_range->end_qp);
	printf("  - Thread index: %d exiting\n", tq_range->tindex);

	return NULL;
}

struct qp_range *
divide_qps_among_threads(int total_qps, int nthreads)
{
	struct qp_range *ranges;
	int remainder, base;
	int start, i;

	ranges = calloc(1, sizeof(struct qp_range) * nthreads);
	if (!ranges) {
		printf("malloc failed\n");
		return NULL;
	}

	int64_t total_slots = (int64_t)g_ctx.num_client * (int64_t)total_qps;

	if (total_slots > MAX_QUEUES)
		total_slots = MAX_QUEUES;
	if (total_slots < 0)
		total_slots = 0;

	base = (int)(total_slots / nthreads);
	remainder = (int)(total_slots % nthreads);

	start = 0;
	for (i = 0; i < nthreads; ++i) {
		int count = base + (i < remainder ? 1 : 0);

		memset(&ranges[i], 0, sizeof(struct qp_range));

		ranges[i].tindex = i;
		ranges[i].start_qp = start;
		ranges[i].end_qp = start + count - 1;
		if (ranges[i].end_qp >= MAX_QUEUES)
			ranges[i].end_qp = MAX_QUEUES - 1;
		if (count == 0)
			ranges[i].end_qp = ranges[i].start_qp - 1; /* empty range */
		ranges[i].count = count;
		start += count;
		printf("Thread %d: QP range %d to %d, count %d\n", i, ranges[i].start_qp,
		       ranges[i].end_qp, ranges[i].count);
	}

	return ranges;
}

static struct option long_options[] = {{.name = "gid-idx", .has_arg = 1, .val = 'g'},
				       {.name = "interval", .has_arg = 1, .val = 'i'},
				       {.name = "num-qp", .has_arg = 1, .val = 'q'},
				       {.name = "num-thread", .has_arg = 1, .val = 't'},
				       {.name = "num-packet", .has_arg = 1, .val = 'n'},
				       {.name = "send", .has_arg = 0, .val = 's'},
				       {.name = "recv", .has_arg = 0, .val = 'r'},
				       {.name = "ib-dev", .has_arg = 1, .val = 'd'},
				       {.name = "size", .has_arg = 1, .val = 'z'},
				       {.name = "mtu", .has_arg = 1, .val = 'm'},
				       {.name = "qp-type", .has_arg = 1, .val = 1},
				       {.name = "op-type", .has_arg = 1, .val = 2},
				       {.name = "nb-sge", .has_arg = 1, .val = 3},
				       {.name = "num-client", .has_arg = 1, .val = 'c'},
				       {.name = "help", .has_arg = 0, .val = 'h'},
				       {.name = "debug", .has_arg = 0, .val = 4},
				       {.name = "max-send-wr", .has_arg = 1, .val = 5},
				       {.name = "signal-every", .has_arg = 1, .val = 6},
				       {.name = "inline", .has_arg = 1, .val = 7},
				       {.name = "pingpong", .has_arg = 0, .val = 8},
				       {.name = "rdma-cm", .has_arg = 0, .val = 12},
				       {.name = "src-addr", .has_arg = 1, .val = 13},
				       {.name = "cm-port", .has_arg = 1, .val = 14},
				       {.name = "no-pingpong", .has_arg = 0, .val = 9},
				       {.name = "stats", .has_arg = 0, .val = 10},
				       {.name = "separate-cq", .has_arg = 0, .val = 11},
				       {0}};

static inline void
parse_command_line(int argc, char *argv[])
{
	while (1) {
		int c;
		int option_index = 0;

		c = getopt_long(argc, argv, "g:c:i:q:t:d:h:m:n:z:sr", long_options, &option_index);
		if (c == -1)
			break;

		switch (c) {
		case 3:
			g_ctx.nb_sge = strtol(optarg, NULL, 0);
			if (g_ctx.nb_sge < 1) {
				fprintf(stderr, "Invalid nb_sge: %d\n", g_ctx.nb_sge);
				usage(argv[0]);
				exit(1);
			}
			break;
		case 'g':
			g_ctx.gidx = strtol(optarg, NULL, 0);
			break;
		case 'i':
			g_ctx.interval = strtol(optarg, NULL, 0);
			break;
		case 'q':
			g_ctx.numqp = strtol(optarg, NULL, 0);
			break;
		case 't':
			g_ctx.num_threads = strtol(optarg, NULL, 0);
			break;
		case 's':
			g_ctx.dir = RDMA_UD_SEND;
			break;
		case 'r':
			g_ctx.dir = RDMA_UD_RECV;
			break;
		case 'n':
			g_ctx.num_pkts = strtol(optarg, NULL, 0);
			g_ctx.num_pkt_set = 1;
			break;
		case 'z':
			g_ctx.msg_size = strtol(optarg, NULL, 0);
			break;
		case 'd':
			g_ctx.ib_devname = strdup(optarg);
			printf("Using IB device: %s\n", g_ctx.ib_devname);
			break;
		case 'm':
			g_ctx.path_mtu = strtol(optarg, NULL, 0);
			int mtu_enum = path_mtu_to_enum(g_ctx.path_mtu);

			if (mtu_enum < IBV_MTU_256 || mtu_enum > IBV_MTU_4096) {
				fprintf(stderr, "Invalid MTU size: %d\n", g_ctx.path_mtu);
				usage(argv[0]);
				exit(1);
			}
			break;
		case 'c':
			g_ctx.num_client = strtol(optarg, NULL, 0);
			break;
		case 'h':
			usage(argv[0]);
			exit(0);
		case 1:
			if (strcmp(optarg, "UD") == 0) {
				g_ctx.qp_type = IBV_QPT_UD;
			} else if (strcmp(optarg, "RC") == 0) {
				g_ctx.qp_type = IBV_QPT_RC;
			} else {
				fprintf(stderr, "Invalid QP type: %s", optarg);
				usage(argv[0]);
				exit(1);
			}
			break;
		case 2:
			if (strcmp(optarg, "SEND") == 0) {
				g_ctx.op_type = IBV_WR_SEND;
			} else if (strcmp(optarg, "WRITE") == 0) {
				g_ctx.op_type = IBV_WR_RDMA_WRITE;
			} else if (strcmp(optarg, "WRITE_IMM") == 0) {
				g_ctx.op_type = IBV_WR_RDMA_WRITE_WITH_IMM;
			} else if (strcmp(optarg, "READ") == 0) {
				g_ctx.op_type = IBV_WR_RDMA_READ;
			} else {
				fprintf(stderr, "Invalid operation type: %s", optarg);
				usage(argv[0]);
				exit(1);
			}
			break;
		case 4:
			g_ctx.debug = true;
			break;
		case 5:
			g_ctx.max_send_wr = strtol(optarg, NULL, 0);
			if (g_ctx.max_send_wr < 1)
				g_ctx.max_send_wr = 1;
			break;
		case 6:
			g_ctx.signal_every = strtol(optarg, NULL, 0);
			if (g_ctx.signal_every < 1)
				g_ctx.signal_every = 1;
			break;
		case 7:
			g_ctx.inline_thresh = strtol(optarg, NULL, 0);
			if (g_ctx.inline_thresh < 0)
				g_ctx.inline_thresh = 0;
			break;
		case 8:
			g_ctx.pingpong = true;
			break;
		case 9:
			g_ctx.pingpong = false;
			break;
		case 10:
			g_ctx.stats_enabled = true;
			break;
		case 11:
			g_ctx.separate_cq = true;
			break;
		case 12:
			g_ctx.use_rdma_cm = true;
			break;
		case 13:
			g_ctx.src_addr = strdup(optarg);
			break;
		case 14:
			g_ctx.port = strdup(optarg);
			break;
		default:
			usage(argv[0]);
			exit(1);
		}
	}

	if (g_ctx.qp_type == IBV_QPT_UD && g_ctx.op_type != IBV_WR_SEND) {
		fprintf(stderr, "UD QP type only supports SEND operation.\n");
		usage(argv[0]);
		exit(1);
	}
}

static void
rdma_mark_all_qp_data_for_deletion(int csock)
{
	int i;
	struct qp_data *qdata;

	int limit = g_ctx.total_slots;

	if (limit < 0)
		limit = 0;
	if (limit > MAX_QUEUES)
		limit = MAX_QUEUES;

	for (i = 0; i < limit; i++) {
		qdata = g_ctx.qp_data[i];
		if (qdata && qdata->csock == csock) {
			printf("Marking QP %u for deletion\n", qdata->local_info.qp_num);
			qdata->delete_me = 1;
			qdata->armed = 0;
		}
	}
}

static bool
all_marked_qps_cleaned(int csock)
{
	int limit = g_ctx.total_slots;

	if (limit > MAX_QUEUES)
		limit = MAX_QUEUES;

	for (int i = 0; i < limit; i++) {
		struct qp_data *qd = g_ctx.qp_data[i];

		if (qd && qd->csock == csock && qd->delete_me)
			return false;
	}
	return true;
}

static void
cleanup_client_fd_resources(int epoll_fd, int fd, int *cclient)
{
	int retries = 0;

	close(fd);
	epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
	(*cclient)--;
	rdma_mark_all_qp_data_for_deletion(fd);

	while (!all_marked_qps_cleaned(fd) && retries++ < 200)
		usleep(10000);

	for (int d = 0; d < g_num_devices; ++d) {
		int cidx = g_devices[d].fd_to_client_idx[fd];

		if (cidx >= 0) {
			rdma_cleanup_client_pd(&g_devices[d], cidx);
			g_devices[d].fd_to_client_idx[fd] = -1;
		}
	}
}

static int
handle_server_accept(int epoll_fd, int *cclient, int *csock_fds, int *csock_count)
{
	struct sockaddr_in address;
	socklen_t addrlen = sizeof(address);
	struct sockaddr_in local_addr;
	socklen_t local_addrlen = sizeof(local_addr);
	char local_ip[INET_ADDRSTRLEN];
	struct device_ctx *dev;
	struct epoll_event cevent = {0};
	int csock;

	if (*cclient + 1 > g_ctx.num_client)
		return 0;

	csock = accept(g_ctx.sockfd, (struct sockaddr *)&address, &addrlen);
	if (csock == -1) {
		if (errno != EAGAIN && errno != EWOULDBLOCK)
			printf("accept failed");
		return 0;
	}

	(*cclient)++;
	csock_fds[(*csock_count)++] = csock;

	if (getsockname(csock, (struct sockaddr *)&local_addr, &local_addrlen) == -1) {
		perror("getsockname");
		close(csock);
		return 0;
	}

	inet_ntop(AF_INET, &local_addr.sin_addr, local_ip, sizeof(local_ip));
	dev = find_device_by_ip(local_ip);
	if (!dev) {
		printf("[ERROR] No IB device found for local IP %s\n", local_ip);
		close(csock);
		return 0;
	}

	printf("[INFO] Accepted connection on IP %s, using IB device %s\n", local_ip,
	       dev->ib_devname);
	cevent.data.fd = csock;
	cevent.events = EPOLLIN | EPOLLHUP | EPOLLERR;
	epoll_ctl(epoll_fd, EPOLL_CTL_ADD, csock, &cevent);

	if (rdma_mq_init(csock, dev) < 0) {
		printf("Client connection init failed\n");
		epoll_ctl(epoll_fd, EPOLL_CTL_DEL, csock, NULL);
		close(csock);
		(*cclient)--;
		(*csock_count)--;
		if (*csock_count < 0)
			*csock_count = 0;
		return 0;
	}
	g_ctx.init_done = true;
	printf("Server Connected to remote\n");
	return 0;
}

static void
save_qp_stats(int slot, struct qp_data *qdata)
{
	struct qp_stats_record *rec = &g_saved_stats[slot];

	rec->stats = qdata->stats;
	rec->qp_num = qdata->local_info.qp_num;
	rec->dir = qdata->dir;
	rec->valid = true;
}

static const char *
op_type_str(enum ibv_wr_opcode op)
{
	switch (op) {
	case IBV_WR_SEND:
		return "SEND";
	case IBV_WR_RDMA_WRITE:
		return "WRITE";
	case IBV_WR_RDMA_WRITE_WITH_IMM:
		return "WRITE_IMM";
	case IBV_WR_RDMA_READ:
		return "READ";
	default:
		return "UNKNOWN";
	}
}

static const char *
qp_type_str(enum ibv_qp_type qpt)
{
	switch (qpt) {
	case IBV_QPT_UD:
		return "UD";
	case IBV_QPT_RC:
		return "RC";
	default:
		return "UNKNOWN";
	}
}

static const char *
dir_str(int dir)
{
	switch (dir) {
	case RDMA_UD_SEND_RECV:
		return "SEND_RECV";
	case RDMA_UD_SEND:
		return "SEND";
	case RDMA_UD_RECV:
		return "RECV";
	case RDMA_WRITE_REQ:
		return "WRITE";
	case RDMA_READ_REQ:
		return "READ";
	default:
		return "UNKNOWN";
	}
}

static void
rdma_dump_stats(void)
{
	char filename[256];
	time_t now = time(NULL);
	struct tm *tm_info = localtime(&now);
	FILE *fp;
	int i;
	uint64_t total_send_wr = 0, total_recv_wr = 0;
	uint64_t total_send_cqe = 0, total_recv_cqe = 0;
	uint64_t total_cqe_err = 0;
	uint64_t total_send_fail = 0, total_recv_fail = 0;

	strftime(filename, sizeof(filename), "/tmp/trf_stats_%Y%m%d_%H%M%S.txt", tm_info);

	fp = fopen(filename, "w");
	if (!fp) {
		fprintf(stderr, "Failed to open stats file %s: %s\n", filename, strerror(errno));
		return;
	}

	fprintf(fp, "========================================\n");
	fprintf(fp, "  TRF Per-QP Statistics Dump\n");
	fprintf(fp, "========================================\n");
	{
		char timebuf[64];

		strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", tm_info);
		fprintf(fp, "  Timestamp   : %s\n", timebuf);
	}
	fprintf(fp, "  Role        : %s\n", g_ctx.is_server ? "Server" : "Client");
	fprintf(fp, "  QP Type     : %s\n", qp_type_str(g_ctx.qp_type));
	fprintf(fp, "  Op Type     : %s\n", op_type_str(g_ctx.op_type));
	fprintf(fp, "  Num QPs     : %d\n", g_ctx.qpcount);
	fprintf(fp, "  Msg Size    : %u\n", g_ctx.msg_size);
	fprintf(fp, "  Pingpong    : %s\n", g_ctx.pingpong ? "yes" : "no");
	fprintf(fp, "  Signal Every: %d\n", g_ctx.signal_every);
	fprintf(fp, "========================================\n\n");

	for (i = 0; i < g_ctx.total_slots; i++) {
		struct qp_stats_record *rec = &g_saved_stats[i];

		if (!rec->valid)
			continue;

		struct qp_stats *s = &rec->stats;

		fprintf(fp, "--- QP slot %d  (QPN %u) ---\n", i, rec->qp_num);
		fprintf(fp, "  QP Type          : %s\n", qp_type_str(g_ctx.qp_type));
		fprintf(fp, "  Op Type          : %s\n", op_type_str(g_ctx.op_type));
		fprintf(fp, "  Direction        : %s\n", dir_str(rec->dir));
		fprintf(fp, "  Send WR posted   : %" PRIu64 "\n", s->send_wr_posted);
		fprintf(fp, "  Send WR failed   : %" PRIu64 "\n", s->send_wr_failed);
		fprintf(fp, "  Recv WR posted   : %" PRIu64 "\n", s->recv_wr_posted);
		fprintf(fp, "  Recv WR failed   : %" PRIu64 "\n", s->recv_wr_failed);
		fprintf(fp, "  Send CQE (ok)    : %" PRIu64 "\n", s->send_cqe_ok);
		fprintf(fp, "  Recv CQE (ok)    : %" PRIu64 "\n", s->recv_cqe_ok);
		fprintf(fp, "  CQE errors       : %" PRIu64 "\n", s->cqe_err);
		fprintf(fp, "\n");

		total_send_wr += s->send_wr_posted;
		total_recv_wr += s->recv_wr_posted;
		total_send_cqe += s->send_cqe_ok;
		total_recv_cqe += s->recv_cqe_ok;
		total_cqe_err += s->cqe_err;
		total_send_fail += s->send_wr_failed;
		total_recv_fail += s->recv_wr_failed;
	}

	fprintf(fp, "========================================\n");
	fprintf(fp, "  Aggregate Totals (%s / %s)\n", qp_type_str(g_ctx.qp_type),
		op_type_str(g_ctx.op_type));
	fprintf(fp, "========================================\n");
	fprintf(fp, "  Send WR posted   : %" PRIu64 "\n", total_send_wr);
	fprintf(fp, "  Send WR failed   : %" PRIu64 "\n", total_send_fail);
	fprintf(fp, "  Recv WR posted   : %" PRIu64 "\n", total_recv_wr);
	fprintf(fp, "  Recv WR failed   : %" PRIu64 "\n", total_recv_fail);
	fprintf(fp, "  Send CQE (ok)    : %" PRIu64 "\n", total_send_cqe);
	fprintf(fp, "  Recv CQE (ok)    : %" PRIu64 "\n", total_recv_cqe);
	fprintf(fp, "  CQE errors       : %" PRIu64 "\n", total_cqe_err);
	fprintf(fp, "========================================\n");

	fclose(fp);

	printf("\n========================================\n");
	printf("  Aggregate Statistics (%s / %s)\n", qp_type_str(g_ctx.qp_type),
	       op_type_str(g_ctx.op_type));
	printf("========================================\n");
	printf("  Send WR posted   : %" PRIu64 "\n", total_send_wr);
	printf("  Send WR failed   : %" PRIu64 "\n", total_send_fail);
	printf("  Recv WR posted   : %" PRIu64 "\n", total_recv_wr);
	printf("  Recv WR failed   : %" PRIu64 "\n", total_recv_fail);
	printf("  Send CQE (ok)    : %" PRIu64 "\n", total_send_cqe);
	printf("  Recv CQE (ok)    : %" PRIu64 "\n", total_recv_cqe);
	printf("  CQE errors       : %" PRIu64 "\n", total_cqe_err);
	printf("========================================\n");
	printf("Per-QP details: %s\n", filename);
}

/* RDMA CM initialization and connection setup */
static int
init_rdma_cm(void)
{
	printf("Using RDMA CM for connections\n");

	/* Update hints based on QP type and mode (server/client) */
	if (g_ctx.qp_type == IBV_QPT_RC)
		hints.ai_port_space = RDMA_PS_TCP;
	else if (g_ctx.qp_type == IBV_QPT_UD)
		hints.ai_port_space = RDMA_PS_UDP;

	/* Set proper flags based on server/client mode */
	if (g_ctx.servername)
		hints.ai_flags = 0;
	else
		hints.ai_flags = RAI_PASSIVE;

	printf("RDMA CM: Using port space %s for QP type %s, mode: %s\n",
	       (hints.ai_port_space == RDMA_PS_TCP) ? "TCP" : "UDP",
	       (g_ctx.qp_type == IBV_QPT_RC) ? "RC" : "UD", g_ctx.servername ? "CLIENT" : "SERVER");

	cm_test.connects_left = 1; /* Expect one connection */
	cm_test.channel = create_event_channel();
	if (!cm_test.channel) {
		printf("Failed to create RDMA CM event channel\n");
		return -1;
	}

	if (cm_alloc_nodes()) {
		printf("Failed to allocate RDMA CM nodes\n");
		rdma_destroy_event_channel(cm_test.channel);
		return -1;
	}

	printf("RDMA CM infrastructure initialized successfully\n");

	/* Initialize available IB devices for threading system */
	struct ibv_device **dev_list = ibv_get_device_list(NULL);

	if (!dev_list) {
		printf("No IB devices found for RDMA CM\n");
		return -1;
	}

	if (!dev_list[0]) {
		printf("No IB devices available for RDMA CM\n");
		ibv_free_device_list(dev_list);
		return -1;
	}

	struct device_ctx *dev = &g_devices[0];

	dev->dev_ctx = ibv_open_device(dev_list[0]);
	if (!dev->dev_ctx) {
		printf("Failed to open IB device for RDMA CM\n");
		ibv_free_device_list(dev_list);
		return -1;
	}

	dev->ib_devname = strdup(ibv_get_device_name(dev_list[0]));
	dev->ip_list = NULL;
	dev->ip_count = 0;

	for (int k = 0; k < MAX_CLIENTS; ++k) {
		dev->client_pds[k] = NULL;
		dev->fd_to_client_idx[k] = -1;
	}

	g_num_devices = 1;
	g_ctx.dev_ctx = dev->dev_ctx;

	printf("RDMA CM: Initialized device %s for threading system\n", dev->ib_devname);
	ibv_free_device_list(dev_list);

	return 0;
}

/* TCP device enumeration and setup */
static int
init_tcp_devices(struct ibv_device ***dev_list, struct ibv_device **ib_dev)
{
	int i;

	*dev_list = ibv_get_device_list(NULL);
	if (!*dev_list) {
		printf("Dev list get failed\n");
		return -1;
	}

	if (!g_ctx.ib_devname) {
		printf("No IB device specified, using first available device\n");
		*ib_dev = **dev_list;
		g_ctx.ib_devname = strdup(ibv_get_device_name(*ib_dev));
		if (!*ib_dev) {
			printf("No IB devices found\n");
			return -1;
		}
	} else {
		for (i = 0; (*dev_list)[i]; ++i)
			if (!strcmp(ibv_get_device_name((*dev_list)[i]), g_ctx.ib_devname))
				break;
		*ib_dev = (*dev_list)[i];
		if (!*ib_dev) {
			printf("IB device %s not found\n", g_ctx.ib_devname);
			return -1;
		}
	}

	if (enumerate_ib_devices_and_ips() < 0) {
		printf("Failed to enumerate IB devices and IPs\n");
		return -1;
	}

	g_ctx.dev_ctx = ibv_open_device(*ib_dev);
	if (!g_ctx.dev_ctx) {
		printf("Couldn't get context for %s\n", ibv_get_device_name(*ib_dev));
		return -1;
	}

	return 0;
}

/* Common thread setup for both RDMA CM and TCP */
static int
setup_worker_threads(pthread_t **threads, struct qp_range **range)
{
	int i, subset_size;

	*range = divide_qps_among_threads(g_ctx.numqp, g_ctx.num_threads);
	if (*range == NULL)
		return -1;

	// Compute total QP slots from ranges
	g_ctx.total_slots = 0;
	for (i = 0; i < g_ctx.num_threads; ++i)
		g_ctx.total_slots += (*range)[i].count;

	subset_size = g_ctx.max_cpu_cores - g_ctx.min_cpu_cores + 1;
	*threads = malloc(sizeof(pthread_t) * g_ctx.num_threads);
	for (i = 0; i < g_ctx.num_threads; i++) {
		(*range)[i].coreid = g_ctx.min_cpu_cores + (i % subset_size);
		if (pthread_create(&(*threads)[i], NULL, rdma_mq_thread, &(*range)[i])) {
			printf("Failed to create thread for QP %d\n", i);
			return -1;
		}
	}

	printf("Created %d worker threads\n", g_ctx.num_threads);
	return 0;
}

/* RDMA CM execution logic */
static int
run_rdma_cm(pthread_t *threads)
{
	int i;

	printf("RDMA CM mode: Starting connections now that QP slots are allocated\n");

	/* Now establish RDMA CM connections with QP slots available */
	if (g_ctx.servername) {
		/* Client mode */
		if (cm_run_client() < 0) {
			printf("RDMA CM client failed\n");
			cm_destroy_nodes();
			rdma_destroy_event_channel(cm_test.channel);
			if (cm_test.rai)
				rdma_freeaddrinfo(cm_test.rai);
			return -1;
		}
	} else {
		/* Server mode */
		if (cm_run_server() < 0) {
			printf("RDMA CM server failed\n");
			cm_destroy_nodes();
			rdma_destroy_event_channel(cm_test.channel);
			if (cm_test.rai)
				rdma_freeaddrinfo(cm_test.rai);
			return -1;
		}
	}

	printf("RDMA CM connections established successfully\n");
	printf("RDMA CM mode: QPs ready, threads started\n");
	g_ctx.init_done = true;

	/* Wait for threads to complete */
	for (i = 0; i < g_ctx.num_threads; i++)
		pthread_join(threads[i], NULL);

	printf("✅ All %d worker threads completed successfully!\n", g_ctx.num_threads);

	/* Cleanup RDMA CM resources */
	cm_destroy_nodes();
	rdma_destroy_event_channel(cm_test.channel);
	if (cm_test.rai)
		rdma_freeaddrinfo(cm_test.rai);

	printf("========================================\n");
	printf("RDMA CM TEST COMPLETED SUCCESSFULLY!\n");
	printf("Mode: %s\n", g_ctx.servername ? "Client" : "Server");
	printf("QP Type: %s\n", (g_ctx.qp_type == IBV_QPT_UD) ? "UD" : "RC");
	printf("Operation: %s\n", (g_ctx.op_type == IBV_WR_SEND)                ? "SEND" :
				  (g_ctx.op_type == IBV_WR_RDMA_READ)           ? "READ" :
				  (g_ctx.op_type == IBV_WR_RDMA_WRITE_WITH_IMM) ? "WRITE_WITH_IMM" :
										  "WRITE");
	printf("Messages: %d\n", g_ctx.num_pkts);
	printf("Message Size: %u bytes\n", g_ctx.msg_size);
	printf("QP Count: %d\n", g_ctx.numqp);
	printf("Threads: %d\n", g_ctx.num_threads);
	printf("========================================\n");
	return 0;
}

/* TCP server/client execution logic */
static int
run_tcp_mode(void)
{
	struct epoll_event events[MAX_EVENTS];
	int epoll_fd, cclient;
	int csock, i;
	int csock_fds[MAX_CLIENTS];
	int csock_count = 0;

	if (!g_ctx.servername) {
		/* TCP Server mode */
		epoll_fd = tcp_server_listen(TCP_PORT);
		printf("Server listening on port %d...\n", TCP_PORT);

		while (!g_ctx.force_quit) {
			int n = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);

			if (n == -1) {
				if (errno == EINTR)
					continue;
				printf("epoll_wait error\n");
				break;
			}

			for (i = 0; i < n; i++) {
				int fd = events[i].data.fd;

				if (fd == g_ctx.shutdown_fd) {
					/* Shutdown signal received */
					uint64_t val;

					if (read(g_ctx.shutdown_fd, &val, sizeof(val)) !=
					    sizeof(val))
						printf("Read g_ctx.shutdown_fd failed\n");
					goto tcp_exit;
				}

				if (fd == g_ctx.sockfd) {
					handle_server_accept(epoll_fd, &cclient, csock_fds,
							     &csock_count);
					continue;
				}

				// Handle csock events (disconnect)
				int is_csock = 0;

				for (int j = 0; j < csock_count; ++j) {
					if (fd == csock_fds[j]) {
						is_csock = 1;
						break;
					}
				}
				if (is_csock) {
					if (events[i].events & (EPOLLERR | EPOLLHUP)) {
						printf("Client socket %d disconnected (event)\n",
						       fd);
						cleanup_client_fd_resources(epoll_fd, fd, &cclient);
						continue;
					}
					// Optionally, check for read=0 (client closed)
					char tmpbuf[1];
					ssize_t r = recv(fd, tmpbuf, 1, MSG_PEEK);

					if (r == 0) {
						printf("Client socket %d disconnected (read=0)\n",
						       fd);
						cleanup_client_fd_resources(epoll_fd, fd, &cclient);
						continue;
					}
				}
			}

			// Server stays up until explicit shutdown (signal/epoll), even if QPs
			// complete
		}
	} else {
		/* TCP Client mode */
		csock = tcp_connect_to_server(g_ctx.servername, TCP_PORT);
		if (csock < 0) {
			printf("Client socket connect error\n");
			return -1;
		}

		/* Determine which local IP/IB device this TCP socket picked (important on
		 * multi-homed hosts) */
		struct sockaddr_in local_addr = {0};
		socklen_t local_addrlen = sizeof(local_addr);

		if (getsockname(csock, (struct sockaddr *)&local_addr, &local_addrlen) == -1) {
			perror("getsockname");
			close(csock);
			return -1;
		}
		char local_ip[INET_ADDRSTRLEN] = {0};

		inet_ntop(AF_INET, &local_addr.sin_addr, local_ip, sizeof(local_ip));
		struct device_ctx *dev = find_device_by_ip(local_ip);

		if (!dev) {
			printf("[ERROR] No IB device found for local IP %s (client)\n", local_ip);
			close(csock);
			return -1;
		}
		printf("[INFO] Client connected using local IP %s, using IB device %s\n", local_ip,
		       dev->ib_devname);

		if (rdma_mq_init(csock, dev) < 0) {
			printf("Client connection init failed\n");
			return -1;
		}
		g_ctx.init_done = true;
		printf("Client Connected to remote\n");
	}

tcp_exit:
	printf("========================================\n");
	printf("TCP TEST COMPLETED SUCCESSFULLY!\n");
	printf("Mode: %s\n", g_ctx.servername ? "Client" : "Server");
	printf("QP Type: %s\n", (g_ctx.qp_type == IBV_QPT_UD) ? "UD" : "RC");
	printf("Operation: %s\n", (g_ctx.op_type == IBV_WR_SEND)                ? "SEND" :
				  (g_ctx.op_type == IBV_WR_RDMA_READ)           ? "READ" :
				  (g_ctx.op_type == IBV_WR_RDMA_WRITE_WITH_IMM) ? "WRITE_WITH_IMM" :
										  "WRITE");
	printf("Messages: %d\n", g_ctx.num_pkts);
	printf("Message Size: %u bytes\n", g_ctx.msg_size);
	printf("QP Count: %d\n", g_ctx.numqp);
	printf("Threads: %d\n", g_ctx.num_threads);
	printf("========================================\n");
	return 0;
}

/* Cleanup function for main */
static void
cleanup_main(pthread_t *threads, struct ibv_device **dev_list)
{
	int i, qp_id;
	struct qp_data *qdata;

	for (i = 0; i < g_ctx.num_threads; i++)
		pthread_join(threads[i], NULL);
	free(threads);

	/* Save stats for any QPs still alive (e.g. Ctrl+C path) and clean up */
	for (qp_id = 0; qp_id < g_ctx.total_slots; qp_id++) {
		qdata = g_ctx.qp_data[qp_id];
		if (qdata) {
			if (g_ctx.stats_enabled && !g_saved_stats[qp_id].valid)
				save_qp_stats(qp_id, qdata);
			printf("Cleaning up QP slot %d (QPN %u)\n", qp_id,
			       qdata->local_info.qp_num);
			rdma_cleanup(qdata);
			g_ctx.qp_data[qp_id] = NULL;
			free(qdata);
		}
	}

	if (g_ctx.stats_enabled)
		rdma_dump_stats();

	// Deallocate all PDs and close all device contexts
	for (i = 0; i < g_num_devices; ++i) {
		for (int cidx = 0; cidx < MAX_CLIENTS; ++cidx) {
			if (g_devices[i].client_pds[cidx]) {
				if (ibv_dealloc_pd(g_devices[i].client_pds[cidx])) {
					printf("Couldn't deallocate PD for %s (client %d)\n",
					       g_devices[i].ib_devname, cidx);
				}
				g_devices[i].client_pds[cidx] = NULL;
			}
		}
		if (g_devices[i].dev_ctx) {
			if (ibv_close_device(g_devices[i].dev_ctx)) {
				printf("Couldn't release context for %s\n",
				       g_devices[i].ib_devname);
			}
		}
		if (g_devices[i].ib_devname)
			free(g_devices[i].ib_devname);
		if (g_devices[i].ip_list) {
			for (int j = 0; j < g_devices[i].ip_count; ++j)
				free(g_devices[i].ip_list[j]);
			free(g_devices[i].ip_list);
		}
	}

	if (dev_list)
		ibv_free_device_list(dev_list);

	if (g_ctx.ib_devname)
		free(g_ctx.ib_devname);

	if (g_ctx.sockfd > 0)
		close(g_ctx.sockfd);

	free(g_ctx.servername);
}

int
main(int argc, char **argv)
{
	struct ibv_device **dev_list = NULL;
	struct ibv_device *ib_dev;
	struct qp_range *range;
	int available_cpus;
	pthread_t *threads;

	/* Program can run as server with no positional args, or as client with a hostname */

	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);

	rdma_init_default();

	available_cpus = sysconf(_SC_NPROCESSORS_ONLN);
	if (g_ctx.max_cpu_cores >= available_cpus) {
		fprintf(stderr,
			"Warning: Reducing g_ctx.max_cpu_cores (%d) "
			"to available CPUs (%d)\n",
			g_ctx.max_cpu_cores, available_cpus);
		g_ctx.max_cpu_cores = available_cpus;
	}
	if (g_ctx.min_cpu_cores >= g_ctx.max_cpu_cores)
		g_ctx.min_cpu_cores = 0;
	g_ctx.max_num_threads = g_ctx.max_cpu_cores - g_ctx.min_cpu_cores;

	parse_command_line(argc, argv);

	if (g_ctx.numqp == -1 || g_ctx.numqp == 0) {
		if (g_ctx.use_rdma_cm) {
			g_ctx.numqp = 16; // Larger default for RDMA CM mode to accommodate dynamic
					  // connections
			printf("RDMA CM mode: Using default %d QP slots for dynamic connections\n",
			       g_ctx.numqp);
		} else {
			g_ctx.numqp = DEF_NUM_QPS;
		}
	}

	if (g_ctx.num_threads == -1 || g_ctx.num_threads == 0 ||
	    g_ctx.num_threads > g_ctx.max_num_threads) {
		printf("Defaulting max number if threads equal to cpu cores: %d\n",
		       g_ctx.max_num_threads);
		g_ctx.num_threads = g_ctx.max_num_threads;
	}

	if (optind == argc - 1)
		g_ctx.servername = strdup(argv[optind]);

	g_ctx.is_server = g_ctx.servername ? 0 : 1;

	if (g_ctx.num_client == -1 || g_ctx.num_client == 0 || g_ctx.servername)
		g_ctx.num_client = 1;

	/* Initialize connection type - RDMA CM or TCP */
	if (g_ctx.use_rdma_cm) {
		if (init_rdma_cm() < 0)
			return -1;
	} else {
		if (init_tcp_devices(&dev_list, &ib_dev) < 0)
			return -1;
	}

	/* Setup worker threads - common for both RDMA CM and TCP */
	if (setup_worker_threads(&threads, &range) < 0)
		return -1;

	/* Execute based on connection type */
	int ret;

	if (g_ctx.use_rdma_cm)
		ret = run_rdma_cm(threads);
	else
		ret = run_tcp_mode();

	cleanup_main(threads, dev_list);

	if (ret == 0)
		printf("\nALL TESTS COMPLETED SUCCESSFULLY!\n");
	else
		printf("\nTEST EXECUTION FAILED\n");

	return ret;
}
