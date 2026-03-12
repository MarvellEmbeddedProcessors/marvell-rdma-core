/* SPDX-License-Identifier: Marvell-MIT
 * Copyright (c) 2025 Marvell.
 */

#define _GNU_SOURCE
#include <sched.h>
#include <config.h>
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
static struct qp_stats_record g_saved_stats[MAX_QUEUES];
static void save_qp_stats(int slot, struct qp_data *qdata);

/* Bitmap helpers forward declarations */
void atomic_bitmap_set(uint16_t count_id);
void atomic_bitmap_clear(uint16_t count_id);
int atomic_bitmap_is_set(uint16_t count_id);

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

struct qp_range *divide_qps_among_threads(int total_qps, int nthreads);
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
	for (int i = 0; i < g_ctx.total_slots; ++i) {
		if (!atomic_bitmap_is_set(i) && g_ctx.qp_data[i] == NULL)
			return i;
	}
	return -1;
}

// Connection parameters sent from client to server
struct conn_params {
	int qp_type;
	int op_type;
	int num_pkts;
	int msg_size;
	int numqp; // Number of QPs requested by client
};

int rdma_mq_init(int csock, struct device_ctx *dev);
void *rdma_mq_thread(void *arg);

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
	printf("  --qp-type=<UD|RC> QP type (default: UD)\n");
	printf("  --op-type=<SEND|WRITE|WRITE_IMM|READ> Operation type (default: SEND)\n");
	printf("  --max-send-wr=<N>   SQ depth per QP (default 2)\n");
	printf("  --signal-every=<N>  Signal every N sends (default 1 = signal all)\n");
	printf("  --inline=<bytes>    Use inline for SEND up to this size (0=disable)\n");
	printf("  --pingpong          For SEND: only SEND after a RECV (default on)\n");
	printf("  --no-pingpong       Disable ping-pong; allow SEND when idle\n");
	printf("  --stats             Enable per-QP statistics; dump to file on exit\n");
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
	g_ctx.signal_every = 1; // signal every send by default
	g_ctx.inline_thresh = 0;
	g_ctx.pingpong = true;
	g_ctx.stats_enabled = false;
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

// Exchange QP info as before
static int
tcp_exchange_info(int rsock, struct qp_info *local, struct qp_info *remote)
{
	if (write(rsock, local, sizeof(*local)) != sizeof(*local))
		return -1;
	if (read(rsock, remote, sizeof(*remote)) != sizeof(*remote))
		return -1;
	return 0;
}

// Send connection params (client) and receive (server)
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
	printf("Cleaning up QP %u device cleanup\n", qdata->local_info.qp_num);

	qdata->armed = 0;

	struct ibv_qp *qp = qdata->qp;
	struct ibv_cq *cq = qdata->cq;

	qdata->qp = NULL;
	qdata->cq = NULL;

	if (qp) {
		struct ibv_qp_attr attr = {.qp_state = IBV_QPS_ERR};

		if (ibv_modify_qp(qp, &attr, IBV_QP_STATE))
			printf("Warning: failed to move QP %u to ERROR state\n",
			       qdata->local_info.qp_num);
	}

	if (cq) {
		struct ibv_wc wc;

		while (ibv_poll_cq(cq, 1, &wc) > 0)
			;
	}

	if (qp && ibv_destroy_qp(qp)) {
		printf("Couldn't destroy QP\n");
		return -1;
	}

	if (cq && ibv_destroy_cq(cq)) {
		printf("Couldn't destroy CQ\n");
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

	if (qdata->ah && ibv_destroy_ah(qdata->ah)) {
		printf("Couldn't destroy AH\n");
		return -1;
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

	for (int sge_idx = 0; sge_idx < g_ctx.nb_sge; ++sge_idx) {
		recv_sge_arr[sge_idx].addr = (uintptr_t)qdata->buf_arr[sge_idx];
		recv_sge_arr[sge_idx].length = g_ctx.msg_size + 40;
		recv_sge_arr[sge_idx].lkey = qdata->mr_arr[sge_idx]->lkey;
	}
	recv_wr.wr_id = wr_id;
	recv_wr.sg_list = &recv_sge_arr[0];
	recv_wr.num_sge = g_ctx.nb_sge;

	for (j = 0; j < rxdepth; j++) {
		if (ibv_post_recv(qdata->qp, &recv_wr, &bad_recv)) {
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
	int (*func_ptr)(int sock, struct qp_info *local, struct qp_info *remote);
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

	// If server, receive conn_params from client and set QP/op type and num_pkts
	if (g_ctx.is_server) {
		struct conn_params params;

		if (recv_conn_params(csock, &params) < 0) {
			printf("[ERROR] Failed to receive conn_params from client\n");
			return -1;
		}
		g_ctx.qp_type = params.qp_type;
		g_ctx.op_type = params.op_type;
		g_ctx.num_pkts = params.num_pkts;
		g_ctx.msg_size = params.msg_size;
		g_ctx.numqp = params.numqp; // Set number of QPs from client
		g_ctx.num_pkt_set = 1;
	} else {
		// Send conn_params to server
		struct conn_params params = {.qp_type = g_ctx.qp_type,
					     .op_type = g_ctx.op_type,
					     .num_pkts = g_ctx.num_pkts,
					     .msg_size = g_ctx.msg_size,
					     .numqp = g_ctx.numqp};
		if (send_conn_params(csock, &params) < 0) {
			printf("[ERROR] Failed to send conn_params to server\n");
			return -1;
		}
		// make_socket_non_blocking(csock);
	}
	/* Track of QP count per client is not maintained in this build. */

	/* Retrieve or assign a compact client index for this fd */
	int client_idx = -1;

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

	// Ensure we have enough free slots to host this client's QPs
	if (count_free_slots() < g_ctx.numqp) {
		printf("Not enough free QP slots available (need %d, have %d)\n", g_ctx.numqp,
		       count_free_slots());
		return -1;
	}

	for (i = 0; i < g_ctx.numqp; i++) {
		int slot = find_free_slot();

		if (slot < 0) {
			printf("No free QP slot found\n");
			return 0;
		}

		data = (struct qp_data *)calloc(1, sizeof(struct qp_data));
		if (data == NULL) {
			printf("Failed to allocate memory for thread\n");
			return -1;
		}

		/* Track the device this QP belongs to for proper cleanup */
		data->dev = dev;

		data->csock = csock;
		data->dir = g_ctx.dir;

		data->cq = ibv_create_cq(dev->dev_ctx, g_ctx.rx_depth + 1, NULL, NULL, 0);
		if (g_ctx.debug) {
			printf("[DEBUG] Created CQ %p (rx_depth %u) on dev %s\n", (void *)data->cq,
			       g_ctx.rx_depth + 1, dev->ib_devname);
		}
		if (!data->cq) {
			printf("CQ create failed for thread\n");
			goto cleanup;
		}

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

			printf("Registering MR for SGE %d: buf %p, size %u\n", sge_idx,
			       data->buf_arr[sge_idx], g_ctx.msg_size + 40);

			data->mr_arr[sge_idx] =
				ibv_reg_mr(dev->client_pds[client_idx], data->buf_arr[sge_idx],
					   g_ctx.msg_size + 40,
					   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
						   IBV_ACCESS_REMOTE_READ);
			if (!data->mr_arr[sge_idx]) {
				printf("Memory registration failed for SGE %d\n", sge_idx);
				goto cleanup;
			}
		}

		data->mr = data->mr_arr[0];

		attr.send_cq = data->cq;
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
			printf("Error creating QP for queue: %d\n", g_ctx.qpcount + i);
			goto cleanup;
		}

		/* Initialize QP state */
		get_qp_modify_attr(&attr_mod, &flags, IBV_QPS_INIT, data);
		if (ibv_modify_qp(data->qp, &attr_mod, flags)) {
			printf("Error modifying QP %u to INIT state\n", data->qp->qp_num);
			goto cleanup;
		}
		if (g_ctx.debug)
			printf("[DEBUG] QP %u moved to INIT\n", data->qp->qp_num);

		data->local_info.qp_num = data->qp->qp_num;
		/* Compute sgid index that matches this connection's local IP */
		char local_ip_sel[INET_ADDRSTRLEN] = {0};
		struct sockaddr_in laddr = {0};
		socklen_t laddrlen = sizeof(laddr);

		if (getsockname(csock, (struct sockaddr *)&laddr, &laddrlen) == 0)
			inet_ntop(AF_INET, &laddr.sin_addr, local_ip_sel, sizeof(local_ip_sel));

		int sgid_index = choose_gid_index_for_local(dev->dev_ctx, dev->ib_devname, 1,
							    local_ip_sel[0] ? local_ip_sel : NULL,
							    g_ctx.gidx);
		/* Store globally for helper paths that still consult g_ctx.gidx */
		g_ctx.gidx = sgid_index;

		if (ibv_query_gid(dev->dev_ctx, 1, sgid_index, &data->local_info.gid)) {
			printf("query gid failed\n");
			goto cleanup;
		}

		ibv_query_port(dev->dev_ctx, 1, &port_attr);
		data->local_info.lid = port_attr.lid;

		data->local_info.psn = rand() & 0xffffff;
		data->local_info.rkey = data->mr->rkey;
		data->local_info.remote_addr = (uintptr_t)data->buf_arr[0] + 40; // Remote address
										 // for write/read

		func_ptr = g_ctx.is_server ? recv_remote_info : tcp_exchange_info;
		if (func_ptr(csock, &data->local_info, &data->remote_info) < 0) {
			printf("Error exchanging information for QP %u line %u\n",
			       data->local_info.qp_num, __LINE__);
			goto cleanup;
		}

		inet_ntop(AF_INET6, &data->local_info.gid, lgid, sizeof(lgid));
		inet_ntop(AF_INET6, &data->remote_info.gid, rgid, sizeof(rgid));
		printf("  local address: QPN 0x%06x, GID %s buf address %p len %u -- remote address: QPN 0x%06x, GID %s buf address %p len %u remote rkey %x\n",
		       data->local_info.qp_num, lgid, (void *)data->local_info.remote_addr,
		       g_ctx.msg_size, data->remote_info.qp_num, rgid,
		       (void *)data->remote_info.remote_addr, g_ctx.msg_size,
		       data->remote_info.rkey);

		get_qp_modify_attr(&attr_mod, &flags, IBV_QPS_RTR, data);
		if (ibv_modify_qp(data->qp, &attr_mod, flags)) {
			printf("Error modifying QP %u to RTR state\n", data->qp->qp_num);
			goto cleanup;
		}
		if (g_ctx.debug)
			printf("[DEBUG] QP %u moved to RTR\n", data->qp->qp_num);

		wr_id = g_ctx.op_type == IBV_WR_SEND      ? RDMA_UD_RECV :
			g_ctx.op_type == IBV_WR_RDMA_READ ? RDMA_READ_REQ :
							    RDMA_WRITE_REQ;
		/* Post receives for UD, and for RC when op-type is SEND */
		if (g_ctx.qp_type == IBV_QPT_UD ||
		    (g_ctx.qp_type == IBV_QPT_RC && g_ctx.op_type == IBV_WR_SEND))
			j = post_recv(data, g_ctx.rx_depth - 1, wr_id);
		else
			j = 0;
		if (j == 0) {
			if (g_ctx.qp_type == IBV_QPT_UD ||
			    (g_ctx.qp_type == IBV_QPT_RC && g_ctx.op_type == IBV_WR_SEND)) {
				printf("Error posting receive buffer. Cleaning up..\n");
				goto cleanup;
			}
			if (g_ctx.debug)
				printf("[DEBUG] Skipped posting recvs for RC QP %u (op %d)\n",
				       data->qp->qp_num, g_ctx.op_type);
		}

		if (g_ctx.is_server && send_local_info(csock, &data->local_info) < 0) {
			printf("Error exchanging information for QP %u line %u\n",
			       data->local_info.qp_num, __LINE__);
			goto cleanup;
		}

		get_qp_modify_attr(&attr_mod, &flags, IBV_QPS_RTS, data);
		if (ibv_modify_qp(data->qp, &attr_mod, flags)) {
			printf("Error modifying QP %u to RTS state\n", data->qp->qp_num);
			goto cleanup;
		}
		if (g_ctx.debug) {
			printf("[DEBUG] QP %u moved to RTS\n", data->qp->qp_num);
			struct ibv_qp_attr qs = {0};
			struct ibv_qp_init_attr qia = {0};

			if (ibv_query_qp(data->qp, &qs,
					 IBV_QP_STATE | IBV_QP_CUR_STATE | IBV_QP_QKEY |
						 IBV_QP_PATH_MTU | IBV_QP_DEST_QPN,
					 &qia) == 0) {
				printf("[DEBUG] QP %u state=%d path_mtu=%d dest_qpn=%u\n",
				       data->qp->qp_num, qs.qp_state, qs.path_mtu, qs.dest_qp_num);
			}
		}

		if (g_ctx.qp_type == IBV_QPT_UD) {
			ah_attr.is_global = 1;
			ah_attr.port_num = 1;
			ah_attr.grh.dgid = data->remote_info.gid;
			ah_attr.grh.sgid_index = sgid_index;
			ah_attr.grh.hop_limit = 8;
			data->ah = ibv_create_ah(dev->client_pds[client_idx], &ah_attr);
			if (!data->ah) {
				printf("AH create failed\n");
				goto cleanup;
			}
		}

		data->pending = g_ctx.op_type == IBV_WR_SEND ? RDMA_UD_RECV : 0;
		data->rcnt = j;
		data->init = 0;
		data->num_pkt = g_ctx.num_pkts;
		data->armed = 1;
		data->send_posted_count = 0;
		data->pending_echo_count = 0;

		g_ctx.qp_data[slot] = data;
		continue;

	cleanup:
		rdma_cleanup(data);
		free(data);
		data = NULL;
	}

	n = i;
	// Mark the slots we just filled as active in the bitmap
	for (int idx = 0, marked = 0; idx < g_ctx.total_slots && marked < n; ++idx) {
		if (g_ctx.qp_data[idx] && !atomic_bitmap_is_set(idx)) {
			atomic_bitmap_set(idx);
			marked++;
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

	if (g_ctx.num_pkt_set)
		--qdata->num_pkt;
	for (int sge_idx = 0; sge_idx < g_ctx.nb_sge; ++sge_idx) {
		send_sge_arr[sge_idx].addr = (uintptr_t)qdata->buf_arr[sge_idx] + 40;
		send_sge_arr[sge_idx].length = g_ctx.msg_size;
		send_sge_arr[sge_idx].lkey = qdata->mr_arr[sge_idx]->lkey;
	}
	send_wr.wr_id = wr_id;
	send_wr.sg_list = &send_sge_arr[0];
	send_wr.num_sge = g_ctx.nb_sge;
	send_wr.opcode = opcode;
	// Signal rate limiting
	qdata->send_posted_count++;
	if (g_ctx.signal_every <= 1 || (qdata->send_posted_count % g_ctx.signal_every) == 0)
		send_wr.send_flags |= IBV_SEND_SIGNALED;

	// Inline when small and supported
	if (g_ctx.inline_thresh > 0 && (int)g_ctx.msg_size <= g_ctx.inline_thresh)
		send_wr.send_flags |= IBV_SEND_INLINE;
	send_wr.imm_data = 0x44333377;

	if (g_ctx.qp_type == IBV_QPT_UD) {
		send_wr.wr.ud.ah = qdata->ah;
		send_wr.wr.ud.remote_qpn = qdata->remote_info.qp_num;
		send_wr.wr.ud.remote_qkey = 0x11111111;
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
	struct ibv_wc wc[2];
	int ne, i, j;

	static __thread uint64_t dbg_ticks;

	if (qdata->armed == 0 || qdata->cq == NULL || qdata->qp == NULL) {
		if (g_ctx.debug)
			printf("[DEBUG][T:%d] Skipping QP with null cq/qp (qid unknown)\n", tindex);
		return 0;
	}

	if ((g_ctx.servername && qdata->init == 0 && qdata->dir == RDMA_UD_SEND_RECV) ||
	    qdata->dir == RDMA_UD_SEND) {
		if (post_send(qdata, RDMA_UD_SEND, IBV_WR_SEND)) {
			printf("Error posting send for QP %u\n", qdata->local_info.qp_num);
			return -1;
		}
		qdata->init = 1;
		qdata->pending |= RDMA_UD_SEND;
		/* Client expects echo for this initiated send */
		if (g_ctx.pingpong && g_ctx.qp_type == IBV_QPT_RC && g_ctx.op_type == IBV_WR_SEND)
			qdata->pending_echo_count++;
	}

	ne = ibv_poll_cq(qdata->cq, 2, wc);
	if (ne < 0) {
		printf("poll CQ failed %d\n", ne);
		return -1;
	}
	if (g_ctx.debug && ne == 0) {
		if ((dbg_ticks++ & 0x3fff) == 0)
			printf("[DEBUG][T:%d] CQ poll returned 0 for QP %u (pending=%x rcnt=%d)\n",
			       tindex, qdata->local_info.qp_num, qdata->pending, qdata->rcnt);
	}

	for (i = 0; i < ne; i++) {
		if (wc[i].status == IBV_WC_SUCCESS && wc[i].wr_id == RDMA_UD_RECV) {
			qdata->stats.recv_cqe_ok++;
			qdata->rcnt--;

			if (g_ctx.servername && g_ctx.pingpong && g_ctx.qp_type == IBV_QPT_RC &&
			    g_ctx.op_type == IBV_WR_SEND && qdata->pending_echo_count > 0)
				qdata->pending_echo_count--;

			if (qdata->rcnt < g_ctx.rx_thold) {
				int n = g_ctx.rx_depth - qdata->rcnt - 1;

				j = post_recv(qdata, n, RDMA_UD_RECV);
				if (j == 0) {
					qdata->pending &= ~(int)wc[i].wr_id;
					return 0;
				}
			}

			if (g_ctx.pingpong && g_ctx.qp_type == IBV_QPT_RC &&
			    g_ctx.op_type == IBV_WR_SEND && (qdata->dir != RDMA_UD_RECV) &&
			    (!g_ctx.num_pkt_set || qdata->num_pkt)) {
				if (qdata->pending & RDMA_UD_SEND) {
					if (g_ctx.debug)
						printf("[DEBUG][T:%d] Skipping SEND-on-RECV on QP %u (SEND already pending)\n",
						       tindex, qdata->local_info.qp_num);
				} else {
					rdma_post_echo_send(qdata, tindex);
				}
			}
		} else if (wc[i].status == IBV_WC_SUCCESS && wc[i].wr_id == RDMA_UD_SEND) {
			qdata->stats.send_cqe_ok++;
		} else if (wc[i].status == IBV_WC_SUCCESS && wc[i].opcode == IBV_WC_SEND) {
			qdata->stats.send_cqe_ok++;
		} else {
			qdata->stats.cqe_err++;
		}

		qdata->pending &= ~(int)wc[i].wr_id;
		if ((!g_ctx.pingpong || g_ctx.qp_type != IBV_QPT_RC ||
		     g_ctx.op_type != IBV_WR_SEND) &&
		    qdata->dir != RDMA_UD_RECV && !(qdata->pending & RDMA_UD_SEND) &&
		    (!g_ctx.num_pkt_set || qdata->num_pkt)) {
			if (g_ctx.debug)
				printf("[DEBUG][T:%d] Posting SEND on QP %u (loop tail)\n", tindex,
				       qdata->local_info.qp_num);
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

	if (qdata->armed == 0 || qdata->cq == NULL || qdata->qp == NULL) {
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
		post_send(qdata, wr_id, opcode);
		qdata->pending = wr_id;
	}

	ne = ibv_poll_cq(qdata->cq, 1, wc);
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
	struct qp_data *qdata;

	for (i = tq_range->start_qp; i <= tq_range->end_qp; i++) {
		qdata = g_ctx.qp_data[i];
		if (qdata && qdata->num_pkt)
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

		if ((g_ctx.num_pkt_set && qdata->num_pkt == 0 && qdata->pending_echo_count <= 0) ||
		    qdata->delete_me) {
			qdata->armed = 0;
			atomic_bitmap_clear(qid);
			save_qp_stats(qid, qdata);
			g_ctx.qp_data[qid] = NULL;
			if (!rdma_cleanup(qdata))
				free(qdata);
		}

		if (!g_ctx.is_server && g_ctx.num_pkt_set &&
		    rdma_check_all_qp_num_pkt_count(tq_range)) {
			printf("All QP's have completed sending packets. Exiting thread %d\n",
			       tq_range->tindex);
			goto done;
		}

		if (g_ctx.interval)
			usleep(g_ctx.interval);

		qid++;
	}

done:
	printf("Thread index: %d exiting.\n", tq_range->tindex);

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
				       {.name = "no-pingpong", .has_arg = 0, .val = 9},
				       {.name = "stats", .has_arg = 0, .val = 10},
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
	printf("Stats dumped to %s\n", filename);
}

int
main(int argc, char **argv)
{
	struct epoll_event events[MAX_EVENTS];
	struct ibv_device **dev_list = NULL;
	struct ibv_device *ib_dev;
	struct qp_range *range;
	struct qp_data *qdata;
	int available_cpus;
	pthread_t *threads;
	int subset_size;
	int qp_id = 0;
	int epoll_fd;
	int cclient;
	int csock;
	int i;
	int csock_fds[MAX_EVENTS] = {0}; // Track csock fds
	int csock_count = 0;

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

	if (g_ctx.numqp == -1 || g_ctx.numqp == 0)
		g_ctx.numqp = DEF_NUM_QPS;

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

	dev_list = ibv_get_device_list(NULL);
	if (!dev_list) {
		printf("Dev list get failed\n");
		return -1;
	}

	if (!g_ctx.ib_devname) {
		printf("No IB device specified, using first available device\n");
		ib_dev = *dev_list;
		g_ctx.ib_devname = strdup(ibv_get_device_name(ib_dev));
		if (!ib_dev) {
			printf("No IB devices found\n");
			return -1;
		}
	} else {
		for (i = 0; dev_list[i]; ++i)
			if (!strcmp(ibv_get_device_name(dev_list[i]), g_ctx.ib_devname))
				break;
		ib_dev = dev_list[i];
		if (!ib_dev) {
			printf("IB device %s not found\n", g_ctx.ib_devname);
			return -1;
		}
	}
	if (enumerate_ib_devices_and_ips() < 0) {
		printf("Failed to enumerate IB devices and IPs\n");
		return -1;
	}

	g_ctx.dev_ctx = ibv_open_device(ib_dev);
	if (!g_ctx.dev_ctx) {
		printf("Couldn't get context for %s\n", ibv_get_device_name(ib_dev));
		return -1;
	}

	range = divide_qps_among_threads(g_ctx.numqp, g_ctx.num_threads);
	if (range == NULL)
		return -1;

	// Compute total QP slots from ranges
	g_ctx.total_slots = 0;
	for (i = 0; i < g_ctx.num_threads; ++i)
		g_ctx.total_slots += range[i].count;

	subset_size = g_ctx.max_cpu_cores - g_ctx.min_cpu_cores + 1;
	threads = malloc(sizeof(pthread_t) * g_ctx.num_threads);
	for (i = 0; i < g_ctx.num_threads; i++) {
		range[i].coreid = g_ctx.min_cpu_cores + (i % subset_size);
		if (pthread_create(&threads[i], NULL, rdma_mq_thread, &range[i])) {
			printf("Failed to create thread for QP %d\n", i);
			return -1;
		}
	}

	cclient = 0;
	if (!g_ctx.servername) {
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
					goto exit;
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
		printf("Client Connected to remote\n");
	}

exit:
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

	return 0;
}
