/* SPDX-License-Identifier: Marvell-MIT
 * Copyright (c) 2025 Marvell.
 */

#ifndef __UD_MQ_TRF_H__
#define __UD_MQ_TRF_H__

#include <stdatomic.h>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#define MIN_CPU_CORE 2
#define MAX_CPU_CORE 10

#define MAX_EVENTS 100000

#define DEF_NUM_QPS 4
#define TCP_PORT    18515

#define RDMA_UD_SEND_RECV 0
#define RDMA_UD_SEND      1
#define RDMA_UD_RECV      2
#define RDMA_WRITE_REQ    3
#define RDMA_READ_REQ     4

#define MAX_QUEUES 2048
/* Enough 64-bit words to cover MAX_QUEUES bits */
#define BITMAP_WORDS    (MAX_QUEUES / 64) /* 32 words for 2048 QPs */
#define MAX_NUM_THREADS (MAX_CPU_CORE - MIN_CPU_CORE)

struct queue_bitmap {
	atomic_uint_fast64_t bits[BITMAP_WORDS];
};

struct qp_range {
	int coreid;
	int tindex;
	int start_qp;
	int end_qp;
	int count;
};

struct qp_info {
	uint32_t lid;
	uint32_t qp_num;
	uint32_t psn;
	uint32_t rkey;
	uint64_t remote_addr;
	union ibv_gid gid;
};

struct qp_stats {
	uint64_t send_wr_posted;
	uint64_t recv_wr_posted;
	uint64_t send_cqe_ok;
	uint64_t recv_cqe_ok;
	uint64_t cqe_err;
	uint64_t send_wr_failed;
	uint64_t recv_wr_failed;
};

struct qp_stats_record {
	struct qp_stats stats;
	uint32_t qp_num;
	int dir;
	bool valid;
};

struct qp_data {
	struct ibv_qp *qp;
	struct ibv_cq *cq;      /* recv CQ (or shared CQ when !separate_cq) */
	struct ibv_cq *send_cq; /* send CQ (NULL when using shared CQ) */
	struct ibv_mr *mr;
	struct ibv_ah *ah;
	struct qp_info local_info;
	struct qp_info remote_info;
	uint64_t num_pkt;
	int pending;
	int csock;
	int init;
	struct device_ctx *dev;
	int rcnt;
	int dir;
	void **buf_arr;
	struct ibv_mr **mr_arr;
	int delete_me;
	int armed;         // 1 when QP/CQ are fully ready and safe to poll
	int cleaned_up;    // 1 when cleanup has been called to prevent double cleanup
	int is_rdma_cm;    // 1 if this QP was created by RDMA CM, 0 for manually created QPs
	int is_ah_rdma_cm; // 1 if this AH was created by RDMA CM, 0 for manually created AHs
	int is_cq_rdma_cm; // 1 if this CQ was created by RDMA CM, 0 for manually created CQs
	uint64_t send_posted_count; // number of sends posted (for signaled interval)
	int pending_echo_count;
	int deferred_echo;
	struct qp_stats stats;
	uint32_t remote_qkey;
	struct cm_node *cm_node; /* back-pointer for UD cleanup (no DISCONNECT event) */
};

/* Connection parameters sent from client to server */
struct conn_params {
	int qp_type;
	int op_type;
	int num_pkts;
	int msg_size;
	int numqp; // Number of QPs requested by client
};

/* RDMA CM node structure similar to udaddy.c */
struct cm_node {
	int id;
	struct rdma_cm_id *cma_id;
	int connected;
	int initialized; // Flag to prevent multiple initialization
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_mr *mr;
	struct ibv_ah *ah;
	uint32_t remote_qpn;
	uint32_t remote_qkey;
	void *mem;
	struct qp_data *qp_data_list; // List of QP data for this connection
	int qp_count;
	struct conn_params conn_params; // Connection parameters for threading integration

	/* MR info exchanged via CM private_data */
	uint32_t exchange_rkey;
	uint64_t exchange_addr;
	uint32_t remote_rkey_cm;
	uint64_t remote_addr_cm;
	int mr_info_valid;
	struct ibv_mr *data_mr;
	void *data_buf;

	/* Per-connection bookkeeping for disconnect-time cleanup */
	struct device_ctx *dev_ctx;
	int client_idx;
};

/* Connection context for both TCP and RDMA CM */
struct conn_ctx {
	enum { CONN_TYPE_TCP, CONN_TYPE_RDMA_CM } type;
	union {
		struct {
			int csock;
		} tcp;
		struct {
			struct cm_node *node;
		} cm;
	} u;
	struct device_ctx *dev;
	int client_idx;
};

/* RDMA CM test context */
struct cm_test {
	struct rdma_event_channel *channel;
	struct cm_node *nodes;
	int conn_index;
	int connects_left;
	struct rdma_addrinfo *rai;
};

struct app_ctx {
	enum ibv_qp_type qp_type;
	struct qp_data *qp_data[MAX_QUEUES];
	volatile bool force_quit;
	int max_num_threads;
	int max_cpu_cores;
	int min_cpu_cores;
	enum ibv_wr_opcode op_type;
	unsigned int rx_depth;
	unsigned int rx_thold;
	struct queue_bitmap qbmap;
	char *servername;
	struct ibv_context *dev_ctx;
	char *ib_devname;
	unsigned int interval;
	unsigned int num_pkts;
	bool use_rdma_cm; // Enable RDMA CM mode instead of TCP
	char *src_addr;   // Source address for RDMA CM
	char *port;       // Port for RDMA CM
	bool num_pkt_set;
	unsigned int msg_size;
	int num_threads;
	int num_client;
	struct ibv_pd *pd;
	int shutdown_fd;
	int sockfd;
	int numqp;
	int gidx;
	int qpcount;
	int dir;
	int path_mtu;
	bool is_server;
	int nb_sge;        // Number of SGEs per WR
	bool debug;        // Enable verbose debug prints
	int max_send_wr;   // SQ depth
	int signal_every;  // signal every N sends (1 = signal all)
	int inline_thresh; // inline threshold in bytes (0 = disabled)
	int total_slots;
	bool pingpong;
	bool stats_enabled;
	bool separate_cq;
	volatile bool init_done;
	/* Gate for worker threads to wait until all CM connections are up */
	volatile bool cm_connections_ready;
};

#define MAX_CLIENTS 2048
struct device_ctx {
	struct ibv_context *dev_ctx;
	char *ib_devname;
	char **ip_list;
	int ip_count;
	struct ibv_pd *client_pds[MAX_CLIENTS];
	/* Map OS socket fd -> compact client index; -1 means unmapped */
	int fd_to_client_idx[MAX_CLIENTS];
};

static inline int
path_mtu_to_enum(int mtu)
{
	switch (mtu) {
	case 256:
		return IBV_MTU_256;
	case 512:
		return IBV_MTU_512;
	case 1024:
		return IBV_MTU_1024;
	case 2048:
		return IBV_MTU_2048;
	case 4096:
		return IBV_MTU_4096;
	default:
		return 0;
	}
}
#endif /* __UD_MQ_TRF_H__ */
