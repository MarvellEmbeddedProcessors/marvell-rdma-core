/* SPDX-License-Identifier: Marvell-MIT
 * Copyright (c) 2025 Marvell.
 */

#ifndef __OCTEP_RDMA_VERBS_H__
#define __OCTEP_RDMA_VERBS_H__

#include "octep_rdma-abi.h"

#define SQEBB_SHIFT            5
#define OCTEP_RDMA_MAX_SEND_WR 8192
#define OCTEP_RDMA_MAX_RECV_WR 8192
#define OCTEP_RDMA_MIN_SEND_WR 64
#define OCTEP_RDMA_MIN_RECV_WR 64

struct octep_rdma_cq {
	struct ibv_cq ibcq;
	struct octep_rdma_device *rdma_dev;
	uint32_t id;

	uint32_t event_stats;

	uint32_t depth;
	uint32_t qmask;
	uint32_t ci;
	void *q_base;
	volatile atomic_ushort *pi_dbl;
	volatile atomic_ushort *ci_dbl;

	uint32_t cq_size;
	uint32_t comp_vector;

	pthread_spinlock_t lock;
};

struct octep_rdma_queue {
	void *qbuf;
	void *db;
	volatile atomic_ushort *pi_dbl;
	volatile atomic_ushort *ci_dbl;

	uint16_t rsvd0;
	uint16_t depth;
	uint16_t qmask;
	uint32_t size;

	uint16_t pi;

	uint32_t rsvd1;
	uint64_t *wr_tbl;
};

struct octep_rdma_qp {
	struct ibv_qp ibqp;
	struct octep_rdma_device *octep_rdma_dev;

	uint32_t id;           /* qpn */
	enum ibv_qp_type type; /* qp type */

	pthread_spinlock_t sq_lock;
	pthread_spinlock_t rq_lock;

	int sq_sig_all;

	struct octep_rdma_queue sq;
	struct octep_rdma_queue rq;

	void *qbuf;
	size_t qbuf_size;
	void *db_region;
	uint64_t db_region_sz;
};

struct octep_rdma_ah {
	struct ibv_ah ibv_ah;
	int ah_num;
};

extern struct verbs_context_ops octep_rdma_pts_ctx_ops;
int octep_rdma_query_device(struct ibv_context *ctx, const struct ibv_query_device_ex_input *input,
			    struct ibv_device_attr_ex *attr, size_t attr_size);
int octep_rdma_query_port(struct ibv_context *ctx, uint8_t port, struct ibv_port_attr *attr);
struct ibv_pd *octep_rdma_alloc_pd(struct ibv_context *ctx);
int octep_rdma_dealloc_pd(struct ibv_pd *pd);
struct ibv_mr *octep_rdma_reg_mr(struct ibv_pd *pd, void *addr, size_t len, uint64_t hca_va,
				 int access);
int octep_rdma_dereg_mr(struct verbs_mr *vmr);
struct ibv_cq *octep_rdma_create_cq(struct ibv_context *ctx, int num_cqe,
				    struct ibv_comp_channel *channel, int comp_vector);
int octep_rdma_destroy_cq(struct ibv_cq *base_cq);
int octep_rdma_poll_cq(struct ibv_cq *ibcq, int num_entries, struct ibv_wc *wc);
void octep_rdma_free_context(struct ibv_context *ibv_ctx);

struct ibv_qp *octep_rdma_create_qp(struct ibv_pd *pd, struct ibv_qp_init_attr *attr);
int octep_rdma_modify_qp(struct ibv_qp *base_qp, struct ibv_qp_attr *attr, int attr_mask);
int octep_rdma_destroy_qp(struct ibv_qp *base_qp);
int octep_rdma_query_qp(struct ibv_qp *qp, struct ibv_qp_attr *attr, int attr_mask,
			struct ibv_qp_init_attr *init_attr);
struct ibv_ah *octep_rdma_create_ah(struct ibv_pd *pd, struct ibv_ah_attr *attr);
int octep_rdma_destroy_ah(struct ibv_ah *ibah);
int octep_rdma_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr_list,
			 struct ibv_send_wr **bad_wr);
int octep_rdma_post_recv(struct ibv_qp *base_qp, struct ibv_recv_wr *wr,
			 struct ibv_recv_wr **bad_wr);
int octep_rdma_pts_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr_list,
			     struct ibv_send_wr **bad_wr);
int octep_rdma_pts_post_recv(struct ibv_qp *base_qp, struct ibv_recv_wr *wr,
			     struct ibv_recv_wr **bad_wr);
int octep_rdma_pts_poll_cq(struct ibv_cq *ibcq, int num_entries, struct ibv_wc *wc);
#endif /* __OCTEP_RDMA_VERBS_H__ */
