/* SPDX-License-Identifier: Marvell-MIT
 * Copyright (c) 2025 Marvell.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <util/util.h>
#include <util/udma_barrier.h>
#include <unistd.h>

#include "octep_rdma.h"

static const enum ibv_wr_opcode octep_rdma_pts_supported_opcodes[] = {
	IBV_WR_SEND,      IBV_WR_SEND_WITH_IMM, IBV_WR_RDMA_WRITE, IBV_WR_RDMA_WRITE_WITH_IMM,
	IBV_WR_RDMA_READ,
};

struct verbs_context_ops octep_rdma_pts_ctx_ops = {
	.free_context = octep_rdma_free_context,
	.query_device_ex = octep_rdma_query_device,
	.query_port = octep_rdma_query_port,
	.alloc_pd = octep_rdma_alloc_pd,
	.dealloc_pd = octep_rdma_dealloc_pd,
	.reg_mr = octep_rdma_reg_mr,
	.dereg_mr = octep_rdma_dereg_mr,
	.create_cq = octep_rdma_create_cq,
	.destroy_cq = octep_rdma_destroy_cq,
	.create_qp = octep_rdma_create_qp,
	.query_qp = octep_rdma_query_qp,
	.modify_qp = octep_rdma_modify_qp,
	.destroy_qp = octep_rdma_destroy_qp,
	.create_ah = octep_rdma_create_ah,
	.destroy_ah = octep_rdma_destroy_ah,
	.post_send = octep_rdma_pts_post_send,
	.post_recv = octep_rdma_pts_post_recv,
	.poll_cq = octep_rdma_pts_poll_cq,
};

static octep_always_inline bool
octep_rdma_pts_is_supported_opcode(enum ibv_wr_opcode opcode)
{
	uint32_t size, i = 0;

	size = sizeof(octep_rdma_pts_supported_opcodes) /
	       sizeof(octep_rdma_pts_supported_opcodes[0]);

	for (i = 0; i < size; i++) {
		if (octep_rdma_pts_supported_opcodes[i] == opcode)
			return true;
	}
	return false;
}

static octep_always_inline bool
is_queue_full(uint16_t pi, uint16_t ci, uint16_t qmask)
{
	return ((pi + 1 - ci) & qmask) == 0;
}

static octep_always_inline bool
is_queue_empty(uint16_t pi, uint16_t ci)
{
	return (pi == ci);
}

int
octep_rdma_pts_poll_cq(struct ibv_cq *ibcq, int num_entries, struct ibv_wc *wc)
{
	struct octep_rdma_cq *cq = to_octep_rdma_cq(ibcq);
	struct octep_rdma_cqe *q_base = cq->q_base, *cqe;
	uint16_t ci, pi, avail;
	uint32_t cq_sz, qmask;
	uint16_t *pi_dbl;
	int i;

	pi_dbl = (uint16_t *)cq->pi_dbl;
	cq_sz = cq->depth;
	qmask = cq->qmask;

	/* Check for entries */
	pthread_spin_lock(&cq->lock);
	ci = cq->ci;
	__atomic_load((uint16_t *)pi_dbl, &pi, __ATOMIC_ACQUIRE);
	if (is_queue_empty(pi, ci)) {
		pthread_spin_unlock(&cq->lock);
		return 0;
	}
	avail = pi >= ci ? pi - ci : cq_sz - (ci - pi);
	avail = OCTEP_MIN(avail, num_entries);

	/* Copy entries */
	for (i = 0; i < avail; i++) {
		cqe = (struct octep_rdma_cqe *)q_base + ci;
		wc[i].wr_id = cqe->wr_id;
		wc[i].status = cqe->status;
		wc[i].opcode = cqe->opcode;
		wc[i].byte_len = cqe->byte_len;
		wc[i].vendor_err = cqe->vendor_err;
		wc[i].qp_num = cqe->qp_id;
		ci = (ci + 1) & qmask;
	}
	/* Update consumer index */
	cq->ci = ci;
	__atomic_store_n((uint16_t *)cq->ci_dbl, ci, __ATOMIC_RELEASE);
	pthread_spin_unlock(&cq->lock);
	return avail;
}

int
octep_rdma_pts_post_recv(struct ibv_qp *ibqp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad_wr)
{
	struct octep_rdma_qp *qp = to_octep_rdma_qp(ibqp);
	struct octep_rdma_queue *rq = &qp->rq;
	struct ibv_sge *sg_list;
	union octep_rdma_rqe *rqe;
	uint16_t qmask = rq->qmask;
	void *qbuf = rq->qbuf;
	uint16_t num_sge, cnt;
	uint16_t pi, ci;
	int rv = 0;

	pthread_spin_lock(&qp->rq_lock);
	pi = rq->pi;
	ci = atomic_load(rq->ci_dbl);

	while (wr) {
		if (is_queue_full(pi, ci, rq->qmask)) {
			verbs_err(verbs_get_ctx(ibqp->context), "QP[%d]: RQ overflow, idx %d\n",
				  qp->id, pi);
			rv = -ENOMEM;
			*bad_wr = wr;
			break;
		}
		rqe = ((union octep_rdma_rqe *)qbuf) + pi;
		num_sge = wr->num_sge;
		sg_list = wr->sg_list;
		rqe->wr_id = wr->wr_id;
		rqe->num_sge = num_sge;

		/* Copy first SGE */
		memcpy(rqe->sges0, sg_list, sizeof(struct ibv_sge) * 1);
		sg_list++;
		rqe->sges0[0].addr = wr->sg_list[0].addr;
		rqe->sges0[0].length = wr->sg_list[0].length;
		rqe->sges0[0].key = wr->sg_list[0].lkey;

		verbs_debug_datapath(verbs_get_ctx(ibqp->context),
				     "idx %d wr_id %ld rqe %p addr %llx length %d key %x\n", pi,
				     rqe->wr_id, rqe, rqe->sges0[0].addr, rqe->sges0[0].length,
				     rqe->sges0[0].key);
		pi = (pi + 1) & qmask;
		num_sge--;

		while (num_sge) {
			rqe = ((union octep_rdma_rqe *)qbuf) + pi;
			cnt = num_sge > 2 ? 2 : num_sge;
			memcpy(rqe->sges1, sg_list, sizeof(struct ibv_sge) * cnt);
			num_sge -= cnt;
			sg_list += cnt;
			pi = (pi + 1) & qmask;
		}
		wr = wr->next;
	}
	rq->pi = pi;
	__atomic_store((uint16_t *)rq->pi_dbl, &rq->pi, __ATOMIC_RELAXED);
	pthread_spin_unlock(&qp->rq_lock);
	return rv;
}

static inline int
octep_rdma_pts_validate_send_wr(struct octep_rdma_qp *qp, struct ibv_send_wr *wr_list)
{
	enum ibv_wr_opcode opcode = wr_list->opcode;

	if (wr_list->num_sge > OCTEP_RDMA_MAX_SGE_COUNT)
		return -EINVAL;

	if (octep_rdma_pts_is_supported_opcode(opcode))
		return -EINVAL;

	return 0;
}

static inline void
octep_rdma_pts_update_sqe(struct octep_rdma_qp *qp, union octep_rdma_sqe *sqe, uint8_t opcode,
			  struct ibv_send_wr *wr_list)
{
	struct octep_rdma_ah *ah = NULL;

	switch (opcode) {
	case IBV_WR_SEND:
	case IBV_WR_SEND_WITH_IMM:
		if (qp->type == IBV_QPT_UD) {
			ah = to_octep_rdma_ah(wr_list->wr.ud.ah);
			sqe->ud.remote_qpn = wr_list->wr.ud.remote_qpn;
			sqe->ud.qkey = wr_list->wr.ud.remote_qkey;
			sqe->ud.ah = ah->ah_num;
		}
		break;
	case IBV_WR_RDMA_WRITE_WITH_IMM:
	case IBV_WR_RDMA_WRITE:
	case IBV_WR_RDMA_READ:
		sqe->rdma.remote_addr = wr_list->wr.rdma.remote_addr;
		sqe->rdma.rkey = wr_list->wr.rdma.rkey;
		break;
	default:
		break;
	}
}

static inline int
octep_rdma_pts_post_one_send(struct octep_rdma_qp *qp, struct octep_rdma_queue *sq,
			     struct ibv_send_wr *wr_list)
{
	union octep_rdma_sqe *sqe = NULL;
	uint16_t depth = sq->depth, qmask = sq->qmask;
	struct ibv_sge *sg_list;
	void *qbuf = sq->qbuf;
	uint16_t pi = sq->pi;
	uint16_t ci;
	uint8_t opcode, cnt;
	int num_sge = 0;
	int ret = 0;

#ifdef OCTEP_RDMA_DEBUG
	int err = 0;

	err = octep_rdma_pts_validate_send_wr(qp, wr_list);
	if (err) {
		verbs_err(verbs_get_ctx(qp->ibqp.context), "validate send work request failed\n");
		return err;
	}
#endif
	opcode = wr_list->opcode;
	sg_list = wr_list->sg_list;
	num_sge = wr_list->num_sge;

	ci = atomic_load(sq->ci_dbl);
	if (is_queue_full(pi, ci, qmask)) {
		verbs_err(verbs_get_ctx(qp->ibqp.context), "QP[%d]: SQ overflow, idx %d\n", qp->id,
			  pi);
		return -ENOMEM;
	}

	sqe = (union octep_rdma_sqe *)qbuf + pi;
	sqe->send_flags = wr_list->send_flags;
	sqe->imm_data = wr_list->imm_data;
	sqe->opcode = wr_list->opcode;
	sqe->wr_id = wr_list->wr_id;
	sqe->num_sges = num_sge;

	octep_rdma_pts_update_sqe(qp, sqe, opcode, wr_list);
	/* Copy first SGE */
	memcpy(sqe->sges0, sg_list, sizeof(struct ibv_sge) * 1);
	sg_list++;
	num_sge--;

	/* Copy the rest of the SGEs */
	if (unlikely(num_sge)) {
		memcpy(&sqe->sges0[1], sg_list, sizeof(struct ibv_sge) * 1);
		num_sge--;
		sg_list++;
		pi = (pi + 1) & (depth - 1);

		while (num_sge) {
			sqe = (union octep_rdma_sqe *)qbuf + pi;
			cnt = num_sge > 4 ? 4 : num_sge;

			memcpy(sqe->sges1, sg_list, sizeof(struct ibv_sge) * cnt);
			sg_list += cnt;
			num_sge -= cnt;
			pi = (pi + 1) & qmask;
		}
	} else {
		pi = (pi + 1) & qmask;
	}
	/* TODO: unnecessary load/store to sq */
	sq->pi = pi;
	return ret;
}

int
octep_rdma_pts_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr_list,
			 struct ibv_send_wr **bad_wr)
{
	struct octep_rdma_qp *qp = to_octep_rdma_qp(ibqp);
	struct octep_rdma_queue *sq = &qp->sq;
	int ret = 0;

	if (!bad_wr)
		return -EINVAL;
	*bad_wr = NULL;

	if (!sq->qbuf || !wr_list)
		return -EINVAL;

	pthread_spin_lock(&qp->sq_lock);
	while (wr_list) {
		ret = octep_rdma_pts_post_one_send(qp, sq, wr_list);
		if (ret) {
			*bad_wr = wr_list;
			break;
		}
		wr_list = wr_list->next;
	}
	/* desc should be available before doorbell update */
	udma_to_device_barrier();
	__atomic_store((uint16_t *)sq->pi_dbl, &sq->pi, __ATOMIC_RELEASE);
	pthread_spin_unlock(&qp->sq_lock);
	return ret;
}
