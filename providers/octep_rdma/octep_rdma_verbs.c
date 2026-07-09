/* SPDX-License-Identifier: Marvell-MIT
 * Copyright (c) 2025 Marvell.
 */

#include <netinet/in.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>
#include <util/mmio.h>
#include <util/udma_barrier.h>
#include <util/util.h>

#include "octep_rdma.h"

int
octep_rdma_query_port(struct ibv_context *ctx, uint8_t port, struct ibv_port_attr *attr)
{
	struct ibv_query_port cmd = {};

	return ibv_cmd_query_port(ctx, port, attr, &cmd, sizeof(cmd));
}

int
octep_rdma_query_device(struct ibv_context *ctx, const struct ibv_query_device_ex_input *input,
			struct ibv_device_attr_ex *attr, size_t attr_size)
{
	struct ib_uverbs_ex_query_device_resp resp;
	unsigned int major, minor, sub_minor;
	size_t resp_size = sizeof(resp);
	uint64_t raw_fw_ver;
	int rv;

	rv = ibv_cmd_query_device_any(ctx, input, attr, attr_size, &resp, &resp_size);
	if (rv)
		return rv;

	raw_fw_ver = resp.base.fw_ver;
	major = (raw_fw_ver >> 32) & 0xffff;
	minor = (raw_fw_ver >> 16) & 0xffff;
	sub_minor = raw_fw_ver & 0xffff;

	snprintf(attr->orig_attr.fw_ver, sizeof(attr->orig_attr.fw_ver), "%d.%d.%d", major, minor,
		 sub_minor);

	return 0;
}

struct ibv_pd *
octep_rdma_alloc_pd(struct ibv_context *ctx)
{
	struct ib_uverbs_alloc_pd_resp resp;
	struct ibv_alloc_pd cmd = {};
	struct ibv_pd *pd;

	pd = calloc(1, sizeof(*pd));
	if (!pd)
		return NULL;

	if (ibv_cmd_alloc_pd(ctx, pd, &cmd, sizeof(cmd), &resp, sizeof(resp))) {
		free(pd);
		return NULL;
	}

	return pd;
}

int
octep_rdma_dealloc_pd(struct ibv_pd *pd)
{
	int rv;

	rv = ibv_cmd_dealloc_pd(pd);
	if (rv)
		return rv;

	free(pd);
	return 0;
}

struct ibv_mr *
octep_rdma_reg_mr(struct ibv_pd *pd, void *addr, size_t length, uint64_t hca_va, int access)
{
	struct verbs_mr *vmr;
	struct ibv_reg_mr cmd;
	struct ib_uverbs_reg_mr_resp resp;
	int ret;

	vmr = calloc(1, sizeof(*vmr));
	if (!vmr)
		return NULL;

	ret = ibv_cmd_reg_mr(pd, addr, length, hca_va, access, vmr, &cmd, sizeof(cmd), &resp,
			     sizeof(resp));
	if (ret) {
		free(vmr);
		return NULL;
	}

	return &vmr->ibv_mr;
}

int
octep_rdma_dereg_mr(struct verbs_mr *vmr)
{
	int ret;

	ret = ibv_cmd_dereg_mr(vmr);
	if (ret)
		return ret;

	free(vmr);
	return 0;
}

static int
octep_rdma_alloc_buf(struct octep_rdma_cq *cq, unsigned int cq_size, int page_size)
{
	int ret = 0;

	cq->cq_size = align(cq_size, page_size);
	cq->q_base =
		mmap(NULL, cq->cq_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (cq->q_base == MAP_FAILED) {
		verbs_err(verbs_get_ctx(cq->ibcq.context), "mmap failed, err %d\n", errno);
		ret = errno;
		goto err;
	}
	verbs_debug(verbs_get_ctx(cq->ibcq.context), "[%s] cq  %p cq size %d\n", __func__,
		    cq->q_base, cq->cq_size);

	ret = ibv_dontfork_range(cq->q_base, cq->cq_size);
	if (ret) {
		verbs_err(verbs_get_ctx(cq->ibcq.context), "ibv_dontfork_range failed, err %d\n",
			  ret);
		munmap(cq->q_base, cq->cq_size);
		goto err;
	}

	memset(cq->q_base, 0, cq->cq_size);
err:
	return ret;
}

struct ibv_cq *
octep_rdma_create_cq(struct ibv_context *ibv_ctx, int num_cqe, struct ibv_comp_channel *channel,
		     int comp_vector)
{
	struct octep_rdma_ctx *ctx = to_octep_rdma_ctx(ibv_ctx);
	struct octep_rdma_cmd_create_cq_resp resp = {};
	struct octep_rdma_cmd_create_cq cmd = {};
	struct octep_rdma_cq *cq;
	size_t cq_size;
	int rv;

	cq = calloc(1, sizeof(*cq));
	if (!cq)
		return NULL;

	/* FIXME get the real max_cqe from the device */
	if (num_cqe < 64)
		num_cqe = 64;

	cq->ibcq.context = ibv_ctx;
	num_cqe = roundup_pow_of_two(num_cqe + 1);
	cq_size = align(num_cqe * sizeof(struct octep_rdma_cqe), OCTEP_RDMA_PAGE_SIZE);

	if (octep_rdma_alloc_buf(cq, cq_size, OCTEP_RDMA_PAGE_SIZE)) {
		verbs_err(verbs_get_ctx(cq->ibcq.context), "octep_rdma_alloc_buf failed\n");
		goto err;
	}

	cmd.qbuf_va = (uintptr_t)cq->q_base;
	cmd.qbuf_len = cq_size;
	num_cqe--;

	rv = ibv_cmd_create_cq(ibv_ctx, num_cqe, channel, comp_vector, &cq->ibcq, &cmd.ibv_cmd,
			       sizeof(cmd), &resp.ibv_resp, sizeof(resp));
	if (rv) {
		errno = EIO;
		goto error_alloc;
	}

	pthread_spin_init(&cq->lock, PTHREAD_PROCESS_PRIVATE);

	cq->id = resp.cq_id;
	cq->depth = resp.num_cqe;
	cq->qmask = cq->depth - 1;
	cq->comp_vector = comp_vector;
	cq->pi_dbl = (void *)ctx->db_region +
		     (((cq->id * OCTEP_RDMA_QS_MULTIPLIER) + 2) * ctx->notify_off_multiplier);
	cq->ci_dbl = cq->pi_dbl + 1;

	verbs_debug(verbs_get_ctx(cq->ibcq.context),
		    "[%s] cq %p cq->q_base 0x%lx size %lx id %d depth %d comp_vector %d\n",
		    __func__, cq, (uintptr_t)cq->q_base, cq_size, cq->id, cq->depth,
		    cq->comp_vector);

	return &cq->ibcq;

error_alloc:
	if (cq->q_base) {
		ibv_dofork_range(cq->q_base, cq->cq_size);
		munmap(cq->q_base, cq->cq_size);
	}
err:
	free(cq);

	return NULL;
}

int
octep_rdma_destroy_cq(struct ibv_cq *base_cq)
{
	struct octep_rdma_cq *cq = to_octep_rdma_cq(base_cq);
	int rv;

	pthread_spin_lock(&cq->lock);
	rv = ibv_cmd_destroy_cq(base_cq);
	if (rv) {
		pthread_spin_unlock(&cq->lock);
		errno = EIO;
		return rv;
	}
	pthread_spin_destroy(&cq->lock);

	if (cq->q_base) {
		ibv_dofork_range(cq->q_base, cq->cq_size);
		munmap(cq->q_base, cq->cq_size);
	}

	free(cq);

	return 0;
}

static int
__octep_rdma_poll_one_cqe(struct octep_rdma_cq *cq, struct ibv_wc *wc)
{
	struct octep_rdma_cqe *cqe = (struct octep_rdma_cqe *)cq->q_base;

	if (!cqe) {
		verbs_err(verbs_get_ctx(cq->ibcq.context), "Invalid CQ\n");
		return -EAGAIN;
	}

	if (cqe->wr_id == 0)
		return -EAGAIN;
	cq->ci++;
	udma_from_device_barrier();

	wc->wr_id = cqe->wr_id;
	wc->byte_len = be32toh(cqe->byte_len);
	wc->opcode = cqe->opcode;
	wc->qp_num = cqe->qp_id;
	wc->status = cqe->status;
	if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
		wc->imm_data = htobe32(le32toh(cqe->imm_data));
		wc->wc_flags |= IBV_WC_WITH_IMM;
	}

	verbs_debug(verbs_get_ctx(cq->ibcq.context),
		    "opcode %x qp_id %d status %d wr_id %ld imm_data %x\n", cqe->opcode, cqe->qp_id,
		    cqe->status, cqe->wr_id, cqe->imm_data);

	memset(cqe, 0, sizeof(*cqe));

	return 0;
}

int
octep_rdma_poll_cq(struct ibv_cq *ibcq, int num_entries, struct ibv_wc *wc)
{
	struct octep_rdma_cq *cq = to_octep_rdma_cq(ibcq);
	int ret, npolled = 0;

	pthread_spin_lock(&cq->lock);

	while (npolled < num_entries) {
		ret = __octep_rdma_poll_one_cqe(cq, wc + npolled);
		if (ret == -EAGAIN) /* CQ is empty, break the loop. */
			break;
		else if (ret) /* We handle the polling error silently. */
			continue;
		npolled++;
	}

	pthread_spin_unlock(&cq->lock);

	return npolled;
}

static int
octep_rdma_store_qp(struct octep_rdma_ctx *ctx, struct octep_rdma_qp *qp)
{
	uint32_t tbl_idx, tbl_off;
	int rv = 0;

	pthread_mutex_lock(&ctx->qp_table_mutex);
	tbl_idx = qp->id >> OCTEP_RDMA_QP_TABLE_SHIFT;
	tbl_off = qp->id & OCTEP_RDMA_QP_TABLE_MASK;

	if (ctx->qp_table[tbl_idx].refcnt == 0) {
		ctx->qp_table[tbl_idx].table =
			calloc(OCTEP_RDMA_QP_TABLE_SIZE, sizeof(struct octep_rdma_qp *));
		if (!ctx->qp_table[tbl_idx].table) {
			rv = -ENOMEM;
			goto out;
		}
	}

	/* exist qp */
	if (ctx->qp_table[tbl_idx].table[tbl_off]) {
		rv = -EBUSY;
		goto out;
	}

	ctx->qp_table[tbl_idx].table[tbl_off] = qp;
	ctx->qp_table[tbl_idx].refcnt++;

out:
	pthread_mutex_unlock(&ctx->qp_table_mutex);

	return rv;
}

static void
octep_rdma_clear_qp(struct octep_rdma_ctx *ctx, struct octep_rdma_qp *qp)
{
	uint32_t tbl_idx, tbl_off;

	pthread_mutex_lock(&ctx->qp_table_mutex);
	tbl_idx = qp->id >> OCTEP_RDMA_QP_TABLE_SHIFT;
	tbl_off = qp->id & OCTEP_RDMA_QP_TABLE_MASK;

	ctx->qp_table[tbl_idx].table[tbl_off] = NULL;
	ctx->qp_table[tbl_idx].refcnt--;

	if (ctx->qp_table[tbl_idx].refcnt == 0) {
		free(ctx->qp_table[tbl_idx].table);
		ctx->qp_table[tbl_idx].table = NULL;
	}

	pthread_mutex_unlock(&ctx->qp_table_mutex);
}

static int
octep_rdma_alloc_wrid_tbl(struct octep_rdma_qp *qp)
{
	qp->rq.wr_tbl = calloc(qp->rq.depth, sizeof(uint64_t));
	if (!qp->rq.wr_tbl)
		return -ENOMEM;

	qp->sq.wr_tbl = calloc(qp->sq.depth, sizeof(uint64_t));
	if (!qp->sq.wr_tbl) {
		free(qp->rq.wr_tbl);
		return -ENOMEM;
	}

	return 0;
}

static void
octep_rdma_free_wrid_tbl(struct octep_rdma_qp *qp)
{
	free(qp->sq.wr_tbl);
	free(qp->rq.wr_tbl);
}

static int
octep_rdma_alloc_qp_buf_and_db(struct octep_rdma_qp *qp, struct ibv_qp_init_attr *attr,
			       int page_size)
{
	uint32_t num_sqe, num_rqe;
	size_t queue_size;
	int rv;

	num_sqe = roundup_pow_of_two(attr->cap.max_send_wr + 1);
	if (num_sqe < OCTEP_RDMA_MIN_SEND_WR)
		num_sqe = OCTEP_RDMA_MIN_SEND_WR;
	else if (num_sqe > OCTEP_RDMA_MAX_SEND_WR)
		num_sqe = OCTEP_RDMA_MAX_SEND_WR;

	queue_size = align(num_sqe * sizeof(union octep_rdma_sqe), page_size);
	verbs_debug(verbs_get_ctx(qp->ibqp.context),
		    "SQE: queue_size %ld - num_sqe %d sizeof sqe %ld\n", queue_size, num_sqe,
		    sizeof(union octep_rdma_sqe));

	num_rqe = roundup_pow_of_two(attr->cap.max_recv_wr + 1);
	if (num_rqe < OCTEP_RDMA_MIN_RECV_WR)
		num_rqe = OCTEP_RDMA_MIN_RECV_WR;
	else if (num_rqe > OCTEP_RDMA_MAX_RECV_WR)
		num_rqe = OCTEP_RDMA_MAX_RECV_WR;

	queue_size += align(num_rqe * sizeof(union octep_rdma_rqe), page_size);
	verbs_debug(verbs_get_ctx(qp->ibqp.context),
		    "RQE: queue_size %ld - num_rqe %d sizeof rqe %ld\n", queue_size, num_rqe,
		    sizeof(union octep_rdma_rqe));

	qp->qbuf_size = queue_size;
	qp->qbuf = mmap(NULL, qp->qbuf_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
			-1, 0);
	if (qp->qbuf == MAP_FAILED) {
		verbs_err(verbs_get_ctx(qp->ibqp.context), "QP mmap failed, err %d\n", errno);
		rv = errno;
		goto err;
	}

	rv = ibv_dontfork_range(qp->qbuf, queue_size);
	if (rv) {
		errno = rv;
		goto err_dontfork;
	}
	qp->sq.depth = num_sqe;
	qp->rq.depth = num_rqe;

	return 0;
err_dontfork:
	free(qp->qbuf);
err:
	return -1;
}

struct ibv_qp *
octep_rdma_create_qp(struct ibv_pd *pd, struct ibv_qp_init_attr *attr)
{
	struct octep_rdma_ctx *ctx = to_octep_rdma_ctx(pd->context);
	struct octep_rdma_cmd_create_qp_resp resp = {};
	struct octep_rdma_cmd_create_qp cmd = {};
	struct octep_rdma_qp *qp;
	int rv;

	memset(&cmd, 0, sizeof(cmd));
	memset(&resp, 0, sizeof(resp));
	verbs_debug(verbs_get_ctx(pd->context), "[%s] sq size %d rq size %d\n", __func__,
		    attr->cap.max_send_wr, attr->cap.max_recv_wr);

	qp = calloc(1, sizeof(*qp));
	if (!qp) {
		verbs_err(verbs_get_ctx(pd->context), "calloc failed\n");
		return NULL;
	}

	qp->ibqp.context = pd->context;
	rv = octep_rdma_alloc_qp_buf_and_db(qp, attr, OCTEP_RDMA_PAGE_SIZE);
	if (rv) {
		verbs_err(verbs_get_ctx(pd->context), "QP buf and db allocation failed\n");
		goto err;
	}

	cmd.qbuf_va = (uintptr_t)qp->qbuf;
	cmd.qbuf_len = qp->qbuf_size;
	cmd.num_sqe = qp->sq.depth;
	cmd.num_rqe = qp->rq.depth;
	rv = ibv_cmd_create_qp(pd, &qp->ibqp, attr, &cmd.ibv_cmd, sizeof(cmd), &resp.ibv_resp,
			       sizeof(resp));
	if (rv) {
		verbs_err(verbs_get_ctx(pd->context), "ibv_cmd_create_qp failed\n");
		goto err_cmd;
	}
	if (resp.sq_key == OCTEP_RDMA_INVAL_UOBJ_KEY || resp.rq_key == OCTEP_RDMA_INVAL_UOBJ_KEY) {
		verbs_err(verbs_get_ctx(pd->context), "liboctep_rdma: prepare QP mapping failed\n");
		goto err_cmd;
	}

	qp->type = attr->qp_type;
	qp->id = resp.qp_id;
	qp->sq.qmask = qp->sq.depth - 1;
	qp->rq.qmask = qp->rq.depth - 1;
	qp->sq_sig_all = attr->sq_sig_all;
	qp->sq.size = qp->sq.depth * sizeof(union octep_rdma_sqe);
	qp->rq.size = qp->rq.depth * sizeof(union octep_rdma_rqe);

	qp->sq.qbuf = qp->qbuf;
	qp->rq.qbuf = qp->qbuf + resp.rq_offset;
	verbs_debug(verbs_get_ctx(qp->ibqp.context),
		    "[%s] qp->sq.qbuf %p qp->rq.qbuf %p rq_offset %d\n", __func__, qp->sq.qbuf,
		    qp->rq.qbuf, resp.rq_offset);

	verbs_debug(verbs_get_ctx(pd->context),
		    "[%s] num sqe %d num_rqe %d sq size %d rq size %d\n", __func__, qp->sq.depth,
		    qp->rq.depth, qp->sq.size, qp->rq.size);

	pthread_spin_init(&qp->sq_lock, PTHREAD_PROCESS_PRIVATE);
	pthread_spin_init(&qp->rq_lock, PTHREAD_PROCESS_PRIVATE);

	qp->db_region = ctx->db_region;
	qp->sq.pi_dbl =
		qp->db_region + ((qp->id * OCTEP_RDMA_QS_MULTIPLIER) * ctx->notify_off_multiplier);
	qp->sq.ci_dbl = qp->sq.pi_dbl + 2;

	qp->rq.pi_dbl = qp->db_region +
			(((qp->id * OCTEP_RDMA_QS_MULTIPLIER) + 1) * ctx->notify_off_multiplier);
	qp->rq.ci_dbl = qp->rq.pi_dbl + 2;
	rv = octep_rdma_alloc_wrid_tbl(qp);
	if (rv) {
		verbs_err(verbs_get_ctx(pd->context),
			  "liboctep_rdma: wrid table allocation failed: %d", rv);
		goto err_sendq;
	}

	rv = octep_rdma_store_qp(ctx, qp);
	if (rv) {
		verbs_err(verbs_get_ctx(pd->context), "liboctep_rdma: QP store failed: %d", rv);
		errno = -rv;
		goto err_store;
	}
	return &qp->ibqp;

err_store:
	octep_rdma_free_wrid_tbl(qp);
err_sendq:
	ibv_cmd_destroy_qp(&qp->ibqp);
err_cmd:
	ibv_dofork_range(qp->qbuf, qp->qbuf_size);
	munmap(qp->qbuf, qp->qbuf_size);
err:
	free(qp);

	return NULL;
}

int
octep_rdma_modify_qp(struct ibv_qp *ibqp, struct ibv_qp_attr *attr, int attr_mask)
{
	struct octep_rdma_qp *qp = to_octep_rdma_qp(ibqp);
	struct ibv_modify_qp cmd = {};
	int rv;

	pthread_spin_lock(&qp->sq_lock);
	pthread_spin_lock(&qp->rq_lock);

	rv = ibv_cmd_modify_qp(ibqp, attr, attr_mask, &cmd, sizeof(cmd));

	pthread_spin_unlock(&qp->rq_lock);
	pthread_spin_unlock(&qp->sq_lock);

	return rv;
}

int
octep_rdma_destroy_qp(struct ibv_qp *ibqp)
{
	struct ibv_context *base_ctx = ibqp->pd->context;
	struct octep_rdma_ctx *ctx = to_octep_rdma_ctx(base_ctx);
	struct octep_rdma_qp *qp = to_octep_rdma_qp(ibqp);
	int rv;

	octep_rdma_clear_qp(ctx, qp);

	rv = ibv_cmd_destroy_qp(ibqp);
	if (rv)
		return rv;

	octep_rdma_free_wrid_tbl(qp);
	ibv_dofork_range(qp->qbuf, qp->qbuf_size);
	munmap(qp->qbuf, qp->qbuf_size);

	free(qp);

	return 0;
}

int
octep_rdma_query_qp(struct ibv_qp *qp, struct ibv_qp_attr *attr, int attr_mask,
		    struct ibv_qp_init_attr *init_attr)
{
	struct ibv_query_qp cmd = {};

	return ibv_cmd_query_qp(qp, attr, attr_mask, init_attr, &cmd, sizeof(cmd));
}

struct ibv_ah *
octep_rdma_create_ah(struct ibv_pd *pd, struct ibv_ah_attr *attr)
{
	struct octep_rdma_ah *ah;
	struct octep_rdma_cmd_create_ah_resp resp = {};
	int ret;

	ah = calloc(1, sizeof(*ah));
	if (!ah)
		return NULL;

	ret = ibv_cmd_create_ah(pd, &ah->ibv_ah, attr, &resp.ibv_resp, sizeof(resp));
	if (ret)
		goto err_free;

	ah->ah_num = resp.ah_num;

	verbs_debug(verbs_get_ctx(pd->context), "[%s] ah_num %d ibah %p\n", __func__, ah->ah_num,
		    &ah->ibv_ah);

	return &ah->ibv_ah;

err_free:
	free(ah);
	return NULL;
}

int
octep_rdma_destroy_ah(struct ibv_ah *ibah)
{
	struct octep_rdma_ah *ah = to_octep_rdma_ah(ibah);
	int ret;

	ret = ibv_cmd_destroy_ah(&ah->ibv_ah);
	if (!ret)
		free(ah);

	return ret;
}

static const struct {
	enum ibv_wr_opcode base;
	enum octep_rdma_opcode octep_rdma;
} map_send_opcode[IBV_WR_DRIVER1 + 1] = {{IBV_WR_RDMA_WRITE, OCTEP_RDMA_OP_WRITE},
					 {IBV_WR_RDMA_WRITE_WITH_IMM, OCTEP_RDMA_NUM_OPCODES + 1},
					 {IBV_WR_SEND, OCTEP_RDMA_OP_SEND},
					 {IBV_WR_SEND_WITH_IMM, OCTEP_RDMA_NUM_OPCODES + 1},
					 {IBV_WR_RDMA_READ, OCTEP_RDMA_OP_READ},
					 {IBV_WR_ATOMIC_CMP_AND_SWP, OCTEP_RDMA_NUM_OPCODES + 1},
					 {IBV_WR_ATOMIC_FETCH_AND_ADD, OCTEP_RDMA_NUM_OPCODES + 1},
					 {IBV_WR_LOCAL_INV, OCTEP_RDMA_NUM_OPCODES + 1},
					 {IBV_WR_BIND_MW, OCTEP_RDMA_NUM_OPCODES + 1},
					 {IBV_WR_SEND_WITH_INV, OCTEP_RDMA_OP_SEND_REMOTE_INV},
					 {IBV_WR_TSO, OCTEP_RDMA_NUM_OPCODES + 1},
					 {IBV_WR_DRIVER1, OCTEP_RDMA_NUM_OPCODES + 1}};

static inline uint16_t
map_send_flags(int ibv_flags)
{
	uint16_t flags = OCTEP_RDMA_WQE_VALID;

	if (ibv_flags & IBV_SEND_SIGNALED)
		flags |= OCTEP_RDMA_WQE_SIGNALLED;
	if (ibv_flags & IBV_SEND_SOLICITED)
		flags |= OCTEP_RDMA_WQE_SOLICITED;
	if (ibv_flags & IBV_SEND_INLINE)
		flags |= OCTEP_RDMA_WQE_INLINE;
	if (ibv_flags & IBV_SEND_FENCE)
		flags |= OCTEP_RDMA_WQE_READ_FENCE;

	return flags;
}

static inline int
push_send_wqe(struct ibv_qp *ibqp, struct ibv_send_wr *base_wr, union octep_rdma_sqe *sqe,
	      int sig_all)
{
	uint32_t flags = map_send_flags(base_wr->send_flags);
	atomic_ushort *fp = (atomic_ushort *)&sqe->flags;
	struct octep_rdma_qp *qp = to_octep_rdma_qp(ibqp);
	struct octep_rdma_ah *ah = NULL;
	int i = 0;

	ah = to_octep_rdma_ah(base_wr->wr.ud.ah);
	sqe->wr_id = base_wr->wr_id;
	sqe->num_sges = base_wr->num_sge;

	if (qp->type == IBV_QPT_UD) {
		sqe->ud.ah = ah->ah_num;
		sqe->ud.remote_qpn = base_wr->wr.ud.remote_qpn;
		sqe->ud.qkey = base_wr->wr.ud.remote_qkey;
	}

	if (qp->type == IBV_QPT_RC) {
		sqe->rdma.remote_addr = base_wr->wr.rdma.remote_addr;
		sqe->rdma.rkey = base_wr->wr.rdma.rkey;
	}

	/* Generate completion event for all WQEs */
	if (sig_all)
		flags |= OCTEP_RDMA_WQE_SIGNALLED;

	sqe->opcode = map_send_opcode[base_wr->opcode].octep_rdma;
	sqe->opcode = base_wr->opcode;

	if (sqe->opcode > OCTEP_RDMA_NUM_OPCODES) {
		verbs_err(verbs_get_ctx(ibqp->context), "liboctep_rdma: opcode %d unsupported\n",
			  base_wr->opcode);
		return -EINVAL;
	}
	if (flags & OCTEP_RDMA_WQE_INLINE) {
		char *data = (char *)&sqe->sges0[1];
		int bytes = 0;

		/* Allow more than OCTEP_RDMA_MAX_SGE, since content copied here */
		while (i < base_wr->num_sge) {
			bytes += base_wr->sg_list[i].length;
			if (bytes > (int)OCTEP_RDMA_MAX_INLINE) {
				verbs_err(verbs_get_ctx(ibqp->context),
					  "liboctep_rdma: inline data: %d:%d\n", bytes,
					  (int)OCTEP_RDMA_MAX_INLINE);
				return -EINVAL;
			}
			memcpy(data, (void *)(uintptr_t)base_wr->sg_list[i].addr,
			       base_wr->sg_list[i].length);
			verbs_info(verbs_get_ctx(ibqp->context), "-----sg data -----\n");
			data += base_wr->sg_list[i++].length;
		}
		sqe->sges0[0].length = bytes;

	} else {
		if (sqe->num_sges > OCTEP_RDMA_MAX_SGE_COUNT)
			return -EINVAL;

		for (i = 0; i < base_wr->num_sge; i++) {
			sqe->sges0[i].addr = base_wr->sg_list[i].addr;
			sqe->sges0[i].length = base_wr->sg_list[i].length;
			sqe->sges0[i].key = base_wr->sg_list[i].lkey;
			verbs_info(verbs_get_ctx(ibqp->context), "addr %llx length %d lkey %x\n",
				   sqe->sges0[i].addr, sqe->sges0[i].length, sqe->sges0[i].key);
		}
	}
	atomic_store(fp, flags);
	verbs_info(verbs_get_ctx(ibqp->context), "wr_id %ld num_sges %d ah %d rem_qpn %d qkey %x\n",
		   sqe->wr_id, sqe->num_sges, sqe->ud.ah, sqe->ud.remote_qpn, sqe->ud.qkey);

	return 0;
}

/* send a null post send as a doorbell */
static int
post_send_db(struct ibv_qp *ibqp)
{
	struct ibv_post_send cmd;
	struct ib_uverbs_post_send_resp resp;

	cmd.hdr.command = IB_USER_VERBS_CMD_POST_SEND;
	cmd.hdr.in_words = sizeof(cmd) / 4;
	cmd.hdr.out_words = sizeof(resp) / 4;
	cmd.response = (uintptr_t)&resp;
	cmd.qp_handle = ibqp->handle;
	cmd.wr_count = 0;
	cmd.sge_count = 0;
	cmd.wqe_size = sizeof(struct ibv_send_wr);

	if (write(ibqp->context->cmd_fd, &cmd, sizeof(cmd)) != sizeof(cmd))
		return errno;

	return 0;
}

/* this API does not make a distinction between
 * restartable and non-restartable errors
 */
int
octep_rdma_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr_list, struct ibv_send_wr **bad_wr)
{
	struct octep_rdma_qp *qp = to_octep_rdma_qp(ibqp);
	int new_sqe = 0, rv = 0;
	uint16_t sq_pi, idx;
	atomic_ushort *fp;

	if (!bad_wr)
		return -EINVAL;

	if (!wr_list)
		return -EINVAL;

	*bad_wr = NULL;

	if (ibqp->state == IBV_QPS_ERR) {
		*bad_wr = wr_list;
		return -EIO;
	}

	/*FIXME: add validate wqe from octep_rdma */
	pthread_spin_lock(&qp->sq_lock);

	sq_pi = qp->sq.pi;
	while (wr_list) {
		idx = sq_pi % qp->sq.depth;
		union octep_rdma_sqe *sqe = (union octep_rdma_sqe *)(qp->sq.qbuf) + idx;
		uint16_t sqe_flags;

		fp = (atomic_ushort *)&sqe->flags;
		sqe_flags = atomic_load(fp);

		if (!(sqe_flags & OCTEP_RDMA_WQE_VALID)) {
			memset(sqe, 0, sizeof(*sqe));
			rv = push_send_wqe(ibqp, wr_list, sqe, qp->sq_sig_all);
			if (rv) {
				*bad_wr = wr_list;
				break;
			}
			new_sqe++;
		} else {
			verbs_err(verbs_get_ctx(ibqp->context),
				  "liboctep_rdma: QP[%d]: SQ overflow, idx %d\n", qp->id, idx);
			rv = -ENOMEM;
			*bad_wr = wr_list;
			break;
		}
		qp->sq.wr_tbl[idx] = wr_list->wr_id;
		sq_pi++;
		if (sq_pi == qp->sq.depth)
			sq_pi = 0;
		wr_list = wr_list->next;
	}

	if (new_sqe) {
		rv = post_send_db(ibqp);
		if (rv) {
			*bad_wr = wr_list;
			printf("ERR: post_send_db failed: rv %d\n", rv);
			if (rv == EINVAL)
				printf("ERR: idx %d new_sqe %d\n", idx, new_sqe);
		}
		qp->sq.pi = sq_pi;
	}
	pthread_spin_unlock(&qp->sq_lock);

	return rv;
}

/* send a null post send as a doorbell */
static int
post_recv_db(struct ibv_qp *ibqp)
{
	struct ibv_post_recv cmd;
	struct ib_uverbs_post_recv_resp resp;

	cmd.hdr.command = IB_USER_VERBS_CMD_POST_RECV;
	cmd.hdr.in_words = sizeof(cmd) / 4;
	cmd.hdr.out_words = sizeof(resp) / 4;
	cmd.response = (uintptr_t)&resp;
	cmd.qp_handle = ibqp->handle;
	cmd.wr_count = 0;
	cmd.sge_count = 0;
	cmd.wqe_size = sizeof(struct ibv_recv_wr);

	if (write(ibqp->context->cmd_fd, &cmd, sizeof(cmd)) != sizeof(cmd))
		return errno;

	return 0;
}

static inline int
push_recv_wqe(struct ibv_recv_wr *base_wr, union octep_rdma_rqe *rqe)
{
	atomic_ushort *fp = (atomic_ushort *)&rqe->flags;

	rqe->wr_id = base_wr->wr_id;
	rqe->num_sge = base_wr->num_sge;

	if (base_wr->num_sge == 1) {
		rqe->sges0[0].addr = base_wr->sg_list[0].addr;
		rqe->sges0[0].length = base_wr->sg_list[0].length;
		rqe->sges0[0].key = base_wr->sg_list[0].lkey;
	} else if (base_wr->num_sge && base_wr->num_sge <= OCTEP_RDMA_MAX_SGE_COUNT) {
		/* this assumes same layout of siw and base SGE */
		memcpy(rqe->sges0, base_wr->sg_list, sizeof(struct ibv_sge) * base_wr->num_sge);
	} else {
		return -EINVAL;
	}

	atomic_store(fp, OCTEP_RDMA_WQE_VALID);

	return 0;
}

int
octep_rdma_post_recv(struct ibv_qp *ibqp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad_wr)
{
	struct octep_rdma_qp *qp = to_octep_rdma_qp(ibqp);
	uint32_t rq_pi = 0;
	int rv = 0;

	pthread_spin_lock(&qp->rq_lock);

	rq_pi = qp->rq.pi;

	while (wr) {
		int idx = rq_pi % qp->rq.depth;
		union octep_rdma_rqe *rqe = (union octep_rdma_rqe *)(qp->rq.qbuf) + idx;
		atomic_ushort *fp = (atomic_ushort *)&rqe->flags;
		uint16_t rqe_flags = atomic_load(fp);

		if (!(rqe_flags & OCTEP_RDMA_WQE_VALID)) {
			memset(rqe, 0, sizeof(*rqe));
			if (push_recv_wqe(wr, rqe)) {
				verbs_err(verbs_get_ctx(ibqp->context),
					  "QP[%d]: push_recv_wqe failed\n", qp->id);
				verbs_err(verbs_get_ctx(ibqp->context), "push_recv_wqe failed\n");
				*bad_wr = wr;
				rv = -EINVAL;
				break;
			}
		} else {
			verbs_err(verbs_get_ctx(ibqp->context), "QP[%d]: RQ overflow, idx %d\n",
				  qp->id, idx);
			rv = -ENOMEM;
			*bad_wr = wr;
			break;
		}
		qp->rq.wr_tbl[idx] = wr->wr_id;
		verbs_debug(verbs_get_ctx(ibqp->context),
			    "idx %d wr_id %ld rqe %p addr %llx length %d key %x\n", idx, rqe->wr_id,
			    rqe, rqe->sges0[0].addr, rqe->sges0[0].length, rqe->sges0[0].key);
		rq_pi++;
		if (rq_pi == qp->rq.depth)
			rq_pi = 0;
		wr = wr->next;
	}
	qp->rq.pi = rq_pi;
	rv = post_recv_db(ibqp);

	pthread_spin_unlock(&qp->rq_lock);

	return rv;
}
