/* SPDX-License-Identifier: Marvell-MIT
 * Copyright (c) 2025 Marvell.
 */

#ifndef __OCTEP_RDMA_H__
#define __OCTEP_RDMA_H__

#include <infiniband/driver.h>
#include <infiniband/kern-abi.h>
#include <sys/param.h>

#include "octep_rdma-abi.h"
#include "octep_rdma_verbs.h"

#define OCTEP_RDMA_ABI_VERSION 1
#define OCTEP_RDMA_PAGE_SIZE   4096

#define OCTEP_RDMA_QP_TABLE_SIZE  4096
#define OCTEP_RDMA_QP_TABLE_SHIFT 12
#define OCTEP_RDMA_QP_TABLE_MASK  0xFFF
#define OCTEP_RDMA_QS_MULTIPLIER  3

#define OCTEP_RDMA_MAX_WQE_PER_SQE 64
#define OCTEP_MIN(a, b)            ((a) < (b) ? (a) : (b))
#define octep_always_inline        __always_inline

struct octep_rdma_dev {
	struct verbs_device ibv_dev;
};

struct octep_rdma_ctx {
	struct verbs_context ibv_ctx;
	uint32_t dev_id;
	struct {
		struct octep_rdma_qp **table;
		int refcnt;
	} qp_table[OCTEP_RDMA_QP_TABLE_SIZE];
	pthread_mutex_t qp_table_mutex;
	uint32_t page_size;
	uint32_t notify_off_multiplier;
	uint64_t *db_region;
	uint64_t db_region_sz;
};

static inline struct octep_rdma_ctx *
to_octep_rdma_ctx(struct ibv_context *ibv_ctx)
{
	return container_of(ibv_ctx, struct octep_rdma_ctx, ibv_ctx.context);
}

static inline struct octep_rdma_dev *
to_octep_rdma_dev(struct ibv_device *ibv_dev)
{
	return container_of(ibv_dev, struct octep_rdma_dev, ibv_dev.device);
}

static inline struct octep_rdma_cq *
to_octep_rdma_cq(struct ibv_cq *ibcq)
{
	return container_of(ibcq, struct octep_rdma_cq, ibcq);
}

static inline struct octep_rdma_qp *
to_octep_rdma_qp(struct ibv_qp *ibqp)
{
	return ibqp ? container_of(ibqp, struct octep_rdma_qp, ibqp) : NULL;
}

static inline struct octep_rdma_ah *
to_octep_rdma_ah(struct ibv_ah *ibv_ah)
{
	return ibv_ah ? container_of(ibv_ah, struct octep_rdma_ah, ibv_ah) : NULL;
}

#endif /* __OCTEP_RDMA_H__ */
