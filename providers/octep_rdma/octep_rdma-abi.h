/* SPDX-License-Identifier: Marvell-MIT
 * Copyright (c) 2025 Marvell.
 */

#ifndef __OCTEP_RDMA_ABI_H__
#define __OCTEP_RDMA_ABI_H__

#include <infiniband/kern-abi.h>
#include <kernel-abi/octep_rdma-abi.h>
#include <rdma/octep_rdma-abi.h>

DECLARE_DRV_CMD(octep_rdma_cmd_alloc_context, IB_USER_VERBS_CMD_GET_CONTEXT, empty,
		octep_rdma_uresp_alloc_ctx);
DECLARE_DRV_CMD(octep_rdma_cmd_create_pd, IB_USER_VERBS_CMD_ALLOC_PD, empty,
		octep_rdma_uresp_alloc_pd);
DECLARE_DRV_CMD(octep_rdma_cmd_create_cq, IB_USER_VERBS_CMD_CREATE_CQ, octep_rdma_ureq_create_cq,
		octep_rdma_uresp_create_cq);
DECLARE_DRV_CMD(octep_rdma_cmd_create_qp, IB_USER_VERBS_CMD_CREATE_QP, octep_rdma_ureq_create_qp,
		octep_rdma_uresp_create_qp);
DECLARE_DRV_CMD(octep_rdma_cmd_create_ah, IB_USER_VERBS_CMD_CREATE_AH, empty,
		octep_rdma_uresp_create_ah);
DECLARE_DRV_CMD(octep_rdma_cmd_reg_mr, IB_USER_VERBS_CMD_REG_MR, octep_rdma_ureq_reg_mr,
		octep_rdma_uresp_reg_mr);

#endif /* __OCTEP_RDMA_ABI_H__ */
