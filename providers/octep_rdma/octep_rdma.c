/* SPDX-License-Identifier: Marvell-MIT
 * Copyright (c) 2025 Marvell.
 */

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>

#include <util/util.h>

#include "octep_rdma.h"
#include "octep_rdma_hw.h"

void
octep_rdma_free_context(struct ibv_context *ibv_ctx)
{
	struct octep_rdma_ctx *ctx = to_octep_rdma_ctx(ibv_ctx);

	munmap(ctx->db_region, ctx->db_region_sz);
	verbs_uninit_context(&ctx->ibv_ctx);
	free(ctx);
}

static const struct verbs_context_ops octep_rdma_ctx_ops = {
	.free_context = octep_rdma_free_context,
	.query_device_ex = octep_rdma_query_device,
	.query_port = octep_rdma_query_port,

	.alloc_pd = octep_rdma_alloc_pd,
	.dealloc_pd = octep_rdma_dealloc_pd,

	.reg_mr = octep_rdma_reg_mr,
	.dereg_mr = octep_rdma_dereg_mr,

	.create_cq = octep_rdma_create_cq,
	.poll_cq = octep_rdma_poll_cq,
	.destroy_cq = octep_rdma_destroy_cq,

	.create_qp = octep_rdma_create_qp,
	.query_qp = octep_rdma_query_qp,
	.modify_qp = octep_rdma_modify_qp,
	.destroy_qp = octep_rdma_destroy_qp,

	.create_ah = octep_rdma_create_ah,
	.destroy_ah = octep_rdma_destroy_ah,

	.post_send = octep_rdma_post_send,
	.post_recv = octep_rdma_post_recv,
};

static struct verbs_context *
octep_rdma_alloc_context(struct ibv_device *ibv_dev, int cmd_fd, void *private_data)
{
	struct octep_rdma_cmd_alloc_context_resp resp = {};
	struct octep_rdma_ctx *ctx;
	struct ibv_get_context cmd;
	char *env;

	ctx = verbs_init_and_alloc_context(ibv_dev, cmd_fd, ctx, ibv_ctx, RDMA_DRIVER_OCTEP);
	if (!ctx)
		return NULL;

	if (ibv_cmd_get_context(&ctx->ibv_ctx, &cmd, sizeof(struct ibv_get_context), &resp.ibv_resp,
				sizeof(resp))) {
		printf("Failed to get context\n");
		goto fail;
	}

	ctx->db_region_sz = align(resp.db_region_sz, OCTEP_RDMA_PAGE_SIZE);
	ctx->db_region = mmap(NULL, ctx->db_region_sz, PROT_READ | PROT_WRITE, MAP_SHARED, cmd_fd,
			      resp.db_region);
	if (ctx->db_region == MAP_FAILED) {
		printf("Failed to mmap db region\n");
		goto fail;
	}

	/* Clear the doorbell region */
	memset(ctx->db_region, 0, ctx->db_region_sz);
	ctx->dev_id = resp.dev_id;
	ctx->page_size = OCTEP_RDMA_PAGE_SIZE;

	verbs_set_ops(&ctx->ibv_ctx, &octep_rdma_ctx_ops);

	/* Default to transport verbs */
	env = getenv("OCTEON_TRANSPORT_VERBS");
	if (!env || !strcmp(env, "0")) {
		verbs_set_ops(&ctx->ibv_ctx, &octep_rdma_pts_ctx_ops);
		verbs_info(&ctx->ibv_ctx, "Enabled Transport FP verbs\n");
	}
	return &ctx->ibv_ctx;
fail:
	verbs_uninit_context(&ctx->ibv_ctx);
	free(ctx);
	return NULL;
}

static struct verbs_device *
octep_rdma_alloc_device(struct verbs_sysfs_dev *sysfs_dev)
{
	struct octep_rdma_dev *rdma_dev;

	rdma_dev = calloc(1, sizeof(struct octep_rdma_dev));
	if (!rdma_dev)
		return NULL;

	return &rdma_dev->ibv_dev;
}

static void
octep_rdma_uninit_device(struct verbs_device *verbs_dev)
{
	struct octep_rdma_dev *rdma_dev = to_octep_rdma_dev(&verbs_dev->device);

	free(rdma_dev);
}

static const struct verbs_match_ent octep_rdma_match_table[] = {
	VERBS_DRIVER_ID(RDMA_DRIVER_OCTEP),
	VERBS_PCI_MATCH(PCI_VENDOR_ID_CAVIUM, OCTEP_RDMA_DEVID_CN10KA_PF, NULL),
	VERBS_PCI_MATCH(PCI_VENDOR_ID_CAVIUM, OCTEP_RDMA_DEVID_CN10KA_VF, NULL),
	VERBS_PCI_MATCH(PCI_VENDOR_ID_CAVIUM, OCTEP_RDMA_DEVID_CNF10KA_PF, NULL),
	VERBS_PCI_MATCH(PCI_VENDOR_ID_CAVIUM, OCTEP_RDMA_DEVID_CNF10KA_PF, NULL),
	VERBS_PCI_MATCH(PCI_VENDOR_ID_CAVIUM, OCTEP_RDMA_DEVID_CN103K_PF, NULL),
	{},
};

static const struct verbs_device_ops octep_rdma_dev_ops = {
	.name = "octep_rdma",
	.match_min_abi_version = 0,
	.match_max_abi_version = OCTEP_RDMA_ABI_VERSION,
	.match_table = octep_rdma_match_table,
	.alloc_device = octep_rdma_alloc_device,
	.uninit_device = octep_rdma_uninit_device,
	.alloc_context = octep_rdma_alloc_context,
};

PROVIDER_DRIVER(octep, octep_rdma_dev_ops);
