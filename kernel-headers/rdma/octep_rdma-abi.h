/* SPDX-License-Identifier: Marvell-MIT
 * Copyright (c) 2024 Marvell.
 */

#ifndef __OCTEP_RDMA_KERN_ABI_H__
#define __OCTEP_RDMA_KERN_ABI_H__

#include <linux/in.h>
#include <linux/in6.h>
#include <linux/socket.h>
#include <linux/types.h>

#define OCTEP_RDMA_ABI_VERSION 1

#define OCTEP_RDMA_UOBJ_MAX_KEY   0x08FFFF
#define OCTEP_RDMA_INVAL_UOBJ_KEY (OCTEP_RDMA_UOBJ_MAX_KEY + 1)
#define OCTEP_RDMA_MAX_SGE_COUNT  6
#define OCTEP_RDMA_MAX_INLINE     (sizeof(struct octep_rdma_sge) * (OCTEP_RDMA_MAX_SGE_COUNT))
enum octep_rdma_wqe_flags {
	OCTEP_RDMA_WQE_VALID = 1,
	OCTEP_RDMA_WQE_INLINE = (1 << 1),
	OCTEP_RDMA_WQE_SIGNALLED = (1 << 2),
	OCTEP_RDMA_WQE_SOLICITED = (1 << 3),
	OCTEP_RDMA_WQE_READ_FENCE = (1 << 4),
	OCTEP_RDMA_WQE_REM_INVAL = (1 << 5),
	OCTEP_RDMA_WQE_COMPLETED = (1 << 6)
};

enum octep_rdma_opcode {
	OCTEP_RDMA_OP_WRITE,
	OCTEP_RDMA_OP_READ,
	OCTEP_RDMA_OP_READ_LOCAL_INV,
	OCTEP_RDMA_OP_SEND,
	OCTEP_RDMA_OP_SEND_WITH_IMM,
	OCTEP_RDMA_OP_SEND_REMOTE_INV,

	/* Unsupported */
	OCTEP_RDMA_OP_FETCH_AND_ADD,
	OCTEP_RDMA_OP_COMP_AND_SWAP,

	OCTEP_RDMA_OP_RECEIVE,
	/* provider internal SQE */
	OCTEP_RDMA_OP_READ_RESPONSE,
	/*
	 * below opcodes valid for
	 * in-kernel clients only
	 */
	OCTEP_RDMA_OP_INVAL_STAG,
	OCTEP_RDMA_OP_REG_MR,
	/* FIXME revist */
	OCTEP_RDMA_OP_RECV_IMM,
	OCTEP_RDMA_OP_RECV_INV,

	OCTEP_RDMA_OP_REQ_ERR,
	OCTEP_RDMA_OP_WRITE_WITH_IMM,

	OCTEP_RDMA_OP_RECV_ERR,

	OCTEP_RDMA_OP_INVALIDATE,
	OCTEP_RDMA_OP_RSP_SEND_IMM,
	OCTEP_RDMA_OP_SEND_WITH_INV,

	OCTEP_RDMA_OP_LOCAL_INV,
	OCTEP_RDMA_OP_READ_WITH_INV,
	OCTEP_RDMA_OP_ATOMIC_CAS,
	OCTEP_RDMA_OP_ATOMIC_FAD,
	OCTEP_RDMA_NUM_OPCODES,
	OCTEP_RDMA_OP_INVALID = OCTEP_RDMA_NUM_OPCODES + 1
};

struct octep_rdma_uresp_alloc_ctx {
	__u32 dev_id;
	__u32 pad;
	__aligned_u64 db_region;
	__aligned_u64 db_region_sz;
};

struct octep_rdma_uresp_alloc_pd {
	__u32 pdn;
};

struct octep_rdma_ureq_create_cq {
	__aligned_u64 db_record_va;
	__aligned_u64 qbuf_va;
	__u32 qbuf_len;
	__u32 rsvd0;
};

struct octep_rdma_uresp_create_cq {
	__u32 cq_id;
	__u32 num_cqe;
};

struct octep_rdma_ureq_create_qp {
	__aligned_u64 db_record_va;
	__aligned_u64 qbuf_va;
	__u32 qbuf_len;
	__u32 rsvd0;
};

struct octep_rdma_uresp_create_qp {
	__u32 qp_id;
	__u32 num_sqe;
	__u32 num_rqe;
	__u32 rq_offset;
	__aligned_u64 sq_key;
	__aligned_u64 rq_key;
};

struct octep_rdma_ureq_reg_mr {
	__u8 stag_key;
	__u8 reserved[3];
	__u32 pad;
};

struct octep_rdma_uresp_reg_mr {
	__u32 stag;
	__u32 pad;
};

struct octep_rdma_uresp_create_ah {
	__u32 ah_num;
	__u32 reserved;
};

struct octep_rdma_create_ah_resp {
	__u32 ah_num;
	__u32 reserved;
};

enum {
	OCTEP_RDMA_NETWORK_TYPE_IPV4 = 1,
	OCTEP_RDMA_NETWORK_TYPE_IPV6 = 2,
};

union octep_rdma_gid {
	__u8 raw[16];
	struct {
		__be64 subnet_prefix;
		__be64 interface_id;
	} global;
};

struct octep_rdma_global_route {
	union octep_rdma_gid dgid;
	__u32 flow_label;
	__u8 sgid_index;
	__u8 hop_limit;
	__u8 traffic_class;
};

struct octep_rdma_av {
	__u8 port_num;
	/* From RXE_NETWORK_TYPE_* */
	__u8 network_type;
	__u8 dmac[6];
	struct octep_rdma_global_route grh;
	union {
		struct sockaddr_in _sockaddr_in;
		struct sockaddr_in6 _sockaddr_in6;
	} sgid_addr, dgid_addr;
};

struct octep_rdma_sge {
	__le32 length;
	__le32 key;
	__aligned_le64 addr;
};

struct octep_rdma_sqe {
	struct {
		/* WORD 0 */
		uint8_t opcode;
		uint8_t send_flags;
		uint8_t flags;
		uint8_t num_sges;
		uint32_t imm_data;
		/* WORD 1 */
		uint64_t wr_id;
		/* WORD 2-3 */
		union {
			struct {
				uint64_t remote_addr;
				uint32_t rkey;
				uint32_t reserved;
			} rdma;
			struct {
				uint32_t ah;
				uint32_t remote_qpn;
				uint32_t qkey;
			} ud;
		};
		/* WORD 4-7 */
		struct octep_rdma_sge sges0[2];
	};
	/* WORD 0-7 */
	struct octep_rdma_sge sges1[4];
};

struct octep_rdma_rqe {
	struct {
		/* WORD 0 */
		uint32_t flags;
		uint8_t opcode;
		uint8_t reserved;
		uint16_t num_sge;
		/* WORD 1 */
		uint64_t wr_id;
		/* WORD 2-3 */
		struct octep_rdma_sge sges0[1];
	};
	/* WORD 0-3 */
	struct octep_rdma_sge sges1[2];
};

/* Structure for RDMA CQE DESCRIPTOR */
struct octep_rdma_cqe {
	/* WORD 0 */
	uint8_t opcode;
	uint8_t status;
	uint16_t vendor_err;
	uint32_t byte_len;
	/* WORD 1 */
	uint64_t wr_id;
	/* WORD 2 */
	uint32_t reserved1;
	uint32_t imm_data;
	/* WORD 3 */
	uint32_t qp_id;
	uint32_t reserved2;
	/* WORD 4-7 */
	uint64_t reserved3[4];
};

#endif /* __OCTEP_RDMA_KERN_ABI_H__ */
