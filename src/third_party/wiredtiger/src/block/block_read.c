/*-
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
#if defined (UNIV_PMEMOBJ_BUF)
#include <assert.h>
extern PMEM_WRAPPER* gb_pmw; //defined in conn_api.c
extern bool is_skip_first_open;
#endif
/*
 * __wt_bm_preload --
 *	Pre-load a page.
 */
int
__wt_bm_preload(
    WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	WT_BLOCK *block;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_FILE_HANDLE *handle;
	wt_off_t offset;
	uint32_t checksum, size;
	bool mapped;

	block = bm->block;

	WT_STAT_CONN_INCR(session, block_preload);

	/* Crack the cookie. */
	WT_RET(
	    __wt_block_buffer_to_addr(block, addr, &offset, &size, &checksum));

	handle = block->fh->handle;
	mapped = bm->map != NULL && offset + size <= (wt_off_t)bm->maplen;
	if (mapped && handle->fh_map_preload != NULL)
		ret = handle->fh_map_preload(handle, (WT_SESSION *)session,
		    (uint8_t *)bm->map + offset, size, bm->mapped_cookie);
	if (!mapped && handle->fh_advise != NULL)
		ret = handle->fh_advise(handle, (WT_SESSION *)session,
		    offset, (wt_off_t)size, WT_FILE_HANDLE_WILLNEED);
	if (ret != EBUSY && ret != ENOTSUP)
		return (ret);

	/* If preload isn't supported, do it the slow way. */
	WT_RET(__wt_scr_alloc(session, 0, &tmp));
	ret = __wt_bm_read(bm, session, tmp, addr, addr_size);
	__wt_scr_free(session, &tmp);

	return (ret);
}

/*
 * __wt_bm_read --
 *	Map or read address cookie referenced block into a buffer.
 */
int
__wt_bm_read(WT_BM *bm, WT_SESSION_IMPL *session,
    WT_ITEM *buf, const uint8_t *addr, size_t addr_size)
{
	WT_BLOCK *block;
	WT_DECL_RET;
	WT_FILE_HANDLE *handle;
	wt_off_t offset;
	uint32_t checksum, size;
	bool mapped;

	WT_UNUSED(addr_size);
	block = bm->block;

	/* Crack the cookie. */
	WT_RET(
	    __wt_block_buffer_to_addr(block, addr, &offset, &size, &checksum));

	/*
	 * Map the block if it's possible.
	 */
	handle = block->fh->handle;
	mapped = bm->map != NULL && offset + size <= (wt_off_t)bm->maplen;
	if (mapped && handle->fh_map_preload != NULL) {
		buf->data = (uint8_t *)bm->map + offset;
		buf->size = size;
		ret = handle->fh_map_preload(handle, (WT_SESSION *)session,
		    buf->data, buf->size,bm->mapped_cookie);

		WT_STAT_CONN_INCR(session, block_map_read);
		WT_STAT_CONN_INCRV(session, block_byte_map_read, size);
		return (ret);
	}

#ifdef HAVE_DIAGNOSTIC
	/*
	 * In diagnostic mode, verify the block we're about to read isn't on
	 * the available list, or for live systems, the discard list.
	 */
	WT_RET(__wt_block_misplaced(
	    session, block, "read", offset, size, bm->is_live));
#endif
	/* Read the block. */
	WT_RET(
	    __wt_block_read_off(session, block, buf, offset, size, checksum));

	/* Optionally discard blocks from the system's buffer cache. */
	WT_RET(__wt_block_discard(session, block, (size_t)size));

	return (0);
}

/*
 * __wt_bm_corrupt_dump --
 *	Dump a block into the log in 1KB chunks.
 */
static int
__wt_bm_corrupt_dump(WT_SESSION_IMPL *session,
    WT_ITEM *buf, wt_off_t offset, uint32_t size, uint32_t checksum)
    WT_GCC_FUNC_ATTRIBUTE((cold))
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	size_t chunk, i, nchunks;

#define	WT_CORRUPT_FMT	"{%" PRIuMAX ", %" PRIu32 ", %" PRIu32 "}"
	if (buf->size == 0) {
		__wt_errx(session,
		    WT_CORRUPT_FMT ": empty buffer, no dump available",
		    (uintmax_t)offset, size, checksum);
		return (0);
	}

	WT_RET(__wt_scr_alloc(session, 4 * 1024, &tmp));

	nchunks = buf->size / 1024 + (buf->size % 1024 == 0 ? 0 : 1);
	for (chunk = i = 0;;) {
		WT_ERR(__wt_buf_catfmt(
		    session, tmp, "%02x ", ((uint8_t *)buf->data)[i]));
		if (++i == buf->size || i % 1024 == 0) {
			__wt_errx(session,
			    WT_CORRUPT_FMT
			    ": (chunk %" WT_SIZET_FMT " of %" WT_SIZET_FMT
			    "): %.*s",
			    (uintmax_t)offset, size, checksum,
			    ++chunk, nchunks,
			    (int)tmp->size, (char *)tmp->data);
			if (i == buf->size)
				break;
			WT_ERR(__wt_buf_set(session, tmp, "", 0));
		}
	}

err:	__wt_scr_free(session, &tmp);
	return (ret);
}

/*
 * __wt_bm_corrupt --
 *	Report a block has been corrupted, external API.
 */
int
__wt_bm_corrupt(WT_BM *bm,
    WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	wt_off_t offset;
	uint32_t checksum, size;

	/* Read the block. */
	WT_RET(__wt_scr_alloc(session, 0, &tmp));
	WT_ERR(__wt_bm_read(bm, session, tmp, addr, addr_size));

	/* Crack the cookie, dump the block. */
	WT_ERR(__wt_block_buffer_to_addr(
	    bm->block, addr, &offset, &size, &checksum));
	WT_ERR(__wt_bm_corrupt_dump(session, tmp, offset, size, checksum));

err:	__wt_scr_free(session, &tmp);
	return (ret);
}

#ifdef HAVE_DIAGNOSTIC
/*
 * __wt_block_read_off_blind --
 *	Read the block at an offset, try to figure out what it looks like,
 * debugging only.
 */
int
__wt_block_read_off_blind(
    WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf, wt_off_t offset)
{
	WT_BLOCK_HEADER *blk;
	uint32_t checksum, size;

#if defined (UNIV_PMEMOBJ_BUF)
	WT_CONNECTION_IMPL *conn;
	conn = S2C(session);
#endif //if defined (UNIV_PMEMOBJ_BUF)

	/*
	 * Make sure the buffer is large enough for the header and read the
	 * the first allocation-size block.
	 */
	WT_RET(__wt_buf_init(session, buf, block->allocsize));
#if defined (UNIV_PMEMOBJ_BUF)
	if (is_skip_first_open){
			WT_RET(__wt_read(session, block->fh, offset, (size_t)block->allocsize, buf->mem));
	}
	else {
		const PMEM_BUF_BLOCK* pblock =
			pm_buf_read(gb_pmw, session, block->fh->name, block->fh->name_hash, offset, block->allocsize, buf->mem);
		//pm_buf_read(gb_pmw, block->fh->name, block->fh->name_hash, offset, block->allocsize, buf->mem);
		//pm_buf_read(conn->pmw, block->fh->name, block->fh->name_hash, offset, block->allocsize, buf->mem);
		if (pblock == NULL){
			WT_RET(__wt_read(session, block->fh, offset, (size_t)block->allocsize, buf->mem));
		}
	}
#else
	WT_RET(__wt_read(
	    session, block->fh, offset, (size_t)block->allocsize, buf->mem));
#endif
	blk = WT_BLOCK_HEADER_REF(buf->mem);
	__wt_block_header_byteswap(blk);

	/*
	 * Copy out the size and checksum (we're about to re-use the buffer),
	 * and if the size isn't insane, read the rest of the block.
	 */
	size = blk->disk_size;
	checksum = blk->checksum;
	if (__wt_block_offset_invalid(block, offset, size))
		WT_RET_MSG(session, EINVAL,
		    "block at offset %" PRIuMAX " cannot be a valid block, no "
		    "read attempted",
		    (uintmax_t)offset);
	return (
	    __wt_block_read_off(session, block, buf, offset, size, checksum));
}
#endif

/*
 * __wt_block_read_off --
 *	Read an addr/size pair referenced block into a buffer.
 */
int
__wt_block_read_off(WT_SESSION_IMPL *session, WT_BLOCK *block,
    WT_ITEM *buf, wt_off_t offset, uint32_t size, uint32_t checksum)
{
	WT_BLOCK_HEADER *blk, swap;
	size_t bufsize;
	uint32_t page_checksum;
#if defined (UNIV_PMEMOBJ_BUF)
	WT_CONNECTION_IMPL *conn;
	conn = S2C(session);
#endif //if defined (UNIV_PMEMOBJ_BUF)
	__wt_verbose(session, WT_VERB_READ,
	    "off %" PRIuMAX ", size %" PRIu32 ", checksum %" PRIu32,
	    (uintmax_t)offset, size, checksum);

	WT_STAT_CONN_INCR(session, block_read);
	WT_STAT_CONN_INCRV(session, block_byte_read, size);

	/*
	 * Grow the buffer as necessary and read the block.  Buffers should be
	 * aligned for reading, but there are lots of buffers (for example, file
	 * cursors have two buffers each, key and value), and it's difficult to
	 * be sure we've found all of them.  If the buffer isn't aligned, it's
	 * an easy fix: set the flag and guarantee we reallocate it.  (Most of
	 * the time on reads, the buffer memory has not yet been allocated, so
	 * we're not adding any additional processing time.)
	 */
	if (F_ISSET(buf, WT_ITEM_ALIGNED))
		bufsize = size;
	else {
		F_SET(buf, WT_ITEM_ALIGNED);
		bufsize = WT_MAX(size, buf->memsize + 10);
	}
	WT_RET(__wt_buf_init(session, buf, bufsize));
#if defined(UNIV_PMEMOBJ_BUF)
	const PMEM_BUF_BLOCK* pblock;
	if (is_skip_first_open){
		//Read as the original
		WT_RET(__wt_read(session, block->fh, offset, size, buf->mem));
	}
	else {
		//skip read metadata file
		pblock =
			pm_buf_read(gb_pmw, session, block->fh->name, block->fh->name_hash, offset, size, buf->mem);
		//#if defined(CHECKSUM_DEBUG)
		//	if (strstr(block->fh->name, "ycsb") != NULL && pblock!=NULL )
		//		printf("pm_buf_read file %s offset %zu size %zu result %d\n", block->fh->name, offset, (size_t)size, (pblock!=NULL));
		//#endif
		if (pblock == NULL){
			WT_RET(__wt_read(session, block->fh, offset, size, buf->mem));
		}
	}
#else
	WT_RET(__wt_read(session, block->fh, offset, size, buf->mem));
#endif //defined (UNIV_PMEMOBJ_BUF)
	buf->size = size;

	/*
	 * We incrementally read through the structure before doing a checksum,
	 * do little- to big-endian handling early on, and then select from the
	 * original or swapped structure as needed.
	 */
	blk = WT_BLOCK_HEADER_REF(buf->mem);
	__wt_block_header_byteswap_copy(blk, &swap);

#if defined(UNIV_PMEMOBJ_BUF)
#if defined(CHECKSUM_DEBUG)
		//Check the first read from disk checksum number
	ulint hashed;
	uint32_t checksum_tem;
	uint32_t page_checksum_tem;
	checksum_tem = blk->checksum;
	blk->checksum = 0;
	page_checksum_tem = __wt_checksum(buf->mem, size);
	blk->checksum = checksum_tem;

	 //PMEM_HASH_KEY(hashed, offset, block->fh->name_hash, conn->pmw->PMEM_N_BUCKETS);
	 PMEM_WRAPPER* pmw = conn->pmw;
	 PMEM_HASH_KEY(hashed, offset, block->fh->name_hash, pmw->PMEM_BUF_MAX_RANGE, pmw->PMEM_N_BUCKETS);
	 //hashed = ((offset  +  block->fh->name_hash ) / 4096) % pmw->PMEM_N_BUCKETS;
	if (strstr(block->fh->name, "ycsb") != 0)
		printf("DAT pm_buf_read file %s offset %zu checksum %u blk->checksum %u swap.checksum %u page_checksume %u size %zu hashed %zu is read from pmem %d max_offset %zu pblock->checksum %u\n", block->fh->name, offset, checksum, blk->checksum, swap.checksum, page_checksum_tem, (size_t)size, hashed, (pblock != NULL), pmw->pbuf->max_offset, (pblock != NULL ? pblock->checksum : 0));
#endif
#endif
	if (swap.checksum == checksum) {
		blk->checksum = 0;
		page_checksum = __wt_checksum(buf->mem,
		    F_ISSET(&swap, WT_BLOCK_DATA_CKSUM) ?
		    size : WT_BLOCK_COMPRESS_SKIP);
		if (page_checksum == checksum) {
			/*
			 * Swap the page-header as needed; this doesn't belong
			 * here, but it's the best place to catch all callers.
			 */
			__wt_page_header_byteswap(buf->mem);

			return (0);
		}

#if defined(UNIV_PMEMOBJ_BUF)
	printf("CHECKSUM ERROR #1: read size %"PRIu32" offset %zu checksum %"PRIu32" page_checksum %"PRIu32" is read from pmem %d \n",
			size, offset, checksum, page_checksum, (pblock != NULL));
		//because WT update blk->checksum when write a block to disk, it get the different checksum value every next __wt_checksum()
		//So, it's ok if swap.checksum == checksum (we read what we written)
		//__wt_page_header_byteswap(buf->mem);
		//return (0);
	assert(0);
#endif
		if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
			__wt_errx(session,
			    "read checksum error for %" PRIu32 "B block at "
			    "offset %" PRIuMAX ": calculated block checksum "
			    "of %" PRIu32 " doesn't match expected checksum "
			    "of %" PRIu32,
			    size, (uintmax_t)offset, page_checksum, checksum);
	} else
		if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
			__wt_errx(session,
			    "read checksum error for %" PRIu32 "B block at "
			    "offset %" PRIuMAX ": block header checksum "
			    "of %" PRIu32 " doesn't match expected checksum "
			    "of %" PRIu32,
			    size, (uintmax_t)offset, swap.checksum, checksum);

	if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
		WT_IGNORE_RET(
		    __wt_bm_corrupt_dump(session, buf, offset, size, checksum));
#if defined(UNIV_PMEMOBJ_BUF)
	printf("CHECKSUM ERROR #2: read size %"PRIu32" offset %zu checksum %"PRIu32" swap.checksum %"PRIu32" is read from pmem %d \n",
			size, offset, checksum, swap.checksum, (pblock != NULL));
	assert(0);
#endif
	/* Panic if a checksum fails during an ordinary read. */
	return (block->verify ||
	    F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE) ?
	    WT_ERROR : __wt_illegal_value(session, block->name));
}
