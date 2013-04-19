package org.vanilladb.core.storage.buffer;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.file.*;


/**
 * Manages the pinning and unpinning of buffers to blocks.
 */
class BasicBufferMgr {
	private Buffer[] bufferPool;
	private Map<BlockId, Buffer> blockMap;
	private int numAvailable, lastReplacedBuff;

	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link org.vanilladb.core.storage.log.LogMgr LogMgr} objects that
	 * it gets from the class {@link VanillaDB}. Those objects are created
	 * during system initialization. Thus this constructor cannot be called
	 * until {@link VanillaDB#initFileAndLogMgr(String)} or is called first.
	 * 
	 * @param numBuffs
	 *            the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numBuffs) {
		bufferPool = new Buffer[numBuffs];
		blockMap = new HashMap<BlockId, Buffer>();
		numAvailable = numBuffs;
		lastReplacedBuff = 0;
		for (int i = 0; i < numBuffs; i++)
			bufferPool[i] = new Buffer();
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txNum
	 *            the transaction's id number
	 */
	synchronized void flushAll(long txNum) {
		for (Buffer buff : bufferPool)
			if (buff.isModifiedBy(txNum))
				buff.flush();
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a block ID
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(BlockId blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			BlockId oldBlk = buff.block();
			if (oldBlk != null)
				blockMap.remove(oldBlk);
			buff.assignToBlock(blk);
			blockMap.put(blk, buff);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String fileName, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		BlockId oldBlk = buff.block();
		if (oldBlk != null)
			blockMap.remove(oldBlk);

		buff.assignToNew(fileName, fmtr);
		numAvailable--;
		buff.pin();
		blockMap.put(buff.block(), buff);
		return buff;
	}

	/**
	 * Unpins the specified buffers.
	 * 
	 * @param buffs
	 *            the buffers to be unpinned
	 */
	synchronized void unpin(Buffer... buffs) {
		for (Buffer buff : buffs) {
			buff.unpin();
			if (!buff.isPinned())
				numAvailable++;
		}
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	synchronized int available() {
		return numAvailable;
	}

	private Buffer findExistingBuffer(BlockId blk) {
		Buffer buff = blockMap.get(blk);
		if (buff != null && buff.block().equals(blk))
			return buff;
		return null;
	}

	private Buffer chooseUnpinnedBuffer() {
		int currBlk = (lastReplacedBuff + 1) % bufferPool.length;
		while (currBlk != lastReplacedBuff) {
			Buffer buff = bufferPool[currBlk];
			if (!buff.isPinned()) {
				lastReplacedBuff = currBlk;
				return buff;
			}
			currBlk = (currBlk + 1) % bufferPool.length;
		}
		return null;
	}
}
