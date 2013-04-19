package org.vanilladb.core.storage.buffer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.file.*;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionLifecycleListener;
import org.vanilladb.core.storage.tx.TransactionStartListener;


/**
 * The publicly-accessible buffer manager. A buffer manager wraps a
 * {@link BasicBufferMgr} instance, and provides the same methods. The
 * difference is that the methods {@link #pin(BlockId)} and
 * {@link #pinNew(String, PageFormatter)} will never return false and null
 * respectively. If no buffers are currently available, then the calling thread
 * will be placed on a waiting list. The waiting threads are removed from the
 * list when a buffer becomes available. If a thread has been waiting for a
 * buffer for an excessive amount of time (currently, 10 seconds) then repins
 * all currently holding blocks by the calling transaction. Buffer manager
 * implements {@link TransactionStartListener} and
 * {@link TransactionLifecycleListener} for the purpose of unpinning buffers
 * when transaction commit/rollback/recovery.
 * 
 * <p>
 * A block must be pinned first before its getters/setters can be called.
 * </p>
 * 
 */
public class BufferMgr implements TransactionStartListener,
		TransactionLifecycleListener {
	private static Logger logger = Logger.getLogger(BufferMgr.class.getName());
	protected static final int BUFFER_SIZE;
	private static final long MAX_TIME;
	private static final long EPSILON;

	private BasicBufferMgr bufferMgr;
	private Map<Long, List<Buffer>> pinnedByMap;
	private List<Thread> waitingThreads;

	static {
		String prop = System.getProperty(BufferMgr.class.getName()
				+ ".MAX_TIME");
		MAX_TIME = (prop == null ? 10000 : Long.parseLong(prop.trim()));
		prop = System.getProperty(BufferMgr.class.getName() + ".EPSILON");
		EPSILON = (prop == null ? 50 : Long.parseLong(prop.trim()));
		prop = System.getProperty(BufferMgr.class.getName() + ".BUFFER_SIZE");
		BUFFER_SIZE = (prop == null ? 1024 : Integer.parseInt(prop.trim()));
	}

	/**
	 * Creates a new buffer manager having the specified number of buffers. This
	 * constructor depends on both the {@link FileMgr} and
	 * {@link org.vanilladb.core.storage.log.LogMgr LogMgr} objects that
	 * it gets from the class {@link VanillaDB}. Those objects are created
	 * during system initialization. Thus this constructor cannot be called
	 * until {@link VanillaDB#initFileAndLogMgr(String)} or is called first.
	 * 
	 */
	public BufferMgr() {
		bufferMgr = new BasicBufferMgr(BUFFER_SIZE);
		pinnedByMap = new HashMap<Long, List<Buffer>>();
		waitingThreads = new LinkedList<Thread>();
		if (logger.isLoggable(Level.INFO))
			logger.info("buffer pool size " + BUFFER_SIZE);
	}

	@Override
	public synchronized void onTxCommit(Transaction tx) {
		unpinAll(tx);
	}

	@Override
	public synchronized void onTxRollback(Transaction tx) {
		unpinAll(tx);
	}

	@Override
	public synchronized void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	@Override
	public void onTxStart(Transaction tx) {
		tx.addLifecycleListener(this);
	}

	/**
	 * Pins a buffer to the specified block, potentially waiting until a buffer
	 * becomes available. If no buffer becomes available within a fixed time
	 * period, then repins all currently holding blocks.
	 * 
	 * @param blk
	 *            a block ID
	 * @param txNum
	 *            the calling transaction id
	 * @return the buffer pinned to that block
	 */
	public synchronized Buffer pin(BlockId blk, long txNum) {
		List<Buffer> bufferList = pinnedByMap.get(txNum);
		/*
		 * Throws buffer abort exception if the calling tx requires buffers more
		 * than the size of buffer pool.
		 */
		if (bufferList != null && bufferList.size() == BUFFER_SIZE)
			throw new BufferAbortException();
		try {
			Buffer buff;
			long timestamp = System.currentTimeMillis();
			buff = bufferMgr.pin(blk);
			if (buff == null
					&& !waitingThreads.contains(Thread.currentThread()))
				waitingThreads.add(Thread.currentThread());
			while (buff == null && !waitingTooLong(timestamp)) {
				wait(MAX_TIME);
				if (waitingThreads.get(0).equals(Thread.currentThread()))
					buff = bufferMgr.pin(blk);
			}
			waitingThreads.remove(Thread.currentThread());
			if (buff == null) {
				waitingThreads.add(Thread.currentThread());
				repin(txNum);
				pin(blk, txNum);
			} else {
				if (bufferList == null) {
					bufferList = new LinkedList<Buffer>();
					pinnedByMap.put(txNum, bufferList);
				}
				bufferList.add(buff);
			}
			return buff;
		} catch (InterruptedException e) {
			throw new BufferAbortException();
		}
	}

	/**
	 * Pins a buffer to a new block in the specified file, potentially waiting
	 * until a buffer becomes available. If no buffer becomes available within a
	 * fixed time period, then repins all currently holding blocks.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param fmtr
	 *            the formatter used to initialize the page
	 * @param txNum
	 *            the calling transaction id
	 * @return the buffer pinned to that block
	 */
	public synchronized Buffer pinNew(String fileName, PageFormatter fmtr,
			long txNum) {
		List<Buffer> bufferList = pinnedByMap.get(txNum);
		/*
		 * throws buffer abort exception if the calling tx requires buffers more
		 * than the size of buffer pool
		 */

		if (bufferList != null && bufferList.size() == BUFFER_SIZE)
			throw new BufferAbortException();
		try {
			Buffer buff;
			long timestamp = System.currentTimeMillis();
			buff = bufferMgr.pinNew(fileName, fmtr);
			if (buff == null
					&& !waitingThreads.contains(Thread.currentThread()))
				waitingThreads.add(Thread.currentThread());
			while (buff == null && !waitingTooLong(timestamp)) {
				wait(MAX_TIME);
				if (waitingThreads.get(0).equals(Thread.currentThread()))
					buff = bufferMgr.pinNew(fileName, fmtr);
			}
			waitingThreads.remove(Thread.currentThread());
			if (buff == null) {
				waitingThreads.add(Thread.currentThread());
				repin(txNum);
				buff = pinNew(fileName, fmtr, txNum);
			} else {
				bufferList = pinnedByMap.get(txNum);
				if (bufferList == null) {
					bufferList = new LinkedList<Buffer>();
					pinnedByMap.put(txNum, bufferList);
				}
				bufferList.add(buff);
			}
			return buff;
		} catch (InterruptedException e) {
			throw new BufferAbortException();
		}
	}

	/**
	 * Unpins the specified buffer. If the buffer's pin count becomes 0, then
	 * the threads on the wait list are notified.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	public synchronized void unpin(long txNum, Buffer... buffs) {
		List<Buffer> bufferList = pinnedByMap.get(txNum);
		for (Buffer buff : buffs) {
			bufferMgr.unpin(buff);
			if (bufferList != null && bufferList.contains(buff))
				bufferList.remove(buff);
			if (!buff.isPinned())
				notifyAll();
		}
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txNum
	 *            the transaction's id number
	 */
	public void flushAll(long txNum) {
		bufferMgr.flushAll(txNum);
	}

	/**
	 * Returns the number of available (ie unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	public int available() {
		return bufferMgr.available();
	}
	
	private void unpinAll(Transaction tx) {
		long txNum = tx.getTransactionNumber();
		List<Buffer> buffs = pinnedByMap.get(txNum);
		if (buffs != null)
			unpin(txNum, buffs.toArray(new Buffer[0]));
		pinnedByMap.remove(txNum);
	}

	/**
	 * Unpins all currently pinned buffers of the calling transaction and repins
	 * them.
	 */
	private void repin(long txNum) {
		try {
			List<Buffer> currentPinnedBuffs = pinnedByMap.get(txNum);
			if (currentPinnedBuffs == null)
				return;
			List<Buffer> buffs = new LinkedList<Buffer>(currentPinnedBuffs);
			unpin(txNum, currentPinnedBuffs.toArray(new Buffer[0]));
			wait(MAX_TIME);
			for (Buffer buff : buffs)
				pin(buff.block(), txNum);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private boolean waitingTooLong(long startTime) {
		return System.currentTimeMillis() - startTime + EPSILON > MAX_TIME;
	}
}
