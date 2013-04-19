package org.vanilladb.core.storage.tx.concurrency;

import java.util.*;

/**
 * Checks the compatibility of locking requests on a single item (e.g., file,
 * block, or record). Does <em>not</em> implement a locking protocol to ensure
 * the semantic correctness of locking different items with different
 * granularity.
 * 
 * <p>
 * If a transaction requests to lock an item that causes a conflict with an
 * existing lock on that item, then the transaction is placed into a wait list
 * and will be notified (awaked) to compete for the lock whenever the conflict
 * is resolved. Currently, there is only one wait list for all items.
 * </p>
 */
class LockTable {
	private static final long MAX_TIME;
	private static final long EPSILON;
	final static int IS_LOCK = 0, IX_LOCK = 1, S_LOCK = 2, SIX_LOCK = 3,
			X_LOCK = 4;

	static {
		String prop = System.getProperty(LockTable.class.getName()
				+ ".MAX_TIME");
		MAX_TIME = (prop == null ? 10000 : Long.parseLong(prop.trim()));
		prop = System.getProperty(LockTable.class.getName() + ".EPSILON");
		EPSILON = (prop == null ? 50 : Long.parseLong(prop.trim()));
	}

	class Lockers {
		List<Long> sLockers, ixLockers, isLockers;
		// only one tx can hold xLock(sixLock) on single item
		long sixLocker, xLocker;

		Lockers() {
			sLockers = new LinkedList<Long>();
			ixLockers = new LinkedList<Long>();
			isLockers = new LinkedList<Long>();
			sixLocker = -1; // -1 means none
			xLocker = -1;
		}
	}

	private Map<Object, Lockers> lockerMap = new HashMap<Object, Lockers>();

	/**
	 * Grants an slock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	synchronized void sLock(Object obj, long txNum) {
		if (hasSLock(obj, txNum))
			return;

		try {
			long timestamp = System.currentTimeMillis();
			while (!sLockable(obj, txNum) && !waitingTooLong(timestamp))
				wait(MAX_TIME);
			if (!sLockable(obj, txNum))
				throw new LockAbortException();
			prepareLockers(obj).sLockers.add(txNum);
		} catch (InterruptedException e) {
			throw new LockAbortException();
		}
	}

	/**
	 * Grants an xlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	synchronized void xLock(Object obj, long txNum) {
		if (hasXLock(obj, txNum))
			return;

		try {
			long timestamp = System.currentTimeMillis();
			while (!xLockable(obj, txNum) && !waitingTooLong(timestamp))
				wait(MAX_TIME);
			if (!xLockable(obj, txNum))
				throw new LockAbortException();
			prepareLockers(obj).xLocker = txNum;
		} catch (InterruptedException e) {
			throw new LockAbortException();
		}
	}

	/**
	 * Grants an sixlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 * 
	 */
	synchronized void sixLock(Object obj, long txNum) {
		if (hasSixLock(obj, txNum))
			return;

		try {
			long timestamp = System.currentTimeMillis();
			while (!sixLockable(obj, txNum) && !waitingTooLong(timestamp))
				wait(MAX_TIME);
			if (!sixLockable(obj, txNum))
				throw new LockAbortException();
			prepareLockers(obj).sixLocker = txNum;
		} catch (InterruptedException e) {
			throw new LockAbortException();
		}

	}

	/**
	 * Grants an islock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 */
	synchronized void isLock(Object obj, long txNum) {
		try {
			long timestamp = System.currentTimeMillis();
			while (!isLockable(obj, txNum) && !waitingTooLong(timestamp))
				wait(MAX_TIME);
			if (!isLockable(obj, txNum))
				throw new LockAbortException();
			prepareLockers(obj).isLockers.add(txNum);
		} catch (InterruptedException e) {
			throw new LockAbortException();
		}
	}

	/**
	 * Grants an ixlock on the specified item. If any conflict lock exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the lock is released. If the thread remains on the wait list
	 * for a certain amount of time, then an exception is thrown.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 */
	synchronized void ixLock(Object obj, long txNum) {
		try {
			long timestamp = System.currentTimeMillis();
			while (!ixLockable(obj, txNum) && !waitingTooLong(timestamp))
				wait(MAX_TIME);
			if (!ixLockable(obj, txNum))
				throw new LockAbortException();
			prepareLockers(obj).ixLockers.add(txNum);
		} catch (InterruptedException e) {
			throw new LockAbortException();
		}
	}

	/**
	 * Releases the specified type of lock on an item holding by a transaction.
	 * If a lock is the last lock on that block, then the waiting transactions
	 * are notified.
	 * 
	 * @param obj
	 *            a lockable item
	 * @param txNum
	 *            a transaction number
	 * @param lockType
	 *            the type of lock
	 */
	synchronized void release(Object obj, long txNum, int lockType) {
		Lockers lks = lockerMap.get(obj);

		switch (lockType) {
		case X_LOCK:
			if (lks.xLocker == txNum) {
				lks.xLocker = -1;
				notifyAll();
			}
			return;
		case SIX_LOCK:
			if (lks.sixLocker == txNum) {
				lks.sixLocker = -1;
				notifyAll();
			}
			return;
		case S_LOCK:
			List<Long> sl = lks.sLockers;
			if (sl != null && sl.contains(txNum)) {
				sl.remove((Long) txNum);
				if (sl.isEmpty())
					notifyAll();
			}
			return;
		case IS_LOCK:
			List<Long> isl = lks.isLockers;
			if (isl != null && isl.contains(txNum)) {
				isl.remove((Long) txNum);
				if (isl.isEmpty())
					notifyAll();
			}
			return;
		case IX_LOCK:
			List<Long> ixl = lks.ixLockers;
			if (ixl != null && ixl.contains(txNum)) {
				ixl.remove((Long) txNum);
				if (ixl.isEmpty())
					notifyAll();
			}
			return;
		default:
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Releases all locks held by a transaction. If a lock is the last lock on
	 * that block, then the waiting transactions are notified.
	 * 
	 * @param txNum
	 *            a transaction number
	 * 
	 * @param sLockOnly
	 *            release slocks only
	 */
	synchronized void releaseAll(long txNum, boolean sLockOnly) {
		List<Object> released = new LinkedList<Object>();
		for (Object obj : lockerMap.keySet()) {
			if (hasSLock(obj, txNum))
				release(obj, txNum, S_LOCK);

			if (hasXLock(obj, txNum) && !sLockOnly)
				release(obj, txNum, X_LOCK);

			if (hasSixLock(obj, txNum))
				release(obj, txNum, SIX_LOCK);

			while (hasIsLock(obj, txNum))
				release(obj, txNum, IS_LOCK);

			while (hasIxLock(obj, txNum) && !sLockOnly)
				release(obj, txNum, IX_LOCK);

			if (!sLocked(obj) && !xLocked(obj) && !sixLocked(obj)
					&& !isLocked(obj) && !ixLocked(obj))
				released.add(obj);
		}
		for (Object obj : released) {
			lockerMap.remove(obj);
		}
	}

	private Lockers prepareLockers(Object obj) {
		Lockers lockers = lockerMap.get(obj);
		if (lockers == null) {
			lockers = new Lockers();
			lockerMap.put(obj, lockers);
		}
		return lockers;
	}

	private boolean waitingTooLong(long starttime) {
		return System.currentTimeMillis() - starttime + EPSILON > MAX_TIME;
	}

	/*
	 * Verify if an item is locked.
	 */

	private boolean sLocked(Object obj) {
		Lockers lks = lockerMap.get(obj);
		return lks != null && lks.sLockers.size() > 0;
	}

	private boolean xLocked(Object obj) {
		Lockers lks = lockerMap.get(obj);
		return lks != null && lks.xLocker != -1;
	}

	private boolean sixLocked(Object obj) {
		Lockers lks = lockerMap.get(obj);
		return lks != null && lks.sixLocker != -1;
	}

	private boolean isLocked(Object obj) {
		Lockers lks = lockerMap.get(obj);
		return lks != null && lks.isLockers.size() > 0;
	}

	private boolean ixLocked(Object obj) {
		Lockers lks = lockerMap.get(obj);
		return lks != null && lks.ixLockers.size() > 0;
	}

	private int numberOfSLocker(Object obj) {
		Lockers lks = lockerMap.get(obj);
		return lks.sLockers.size();
	}

	/*
	 * Verify if an item is held by a tx.
	 */

	private boolean hasSLock(Object obj, long txNum) {
		Lockers lks = lockerMap.get(obj);
		return lks != null && lks.sLockers.contains(txNum);
	}

	private boolean hasXLock(Object obj, long txNUm) {
		Lockers lks = lockerMap.get(obj);
		return lks != null && lks.xLocker == txNUm;
	}

	private boolean hasSixLock(Object obj, long txNum) {
		Lockers lks = lockerMap.get(obj);
		return lks != null && lks.sixLocker == txNum;
	}

	private boolean hasIsLock(Object obj, long txNum) {
		Lockers lks = lockerMap.get(obj);
		return lks != null && lks.isLockers.contains(txNum);
	}

	private boolean hasIxLock(Object obj, long txNum) {
		Lockers lks = lockerMap.get(obj);
		return lks != null && lks.ixLockers.contains(txNum);
	}

	/*
	 * Verify if an item is lockable to a tx.
	 */

	private boolean sLockable(Object obj, long txNum) {
		return (!xLocked(obj) || hasXLock(obj, txNum))
				&& (!sixLocked(obj) || hasSixLock(obj, txNum))
				&& (!ixLocked(obj) || hasIxLock(obj, txNum));
	}

	private boolean xLockable(Object obj, long txNum) {
		return (!sLocked(obj) || (hasSLock(obj, txNum) && numberOfSLocker(obj) == 1))
				&& (!sixLocked(obj) || hasSixLock(obj, txNum))
				&& (!ixLocked(obj) || hasIxLock(obj, txNum))
				&& (!isLocked(obj) || hasIsLock(obj, txNum))
				&& (!xLocked(obj) || hasXLock(obj, txNum));
	}

	private boolean sixLockable(Object obj, long txNum) {
		return (!sixLocked(obj) || hasSixLock(obj, txNum))
				&& (!ixLocked(obj) || hasIxLock(obj, txNum))
				&& (!sLocked(obj) || (hasSLock(obj, txNum) && numberOfSLocker(obj) == 1))
				&& (!xLocked(obj) || hasXLock(obj, txNum));
	}

	private boolean ixLockable(Object obj, long txNum) {
		return (!sLocked(obj) || (hasSLock(obj, txNum) && numberOfSLocker(obj) == 1))
				&& (!sixLocked(obj) || hasSixLock(obj, txNum))
				&& (!xLocked(obj) || hasXLock(obj, txNum));
	}

	private boolean isLockable(Object obj, long txNum) {
		return (!xLocked(obj) || hasXLock(obj, txNum));
	}
}
