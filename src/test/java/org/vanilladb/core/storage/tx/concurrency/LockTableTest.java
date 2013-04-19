package org.vanilladb.core.storage.tx.concurrency;

import static org.junit.Assert.fail;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;

public class LockTableTest {
	private static Logger logger = Logger.getLogger(LockTableTest.class
			.getName());

	private static String fileName = "_templocktabletest.0";
	private static int max = 100;
	private static BlockId[] blocks;
	private static RecordId[] records;
	private static long txNum1 = 1;
	private static long txNum2 = 2;

	private static LockTable lockTbl;

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		blocks = new BlockId[max];
		records = new RecordId[max];
		for (int i = 0; i < max; i++) {
			blocks[i] = new BlockId(fileName, i);
			records[i] = new RecordId(blocks[i], 5);
		}
		lockTbl = new LockTable();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN LOCK TABLE TEST");
	}

	@Before
	public void setup() {
		lockTbl.releaseAll(txNum1, false);
		lockTbl.releaseAll(txNum2, false);
	}

	@Test
	public void testSLocks() {
		try {
			for (int i = 0; i < max; i++) {
				lockTbl.sLock(blocks[i], txNum1);
				lockTbl.sLock(blocks[i], txNum1);
				lockTbl.sLock(blocks[i], txNum2);
			}

			lockTbl.releaseAll(txNum1, false);
			lockTbl.releaseAll(txNum2, false);
		} catch (LockAbortException e) {
			fail("*****LockTableTest: bad slocks");
		}
	}

	@Test
	public void testXLocks() {
		try {
			/*
			 * subsequent lock requests are ignored if a transaction has an
			 * xlock
			 */
			lockTbl.xLock(blocks[0], txNum1);
			lockTbl.xLock(blocks[0], txNum1); // ignored
			lockTbl.sLock(blocks[0], txNum1);
			lockTbl.sLock(blocks[0], txNum1); // ignored

			lockTbl.sLock(blocks[1], txNum1);
			lockTbl.sLock(blocks[1], txNum1); // ignored
			lockTbl.xLock(blocks[1], txNum1); // upgraded
			lockTbl.xLock(blocks[1], txNum1); // ignored
		} catch (LockAbortException e) {
			fail("*****LockTableTest: bad xlocks");
		}
		try {
			lockTbl.sLock(blocks[0], txNum2);
			lockTbl.sLock(blocks[1], txNum2);
			fail("*****LockTableTest: slock allowed after xlock");
		} catch (LockAbortException e) {
		}
		try {
			lockTbl.xLock(blocks[0], txNum2);
			lockTbl.xLock(blocks[1], txNum2);
			fail("*****LockTableTest: xlock allowed after xlock");
		} catch (LockAbortException e) {
		}
		try {
			lockTbl.releaseAll(txNum1, false);
			lockTbl.xLock(blocks[0], txNum2);
			lockTbl.xLock(blocks[1], txNum2);
			lockTbl.releaseAll(txNum2, false);
		} catch (LockAbortException e) {
			fail("*****LockTableTest: bad release");
		}
	}

	@Test
	public void testMultiGranularityLocking() {
		try {
			lockTbl.isLock(fileName, txNum1);
			for (int i = 0; i < max; i++) {
				lockTbl.isLock(blocks[i], txNum1);
				lockTbl.sLock(records[i], txNum1);
			}

			try {
				lockTbl.xLock(blocks[0], txNum2);
				fail("*****LockTableTest: xlock allowed after islock");
			} catch (LockAbortException e) {
			}

			try {
				lockTbl.ixLock(fileName, txNum2);
			} catch (LockAbortException e) {
				fail("*****LockTableTest: ixlock disallowed after islock");
			}

			try {
				lockTbl.sixLock(blocks[3], txNum2);
			} catch (LockAbortException e) {
				fail("*****LockTableTest: sixlock disallowed after islock");
			}
			lockTbl.releaseAll(txNum1, false);
			lockTbl.releaseAll(txNum2, false);

			try {
				lockTbl.sLock(fileName, txNum1);
				lockTbl.sLock(fileName, txNum2);
				lockTbl.ixLock(fileName, txNum2);
				fail("*****LockTableTest: ixlock disallowed after slock");
			} catch (LockAbortException e) {

			}
			lockTbl.releaseAll(txNum1, false);
			lockTbl.releaseAll(txNum2, false);
			try {
				lockTbl.sLock(fileName, txNum1);
				lockTbl.sLock(fileName, txNum2);
				lockTbl.xLock(fileName, txNum2);
				fail("*****LockTableTest: xlock disallowed after slock");
			} catch (LockAbortException e) {

			}
			lockTbl.releaseAll(txNum1, false);
			lockTbl.releaseAll(txNum2, false);
			try {
				lockTbl.sLock(fileName, txNum1);
				lockTbl.sLock(fileName, txNum2);
				lockTbl.sixLock(fileName, txNum2);
				fail("*****LockTableTest: sixlock disallowed after slock");
			} catch (LockAbortException e) {

			}

		} catch (LockAbortException e) {
			fail("*****LockTableTest: bad slocks");
		}
	}
}
