package org.vanilladb.core.storage.tx.concurrency;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.Transaction;

public class ConcurrencyTest {
	private static Logger logger = Logger.getLogger(ConcurrencyTest.class
			.getName());

	private static String fileName = "_tempconcurrencytest.0";
	private static int max = 100;
	private static BlockId[] blocks;
	private static Transaction tx1, tx2, tx3, tx4;
	private static SerializableConcurrencyMgr scm1;
	private static SerializableConcurrencyMgr scm2;
	private static RepeatableReadConcurrencyMgr rrcm1;
	private static RepeatableReadConcurrencyMgr rrcm2;

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		blocks = new BlockId[max];
		for (int i = 0; i < max; i++)
			blocks[i] = new BlockId(fileName, i);
		tx1 = VanillaDB.transaction(Connection.TRANSACTION_SERIALIZABLE, false);
		tx2 = VanillaDB.transaction(Connection.TRANSACTION_SERIALIZABLE, false);
		tx3 = VanillaDB.transaction(Connection.TRANSACTION_REPEATABLE_READ,
				false);
		tx4 = VanillaDB.transaction(Connection.TRANSACTION_REPEATABLE_READ,
				false);
		scm1 = (SerializableConcurrencyMgr) tx1.concurrencyMgr();
		scm2 = (SerializableConcurrencyMgr) tx2.concurrencyMgr();
		rrcm1 = (RepeatableReadConcurrencyMgr) tx3.concurrencyMgr();
		rrcm2 = (RepeatableReadConcurrencyMgr) tx4.concurrencyMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN CONCURRENCY TEST");
	}

	@After
	public void teardown() {
		scm1.onTxRollback(tx1);
		scm2.onTxRollback(tx2);
		rrcm1.onTxRollback(tx3);
		rrcm2.onTxRollback(tx4);
	}

	@Test
	public void testSerializableConcurrencyMgr() {
		try {
			scm1.xLock(blocks[1]);
			scm2.sLock(fileName);
			fail("*****ConcurrencyTest: bad serializable concurrency");
		} catch (LockAbortException e) {
			scm1.onTxRollback(tx1);
		}

		try {
			scm2.sLock(fileName);
			scm1.xLock(blocks[1]);
			fail("*****ConcurrencyTest: bad serializable concurrency");
		} catch (LockAbortException e) {
			scm2.onTxRollback(tx2);
		}

		try {
			scm2.sLock(fileName);
			scm1.sLock(blocks[1]);
			scm2.sLock(blocks[2]);
		} catch (LockAbortException e) {
			fail("*****ConcurrencyTest: bad serializable concurrency");
		}
		scm1.onTxRollback(tx1);
		scm2.onTxRollback(tx2);
		try {
			scm1.xLock(fileName);
			scm1.sLock(blocks[2]);
			scm2.sLock(blocks[1]);
			fail("*****ConcurrencyTest: bad serializable concurrency");
		} catch (LockAbortException e) {
		}
	}

	@Test
	public void testSerializablePhantom() {
		try {
			scm1.sLock(blocks[0]);
			scm2.xLock(fileName);
			fail("*****ConcurrencyTest: bad serializable concurrency");
		} catch (LockAbortException e) {
		}
	}

	@Test
	public void testRepeatableReadConcurrency() {
		try {
			rrcm1.sLock(blocks[0]);
			rrcm2.xLock(blocks[0]);
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		} catch (LockAbortException e) {
		}
	}

	@Test
	public void testRepeatableReadPhantom() {
		try {
			rrcm1.sLock(blocks[0]);
			rrcm2.xLock(fileName);
		} catch (LockAbortException e) {
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		}
	}

	@Test
	public void testRepeatableReadEndStatement() {
		try {
			rrcm1.sLock(blocks[0]);
			rrcm1.sLock(blocks[1]);
			rrcm1.onTxEndStatement(tx3);
			rrcm2.xLock(blocks[0]);
		} catch (LockAbortException e) {
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		}
	}

	@Test
	public void testSS() {
		try {
			Plan p1 = new TablePlan("student", tx1);
			Scan s1 = p1.open();
			s1.beforeFirst();
			Plan p2 = new TablePlan("student", tx2);
			UpdateScan s2 = (UpdateScan) p2.open();
			s2.beforeFirst();
			while (s1.next()) {
				s2.next();
			}
			s2.insert();
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		} catch (LockAbortException e) {

		}
	}

	@Test
	public void testSRR() {
		try {
			Plan p1 = new TablePlan("student", tx3);
			Scan s1 = p1.open();
			s1.beforeFirst();
			Plan p2 = new TablePlan("student", tx2);
			UpdateScan s2 = (UpdateScan) p2.open();
			s2.beforeFirst();
			s1.next();
			s2.next();
			// tx3 (RR) has no range lock
			s2.insert();

		} catch (LockAbortException e) {
			fail("*****ConcurrencyTest: bad repeatable read concurrency");
		}
	}
}
