package org.vanilladb.core.storage.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

public class TxTest {
	private static Logger logger = Logger.getLogger(TxTest.class.getName());

	// For testing recovery, filename cannot start with "_temp"
	static String fileName = "txtest.0";
	private static String result;

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN TX TEST");
	}

	@Before
	public void setup() {
		result = "";
	}

	@Test
	public void testCommit() {
		Transaction tx1 = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		BufferMgr bm = VanillaDB.bufferMgr();
		BlockId blk = new BlockId(TxTest.fileName, 0);

		Buffer buff = bm.pin(blk, tx1.getTransactionNumber());
		tx1.concurrencyMgr().xLock(blk);
		Constant int9999 = new IntegerConstant(9999);
		long lsn = tx1.recoveryMgr().setVal(buff, 0, int9999);
		buff.setVal(0, int9999, tx1.getTransactionNumber(), lsn);

		tx1.commit();
		Transaction tx2 = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		// assertTrue("*****TxTest: bad commit", buff.isPinned() == false);
		buff = bm.pin(blk, tx2.getTransactionNumber());
		try {
			tx2.concurrencyMgr().sLock(blk);
		} catch (LockAbortException e) {
			fail("*****TxTest: bad commit");
		}

		assertTrue("*****TxTest: bad commit",
				buff.getVal(0, INTEGER).equals(int9999));
		tx2.commit();
	}

	@Test
	public void testRollback() {
		BufferMgr bm = VanillaDB.bufferMgr();
		BlockId blk = new BlockId(TxTest.fileName, 0);
		// set the initial values in this block
		int txNum = 250;
		Buffer buff = bm.pin(blk, txNum);
		buff.setVal(0, new IntegerConstant(555), txNum, -1);
		bm.flushAll(txNum);
		bm.unpin(txNum, buff);

		Transaction tx1 = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		buff = bm.pin(blk, tx1.getTransactionNumber());
		tx1.concurrencyMgr().xLock(blk);
		Constant int9999 = new IntegerConstant(9999);
		long lsn = tx1.recoveryMgr().setVal(buff, 0, int9999);
		buff.setVal(0, int9999, tx1.getTransactionNumber(), lsn);
		tx1.rollback();

		Transaction tx2 = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		// assertTrue("*****TxTest: bad rollback", bm.isPinned(blk) == false);
		buff = bm.pin(blk, tx2.getTransactionNumber());
		try {
			tx2.concurrencyMgr().sLock(blk);
		} catch (LockAbortException e) {
			fail("*****TxTest: bad rollback");
		}
		assertTrue("*****TxTest: bad rollback",
				buff.getVal(0, INTEGER).equals(new IntegerConstant(555)));
		tx2.commit();
	}

	@Test
	public void testEndStatement() {
		BufferMgr bm = VanillaDB.bufferMgr();
		BlockId blk = new BlockId(TxTest.fileName, 0);
		Transaction tx1 = VanillaDB.transaction(
				Connection.TRANSACTION_REPEATABLE_READ, false);
		Buffer buff = bm.pin(blk, tx1.getTransactionNumber());
		tx1.concurrencyMgr().sLock(blk);
		buff.getVal(0, INTEGER);
		tx1.endStatement();

		Transaction tx2 = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		buff = bm.pin(blk, tx2.getTransactionNumber());
		try {
			tx2.concurrencyMgr().xLock(blk);
		} catch (LockAbortException e) {
			fail("*****TxTest: bad end statement");
		}
		tx2.commit();
		tx1.commit();
	}

	@Test
	public void testConcurrency() {
		TxClientA thA = new TxClientA(0, 2000);
		thA.start();
		TxClientB thB = new TxClientB(1000, 1500);
		thB.start();
		TxClientC thC = new TxClientC(1500, 0);
		thC.start();
		try {
			thA.join();
			thB.join();
			thC.join();
		} catch (InterruptedException e) {
		}
		String expected = "Tx A: read 1 start\n" + "Tx A: read 1 end\n"
				+ "Tx B: write 2 start\n" + "Tx B: write 2 end\n"
				+ "Tx C: write 1 start\n" + "Tx A: read 2 start\n"
				+ "Tx B: read 1 start\n" + "Tx B: read 1 end\n"
				+ "Tx A: read 2 end\n" + "Tx C: write 1 end\n"
				+ "Tx C: read 2 start\n" + "Tx C: read 2 end\n";
		assertEquals("*****TxTest: bad tx history", expected, result);
	}

	@Test
	public void testDeadlock() {
		TxClientB thB = new TxClientB(0, 1000);
		thB.start();
		TxClientC thC = new TxClientC(500, 1000);
		thC.start();
		try {
			thB.join();
			thC.join();
		} catch (InterruptedException e) {
		}
		String expected = "Tx B: write 2 start\n" + "Tx B: write 2 end\n"
				+ "Tx C: write 1 start\n" + "Tx C: write 1 end\n"
				+ "Tx B: read 1 start\n" + "Tx C: read 2 start\n"
				+ "Tx C: read 2 end\n";
		assertEquals("*****TxTest: bad tx history", expected, result);
		assertTrue("*****TxTest: bad tx history", thB.isDeadlockAborted());
	}

	synchronized static void appendToResult(String s) {
		result += s + "\n";
	}
}

abstract class TxClient extends Thread {
	protected int[] pauses;
	protected boolean deadlockAborted;
	protected static BufferMgr bufferMgr = VanillaDB.bufferMgr();

	TxClient(int... pauses) {
		this.pauses = pauses;
	}

	boolean isDeadlockAborted() {
		return deadlockAborted;
	}
}

class TxClientA extends TxClient {
	TxClientA(int... pauses) {
		super(pauses);
	}

	@Override
	public void run() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		try {
			if (pauses[0] > 0)
				Thread.sleep(pauses[0]);

			BlockId blk1 = new BlockId(TxTest.fileName, 0);
			Buffer buff = bufferMgr.pin(blk1, tx.getTransactionNumber());
			TxTest.appendToResult("Tx A: read 1 start");
			tx.concurrencyMgr().sLock(blk1);
			buff.getVal(0, INTEGER);
			TxTest.appendToResult("Tx A: read 1 end");

			if (pauses[1] > 0)
				Thread.sleep(pauses[1]);

			BlockId blk2 = new BlockId(TxTest.fileName, 1);
			buff = bufferMgr.pin(blk2, tx.getTransactionNumber());
			TxTest.appendToResult("Tx A: read 2 start");
			tx.concurrencyMgr().sLock(blk2);
			buff.getVal(0, INTEGER);
			TxTest.appendToResult("Tx A: read 2 end");
		} catch (InterruptedException e) {
		} catch (LockAbortException e) {
			deadlockAborted = true;
		} finally {
			tx.rollback();
		}
	}
}

class TxClientB extends TxClient {
	TxClientB(int... pauses) {
		super(pauses);
	}

	@Override
	public void run() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		try {
			if (pauses[0] > 0)
				Thread.sleep(pauses[0]);

			BlockId blk2 = new BlockId(TxTest.fileName, 1);
			Buffer buff = bufferMgr.pin(blk2, tx.getTransactionNumber());
			TxTest.appendToResult("Tx B: write 2 start");
			tx.concurrencyMgr().xLock(blk2);
			Constant int0 = new IntegerConstant(0);
			long lsn = tx.recoveryMgr().setVal(buff, 0, int0);
			buff.setVal(0, int0, tx.getTransactionNumber(), lsn);
			TxTest.appendToResult("Tx B: write 2 end");

			if (pauses[1] > 0)
				Thread.sleep(pauses[1]);

			BlockId blk1 = new BlockId(TxTest.fileName, 0);
			buff = bufferMgr.pin(blk1, tx.getTransactionNumber());
			TxTest.appendToResult("Tx B: read 1 start");
			tx.concurrencyMgr().sLock(blk1);
			buff.getVal(0, INTEGER);
			TxTest.appendToResult("Tx B: read 1 end");
		} catch (InterruptedException e) {
		} catch (LockAbortException e) {
			deadlockAborted = true;
		} finally {
			tx.rollback();
		}
	}
}

class TxClientC extends TxClient {
	TxClientC(int... pauses) {
		super(pauses);
	}

	@Override
	public void run() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		try {
			if (pauses[0] > 0)
				Thread.sleep(pauses[0]);

			BlockId blk1 = new BlockId(TxTest.fileName, 0);
			Buffer buff = bufferMgr.pin(blk1, tx.getTransactionNumber());
			TxTest.appendToResult("Tx C: write 1 start");
			tx.concurrencyMgr().xLock(blk1);
			Constant int0 = new IntegerConstant(0);
			long lsn = tx.recoveryMgr().setVal(buff, 0, int0);
			buff.setVal(0, int0, tx.getTransactionNumber(), lsn);
			TxTest.appendToResult("Tx C: write 1 end");

			if (pauses[1] > 0)
				Thread.sleep(pauses[1]);

			BlockId blk2 = new BlockId(TxTest.fileName, 1);
			buff = bufferMgr.pin(blk2, tx.getTransactionNumber());
			TxTest.appendToResult("Tx C: read 2 start");
			tx.concurrencyMgr().sLock(blk2);
			buff.getVal(0, INTEGER);
			TxTest.appendToResult("Tx C: read 2 end");
		} catch (InterruptedException e) {
		} catch (LockAbortException e) {
			deadlockAborted = true;
		} finally {
			tx.rollback();
		}
	}
}
