package org.vanilladb.core.storage.tx.recovery;

import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.Transaction;

public class RecoveryTest {
	private static Logger logger = Logger.getLogger(RecoveryTest.class
			.getName());
	/**
	 * Filename cannot start with "_temp" otherwise the operations over the file
	 * will be ignored by {@link RecoveryMgr}
	 */
	private static String fileName = "recoverytest.0";

	private static BlockId blk;
	private static BufferMgr bm;

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		blk = new BlockId(fileName, 12);
		bm = VanillaDB.bufferMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN RECOVERY TEST");
	}

	@Before
	public void setup() {
		// reset initial values in the block
		long txNum = 250;
		Buffer buff = bm.pin(blk, txNum);
		buff.setVal(4, new IntegerConstant(9876), txNum, -1);
		buff.setVal(20, new VarcharConstant("abcdefg"), txNum, -1);
		buff.setVal(40, new VarcharConstant("hijk"), txNum, -1);
		buff.setVal(104, new IntegerConstant(9999), txNum, -1);
		buff.setVal(120, new VarcharConstant("gfedcba"), txNum, -1);
		buff.setVal(140, new VarcharConstant("kjih"), txNum, -1);
		bm.flushAll(txNum);
		bm.unpin(txNum, buff);
	}

	@Test
	public void testRollback() {
		// log and make changes to the block's values

		LinkedList<BlockId> blklist = new LinkedList<BlockId>();
		blklist.add(blk);
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm = tx.recoveryMgr();
		long txNum = tx.getTransactionNumber();
		Buffer buff = bm.pin(blk, txNum);
		long lsn = rm.setVal(buff, 4, new IntegerConstant(1234));
		buff.setVal(4, new IntegerConstant(1234), txNum, lsn);
		lsn = rm.setVal(buff, 20, new VarcharConstant("xyz"));
		buff.setVal(20, new VarcharConstant("xyz"), txNum, lsn);

		bm.unpin(txNum, buff);
		bm.flushAll(txNum);

		// verify that the changes got made
		buff = bm.pin(blk, txNum);
		assertTrue(
				"*****RecoveryTest: rollback changes not made",
				buff.getVal(4, INTEGER).equals(new IntegerConstant(1234))
						&& ((String) buff.getVal(20, VARCHAR).asJavaVal())
								.equals("xyz"));
		bm.unpin(txNum, buff);

		rm.onTxRollback(tx);

		// verify that they got rolled back
		buff = bm.pin(blk, txNum);
		int ti = (Integer) buff.getVal(4, INTEGER).asJavaVal();
		String ts = (String) buff.getVal(20, VARCHAR).asJavaVal();
		assertTrue("*****RecoveryTest: bad rollback",
				ti == 9876 && ts.equals("abcdefg"));
		bm.unpin(txNum, buff);
	}

	@Test
	public void testRecover() {
		// use different txs to log and make changes to those values
		Transaction tx1 = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm1 = tx1.recoveryMgr();
		Transaction tx2 = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm2 = tx2.recoveryMgr();
		Transaction tx3 = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecoveryMgr rm3 = tx3.recoveryMgr();
		long txNum1 = tx1.getTransactionNumber();
		long txNum2 = tx2.getTransactionNumber();
		long txNum3 = tx3.getTransactionNumber();

		Buffer buff = bm.pin(blk, txNum1);
		bm.pin(blk, txNum2);
		bm.pin(blk, txNum3);
		long lsn = rm1.setVal(buff, 104, new IntegerConstant(1234));
		buff.setVal(104, new IntegerConstant(1234), txNum1, lsn);
		lsn = rm2.setVal(buff, 120, new VarcharConstant("xyz"));
		buff.setVal(120, new VarcharConstant("xyz"), txNum2, lsn);
		lsn = rm3.setVal(buff, 140, new VarcharConstant("rst"));
		buff.setVal(140, new VarcharConstant("rst"), txNum3, lsn);
		bm.unpin(txNum1, buff);
		bm.unpin(txNum2, buff);
		bm.unpin(txNum3, buff);

		// verify that the changes got made
		buff = bm.pin(blk, txNum1);
		assertTrue(
				"*****RecoveryTest: recovery changes not made",
				buff.getVal(104, INTEGER).equals(new IntegerConstant(1234))
						&& ((String) buff.getVal(120, VARCHAR).asJavaVal())
								.equals("xyz")
						&& ((String) buff.getVal(140, VARCHAR).asJavaVal())
								.equals("rst"));
		bm.unpin(txNum1, buff);

		rm2.onTxCommit(tx2);
		rm1.recover();
		// verify that tx1 and tx3 got rolled back
		buff = bm.pin(blk, txNum1);
		int ti = (Integer) buff.getVal(104, INTEGER).asJavaVal();
		String ts = (String) buff.getVal(120, VARCHAR).asJavaVal();
		String ts2 = (String) buff.getVal(140, VARCHAR).asJavaVal();
		assertTrue("*****RecoveryTest: bad recovery",
				ti == 9999 && ts.equals("xyz") && ts2.equals("kjih"));
		bm.unpin(txNum1, buff);
	}
}
