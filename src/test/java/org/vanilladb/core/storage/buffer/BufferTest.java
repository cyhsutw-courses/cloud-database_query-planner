package org.vanilladb.core.storage.buffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * All blocks used by this class should be numbered from 0 to
 * {@link VanillaDB#BUFFER_SIZE}.
 */
public class BufferTest {
	private static Logger logger = Logger.getLogger(BufferTest.class.getName());
	static String fileName = "_tempbuffertest.0";
	private static BufferMgr bm;
	private long txNum = 20;
	private static String result;

	@BeforeClass
	public static void init() {
		ServerInit.initData();
		bm = VanillaDB.bufferMgr();
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN BUFFER TEST");
	}

	@Before
	public void setup() {
		result = "";
	}

	@After
	public void tearDown() {
		for (int i = 0; i < BufferMgr.BUFFER_SIZE; i++) {
			BlockId blk = new BlockId(fileName, i);
			Buffer b = bm.pin(blk, txNum); // make sure buffer is initiated
			while (b.isPinned())
				bm.unpin(txNum, b);
		}
		if (logger.isLoggable(Level.FINE))
			logger.fine("available buffer : " + bm.available());
	}

	@Test
	public void testBuffer() {
		BlockId blk = new BlockId(fileName, 3);
		Buffer buff = bm.pin(blk, txNum);
		// fill the buffer with some values, but don't bother to log them
		int pos = 0;
		while (true) {
			if (pos + Page.maxSize(INTEGER) >= BLOCK_SIZE)
				break;
			int val = 1000 + pos;
			buff.setVal(pos, new IntegerConstant(val), 1, -1);
			pos += Page.maxSize(INTEGER);
			String s = "value" + pos;
			int strlen = Page.maxSize(VARCHAR(s.length()));
			if (pos + strlen >= BLOCK_SIZE)
				break;
			buff.setVal(pos, new VarcharConstant(s), 1, -1);
			pos += strlen;
		}
		bm.unpin(txNum, buff);

		Buffer buff2 = bm.pin(blk, txNum);
		pos = 0;
		while (true) {
			if (pos + Page.maxSize(INTEGER) >= BLOCK_SIZE)
				break;
			int val = 1000 + pos;
			assertEquals("*****BufferTest: bad getInt", (Integer) val,
					(Integer) buff2.getVal(pos, INTEGER).asJavaVal());
			pos += Page.maxSize(INTEGER);
			String s = "value" + pos;
			int strlen = Page.maxSize(VARCHAR(s.length()));
			if (pos + strlen >= BLOCK_SIZE)
				break;
			assertEquals("*****BufferTest: bad getString", s, (String) buff2
					.getVal(pos, VARCHAR).asJavaVal());
			pos += strlen;
		}
		bm.unpin(txNum, buff2);
	}

	@Test
	public void testAvailability() {
		int avail1 = bm.available();
		BlockId blk1 = new BlockId(fileName, 0);
		Buffer buff1 = bm.pin(blk1, txNum);
		int avail2 = bm.available();
		assertEquals("*****BufferTest: bad available", avail1 - 1, avail2);

		BlockId blk2 = new BlockId(fileName, 1);
		Buffer buff2 = bm.pin(blk2, txNum);
		int avail3 = bm.available();
		assertEquals("*****BufferTest: bad available", avail2 - 1, avail3);

		BlockId blk3 = new BlockId(fileName, 0);
		Buffer buff3 = bm.pin(blk3, txNum);
		int avail4 = bm.available();
		assertEquals("*****BufferTest: bad available", avail3, avail4);

		bm.unpin(txNum, buff1);
		int avail5 = bm.available();
		assertEquals("*****BufferTest: bad available", avail4, avail5);

		bm.unpin(txNum, buff2);
		int avail6 = bm.available();
		assertEquals("*****BufferTest: bad available", avail5 + 1, avail6);

		bm.unpin(txNum, buff3);
		int avail7 = bm.available();
		assertEquals("*****BufferTest: bad available", avail6 + 1, avail7);

		assertEquals("*****BufferTest: bad available", avail7, avail1);
	}

	/**
	 * Tests the buffer manager when a transaction requires buffers more than
	 * the buffer pool size.
	 */
	@Test
	public void testBufferAbortException() {
		ArrayList<Buffer> pinnedBuff = new ArrayList<Buffer>();
		try {
			int i = 0;
			while (i <= BufferMgr.BUFFER_SIZE) {
				BlockId blk = new BlockId(fileName, i);
				pinnedBuff.add(bm.pin(blk, txNum));
				i++;
			}
			fail("*****BufferTest: bad bufferAbortException");
		} catch (BufferAbortException e) {
			bm.unpin(txNum, pinnedBuff.toArray(new Buffer[0]));
		}
	}

	@Test
	public void testBufferRepinning() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		int avail = bm.available();
		// leave only two buffers available in buffer pool
		for (int i = 0; i < avail - 2; i++) {
			// leave blocks 0 to 3 unpinned
			BlockId blk = new BlockId(BufferTest.fileName, i + 4);
			bm.pin(blk, tx.getTransactionNumber());
		}

		try {
			TxClientD thD = new TxClientD(0, 1000);
			thD.start();
			TxClientE thE = new TxClientE(500, 1500);
			thE.start();
			try {
				thD.join();
				thE.join();
			} catch (InterruptedException e) {
			}
			String expected = "Tx D: pin 1 start\n" + "Tx D: pin 1 end\n"
					+ "Tx E: pin 2 start\n" + "Tx E: pin 2 end\n"
					+ "Tx D: pin 3 start\n" + "Tx E: pin 4 start\n"
					+ "Tx E: pin 4 end\n" + "Tx D: pin 3 end\n";
			assertEquals("*****TxTest: bad tx history", expected, result);
		} finally {
			tx.rollback();
		}
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

class TxClientD extends TxClient {
	TxClientD(int... pauses) {
		super(pauses);
	}

	@Override
	public void run() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		try {
			if (pauses[0] > 0)
				Thread.sleep(pauses[0]);

			BufferTest.appendToResult("Tx D: pin 1 start");
			BlockId blk1 = new BlockId(BufferTest.fileName, 0);
			bufferMgr.pin(blk1, tx.getTransactionNumber());
			BufferTest.appendToResult("Tx D: pin 1 end");

			if (pauses[1] > 0)
				Thread.sleep(pauses[1]);

			BufferTest.appendToResult("Tx D: pin 3 start");
			BlockId blk3 = new BlockId(BufferTest.fileName, 2);
			bufferMgr.pin(blk3, tx.getTransactionNumber());
			BufferTest.appendToResult("Tx D: pin 3 end");
		} catch (InterruptedException e) {
		} finally {
			tx.rollback();
		}
	}
}

class TxClientE extends TxClient {
	TxClientE(int... pauses) {
		super(pauses);
	}

	@Override
	public void run() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		try {
			if (pauses[0] > 0)
				Thread.sleep(pauses[0]);

			BufferTest.appendToResult("Tx E: pin 2 start");
			BlockId blk2 = new BlockId(BufferTest.fileName, 1);
			bufferMgr.pin(blk2, tx.getTransactionNumber());
			BufferTest.appendToResult("Tx E: pin 2 end");

			if (pauses[1] > 0)
				Thread.sleep(pauses[1]);

			BufferTest.appendToResult("Tx E: pin 4 start");
			BlockId blk4 = new BlockId(BufferTest.fileName, 3);
			bufferMgr.pin(blk4, tx.getTransactionNumber());
			BufferTest.appendToResult("Tx E: pin 4 end");
		} catch (InterruptedException e) {
		} finally {
			tx.rollback();
		}
	}
}
