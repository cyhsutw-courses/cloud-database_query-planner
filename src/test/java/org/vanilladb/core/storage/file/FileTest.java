package org.vanilladb.core.storage.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;

public class FileTest {
	private static Logger logger = Logger.getLogger(FileTest.class.getName());

	private static int fileCounter = 0;
	private static VarcharConstant s = new VarcharConstant(
			"abcdefghijklmnopqrstuvwxyz0123456789");
	private static int strSize;
	private static int intTypeCapacity = Page.maxSize(INTEGER);

	private static FileMgr fm;
	private static Page p1;
	private static Page p2;
	private static Page p3;

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		strSize = Page.size(s);
		fm = VanillaDB.fileMgr();
		p1 = new Page();
		p2 = new Page();
		p3 = new Page();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN FILE TEST");
	}

	@After
	public void setup() {
		for (int pos = 0; pos + intTypeCapacity < BLOCK_SIZE; pos += intTypeCapacity) {
			p1.setVal(pos, new IntegerConstant(0));
			p2.setVal(pos, new IntegerConstant(0));
			p3.setVal(pos, new IntegerConstant(0));
		}
	}

	@Test
	public void testReadWriteAppend() {
		String filename = "_tempfiletestrwa." + fileCounter++;

		BlockId blk = new BlockId(filename, 0);
		p1.setVal(0, new IntegerConstant(123));
		p1.setVal(intTypeCapacity, new IntegerConstant(456));
		p1.write(blk);
		p2.read(blk);
		assertTrue(
				"*****FileTest: bad getInt",
				p2.getVal(0, INTEGER).equals(new IntegerConstant(123))
						&& p2.getVal(intTypeCapacity, INTEGER).equals(
								new IntegerConstant(456)));
		long lastblock = fm.size(filename) - 1;
		BlockId blk2 = p1.append(filename);
		assertEquals("*****FileTest: bad append", lastblock + 1, blk2.number());
		p2.read(blk2);
		assertTrue(
				"*****FileTest: bad read",
				p2.getVal(0, INTEGER).equals(new IntegerConstant(123))
						&& p2.getVal(intTypeCapacity, INTEGER).equals(
								new IntegerConstant(456)));
	}

	@Test
	public void testFileList() {
		String filename = "_tempfiletestlist." + fileCounter++;
		BlockId blk = new BlockId(filename, 14);
		p1.write(blk);
		assertEquals("*****FileTest: bad file list", 15, fm.size(filename));
	}

	@Test
	public void testSetAndGet() {
		p1.setVal(0, new IntegerConstant(123));
		p1.setVal(20, s);
		assertTrue("*****FileTest: bad page get/set", p1.getVal(0, INTEGER)
				.equals(new IntegerConstant(123))
				&& p1.getVal(20, VARCHAR).equals(s));
		p1.setVal(2, new IntegerConstant(456));
		assertTrue(
				"*****FileTest: bad overlapping getInt",
				!p1.getVal(0, INTEGER).equals(new IntegerConstant(123))
						&& p1.getVal(2, INTEGER).equals(
								new IntegerConstant(456)));
		p1.setVal(26, s);
		assertTrue("*****FileTest: bad overlapping getString",
				!p1.getVal(20, VARCHAR).equals(s)
						&& p1.getVal(26, VARCHAR).equals(s));

		p2.setVal(0, new IntegerConstant(123));
		p2.setVal(intTypeCapacity, new IntegerConstant(456));
		p2.setVal(2 * intTypeCapacity, new IntegerConstant(789));
		assertTrue(
				"*****FileTest: bad contiguous getInt",
				p2.getVal(0, INTEGER).equals(new IntegerConstant(123))
						&& p2.getVal(intTypeCapacity, INTEGER).equals(
								new IntegerConstant(456))
						&& p2.getVal(intTypeCapacity * 2, INTEGER).equals(
								new IntegerConstant(789)));

		p3.setVal(0, s);
		p3.setVal(strSize, s);
		p3.setVal(2 * strSize, s);
		assertTrue(
				"*****FileTest: bad contiguous getString",
				p3.getVal(0, VARCHAR).equals(s)
						&& p3.getVal(strSize, VARCHAR).equals(s)
						&& p3.getVal(2 * strSize, VARCHAR).equals(s));
	}

	@Test
	public void testBoundaries() {
		try {
			p1.setVal(-2, new IntegerConstant(123));
			fail("*****FileTest: allowed int negative offset");
		} catch (Exception e) {
		}
		try {
			p1.setVal(-2, s);
			fail("*****FileTest: allowed String negative offset");
		} catch (Exception e) {
		}
		try {
			p1.setVal(BLOCK_SIZE - (intTypeCapacity / 2), new IntegerConstant(
					123));
			fail("*****FileTest: allowed int large offset");
		} catch (Exception e) {
		}
		try {
			p1.setVal(BLOCK_SIZE - (strSize / 2), s);
			fail("*****FileTest: allowed String large offset");
		} catch (Exception e) {
		}
	}

	@Test
	public void testBlock() {
		BlockId b1 = new BlockId("abc", 0);
		BlockId b2 = new BlockId("def", 0);
		BlockId b3 = new BlockId("ab" + "c", 0);
		BlockId b4 = new BlockId("abc", 3);

		assertTrue("*****FileTest: bad block equals",
				b1.equals(b3) && !b1.equals(b4) && !b1.equals(b2));

		Map<BlockId, String> m = new HashMap<BlockId, String>();
		m.put(b1, "block 1");
		m.put(b2, "block 2");
		assertTrue(
				"*****FileTest: bad block hashcode",
				m.get(b1).equals("block 1") && m.get(b3).equals("block 1")
						&& m.get(b4) == null);

		String fname = b1.fileName();
		long blknum = b1.number();
		BlockId b = new BlockId(fname, blknum);
		assertTrue("*****FileTest: bad block extraction", b.equals(b1));
	}
}
