package org.vanilladb.core.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;

public class ConstantTest {

	@Test
	public void testConstant() {
		IntegerConstant ic1 = new IntegerConstant(5);
		IntegerConstant ic2 = new IntegerConstant(-9);
		IntegerConstant ic3 = new IntegerConstant(55);

		BigIntConstant lc1 = new BigIntConstant(55);
		BigIntConstant lc2 = new BigIntConstant(559);

		VarcharConstant sc1 = new VarcharConstant("aabdcd");
		VarcharConstant sc2 = new VarcharConstant("aabdcd");
		VarcharConstant sc3 = new VarcharConstant("sssaabdcd");

		assertTrue("*****ConstantTest: bad constant equal to", ic1.equals(ic1));
		assertTrue("*****ConstantTest: bad constant equal to", ic3.equals(lc1));
		assertTrue("*****ConstantTest: bad constant equal to", !ic3.equals(ic2));

		assertTrue("*****ConstantTest: bad constant comparision",
				ic1.compareTo(ic1) == 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				ic1.compareTo(ic2) > 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				ic1.compareTo(ic3) < 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				lc2.compareTo(lc1) > 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				ic1.compareTo(lc2) < 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				ic1.compareTo(lc1) < 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				ic3.compareTo(lc1) == 0);

		assertTrue("*****ConstantTest: bad constant comparision",
				sc1.equals(sc2));
		assertTrue("*****ConstantTest: bad constant comparision",
				!sc1.equals(sc3));
		assertTrue("*****ConstantTest: bad constant comparision",
				sc1.compareTo(sc2) == 0);
		assertTrue("*****ConstantTest: bad constant comparision",
				sc1.compareTo(sc3) < 0);
	}
}
