package org.vanilladb.core.algebra.materialize;

import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.RecordComparator.DIR_DESC;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.aggfn.AggregationFn;
import org.vanilladb.core.sql.aggfn.AvgFn;
import org.vanilladb.core.sql.aggfn.CountFn;
import org.vanilladb.core.sql.aggfn.DistinctCountFn;
import org.vanilladb.core.sql.aggfn.MaxFn;
import org.vanilladb.core.sql.aggfn.MinFn;
import org.vanilladb.core.sql.aggfn.SumFn;
import org.vanilladb.core.storage.tx.Transaction;

public class MaterializeTest {
	private static Logger logger = Logger.getLogger(MaterializeTest.class
			.getName());

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN MATERIALIZE TEST");
	}

	@Before
	public void setup() {
		String homedir = System.getProperty("user.home");
		File dbDirectory = new File(homedir, ServerInit.dbName);
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				if (name.startsWith("_temp"))
					return true;
				else
					return false;
			}
		};
		File[] tempList = dbDirectory.listFiles(filter);
		for (File f : tempList)
			f.delete();
	}

	@Test
	public void testAggregationFn() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		Plan p = new TablePlan("student", tx);
		Set<AggregationFn> agl = new HashSet<AggregationFn>();
		AggregationFn maxGY = new MaxFn("gradyear");
		AggregationFn minSID = new MinFn("sid");
		AggregationFn countGY = new CountFn("gradyear");
		AggregationFn avgSID = new AvgFn("sid");
		AggregationFn sumSID = new SumFn("sid");
		AggregationFn countDistinctGY = new DistinctCountFn("gradyear");
		agl.add(maxGY);
		agl.add(minSID);
		agl.add(countGY);
		agl.add(avgSID);
		agl.add(sumSID);
		agl.add(countDistinctGY);
		// groupFlds can be empty but not null
		Set<String> groupFlds = new HashSet<String>();
		GroupByPlan gp = new GroupByPlan(p, groupFlds, agl, tx);
		Scan s = gp.open();

		s.beforeFirst();
		s.next();

		assertTrue(
				"*****GroupByTest: bad aggregation function",
				s.getVal(maxGY.fieldName()).equals(new IntegerConstant(2009))
						&& s.getVal(minSID.fieldName()).equals(
								new IntegerConstant(0))
						&& s.getVal(countGY.fieldName()).equals(
								new IntegerConstant(900))
						&& s.getVal(avgSID.fieldName()).equals(
								new DoubleConstant(449.5))
						&& s.getVal(sumSID.fieldName()).equals(
								new IntegerConstant(404550))
						&& s.getVal(countDistinctGY.fieldName()).equals(
								new IntegerConstant(50)));
		s.close();
		tx.commit();

	}

	@Test
	public void testGroupBy() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		Plan p = new TablePlan("student", tx);
		Set<AggregationFn> agl = new HashSet<AggregationFn>();
		Set<String> gbf = new HashSet<String>();
		gbf.add("gradyear");
		AggregationFn maxGY = new MaxFn("gradyear");
		AggregationFn minSID = new MinFn("sid");
		agl.add(maxGY);
		agl.add(minSID);

		GroupByPlan gp = new GroupByPlan(p, gbf, agl, tx);

		Scan s = gp.open();
		int groupNumber = 0;
		s.beforeFirst();
		while (s.next())
			groupNumber++;

		s.close();

		assertTrue("*****GroupByTest: bad group by operation",
				groupNumber == 50);

		tx.commit();

	}

	@Test
	public void testSort() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);

		Plan p = new TablePlan("student", tx);
		Scan s = p.open();
		int numRecs = 0;
		s.beforeFirst();
		while (s.next())
			numRecs++;
		s.close();

		// test asc sorting
		List<String> sf = new ArrayList<String>();
		sf.add("gradyear");
		sf.add("sid");
		SortPlan sp = new SortPlan(p, sf, tx);
		Scan ss = sp.open();
		int count = 0;
		ss.beforeFirst();
		int t = 0;
		while (ss.next()) {
			int nextVal = (Integer) ss.getVal("gradyear").asJavaVal();
			assertTrue("*****OrderByTest: bad asc sorting", nextVal >= t);
			t = nextVal;
			count++;
		}
		ss.close();
		assertTrue("*****OrderByTest: bad sorting", count == numRecs);

		// test desc sorting
		sf.clear();
		sf.add("sid");
		List<Integer> dir = new ArrayList<Integer>();
		dir.add(DIR_DESC);
		sp = new SortPlan(p, sf, dir, tx);
		ss = sp.open();
		count = 0;
		ss.beforeFirst();
		t = Integer.MAX_VALUE;
		while (ss.next()) {
			int nextVal = (Integer) ss.getVal("sid").asJavaVal();
			assertTrue("*****OrderByTest: bad desc sorting", nextVal <= t);
			t = nextVal;
			count++;
		}
		ss.close();
		tx.commit();
	}
}
