package org.vanilladb.core;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.vanilladb.core.IsolatedClassLoaderSuite.IsolationRoot;
import org.vanilladb.core.algebra.materialize.MaterializeTest;
import org.vanilladb.core.query.algebra.QueryTest;
import org.vanilladb.core.query.algebra.parse.ParseTest;
import org.vanilladb.core.query.algebra.sql.ConstantRangeTest;
import org.vanilladb.core.query.planner.PlannerTest;
import org.vanilladb.core.query.planner.QueryVerifierTest;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.buffer.BufferTest;
import org.vanilladb.core.storage.file.FileTest;
import org.vanilladb.core.storage.index.btree.BTreeIndexTest;
import org.vanilladb.core.storage.index.hash.HashIndexTest;
import org.vanilladb.core.storage.metadata.MetadataTest;
import org.vanilladb.core.storage.metadata.statistics.HistogramTest;
import org.vanilladb.core.storage.record.RecordTest;
import org.vanilladb.core.storage.tx.TxTest;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyTest;
import org.vanilladb.core.storage.tx.concurrency.LockTableTest;
import org.vanilladb.core.storage.tx.recovery.RecoveryTest;
import org.vanilladb.core.util.ConstantTest;

@RunWith(IsolatedClassLoaderSuite.class)
@SuiteClasses({ FileTest.class, BufferTest.class, RecordTest.class,
		RecoveryTest.class, LockTableTest.class, ConcurrencyTest.class,
		MaterializeTest.class, TxTest.class, MetadataTest.class,
		QueryTest.class, PlannerTest.class, QueryVerifierTest.class,
		ParseTest.class, ConstantRangeTest.class, BTreeIndexTest.class,
		HashIndexTest.class, ConstantTest.class, HistogramTest.class })
@IsolationRoot(VanillaDB.class)
public class VanillaDbTestSuite {
	@BeforeClass
	public static void init() {
		// delete old test bed
		String homedir = System.getProperty("user.home");
		File oldDbDirectory = new File(homedir, ServerInit.dbName);
		if (oldDbDirectory.exists()) {
			for (String filename : oldDbDirectory.list())
				new File(oldDbDirectory, filename).delete();
			oldDbDirectory.delete();
		}
	}
}
