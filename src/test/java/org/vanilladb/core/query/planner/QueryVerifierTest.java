package org.vanilladb.core.query.planner;

import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.tx.Transaction;

public class QueryVerifierTest {
	private static Logger logger = Logger.getLogger(QueryVerifierTest.class
			.getName());

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

		ServerInit.initData();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN QUERY VERIFIER TEST");
	}

	@Before
	public void setup() {

	}

	@Test
	public void testInsertData() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		// test nonexistent table name
		try {
			String qry = "insert into non (sid, sname, majorid, gradyear) values (6, 'kay', 21, 2000)";
			Parser psr = new Parser(qry);
			InsertData data = (InsertData) psr.updateCommand();
			QueryVerifier.verifyInsertData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}

		// test nonexistent field name
		try {
			String qry = "insert into student (sid, sname, non) values (6, 'kay', 21, 2000)";
			Parser psr = new Parser(qry);
			InsertData data = (InsertData) psr.updateCommand();
			QueryVerifier.verifyInsertData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}

		// test non-valid value
		try {
			String qry = "insert into student (sid, sname, majorid, gradyear) values (6, 'kay', '1', 2000)";
			Parser psr = new Parser(qry);
			InsertData data = (InsertData) psr.updateCommand();
			QueryVerifier.verifyInsertData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}

	}

	@Test
	public void testModifyData() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		// test nonexistent table name
		try {
			String qry = "update notexisted set gradyear = add(gradyear, 1) where sid = sid";
			Parser psr = new Parser(qry);
			ModifyData data = (ModifyData) psr.updateCommand();
			QueryVerifier.verifyModifyData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}

		// test nonexistent field name
		try {
			String qry = "update student set gradyear=add(notexisted, 1) where sid=sid";
			Parser psr = new Parser(qry);
			ModifyData data = (ModifyData) psr.updateCommand();
			QueryVerifier.verifyModifyData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}
	}

	@Test
	public void testDeleteData() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		// test nonexistent table name
		try {
			String qry = "delete from notexisted where sid=sid";
			Parser psr = new Parser(qry);
			DeleteData data = (DeleteData) psr.updateCommand();
			QueryVerifier.verifyDeleteData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}
	}

	@Test
	public void testCreateTableData() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		// test existent table name
		try {
			String qry = "create table student (pid int, pname varchar(20))";
			Parser psr = new Parser(qry);
			CreateTableData data = (CreateTableData) psr.updateCommand();
			QueryVerifier.verifyCreateTableData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}
	}

	@Test
	public void testCreateIndexData() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		// test nonexistent table name
		try {
			String qry = "create index idx_student on nonexisted(gradyear)";
			Parser psr = new Parser(qry);
			CreateIndexData data = (CreateIndexData) psr.updateCommand();
			QueryVerifier.verifyCreateIndexData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}

		// test nonexistent field name
		try {
			String qry = "create index idx_student on student(notexisted)";
			Parser psr = new Parser(qry);
			CreateIndexData data = (CreateIndexData) psr.updateCommand();
			QueryVerifier.verifyCreateIndexData(data, tx);
			fail("QueryVerifierTest: bad verification");
			tx.commit();
		} catch (BadSemanticException e) {
			tx.rollback();
		}
	}

	@Test
	public void testCreateViewData() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		// test existent view name
		try {
			String qry = "create view student as select sname, dname from student, dept";
			VanillaDB.planner().executeUpdate(qry, tx);

			Parser psr = new Parser(qry);
			CreateViewData data = (CreateViewData) psr.updateCommand();
			QueryVerifier.verifyCreateViewData(data, tx);
			tx.commit();
			fail("QueryVerifierTest: bad verification");
		} catch (BadSemanticException e) {
			tx.rollback();
		}
		// test non-valid view definition
		try {
			String qry = "create view notexisted as select abc, efg from student, dept";
			Parser psr = new Parser(qry);
			CreateViewData data = (CreateViewData) psr.updateCommand();
			QueryVerifier.verifyCreateViewData(data, tx);
			tx.commit();
			fail("QueryVerifierTest: bad verification");
		} catch (BadSemanticException e) {
			tx.rollback();
		}
	}
}
