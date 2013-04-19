package org.vanilladb.core.query.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.aggfn.AggregationFn;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The verifier which examines the semantic of input query and update
 * statements.
 * 
 */
public class QueryVerifier {

	public static void verifyQueryData(QueryData data, Transaction tx) {
		List<Schema> schs = new ArrayList<Schema>(data.tables().size());
		List<QueryData> views = new ArrayList<QueryData>(data.tables().size());

		// examine the table name
		for (String tblName : data.tables()) {
			String viewdef = VanillaDB.mdMgr().getViewDef(tblName, tx);
			if (viewdef == null) {
				TableInfo ti = VanillaDB.mdMgr().getTableInfo(tblName, tx);
				if (ti == null)
					throw new BadSemanticException("table " + tblName
							+ " does not exist");
				schs.add(ti.schema());
			} else {
				Parser parser = new Parser(viewdef);
				views.add(parser.query());
			}
		}

		// examine the projecting field name
		for (String fldName : data.projectFields()) {
			boolean isValid = verifyField(schs, views, fldName);
			if (!isValid && data.aggregationFn() != null)
				for (AggregationFn aggFn : data.aggregationFn())
					if (fldName.compareTo(aggFn.fieldName()) == 0) {
						isValid = true;
						break;
					}
			if (!isValid)
				throw new BadSemanticException("field " + fldName
						+ " does not exist");
		}

		// examine the aggregation field name
		if (data.aggregationFn() != null)
			for (AggregationFn aggFn : data.aggregationFn()) {
				String aggFld = aggFn.argumentFieldName();
				if (!verifyField(schs, views, aggFld))
					throw new BadSemanticException("field " + aggFld
							+ " does not exist");
			}

		// examine the grouping field name
		if (data.groupFields() != null)
			for (String groupByFld : data.groupFields()) {
				if (!verifyField(schs, views, groupByFld))
					throw new BadSemanticException("field " + groupByFld
							+ " does not exist");
			}

		// Examine the sorting field name
		if (data.sortFields() != null)
			for (String sortFld : data.sortFields()) {
				if (!verifyField(schs, views, sortFld))
					throw new BadSemanticException("field " + sortFld
							+ " does not exist");
			}
	}

	public static void verifyInsertData(InsertData data, Transaction tx) {
		// examine table name
		TableInfo ti = VanillaDB.mdMgr().getTableInfo(data.tableName(), tx);
		if (ti == null)
			throw new BadSemanticException("table " + data.tableName()
					+ " does not exist");

		Schema sch = ti.schema();
		List<String> fields = data.fields();
		List<Constant> vals = data.vals();

		// examine whether values have the same size with fields
		if (fields.size() != vals.size())
			throw new BadSemanticException("#fields and #values does not match");

		String field;
		Constant val;

		// examine the fields existence and type
		for (int i = 0; i < fields.size(); i++) {
			field = fields.get(i);
			val = vals.get(i);
			// check field existence
			if (!sch.hasField(field))
				throw new BadSemanticException("field " + field
						+ " does not exist");
			// check whether field match value type
			if (!matchFieldAndConstant(sch, field, val))
				throw new BadSemanticException("field " + field
						+ " doesn't match corresponding value in type");
		}
	}

	public static void verifyModifyData(ModifyData data, Transaction tx) {
		// examine Table name
		TableInfo ti = VanillaDB.mdMgr().getTableInfo(data.tableName(), tx);
		if (ti == null)
			throw new BadSemanticException("table " + data.tableName()
					+ " does not exist");

		// examine the fields existence and type
		Schema sch = ti.schema();
		for (String field : data.targetFields()) {
			// check field existence
			if (!sch.hasField(field))
				throw new BadSemanticException("field " + field
						+ " does not exist");
			// check whether field match new value type
			if (!data.newValue(field).isApplicableTo(sch))
				throw new BadSemanticException("new value of field " + field
						+ " does not exist");
		}
	}

	public static void verifyDeleteData(DeleteData data, Transaction tx) {
		// examine table name
		TableInfo ti = VanillaDB.mdMgr().getTableInfo(data.tableName(), tx);
		if (ti == null)
			throw new BadSemanticException("table " + data.tableName()
					+ " does not exist");
	}

	public static void verifyCreateTableData(CreateTableData data,
			Transaction tx) {
		// examine table name
		TableInfo ti = VanillaDB.mdMgr().getTableInfo(data.tableName(), tx);
		if (ti != null)
			throw new BadSemanticException("table " + data.tableName()
					+ " already exist");
	}

	public static void verifyCreateIndexData(CreateIndexData data,
			Transaction tx) {
		// examine table name
		String tableName = data.tableName();
		TableInfo ti = VanillaDB.mdMgr().getTableInfo(tableName, tx);
		if (ti == null)
			throw new BadSemanticException("table " + tableName
					+ " does not exist");

		Schema sch = ti.schema();
		String fieldName = data.fieldName();
		// examine if column exist
		if (!sch.hasField(fieldName))
			throw new BadSemanticException("field " + fieldName
					+ " does not exist in table " + tableName);

		// examine the index
		Map<String, IndexInfo> indexInfoes = VanillaDB.mdMgr().getIndexInfo(
				tableName, tx);
		if (indexInfoes.containsKey(fieldName))
			throw new BadSemanticException("field" + fieldName
					+ " has already been indexed");
	}

	public static void verifyCreateViewData(CreateViewData data, Transaction tx) {
		// examine view name
		if (VanillaDB.mdMgr().getViewDef(data.viewName(), tx) != null)
			throw new BadSemanticException("view name duplicated");

		// examine query data
		verifyQueryData(data.viewDefData(), tx);
	}

	private static boolean matchFieldAndConstant(Schema sch, String field,
			Constant val) {
		Type type = sch.type(field);
		if (type.isNumeric() && val instanceof VarcharConstant)
			return false;
		else if (!type.isNumeric() && !(val instanceof VarcharConstant))
			return false;
		else
			return true;
	}

	private static boolean verifyField(List<Schema> schs,
			List<QueryData> views, String fld) {
		boolean isValid = false;
		for (Schema s : schs)
			if (s.hasField(fld)) {
				isValid = true;
				break;
			}
		if (!isValid)
			for (QueryData queryData : views)
				if (queryData.projectFields().contains(fld)) {
					isValid = true;
					break;
				}
		return isValid;
	}
}
