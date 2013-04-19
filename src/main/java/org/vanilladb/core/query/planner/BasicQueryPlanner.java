package org.vanilladb.core.query.planner;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.query.algebra.ExplainQueryPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProductPlan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The simplest, most naive query planner possible.
 */
public class BasicQueryPlanner implements QueryPlanner {

	/**
	 * Creates a query plan as follows. It first takes the product of all tables
	 * and views; it then selects on the predicate; and finally it projects on
	 * the field list.
	 */
	@Override
	public Plan createPlan(QueryData data, Transaction tx) {
		QueryVerifier.verifyQueryData(data, tx);
		// Step 1: Create a plan for each mentioned table or view
		List<Plan> plans = new ArrayList<Plan>();
		for (String tblname : data.tables()) {
			String viewdef = VanillaDB.mdMgr().getViewDef(tblname, tx);
			if (viewdef != null)
				plans.add(VanillaDB.planner().createQueryPlan(viewdef, tx));
			else
				plans.add(new TablePlan(tblname, tx));
		}
		// Step 2: Create the product of all table plans
		Plan p = plans.remove(0);
		for (Plan nextplan : plans)
			p = new ProductPlan(p, nextplan);
		// Step 3: Add a selection plan for the predicate
		p = new SelectPlan(p, data.pred());
		// Step 4: Add a group-by plan if specified
		if (data.groupFields() != null) {
			p = new GroupByPlan(p, data.groupFields(), data.aggregationFn(), tx);
		}
		// Step 5: Project onto the specified fields
		p = new ProjectPlan(p, data.projectFields());
		// Step 6: Add a sort plan if specified
		if (data.sortFields() != null)
			p = new SortPlan(p, data.sortFields(), data.sortDirections(), tx);
		
		//TODO
		//add to explain plan if needed
		if(data.explain())
			p = new ExplainQueryPlan(p);
		
		return p;
	}
}
