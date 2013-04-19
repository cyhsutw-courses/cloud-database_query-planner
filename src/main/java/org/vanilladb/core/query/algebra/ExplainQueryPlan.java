package org.vanilladb.core.query.algebra;

import java.util.ArrayList;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class ExplainQueryPlan implements Plan{
	
	
	private Plan p;
	private Schema schema=new Schema();
	
	
	public ExplainQueryPlan(Plan p){
		this.p=p;
		schema.addField("query-plan", Type.VARCHAR(500));
	}
	
	@Override
	public Scan open(){
		return new ExplainQueryScan(p);
	}
	
	@Override
	public long blocksAccessed(){
		return 0;
	}
	
	@Override
	public Schema schema(){
		return schema;
	}
	
	@Override
	public Histogram histogram(){
		return null;
	}
	
	@Override 
	public long recordsOutput(){
		return 0;
	}
	
	@Override
	public ArrayList<Plan> getUnderlyingPlan(){
		ArrayList<Plan> arr = new ArrayList<Plan>();
		arr.add(p);
		return arr;
	}
} 