package org.vanilladb.core.query.algebra;

import java.util.ArrayList;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainQueryScan implements Scan{
	
	private int id;
	private StringBuffer buff=new StringBuffer();
	
	public ExplainQueryScan(Plan p){
		id=0;
		constructResultPlanTree(p, 0);
		countActualRecords(p);
	}
	
	/**
	 * Recursively traverse the underlying plans to construct the plan tree diagram.
	 * 
	 * @param px
	 * 		The plan currently traversed.
	 * @param level
	 * 		Traverse level, used to indicate the level and print corresponding indents
	 */
	private void constructResultPlanTree(Plan px, int level){
		
		for(int i=0; i<level; i++){
			buff.append("\t");
		}
		
		String className=px.getClass().getName().substring(px.getClass().getName().lastIndexOf('.')+1);
		if(level!=0)
			buff.append("-> ");
		buff.append(className);
		if(className.equals("SelectPlan")){
			if (!px.toString().isEmpty())
				buff.append(" pred("+px.toString()+")");
		}else if(className.equals("TablePlan")){
			buff.append(" on("+px.toString()+")");
		}
		
		buff.append(" (#blks="+px.blocksAccessed()+", #recs="+px.recordsOutput()+") \n");
		
		ArrayList<Plan> arr=px.getUnderlyingPlans();
		if (arr!=null){
			for(Plan pz:arr){
				constructResultPlanTree(pz, level+1);
			}
		}
		return;	
	}
	
	/** 
	 * Run the underlying query plan to get actual number of records.
	 * 
	 * @param p
	 * 		underlying plan (project plan)
	 *
	 */
	private void countActualRecords(Plan p){
		Scan tempScan=p.open();
		int count=0;
		tempScan.beforeFirst();
		while(tempScan.next()) count++;
		tempScan.close();
		buff.append("Actual #recs="+count);
	}
	
	@Override
	public void beforeFirst(){
		id=0;
		return;
	}
	
	/**
	 * use a id to indicate whether the user has accessed the result.
	 */
	@Override
	public boolean next(){
		if(id==0){
			id++;
			return true;
		}
		return false;
	}
	
	@Override
	public void close(){
		return;
	}
	
	@Override
	public boolean hasField(String fieldName){
		return false;
	}
	
	@Override
	public Constant getVal(String fldName) {
		if (fldName.equals("query-plan"))
			return new VarcharConstant(buff.toString());
		else
			throw new RuntimeException("field " + fldName + " not found.");
	}
	
	
}