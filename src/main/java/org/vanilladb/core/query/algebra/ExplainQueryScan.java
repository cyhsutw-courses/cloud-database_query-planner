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
		System.out.println(buff.toString());
	}
	
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
		
		ArrayList<Plan> arr=px.getUnderlyingPlan();
		if (arr!=null){
			for(Plan pz:arr){
				constructResultPlanTree(pz, level+1);
			}
		}
		return;	
	}
	
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
		return;
	}
	
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