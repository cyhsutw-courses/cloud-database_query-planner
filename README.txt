-----------
修改過的檔案
-----------
query.algebra
	-Plan
	-ProductPlan
	-ProjectPlan
	-SelectScan
	-TablePlan

query.algebra.index
	-IndexJoinPlan
	-IndexSelectPlan

query.algebra.materialize
	-GroupByPlan
	-MaterializePlan
	-MergeJoinPlan
	-SortPlan
	
query.algebra.multibuffer
	-HashJoinPlan
	-MultiBufferProductPlan

在query.algebra中的 Plan interface 新增一個method： getUnderlyingPlans 
在每個class中把他下層的Plan存下來，讓之後的ExplainQueryPlan, ExplainQueryScan能夠存取並取得統計資料。

-------------------------------

query.algebra
	-TablePlan
	（Override toString，用來取得要做TableScan的Table Name）
	-SelectScan		
	（Override toString，用來取得Select的Predicate）

-------------------------------

query.parse
	-QueryData
	(新增一個private field - explain 來記錄是否為explain query )
	-Parser
	(修改 query()，讓他可以parse是否有 explain )

-------------------------------

query.planner
	-BasicQueryPlanner
query.planner.opt
	-HeuristicQueryPlanner

修改 createPlan() 在return 之前判斷是否需要explain並且用ExplainQueryPlan包起來

-------------------------------

util
	-ConsoleSQLInterpreter
修正 explain select 會被判斷成update query 的 bug


************************************************************************

---------
新增的檔案
---------

query.algebra
	-ExplainQueryPlan
	-ExplainQueryScan

Refer to the comments.


************************************************************************

Design idea

1. original query

	ProjectPlan
		|
	SelectPlan
		|
	ProductPlan
	  /      \
TablePlan  TablePlan


2. Wrap ExplainQueryPlan
	
	ExplainQuery
		|
	ProjectPlan
		|
	SelectPlan
		|
	ProductPlan
	  /      \
TablePlan  TablePlan


接下來設計一個 ExplainQueryScan 來處理並生成需要的資料

為什麼想這麼做？
	因為發現幾乎所有Plan的constructer 都會吃一個底層的plan
	所以想說直接往上包一個ExplainQueryPlan，然後設計對應的Scan來Handle
	並且用getUnderlyingPlans來抓取下層的Plan並取得需要的統計資料
	意外發現ConsoleSqlInterpreter有一些小Bug XD

