                      THE VANILLADB DATABASE SYSTEM
                  General Information and Instructions

VanillaDB is enhanced based on SimpleDB.

This document contains the following sections:
    I.    SimpleDB 2.10 Release Notes (by Edward Sciore)
    II.   Server Installation
    III.  Running the Server
    IV.   Running Client Programs
    V.    VanillaDB Limitations
    VI.   The Organization of the Server Code
    VII.  Test Suite for VanillaDB
    VIII.  Enhancements based on SimpleDB (by NetDB)


I. SimpleDB 2.10 Release Notes (by Edward Sciore):
   SimpleDB's web site:
   	* www.cs.bc.edu/~sciore/simpledb/intro.html
   
   This release of the SimpleDB system is Version 2.10, which was
   uploaded on January 1, 2013. This release provides the following
   fixes to Version 2.9:

    * The files simpledb.Startup and remote.SimpleDriver have been changed 
      to use a server-specific registry, instead of forcing the user to 
      run rmiregistry as a separate process.
    * A bug was fixed in the file SortScan.java.
    * The new client file StudentMajorNoServer was added.

   SimpleDB is distributed in a WinZip-formatted file. This file contains
   four items:

    * The folder simpledb, which contains the server-side Java code.
    * The folder javadoc, which contains the JavaDoc documentation 
      of the above code.
    * The folder studentClient, which contains some client-side code 
      for an example database.
    * This document.

   The author welcomes all comments, including bug reports, suggestions
   for improvement, and anectodal experiences.  His email address is: 
   sciore@bc.edu
  
II. Installation Instructions:

  1)  Install the Java 1.6 or 1.5 SDK. Java 1.6 is obviously preferable.

  2)  If you do install Java 1.5, you need to make some minor changes 
      to the package org.vanilladb.core.remote.jdbc:
    
      * The classes named xxxAdapter provide default implementations of 
        the interfaces in java.sql. Java 1.6 added several extra methods 
        to these interfaces. If you are using Java 1.5, just comment out 
        those methods. (You can tell which ones they are because you'll 
        get an error when you try to compile them.)
      
      * The classes named VanillaDbXXX call the SQLException constructor 
        with a Throwable argument.  This constructor is new to version
        1.6. To use in 1.5, rewrite the code "throw new SQLException(e)"
        to be "throw new SQLException(e.getMessage())".

  3)  Decide where you want the server-side software to go. Let's assume 
      that the code will go in the folder C:\javalib in Windows, or the 
      folder ~/javalib in UNIX or MacOS.

  4)  Add that folder to your classpath. In other words, the javalib 
      folder must be mentioned in your CLASSPATH environment variable.
    
      * In UNIX, your home directory has an initialization file, 
        typically called .bashrc.  If the file does not set CLASSPATH,
        add the following line to the file:  
               CLASSPATH =.:~/javalib     
               
        Here, the ":" character separates folder names.  The command 
        therefore says that the folder "." (i.e., your current diretory) 
        and "~/javalib" are to be searched whenever Java needs to find a 
        class.  If the file already contains a CLASSPATH setting, modify 
        it to include the javalib directory.
 
      * In Windows, you must set the CLASSPATH variable via the System 
        control panel.  From that control panel, choose the advanced tab 
        and click on the environment variables button.  You want to have 
        a user variable named CLASSPATH that looks like this:
               .;C:\javalib
               
        Here, the ";" character separates the two folder names.

  5)  Copy the vanilladb folder from the distribution file to that
      folder. Within the vanilladb folder should be subfolders 
      containing all of the code for VanillaDB.


III. Running the Server:

  VanillaDB has a client-server architecture. You run the server code on 
  a host machine, where it will sit and wait for connections from clients.
  It is able to handle multiple simultaneous requests from clients, 
  each on possibly different machines. You can then run a client program
  from any machine that is able to connect to the host machine.

  To run the VanillaDB server, run Java on the org.vanilladb.core.
  server.Startup class. You must pass in the name of a folder that 
  VanillaDB will use to hold the database. For example in Windows, if you 
  execute the command:
      
         > start java org.vanilladb.core.server.Startup studentdb
             
  then the server will run in a new window, using studentdb as the
  database folder. You can execute this command from any directory;
  the server will always use the studentdb folder that exists in your
  home directory. If a folder with that name does not exist, then
  one will be created automatically.
 
  If everything is working correctly, when you run the server with a
  new database folder the following will be printed in the server 
  window:

      creating new database
      new transaction: 1
      transaction 1 committed
      database server ready

  If you run the server with an existing database folder, the following
  will be printed instead:

      recovering existing database
      database server ready

  In either case, the server will then sit awaiting connections from
  clients.  As connections arrive, the server will print additional
  messages in its window.


IV. Running Client Programs 

  The VanillaDB server accepts connections from any JDBC client. The client
  program makes its connection via the following code:
            Driver d = new VanillaDbDriver();
            String host = "mymachine.com"; //any DNS name or IP address
            String url = "jdbc:vanilladb://" + host;
            Connection conn = d.connect(url, null);

  Note that VanillaDB does not require a username and password, although
  it is easy enough to modify the server code to do so.

  The driver class VanillaDbDriver is contained in the package 
  org.vanilladb.core.remote.jdbc, along with the other classes that 
  it needs. A client program will not run unless this package in its classpath.
  Note that you could install the entire VanillaDB server code on a client 
  machine, but that is overkill. All you need is 
  org.vanilladb.core.remote.jdbc.

V. VanillaDB Limitations

  VanillaDB is a teaching tool. It deliberately implements a tiny subset
  of SQL and JDBC, and (for simplicity) imposes restrictions not present
  in the SQL standard.  Here we briefly indicate these restrictions.


  VanillaDB SQL
  
  A query in VanillaDB consists only of select-from-where clauses in which
  the select clause contains a list of fieldnames (without the AS 
  keyword), and the from clause contains a list of tablenames (without
  range variables).
 
  The where clause is optional. The only Boolean operator is and. Unlike 
  standard SQL, there are no other Boolean operators and no parentheses. The
  group by, order by clauses and partial aggregation functions are supported. 
  Arithmetic expression is only supported in update command.

  Views can be created, but a view definition can be at most 100 characters.
 
  Because there are no renaming, all field names in a query must be disjoint. 
  Other restrictions:

    * The "*" abbreviation in the select clause is not supported.
    * There are no null values.
    * There are no explicit joins or outer joins in the from clause.
    * The union and except keywords are not supported.
    * Insert statements take explicit values only, not queries.
    * Update statements can have only one assignment in the set clause.
	
  VanillaDB Syntax
  
     // Predicate
    <Field>				:= IdTok
    <Constant>			:= StrTok | NumericTok
    <Expression>	    := <Field> | <Constant>
    <BinaryArithmeticExpression>	
                        := ADD(<Expression>, <Expression>) | 
                           SUB(<Expression>, <Expression>) | 
                           MUL(<Expression>, <Expression>) |
                           DIV(<Expression>, <Expression>)
    <Term>				:= <Expression> = <Expression>  | 
  						   <Expression> > <Expression>  |
  						   <Expression> >= <Expression> | 
  						   <Expression> < <Expression>  | 
  						   <Expression> <= <Expression>
    <Predicate>			:= <Term> [ AND <Predicate>	]

	// Query
    <Query>				:= SELECT <ProjectSet> FROM <TableSet> 
                           [ WHERE <Predicate> ] [ GROUP BY <IdSet> ] 
                           [ ORDER BY <SortList> [ DESC | ASC ] ]
	<IdSet>				:= <Field> [ , <IdSet> ]
	<TableSet> 			:= IdTok [ , <TableSet> ]
	<AggFn>				:= AVG(<Field>) | COUNT(<Field>) |
						   COUNT(DISTINCT <Field>) | MAX(<Field>) |
						   MIN(<Field>) | SUM(Field>)
	<ProjectSet>		:= <Field> | <AggFn> [ , <ProjectSet>]
	<SortList>			:= <Field> | <AggFn> [ , <SortList>]

	// Update
	<UpdateCmd>			:= <Insert> | <Delete> | <Modify> | <Create>
	<Create>			:= <CreateTable> | <CreateView> | <CreateIndex>
	<Insert>			:= INSERT INTO IdTok ( <FieldList> ) 
						   VALUES ( <ConstantList> )
	<FieldList> 		:= <Field> [ , <Field> ]
	<ConstantList> 		:= <Constant> [ , <Constant> ]
	<Delete>			:= DELETE FROM IdTok [ WHERE <Predicate> ]
	<Modify>			:= UPDATE IdTok SET <ModifyTermList> 
						   [ WHERE <Predicate> ]
	<ModifyExpression>	:= <Expression> | <BinaryArithmeticExpression>
	<ModifyTermList>	:= <Field> = <ModifyExpression> [ , <ModifyTermList> ]
	<CreateTable>		:= CREATE TABLE IdTok ( <FieldDefs> )
	<FieldDefs> 		:= <FieldDef> [ , <FieldDef> ]
	<FieldDef>			:= IdTock <TypeDef>
	<TypeDef>			:= INT | LONG | DOUBLE | VARCHAR ( NumericTok ) 
	<CreateView>		:= CREATE VIEW IdTok AS <Query>
	<CreateIndex>		:= CREATE INDEX IdTok ON IdTok ( <Field> )  
  
  VanillaDB JDBC
  
  VanillaDB implements only the following JDBC methods:

   Driver

      public Connection connect(String url, Properties prop);
      // The method ignores the contents of variable prop.

   Connection

      public Statement createStatement();
      public void      close();
	  public void      setAutoCommit(boolean autoCommit);
	  public void	   setReadOnly(boolean readOnly);
	  public void      setTransactionIsolation(int level);
	  public boolean   getAutoCommit();
      public int       getTransactionIsolation();
       
   Statement

      public ResultSet executeQuery(String qry);
      public int       executeUpdate(String cmd);

   ResultSet

      public boolean   next();
      public int       getInt();
      public String    getString();
      public void      close();
      public ResultSetMetaData getMetaData();

   ResultSetMetaData

      public int        getColumnCount();
      public String     getColumnName(int column);
      public int        getColumnType(int column);
      public int        getColumnDisplaySize(int column);



VI. The Organization of the Server Code

  VanillaDB is usable without knowing anything about what the code looks
  like. However, the entire point of the system is to make the code
  easy to read and modify.  The basic packages in VanillaDB are structured
  hierarchically, in the following order:

    * storage.file (Manages OS files as a virtual disk.)
    * storage.log (Manages the log.)
    * storage.buffer (Manages a buffer pool of pages in memory that acts 
    		          as a cache of disk blocks.)
    * storage.tx (Implements transactions with multi-granularity locking.
                  Does concurrency control and logging.)
    * storage.record (Implements fixed-length records inside of pages.)
    * storage.metadata (Maintains metadata in the system catalog.)
    * query.algebra (Implements relational algebra operations.  Each 
                     operation has a plan class, used by the planner, and
                     a scan class, used at runtime.)
    * query.parse (Implements the parser.)
    * query.planner (Implements a naive planner for SQL statements.)
    * sql (Implements the supported SQL and constants.)
    * remote (Implements the server using RMI.)
    * server (The place where the startup and initialization code live. 
              The class Startup contains the main method.)

  The basic server is exceptionally inefficient.  The following packages
  enable more efficient query processing:

    * storage.index (Implements static hash and btree indexes.)
    * query.algebra.index(Implements relational algebra operations to take 
    		              advantage of them.)
    * query.algebra.materialize (Implements implementations of the relational 
                                 operators materialize, sort, groupby, and
                                 mergejoin.)
    * query.algebra.multibuffer (Implements modifications to the sort and 
    						     product operators, in order to make optimum 
    						     use of available buffers.)
    * query.planner.opt (Implements a heuristic query optimizer)
    * query.planner.index (Implements a update planner based on index)
 
   The textbook "Database Design and Implementation" describes the original
   SimpleDB packages in considerably more detail. For further information, go
   to the URL www.wiley.com/college/sciore
   
VII. Test Suite for VanillaDB

  The Java files in this archive constitute a test suite for the VanillaDB
  database system.  The suite runs on the server machine, and embeds the
  VanillaDB server code.  Thus you don't have to have the RMI registry 
  running; you just need to have the vanilladb code in your classpath.
  You then just run the test code.
  
  This test suite is particularly useful if you are modifying the VanillaDB
  code.  After making changes, you can run the tests to ensure that you
  did not break anything.  These tests have not been made publicly 
  available, because instructors may want to assign the creation of some
  of the test code to their students.  By making the code available only
  to instructors, they can choose which portions of it (if any) to
  release to their students.
  
  The test suite contains a class for each VanillaDB package, each having 
  a method named "test".  That method runs various diagnostic tests.
  The test will print an error message if it detects something wrong.
  (Error messages are output lines prepended by the characters "*****".)
  
  The main method of the suite is in the class VanillaDbTestSuite. It opens 
  a database named "testvanilladb", and calls each package's test method.  
  
  These test methods are somewhat rudimentary.  There are many features  
  that are not tested.  Improvements are welcome.  Please send email to
  sciore@bc.edu.

VIII. Enhancements based on SimpleDB (by NetDB)
  
  In VanillaDB, NetDB has implemented some enhancements to improve SimpleDB
  as summarized below:
  
    
  Enhancements:
  	
  	> File level
       > FileMgr implements "atomic write failure" that is required by LogMgr. 
         A write to block is reflected sequentially two times to disk with 
         check sum, so one copy can be used to recover another
       * Block renames to BlockId (update the API doc as well, specially those
         in BasicBufferMgr/BufferMgr/Buffer)
       * Add long variable type
         
    > Buffer level
       * Decouple buffer information from transaction packages, maintain 
         buffer pinned by tx in BufferMgr
       * Synchronized access to buffers
       * Maintain dirty buffer modified by tx in BufferMgr
       * Repin buffers holding by tx when pin timeout 
       * Add long variable type
       * Refine findExistingBuffer
       
    > Transaction level
       * Only maintain lifecycle
       * Add interfaces TransactionLifecycleListener and
         TransactionStartListener for RecoveryMgr, ConcurrencyMgr and BufferMgr
       * Transaction package across all packages
       
    > Concurrency level 
       * Implement TransactionLifecycleListener
       * Multi-granularity locking and index locking
       * Locktable responses for checking different lock types compatibility
         and maintains currently holding locks by a tx 
        
    > Recovery level
       * Implement TransactionLifecycleListener
       * Add SetLongRecord to support long type
      
    > Record level
       * Access API from buffer level directly
       * RID renames to RecordId (update the API doc as well)
       * Open the file until client really access record 
      
    > Index level
       * Access API from buffer level directly 
       * Index crabbing  
       * Btree index supports range query
      
    > Query level
       * Implement aggreagtion functions
         * SUM, AVG
      	 * MAX, MIN
      	 * COUNT DISTINCT 
         * COUNT
       * sortPlan support sort operations ASC and DESC
       * Add binary arithmetic on two expressions as expression type
      	 * ADD(e1, e2) 
      	 * SUB(e1, e2)  
      	 * DIV(e1, e2)   
      	 * MUL(e1, e2)  
       * Supprot different types of Term
      	 * term > term
      	 * term >= term
      	 * term < term
      	 * term <= term
       * Refine the reduction factor methods of Predicate and Term to support 
      	 SelectPlan estimation (change factor from int to double)
      	 * valueReductionFactor
      	 * recordReductionFactor
       * Add BigIntConstant, DoubleConstant
       * Add Constant arithmetic operation
       * SelectPlan supports range query
       * Add preprocessingCost method into all plans (but current planner only 
      	 take into account the recordOutput when planning)
      	      	
    > Parse level
       * Support binary arithmetic on two expression in update command
       * Extend predicate to support > >= <= < term
       * Order by ASC and DESC
       * Aggregation function and group by
       * Parse double constant
       * Parse '_' as word 
      
    > Metadata level
       * Add sample-based histogram
       * Separate statistic info from table/index info and add TableStatInfo/
         IndexStatInfo
    
    > Planner level
       * Support group by and order by operations      
    
    > Remote
       * Add JDBC API setReadOnly() (throws exception if it execute update)
         to support TableScan(Query only)
       * Add setTransactionIsolationLevel() and setAutoCommit()
        
  Testing:
    > ConcurrencyTest add different isolation level test case      
    
  Benchmarking:
  	> Test if it helps for ConcurrencyMgr to obtain multiple locks of different 
  	  granularities or different types (S/X) in a synchronized block over 
  	  lockTbl 
         
         
       