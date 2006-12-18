/**
 * Copyright (c) 2003 Daffodil Software Ltd all rights reserved.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 * There are special exceptions to the terms and conditions of the GPL
 * as it is applied to this software. See the GNU General Public License for more details.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

package com.daffodilwoods.replication.DBHandler;

import java.sql.*;
import java.util.*;

import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.column.*;
import org.apache.log4j.Logger;

/**
 * In replicator because of the feature of multidatabase synchronization, It has
 * become very much necessary to handle different databases deifferently.
 * By identifying the database from Utility class a proper databasehandler is returned,
 * And every handler extends this abstract class. So the abstract methods declared
 * in it are defined accordingly in different handlers.
 * This Class is solely defined for table creations and for appropriate column
 * datatype sequence generation.
 *
 */

public abstract class AbstractDataBaseHandler   {
  protected final static String publication_TableName = "Rep_Publications";
  protected final static String subscription_TableName = "Rep_Subscriptions";
  protected final static String bookmark_TableName = "Rep_BookMarkTable";
  protected final static String rep_TableName = "Rep_RepTable";
  protected final static String log_Table = "Rep_LogTable";
  protected final static String Schedule_TableName = "Rep_ScheduleTable";
  public final static String ignoredColumns_Table = "Rep_IgnoredColumnsTable";
  public final static String trackReplicationTablesUpdation_Table = "Rep_TrackRepTabUpdation";
  public final static String trackPrimaryKeyUpdation_Table = "Rep_trackPrimaryKey";

  protected ConnectionPool connectionPool;
  protected String localServerName;
  protected static int vendorType = -1;
  protected static Logger log =Logger.getLogger(AbstractDataBaseHandler.class.getName());
  /**
   * Creates Log Table and Rep Table.
   * @param pubName
   * @throws SQLException
   * @throws RepException
   */

  abstract protected void createSuperLogTable(String pubName) throws SQLException, RepException;

  abstract protected void createRepTable(String pubName) throws  SQLException, RepException;

  public void CreateSequenceOnLogTable(String pubSubName) throws SQLException,
      RepException {
    // This method os currently implemented by OracleHandler
  }

  public void CreateSequenceOnRepTable(String pubSubName) throws SQLException,
      RepException {
    // This method os currently implemented by OracleHandler
  }

  public void CreateSequenceOnShadowTable(String pubSubName) throws
      SQLException, RepException {
    // This method os currently implemented by OracleHandler
  }

  /**
   * This is a very important function which sets the parameter (instance of
   * Class TypeInfo) with the corresponding typeName of the sqlType for different
   * databases. So that we can make corresponding queries.
   * @param typeInfo
   * @param rs
   * @throws RepException
   * @throws SQLException
   */

  abstract public void setTypeInfo(TypeInfo typeInfo, ResultSet rs) throws
      RepException, SQLException;
  /**
   * This method is added for checking of schema.If schema exist in database
   * then it return true else false.All the database support the schema except
   * My SQL and Firebird.
   * @return boolean
   */

  abstract public boolean isSchemaSupported();
  abstract public boolean isDataTypeOptionalSizeSupported(TypeInfo typeInfo);

  /**
   * According to the sqlType, different column's for different databases are handled
   * saperately. It returns the object of AbstractColumnObject which is extended
   * by each type of column Object classes. This fundamental is used basically for
   * handeling Blob and Clob type of columns.
   * @param typeInfo
   * @return
   * @throws RepException
   */

  abstract public AbstractColumnObject getColumnObject(TypeInfo typeInfo) throws RepException;

  /**
   * Creates Shadow Table as per the database.
   * @param pubsubName
   * @param tableName
   * @param allColSequence
   * @throws RepException
   */

  abstract public void createShadowTable(String pubsubName, String tableName,  String allColSequence,String[] primaryColumns) throws RepException;

  /**
   * Creates ShadowTable triggers on different replication tables.
   * @param pubName
   * @param tableName
   * @param colNameDataType
   * @param primCols
   * @throws RepException
   */

  abstract public void createShadowTableTriggers(String pubName,
                                                 String tableName,
                                                 ArrayList colNameDataType,
                                                 String[] primCols) throws RepException;

  abstract public boolean isPrimaryKeyException(SQLException ex) throws SQLException;

  /**
   * It checks from the  sqlType values whether the datatype is optional size
   * supported or not.
   * @param typeInfo
   * @return
   */

  public void setDefaultSchema(Connection connection) throws RepException {
// no need to implement, currently daffodildb implements its
  }

  /**
   * This method is implemented in child classes if precision exceed the
   * maximum precision of the data type
   * @param columnSize int
   * @throws RepException
   * @return int
   */
  public int getAppropriatePrecision(int columnSize, String datatypeName) {
    return columnSize;
  }

  public void createRemoteSystemTables(String pubName) throws RepException {
    try {
      createPublicationTable(pubName);
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      // Ignore the Exception, System Table alredy exists.
    }
    try {
      createBookMarkTable(pubName);
    }
    catch (RepException ex1) {
      throw ex1;
    }
    catch (SQLException ex1) {
      // Ignore the Exception, System Table alredy exists.
    }
    try {
      createSuperLogTable(pubName);
    }
    catch (RepException ex2) {
      throw ex2;
    }
    catch (SQLException ex2) {
      // Ignore the Exception, System Table alredy exists.
    }

    try {
      CreateSequenceOnLogTable(pubName);
    }
    catch (RepException ex4) {
      throw ex4;
    }
    catch (SQLException ex4) {
    }

    try {
      createRepTable(pubName);
    }
    catch (RepException ex3) {
      throw ex3;
    }
    catch (SQLException ex3) {
      // Ignore the Exception, System Table alredy exists.
    }
    try {
      CreateSequenceOnRepTable(pubName);
    }
    catch (RepException ex5) {
      throw ex5;
    }
    catch (SQLException ex5) {
    }

    try {
      createIgnoredColumnsTable(pubName);
    }
    catch (RepException ex6) {
      throw ex6;
    }
    catch (SQLException ex6) {
    }

    // work for TrackReplicationTablesUpdation table
    try {
      createTrackReplicationTablesUpdationTable(pubName);
    }
    catch (RepException ex5) {
      throw ex5;
    }
    catch (SQLException ex5) {
    }
    // trigger for track table
    try {
      createTriggerForTrackReplicationTablesUpdationTable(pubName);
    }
    catch (RepException ex5) {
      throw ex5;
    }
    catch (SQLException ex5) {
    }


  }

  public int getvendorName() {
    return vendorType;
  }

  public void createClientSystemTables(String subName) throws RepException {
    try {
      createSubscriptionTable(subName);
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
    RepConstants.writeERROR_FILE(ex);
      // Ignore the Exception, System Table alredy exists.
    }
    try {
      createBookMarkTable(subName);
    }
    catch (RepException ex1) {
      throw ex1;
    }
    catch (SQLException ex1) {
      // Ignore the Exception, System Table alredy exists.
    }
    try {
      createSuperLogTable(subName);
    }
    catch (RepException ex2) {
      throw ex2;
    }
    catch (SQLException ex2) {
      // Ignore the Exception, System Table alredy exists.
    }

    try {
      CreateSequenceOnLogTable(subName);
    }
    catch (RepException ex4) {
      throw ex4;
    }
    catch (SQLException ex4) {}
    try {
      createRepTable(subName);
    }
    catch (RepException ex3) {
      throw ex3;
    }
    catch (SQLException ex3) {
      // Ignore the Exception, System Table alredy exists.
    }
    try {
      CreateSequenceOnRepTable(subName);
    }
    catch (RepException ex5) {
      throw ex5;
    }
    catch (SQLException ex5) {}
    try {
      createScheduleTable(subName);
    }
    catch (RepException ex) {
      throw ex;
    }
  catch (SQLException ex5) {
  }

  try {
    createIgnoredColumnsTable(subName);
  }
  catch (RepException ex6) {
    throw ex6;
  }
  catch (SQLException ex6) {
  }

  // work for TrackReplicationTablesUpdation table
  try {
    createTrackReplicationTablesUpdationTable(subName);
  }
  catch (RepException ex5) {
    throw ex5;
    }
  catch (SQLException ex5) {
  }
  // trigger for track table
  try {
    createTriggerForTrackReplicationTablesUpdationTable(subName);
  }
  catch (RepException ex5) {
    throw ex5;
  }
  catch (SQLException ex5) {
  }
}



  protected void createPublicationTable(String pubName) throws RepException, SQLException {
    StringBuffer pubsTableQuery = new StringBuffer();
    pubsTableQuery.append(" Create Table ")
        .append(getPublicationTableName())
        .append(" ( " + RepConstants.publication_pubName1 + " varchar(255) , " +
                RepConstants.publication_conflictResolver2 + " varchar(255) , ")
        .append(" " + RepConstants.publication_serverName3 +
                " varchar (255) , Primary Key (" +
                RepConstants.publication_pubName1 + ") ) ");
    runDDL(pubName, pubsTableQuery.toString());
  }

  protected void createSubscriptionTable(String pubName) throws RepException,
      SQLException {
    String subsTableQuery = " Create Table  "
        + getSubscriptionTableName()
        + " ( " + RepConstants.subscription_subName1 + " varchar(255) , "
        + "   " + RepConstants.subscription_pubName2 + " varchar(255)  , "
        + "   " + RepConstants.subscription_conflictResolver3 +
        " varchar(255) , "
        + "   " + RepConstants.subscription_serverName4 + " varchar (255) , "
        + "   Primary Key (" + RepConstants.subscription_subName1 + ") ) ";
    runDDL(pubName, subsTableQuery);
  }

  protected void createBookMarkTable(String pubName) throws SQLException,
      RepException {
    StringBuffer bookmarkTableQuery = new StringBuffer();
    bookmarkTableQuery.append(" Create Table ")
        .append(getBookMarkTableName())
        .append(" ( " + RepConstants.bookmark_LocalName1 + " varchar(255) , " +
                RepConstants.bookmark_RemoteName2 + " varchar(255) , ")
        .append(" " + RepConstants.bookmark_TableName3 + " varchar(255) , " +
                RepConstants.bookmark_lastSyncId4 + " Long , ")
        .append(" " + RepConstants.bookmark_ConisderedId5 +
                " Long ,"+RepConstants.bookmark_IsDeletedTable+" char(1) default 'N' , Primary Key (" + RepConstants.bookmark_LocalName1 +
                ", " + RepConstants.bookmark_RemoteName2 + ", " +
                RepConstants.bookmark_TableName3 + ") ) ");
    runDDL(pubName, bookmarkTableQuery.toString());
  }

  protected void createScheduleTable(String subName) throws SQLException,
      RepException {
    StringBuffer ScheduleTableQuery = new StringBuffer();
    ScheduleTableQuery.append(" Create Table ")
        .append(getScheduleTableName())
        .append(" ( " + RepConstants.schedule_Name + " varchar(255) , " +
                RepConstants.subscription_subName1 + " varchar(255) unique , ")
        .append("  " + RepConstants.schedule_type + " varchar(255) , ")
        .append(" " + RepConstants.publication_serverName3 + " varchar (255) ," +
                RepConstants.publication_portNo + " varchar(255) ,")
        .append(" " + RepConstants.recurrence_type + " varchar(255) , " +
                RepConstants.replication_type + " varchar(255) ,")
        .append(" " + RepConstants.schedule_time + " Long , ")
        .append(" " + RepConstants.schedule_counter + " Long , Primary Key (" +
                RepConstants.schedule_Name + " , " +
                RepConstants.subscription_subName1 + ") ) ");
    runDDL(subName, ScheduleTableQuery.toString());
  }

  void runDDL(String pubsubName, String query) throws SQLException, RepException {
    Connection connection = connectionPool.getConnection(pubsubName);
    Statement stt = connection.createStatement();
    try {
      log.debug(query);
//System.out.println(" ***********************************************************************************");
//System.out.println("                                                                                 ");
//System.out.println(" query ="+query);
//System.out.println("                                                                                 ");
//System.out.println(" ***********************************************************************************");
      stt.execute(query);
    log.info("Query executed "+query);
//System.out.println(" QUERY EXECUTED SUCCESSFULLY ");

    }
    finally {
      connectionPool.removeSubPubFromMap(pubsubName);
      if(stt!=null)
    stt.close();
  }
  }

  /**
    * This method create the main table, its corresponding shadow table and
    * triggers in the database. If main table already created in the database,
    * then it get existing table query. If table structure is same then it
    * subscribe the table in database otherwise it thorw the exception table
    * already exist with different structure the database.
    * @param subName String
    * @param pubTableQueries String[]
    * @param primCols HashMap
    * @param pubVendorType int
    * @throws RepException
    * @throws SQLException
    */
   public void createSubscribedTablesTriggersAndShadowTables(String subName,
    String[] pubTableQueries,String[] alterTableAddFKQueries,HashMap primCols, int pubVendorType,ArrayList repTables) throws RepException, SQLException {
     Connection connection = connectionPool.getConnection(subName);
     MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, subName);
     Statement stt = connection.createStatement();
     try
     {
     for (int i = 0; i < pubTableQueries.length; i++) {
       try {
         stt.execute(pubTableQueries[i]);
       }
       catch (SQLException ex) {
    /*    SchemaQualifiedName sname = new SchemaQualifiedName(mdi, getTableName(pubTableQueries[i]));
//        MetaDataInfo mdi = new MetaDataInfo(connection);
 >>>>>>> 1.67.6.4
         String existing = null;
         ArrayList foreignKeyQueries=new ArrayList();
         try {
           existing = mdi.getExistingTableQuery(this, sname, pubVendorType);
           ArrayList alterTabelStatements = mdi.getForiegnKeyConstraints(sname.getSchemaName(), sname.getTableName());
// System.out.println("Getting the query : " + alterTabelStatements);
          if (alterTabelStatements != null && alterTabelStatements.size() > 0) {
            foreignKeyQueries.addAll(alterTabelStatements);
          }
        //System.out.println(" existing Query =" + existing);
           existing = existing.toLowerCase().replaceAll(" ", "");
         }
         catch (RepException ex1) {
           if (ex1.getRepCode().equalsIgnoreCase("REP033")) {
             throw ex;
           }
         }
//System.out.println(" pubTableQueries[i] =" + pubTableQueries[i]);

     /*    if (pubTableQueries[i].toLowerCase().replaceAll(" ",
             "").indexOf(existing.toLowerCase()) != 0) {
           throw new RepException("REP024", new Object[] {subName,
                                  sname.toString()});
         } */
         // Ignore the exception if the same table exists with same colums and primary key
       }
     }
     if(alterTableAddFKQueries != null){
       for (int i = 0, size = alterTableAddFKQueries.length; i < size; i++) {
         try {
//System.out.println(" alterTableQueries ="+alterTableAddFKQueries[i]);
           stt.execute(alterTableAddFKQueries[i]);

         }
         catch (SQLException ex) {
           /** @todo
            * if it is required ti check for existence of foreign constraint
            * then we have to get the constraint name and check whether it is defined as required
            * if not then give any other name to the constraint and add it or throw exception
            *  */
//          ex.printStackTrace();
           // Ignore the exception if the constraint is already made
         }
       }
     }
     } finally {
       if(stt!=null)
     stt.close();
     }
     // Just Get the Column Sequenes from Create Table Queries
     // and create Shadow Table and Triggers on Shadow Table
     for (int i = 0; i < pubTableQueries.length; i++) {
       RepTable reptable=(RepTable)repTables.get(i);
       if(reptable.getCreateShadowTable().equalsIgnoreCase(RepConstants.NO))
         continue ;
       String tableName = getTableName(pubTableQueries[i]);
       SchemaQualifiedName sname = new SchemaQualifiedName(mdi,getTableName(pubTableQueries[i]));
       String colSeqenceWithDataType = getColumnSequenceWithDataTypes(pubTableQueries[i]);
       ArrayList colInfoList = getColumnNamesAndDataTypes(colSeqenceWithDataType.trim());
       String[] pcols = (String[]) ( (ArrayList) primCols.get(tableName.toLowerCase())).toArray(new String[0]);
       String allColSequence = getShadowTableColumnDataTypeSequence(colInfoList,pcols,reptable);
       createShadowTable(subName, sname.toString(), allColSequence,pcols);
       makeProvisionForLOBDataTypes(colInfoList);
       createShadowTableTriggers(subName, sname.toString(), colInfoList, pcols);
       //createShadowTablesAndSubscriptionTableTriggers(subName,tableName,colSeqenceWithDataType,(ArrayList) primCols.get(tableName.toLowerCase()));
     }
   }


  /**
   * Get the main table name from CreateTableQuery
   * @param createTableQuery String
   * @return String
   */
  String getTableName(String createTableQuery) {
    int startIndex = createTableQuery.toLowerCase().indexOf("table") + 5 ;
    int endIndex = createTableQuery.indexOf("(") - 1;
    return createTableQuery.substring(startIndex, endIndex).trim();
  }

  String getColumnSequenceWithDataTypes(String createTableQuery) {
    int startIndex = createTableQuery.indexOf("(") + 1;
    int endIndex = createTableQuery.toLowerCase().indexOf("primary key") - 1;
    return createTableQuery.substring(startIndex, endIndex).trim();
  }

  /**
   * Return the list of shadowtabls cols in a string.
   * @param nameTypeList ArrayList
   * @param primCols String[]
   * @return String
   */
  public String getShadowTableColumnDataTypeSequence(ArrayList nameTypeList, String[] primCols,RepTable reptable) {
    StringBuffer toRet = new StringBuffer();
    toRet.append(getColumnDataTypeSequence(nameTypeList,reptable))
        .append(getPrimaryColumnDataTypeSequence(primCols,"rep_old_"));
    return toRet.toString();
  }

  protected String getPrimaryColumnDataTypeSequence(String[] primCols,String prefix) {
    StringBuffer toRet = new StringBuffer();
    for (int i = 0, size = primCols.length; i < size; i++) {
      toRet.append(" , ").append(prefix).append(primCols[i]).append(" ")
          .append(ColumnsInfo.getDataTypeDeclaration(primCols[i]));
    }
    return toRet.toString();
  }
//we will also set allColumns list in Reptable in this method alongwith getting column datatype Sequence
  private String getColumnDataTypeSequence(ArrayList colTypeMap,RepTable reptable) {
    StringBuffer toRet = new StringBuffer();
    String[] allColumns=new String[colTypeMap.size()];
    for (int i = 0, size = colTypeMap.size(); i < size; i++) {
      ColumnsInfo ci = (ColumnsInfo) colTypeMap.get(i);
      String colName = ci.getColumnName();
      allColumns[i]=colName;
      toRet.append(" , ").append(colName).append(" ")
          .append(ci.getDataTypeDeclaration());
    }
    reptable.setAllColumns(allColumns);
    return toRet.toString();
  }

  private String[] getColumnsNames(String columnSeqWithDataTypes) {
    ArrayList colNames = new ArrayList();
    do {
      int index = columnSeqWithDataTypes.indexOf(" ");
      int commaIndex = columnSeqWithDataTypes.indexOf(",");
      if (index == -1 || commaIndex == -1) {
        return (String[]) colNames.toArray(new String[0]);
      }
      colNames.add(columnSeqWithDataTypes.substring(0, index).trim());
      columnSeqWithDataTypes = columnSeqWithDataTypes.substring(commaIndex + 1).trim();
    }
    while (true);
  }

  // creating a columns sequence col1[,col2[,col3,...]] to be used for all shadow table trigger
  String getColumnNameSequence(String[] columnNames, String prefix) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < columnNames.length; i++) {
      sb.append(prefix).append(columnNames[i]).append(" , ");
    }
    return sb.toString();
  }

  String[] getColumnNameWithOldOrNewPrefix(String[] columnNames, String prefix) {
      String[] sb = new String[columnNames.length];
      for (int i = 0; i < columnNames.length; i++) {
        sb[i] = prefix+columnNames[i];
      }
      return sb;
    }


  public void setLocalServerName(String name0) {
    localServerName = name0;
  }

  public String getLocalServerName() {
    return localServerName;
  }

  /**
   * Save the RepTable data which is a system table.
   * It store the Publication/Subscription Name,
   * its corresponding table with filter clause and conflict
   * resolver.
   */

  public String saveRepTableData(Connection connection, String pubsubName, RepTable repTable) throws SQLException, RepException {
    StringBuffer sb = new StringBuffer();
    PreparedStatement repPreparedStmt =null;
    String filter = repTable.getFilterClause();
    String[] ignoredColumns = repTable.getColumnsToBeIgnored();
    if (filter != null) {
      if (!filter.equalsIgnoreCase("")) {

//            Rep_PubSub_Name
//            Rep_Table_Id
//            Rep_Table_Name
//            Rep_filter_clause
//            Rep_createshadowtable
//            Rep_cyclicdependency
//            Rep_conflict_resolver


        sb.append("insert into ").append(getRepTableName())
            .append(" ( ").append(RepConstants.repTable_pubsubName1).append(" , ")
            .append(RepConstants.repTable_tableName2).append(" , ")
            .append(RepConstants.repTable_filter_clause3).append(" , ")
            .append(RepConstants.repTable_createshadowtable6).append(" , ")
            .append(RepConstants.repTable_cyclicdependency7).append(" , ")
            .append(RepConstants.repTable_conflict_resolver4).append(" ) ")
            .append(" values ( ?,?,?,?,?,?) ");
        repPreparedStmt = connection.prepareStatement(sb.toString());
        repPreparedStmt.setString(1, pubsubName);
        repPreparedStmt.setString(2, repTable.getSchemaQualifiedName().toString());
        repPreparedStmt.setString(3, repTable.getFilterClause());
        repPreparedStmt.setString(4, repTable.getCreateShadowTable());
//System.out.println("AbstractDataBaseHandler.saveRepTableData(connection, pubsubName, repTable) hashCode ="+repTable.hashCode() +"  cyclic dependency ="+repTable.getCyclicDependency() );
        repPreparedStmt.setString(5, repTable.getCyclicDependency());
        repPreparedStmt.setString(6, repTable.getConflictResolver());
        repPreparedStmt.execute();
         log.info("Query exceuted"+sb.toString());
         log.info(pubsubName);
         log.info(repTable.getSchemaQualifiedName().toString());
         log.info(repTable.getFilterClause());
         log.info(repTable.getConflictResolver());

      }
    }
    else {
      sb.append("insert into ").append(getRepTableName()).append(" ( ")
          .append(RepConstants.repTable_pubsubName1).append(" , ")
          .append(RepConstants.repTable_tableName2).append(" , ")
          .append(RepConstants.repTable_createshadowtable6).append(" , ")
          .append(RepConstants.repTable_cyclicdependency7).append(" , ")
          .append(RepConstants.repTable_conflict_resolver4).append(" ) ")
          .append(" values(?,?,?,?,?)");
      repPreparedStmt = connection.prepareStatement(sb.toString());
      repPreparedStmt.setString(1, pubsubName);
      repPreparedStmt.setString(2, repTable.getSchemaQualifiedName().toString());
      repPreparedStmt.setString(3, repTable.getCreateShadowTable());
      repPreparedStmt.setString(4, repTable.getCyclicDependency());
      repPreparedStmt.setString(5, repTable.getConflictResolver());
      repPreparedStmt.execute();
    }
    /** @todo statement has been closed */
    repPreparedStmt.close();
    if(ignoredColumns != null){
      int tableId = getRepTableId(connection, pubsubName, repTable);
      String queryToInsert="INSERT INTO "+getIgnoredColumns_Table()+ " VALUES ( ?, ?)";
      PreparedStatement statementToInsert= connection.prepareStatement(queryToInsert);
      for (int i = 0,size=ignoredColumns.length; i < size; i++) {
        statementToInsert.setInt(1,tableId);
        statementToInsert.setString(2,ignoredColumns[i]);
        statementToInsert.execute();
      }
    }
    return sb.toString();
  }

  public void setIgnoredColumns(Connection connection,String pubsubName,RepTable repTable) throws RepException,SQLException{
    int tableId = getRepTableId(connection, pubsubName, repTable);
    ArrayList columnsToBeIgnored=new ArrayList();
    StringBuffer sb = new StringBuffer();
    sb.append("SELECT ")
      .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
      .append(" FROM ")
      .append(getIgnoredColumns_Table())
      .append(" WHERE ")
      .append(RepConstants.ignoredColumnsTable_tableId1)
      .append(" = ")
      .append(tableId);
//    String query="SELECT "+RepConstants.ignoredColumnsTable_ignoredcolumnName2+" FROM "+getIgnoredColumns_Table()+ " WHERE " +RepConstants.ignoredColumnsTable_tableId1 +" = "+tableId;
    Statement statement=connection.createStatement();
    ResultSet rs=statement.executeQuery(sb.toString());
    while(rs.next()){
      String columnName = rs.getString(1);
//System.out.println("Adding the column in the list :  " + columnName);
      columnsToBeIgnored.add(columnName);
    }
    repTable.setColumnsToBeIgnored((String[])columnsToBeIgnored.toArray(new String[0]));
    rs.close();
    statement.close();
  }

  private int getRepTableId(Connection connection, String pubsubName, RepTable repTable) throws SQLException {
     StringBuffer sb =new StringBuffer();
     sb.append("SELECT ")
        .append(RepConstants.repTable_tableId2)
        .append(" FROM ")
        .append(RepConstants.rep_TableName)
        .append(" WHERE ")
        .append(RepConstants.repTable_pubsubName1)
        .append(" = '")
        .append(pubsubName)
        .append("' AND ")
        .append(RepConstants.repTable_tableName2)
        .append(" = '")
        .append(repTable.getSchemaQualifiedName().toString())
        .append("'");
//     String queryToGetTableId="SELECT "+RepConstants.repTable_tableId2+" FROM "+RepConstants.rep_TableName+ " WHERE " + RepConstants.repTable_pubsubName1 +" = ?"+" AND  "+RepConstants.repTable_tableName2+" = ?" ;
//     PreparedStatement statement = connection.prepareStatement(queryToGetTableId);
       Statement statement = connection.createStatement();
//     statement.setString(1,pubsubName);
//     statement.setString(2,repTable.getSchemaQualifiedName().toString());
     ResultSet rs= statement.executeQuery(sb.toString());
     rs.next();
     int tableId=rs.getInt(1);
     rs.close();
     statement.close();
     return tableId;
   }

  protected Object getQuotedStringData(Object value) {
    if (value == null) {
      return value;
    }
    return "'" + value + "'";
  }

  protected HashMap schemas;

  public boolean checkSchema(String schemaName) {
    if (schemas == null) {
      schemas = new HashMap();
      schemas.put(schemaName.toLowerCase(), "");
      return false;
    }
    if (schemas.containsKey(schemaName.toLowerCase())) {
      return true;
    }
    schemas.put(schemaName.toLowerCase(), "");
    return false;
  }

  public void dropTriggersAndShadowTable(Connection connection, String table, String pubsubName) throws SQLException, RepException {
    String tableName = table.substring(table.indexOf('.') + 1);
    fireDropQuery(connection, " drop trigger " + RepConstants.getInsertTriggerName(table));
    fireDropQuery(connection, " drop trigger " + RepConstants.getDeleteTriggerName(table));
    fireDropQuery(connection, " drop trigger " + RepConstants.getUpdateTriggerName(table));
    fireDropQuery(connection, " drop table  " + RepConstants.shadow_Table(table));
    dropSequences(connection,RepConstants.seq_Name(RepConstants.shadow_Table(tableName)));
    dropGenerators(connection,RepConstants.gen_Name(RepConstants.shadow_Table(tableName)));
    fireDropQuery(connection, " delete from " + getLogTableName() + " where " +RepConstants.logTable_tableName2 + " = '" + table + "'");

  }

  public void fireDropQuery(Connection con, String query) throws SQLException {
    Statement stt = con.createStatement();
    try {
      stt.execute(query);
      log.info("query executed "+query);
    }
    catch (SQLException sqlEx) {
      // Ignore the Exception
    }
    finally {
      stt.close();
    }
  }

  /*public void writeSchemaName(OutputStreamWriter os, String schemaName) throws
      java.io.IOException {
    if (schemas == null) {
      os.write("<SchemaName>");
      os.write(schemaName);
      os.write("</SchemaName>");
      schemas = new HashMap();
      schemas.put(schemaName.toLowerCase(), "");
      return;
    }
    if (schemas.containsKey(schemaName.toLowerCase()))
      return;
    os.write("<SchemaName>");
    os.write(schemaName);
    os.write("</SchemaName>");
     }*/

  public String updateDataType(String dataType0) {
    return dataType0;
  }

//  Current this method is implemented by Daffodildb and PostgreSQL
  public void createSchemas(String pubName, ArrayList schemas) throws SQLException, RepException {
  }

  public void dropSequences(Connection con, String sequenceName) throws SQLException {
  }

  public void makeProvisionForLOBDataTypes(ArrayList list) {
  }

  public String getPublicationTableName() {
    return publication_TableName;
  }

  public String getSubscriptionTableName() {
    return subscription_TableName;
  }

  public String getRepTableName() {
    return rep_TableName;
  }

  public String getLogTableName() {
    return log_Table;
  }

  public String getBookMarkTableName() {
    return bookmark_TableName;
  }

  public String getScheduleTableName() {
    return Schedule_TableName;
  }

  public void setColumnPrecisionInTypeInfo(TypeInfo typeInfo, ResultSetMetaData rsmt, int columnIndex) throws SQLException {
    // not implmented . child class sql server, daffodildb,and oracle implement it
  }

  public ArrayList getColumnNamesAndDataTypes1(String columnSequence) {
    // col1 datatyoe, colName datatype
    ArrayList list = new ArrayList();
    String lowerString ;
    do {
      lowerString = columnSequence.toLowerCase();
      int startIndex = columnSequence.indexOf(" ");
      int lastIndex = columnSequence.indexOf(",");

      int notNullIndex = lowerString.indexOf("not null");
      String name = columnSequence.substring(0, startIndex);
      String dataType = "";
      if (notNullIndex != -1 && notNullIndex < lastIndex) {
        dataType = columnSequence.substring(startIndex + 1, notNullIndex - 1);
      }
      else {
        dataType = columnSequence.substring(startIndex + 1, lastIndex - 1);
      }
      int defaultIndex = dataType.toLowerCase().indexOf("default ");
//System.out.println("Getting the index.......... " + defaultIndex);
      if(defaultIndex != -1 ){
        dataType = dataType.substring(0, defaultIndex);
      }
      list.add(new ColumnsInfo(name, dataType));
      columnSequence = columnSequence.substring(lastIndex + 1).trim();
    }
    while (columnSequence.length() > 0); return list;
  }

  public String getTableColumns(int VendorType, String columnName, TypeInfo typeInfo, int columnPrecision, ResultSet rs) throws RepException, SQLException {
    StringBuffer sb = new StringBuffer();
    sb.append(columnName).append(" ").append(typeInfo.getTypeDeclaration(columnPrecision));
    return sb.toString();
  }

  public String getShadowTableName(String tableName) {
    return RepConstants.shadow_Table(tableName);
  }

  /**
   * Create index on syn_id column of shadowTable to
   * improve the performance of Replicator Operation
   * @param tableName
   * @param columnName
   */

  abstract protected void createIndex(String pubsubName,String tableName) throws RepException ;
//to handle the column scale
   public ArrayList getColumnNamesAndDataTypes(String columnSequence) {
    // col1 datatyoe, colName datatype
    ArrayList list = new ArrayList();
    String lowerString = columnSequence;
    do {
      lowerString = columnSequence.toLowerCase();
      int startIndex = columnSequence.indexOf(" ");
      int lastIndex = columnSequence.indexOf(",");
      int notNullIndex = lowerString.indexOf("not null");
      String name = columnSequence.substring(0, startIndex);
      String dataType = "";
      if (notNullIndex != -1 && notNullIndex < lastIndex) {
        dataType=columnSequence.substring(startIndex + 1, notNullIndex-1 );
       if(dataType.indexOf("(")!=-1){
         if(dataType.indexOf(")")==-1){
             String dataType1 = columnSequence.substring(notNullIndex + 1).trim();

           dataType = dataType + " , " + dataType1.substring(dataType1.indexOf(" ")+1, (dataType1.indexOf(",")-1));
            lastIndex= lastIndex=(lastIndex+1)+dataType1.indexOf(",");//(lastIndex+1)+dataType1.length()-1;
           }else{
            dataType = columnSequence.substring(startIndex + 1, notNullIndex-1 );
         }
        }else{
           dataType = columnSequence.substring(startIndex + 1, notNullIndex-1 );
        }
      }
      else{
        dataType=columnSequence.substring(startIndex + 1, lastIndex-1 );
       if(dataType.indexOf("(")!=-1){
         if(dataType.indexOf(")")==-1){
           String dataType1 =columnSequence.substring(lastIndex+1);
           dataType = dataType + " , " + dataType1.substring((dataType1.indexOf(" ")+1), (dataType1.indexOf(",")-1));
           lastIndex= lastIndex=(lastIndex+1)+dataType1.indexOf(",");//(lastIndex+1)+dataType1.length()-1;
          }else{
           dataType = columnSequence.substring(startIndex + 1, lastIndex-1 );
        }
       }else{

          dataType = columnSequence.substring(startIndex + 1, lastIndex-1 );
       }
      }
      list.add(new ColumnsInfo(name, dataType));
      columnSequence = columnSequence.substring(lastIndex + 1).trim();
    }
    while (columnSequence.length() > 0);
    return list;
  }
  //if precison is less than scale ,Exception will be thrown to user depending on subscriber databse
  //---as suggested by Parveen Sir
abstract public int getAppropriateScale(int columnScale) throws RepException;

  public void dropPublisherSystemTables(Connection con) {
      try {
        fireDropQuery(con, " drop table " + getPublicationTableName());
        fireDropQuery(con, " drop table " + getBookMarkTableName());
        fireDropQuery(con, " drop table " + getRepTableName());
        fireDropQuery(con, " drop table " + getLogTableName());
        fireDropQuery(con, " drop table " + getIgnoredColumns_Table());
        fireDropQuery(con, " drop table " + getTrackReplicationTablesUpdation_Table());
        fireDropQuery(con, " drop table " + getTrackPrimayKeyUpdation_Table());
      }
      catch (Exception ex) {
      }
    }
    public void dropSubscriberSystemTables(Connection con) {
        try {
          fireDropQuery(con, " drop table " + getSubscriptionTableName());
          fireDropQuery(con, " drop table " + getBookMarkTableName());
          fireDropQuery(con, " drop table " + getRepTableName());
          fireDropQuery(con, " drop table " + getLogTableName());
          fireDropQuery(con, " drop table " + getScheduleTableName());
          fireDropQuery(con, " drop table " + getIgnoredColumns_Table());
          fireDropQuery(con, " drop table " + getTrackReplicationTablesUpdation_Table());
          fireDropQuery(con, " drop table " + getTrackPrimayKeyUpdation_Table());
        }
        catch (Exception ex) {
        }
      }

      public void deleteRecordsFromSuperLogTable(Statement subStatment) throws
          SQLException {
        // insert one record in superLogTable

        StringBuffer query = new StringBuffer();
        query.append("insert into ").append(getLogTableName()).append(
            " (").
            append(RepConstants.logTable_tableName2).append(
            ") values  ('$$$$$$')");

        subStatment.execute(query.toString());

        query = new StringBuffer();
        // deleting all but one last record from super log table where commonid is maximum
        query.append("Select max(").append(RepConstants.logTable_commonId1).
            append(") from ").append(getLogTableName());
        ResultSet rs = subStatment.executeQuery(query.toString());
        rs.next();
        long maxCID = rs.getLong(1);

        query = new StringBuffer();

        query.append("delete from ").append(getLogTableName()).append(
            " where ")
            .append(RepConstants.logTable_commonId1).append(" !=").append(maxCID);
        subStatment.executeUpdate(query.toString());
        log.debug(query.toString());
      }





      public void dropGenerators(Connection con, String generatorName) throws SQLException {
      }

  /**
   * To get the foreign key error code to resolve
  *  the Synchronization problem when tables have
  *  parent - child relationship.
   * @return String
   */
  abstract public boolean isForiegnKeyException(SQLException ex) throws
      SQLException;

  abstract public PreparedStatement makePrimaryPreperedStatement(String[]
      primaryColumns, String shadowTable, String local_pub_sub_name) throws
      SQLException, RepException;

  public String getIgnoredColumns_Table() {
   return ignoredColumns_Table;
 }

 public String getTrackReplicationTablesUpdation_Table() {
    return trackReplicationTablesUpdation_Table;
  }

  public String getTrackPrimayKeyUpdation_Table() {
    return trackPrimaryKeyUpdation_Table;
  }

 abstract protected void createIgnoredColumnsTable(String pubName) throws SQLException,
      RepException;
  abstract protected void createTrackReplicationTablesUpdationTable(String pubSubName) throws
      RepException, SQLException;
  abstract protected void createTriggerForTrackReplicationTablesUpdationTable(String
     pubSubName) throws RepException, SQLException ;
 public Object getMinValOfSyncIdTodeleteRecordsFromShadowTable(String tableName,Statement stmt) throws SQLException {
     ResultSet rs =null;
    // selecting min of syncid or concideredId  from bookmarks table for one table
   try {
      StringBuffer query = new StringBuffer();
       query.append("Select case when min(").append(RepConstants.
           bookmark_lastSyncId4).append(")< min(").append(
           RepConstants.
           bookmark_ConisderedId5).append(" )then min( ").append(
           RepConstants.
           bookmark_lastSyncId4).append(" ) else min( ").append(
           RepConstants.
           bookmark_ConisderedId5).append(") end from ").append(
           getBookMarkTableName()).append(" where ").append(
           RepConstants.
           bookmark_TableName3).append(" = '").append( tableName).append("'");
//    System.out.println("AbstractDBHandler.deleteRecordsFromShadowTable=::"+query.toString());
     rs = stmt.executeQuery(query.toString());
       rs.next();
      return rs.getObject(1);
    }finally{
      if(rs!=null)
        try {
          rs.close();
        }
        catch (SQLException ex) {
        }
    }
}


  abstract public PreparedStatement makePrimaryPreperedStatementBackwardTraversing(String[] primaryColumns,long lastId,String local_pub_sub_name,String shadowTable) throws SQLException,RepException  ;

  String[] addPrefixWithColumnName(String[] columnNames0, String prefix) {
      String[] columnNames = new String[columnNames0.length];
      for (int i = 0; i < columnNames.length; i++) {
       columnNames[i] = prefix+columnNames0[i];
      }
      return columnNames;
    }

}
