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

public class FireBirdDatabaseHandler
    extends AbstractDataBaseHandler  {
  protected static Logger log = Logger.getLogger(FireBirdDatabaseHandler.class.
                                                 getName());
  public FireBirdDatabaseHandler() {}

  public FireBirdDatabaseHandler(ConnectionPool connectionPool0) {
    connectionPool = connectionPool0;
    vendorType = Utility.DataBase_Firebird;
  }

  //Table command: CREATE TABLE tablename (ID VARCHAR(10) CONSTRAINT pk_id PRIMARY KEY, ...)
  //Sequence command: CREATE SEQUENCE seq tablename minvalue 1 increment by 1 nomaxvalue
  //Insert command: INSERT INTO tablename (ID,...) VALUES(seq_tablename.nextval,....)


  protected void createPublicationTable(String pubName) throws RepException,
      SQLException {
    StringBuffer pubsTableQuery = new StringBuffer();
    pubsTableQuery.append(" Create Table ")
        .append(getPublicationTableName())
        .append(" ( " + RepConstants.publication_pubName1 +
                " varchar(50)  NOT NULL , " +
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
        + " (  " + RepConstants.subscription_subName1 +
        " varchar(50) NOT NULL, "
        + "   " + RepConstants.subscription_pubName2 + " varchar(50)  , "
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
        .append(bookmark_TableName)
        .append(" (  " + RepConstants.bookmark_LocalName1 +
                " varchar(50) not null, " + RepConstants.bookmark_RemoteName2 +
                " varchar(50) not null , ")
        .append(" " + RepConstants.bookmark_TableName3 +
                " varchar(50) not null , " + RepConstants.bookmark_lastSyncId4 +
                " integer , ")
        .append(
        " " + RepConstants.bookmark_ConisderedId5 + " integer ," +
        RepConstants.bookmark_IsDeletedTable +
        " char(1) default 'N' , Primary Key (" +
        RepConstants.bookmark_LocalName1 + " ,  " +
        RepConstants.bookmark_RemoteName2 + " ,  " +
        RepConstants.bookmark_TableName3 + ") ) ");
    runDDL(pubName, bookmarkTableQuery.toString());
  }

  public void createShadowTable(String pubsubName, String tableName,
                                String allColSequence,String[] primaryColumns) throws RepException {
    StringBuffer shadowTableQuery = new StringBuffer();
    String shadowTableName = RepConstants.shadow_Table(tableName);
    shadowTableQuery.append(" Create Table ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_sync_id1).append(" integer ,")
        .append(RepConstants.shadow_common_id2).append(" integer , ")
        .append(RepConstants.shadow_operation3).append(" char(1) , ")
        .append(RepConstants.shadow_status4).append(" char(1) ")
        .append(allColSequence).append(" , ")
        .append(RepConstants.shadow_serverName_n).append(" varchar(255)  ")
        .append(" ,  " + RepConstants.shadow_PK_Changed + "  char(1) ) ");
    StringBuffer genOnShadowTableQuery = new StringBuffer();
    genOnShadowTableQuery.append(" Create GENERATOR ")
        .append(RepConstants.gen_ShadowTableName(shadowTableName));

    try {
      runDDL(pubsubName, shadowTableQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      // Ignore Exception
    }
    try {
      runDDL(pubsubName, genOnShadowTableQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      // Ignore Exception
    }
    createIndex(pubsubName,shadowTableQuery.toString());
  }

  // Oracle
  //  create trigger abc2 after insert on a referencing new as newRow
  //  for each row
  //  declare va int ;
  //  begin
  //  Select max(a)  into va from b;
  //  insert into d values ( va , :newrow.a) ;
  //  end ;
  public void createShadowTableTriggers(String pubsubName, String tableName,
                                        ArrayList colInfoList,
                                        String[] primCols) throws RepException {
    String serverName = getLocalServerName();
    int size = colInfoList.size();
    String[] colNames = new String[size];
    for (int i = 0; i < size; i++) {
      colNames[i] = ( (ColumnsInfo) colInfoList.get(i)).getColumnName();
    }
    String colNameSeqPrefixOldRow = getColumnNameSequence(colNames, "OLD.");
    String colNameSeqPrefixNewRow = getColumnNameSequence(colNames, "NEW.");
    String primColNameSeqPrefixOldRow = getColumnNameSequence(primCols,"OLD.");
    String primColNameSeqPrefixNewRow = getColumnNameSequence(primCols,"NEW.");
    String[] primOld = getColumnNameWithOldOrNewPrefix(primCols,"OLD.");
    String[] primNew  = getColumnNameWithOldOrNewPrefix(primCols,"NEW.");
    StringBuffer insertLogTable = new StringBuffer();
    insertLogTable.append(" Insert into ")
        .append(log_Table).append(" values ( GEN_ID(")
        .append(RepConstants.gen_Name(log_Table) + ", 1) , '")
        .append(tableName).append("'); ");

    StringBuffer shadowTableQuery = new StringBuffer();
    shadowTableQuery.append(" Insert Into ")
        .append(RepConstants.shadow_Table(tableName)).append(" ( ")
        .append(RepConstants.shadow_sync_id1).append(", ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(getColumnNameSequence(colNames, ""))
        .append(getColumnNameSequence(primCols, "REP_OLD_"))
        .append(RepConstants.shadow_serverName_n).append(",")
        .append(RepConstants.shadow_PK_Changed)
        .append(" ) Values ( GEN_ID(")
        .append(RepConstants.gen_ShadowTableName(
        RepConstants.shadow_Table(tableName))).append(", 1),");

    StringBuffer insTriggerQuery = new StringBuffer();
// CREATE TRIGGER TRI_INSERT FOR t1 AFTER INSERT  AS BEGIN  insert into logtable values(GEN_ID(GEN_USER_LOG, 1),'ServerName'); insert into t2 values(NEW.C1,NEW.C2); END;
    insTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getInsertTriggerName(tableName))
        .append(" for ").append(tableName)
        .append(" after insert as  begin ")
        .append(insertLogTable).append(shadowTableQuery.toString())
        .append(" null ,'I', null , ").append(colNameSeqPrefixNewRow)
        .append(primColNameSeqPrefixNewRow)
        .append("'").append(serverName).append("',null) ; end ; ");

    StringBuffer delTriggerQuery = new StringBuffer();
    delTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getDeleteTriggerName(tableName))
        .append(" for ").append(tableName)
        .append(" after delete as begin ")
        .append(insertLogTable).append(shadowTableQuery.toString())
        .append(" null ,'D', null , ").append(colNameSeqPrefixOldRow)
        .append(primColNameSeqPrefixOldRow)
        .append("'").append(serverName).append("',null) ; end ; ");

    StringBuffer updTriggerQuery = new StringBuffer();

    /*   create trigger TRI_UPDATE for t1 after UPDATE  AS declare variable maxlogid integer;
          BEGIN  FOR SELECT max(c1) FROM logtable  INTO :maxlogid do begin
          insert into logtable values(GEN_ID(GEN_USER_LOG, 1),'ServerName');
          insert into t2 values(:maxlogid,NEW.C2);insert into t2 values(:maxlogid,OLD.C2); end  END;
     */
    updTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getUpdateTriggerName(tableName))
        .append(" for ").append(tableName)
        .append(" after update as  declare variable maxlogid integer; declare variable pkchanged char(1); ")
        .append(" begin FOR SELECT max(" + RepConstants.logTable_commonId1 +
                ") FROM " + log_Table +
                "  INTO :maxlogid do begin ").append(insertLogTable)
        .append(shadowTableQuery.toString())
        .append(":maxlogid,'U','B',")
        .append(colNameSeqPrefixOldRow).append(primColNameSeqPrefixOldRow)
        .append("'").append(serverName).append("') ; ")
        .append(" if( ");
        for (int i = 0; i < primOld.length; i++) {
          if (i != 0)
            updTriggerQuery.append(" and ");
          updTriggerQuery.append(primOld[i] )
                                  .append("!=" )
                                  .append(primNew[i]);
        }
        updTriggerQuery.append(" ) Then ")
        .append("  pkchanged :='Y';  ")
        .append(shadowTableQuery.toString())
        .append(":maxlogid,'U','A',")
        .append(colNameSeqPrefixNewRow).append(primColNameSeqPrefixOldRow)
        .append("'").append(serverName).append("',:pkchanged) ; end end ; ");

    try {
      runDDL(pubsubName, insTriggerQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      // Ignore Exception
    }
    try {
      runDDL(pubsubName, delTriggerQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      // Ignore Exception
    }
    try {
      runDDL(pubsubName, updTriggerQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      // Ignore Exception
    }

  }

//  BIGINT             -5
//  [BLOB SUB_TYPE 0   -4
//  [BLOB SUB_TYPE 1   -1
//  [CHAR               1
//  [NUMERIC            2
//  [DECIMAL            3
//  [INTEGER            4
//  [SMALLINT           5
//  [FLOAT              6
//  [DOUBLE PRECISION   8
//  [VARCHAR            12
//  [DATE               91
//  [TIME               92
//  [TIMESTAMP          93
//  [ARRAY              1111
//  [BLOB SUB_TYPE <0   2004

  public boolean isDataTypeOptionalSizeSupported(TypeInfo typeInfo) {
    int sqlType = typeInfo.getSqlType();
    String typeName = typeInfo.getTypeName();
    switch (sqlType) {
      case -4:
      case 4:
      case 5:
      case -1:
      case -5:
      case 8:
      case 91:
      case 92:
      case 93:
      case 2004:
        return false;
      default:
        return true;
    }
  }

  public void setTypeInfo(TypeInfo typeInfo, ResultSet rs) throws RepException,
      SQLException {
    int sqlType = typeInfo.getSqlType();
    String typeName = typeInfo.getTypeName();
//System.out.println("sqlType::" + sqlType);
//System.out.println("typeName::" + typeName);
    switch (sqlType) {
      case Types.BIT:
        typeInfo.setTypeName("SMALLINT");
        break; //-7
      case Types.TINYINT:
        typeInfo.setTypeName("SMALLINT");
        break; //-6
      case Types.SMALLINT:
        typeInfo.setTypeName("SMALLINT");
        break; // 5;
      case Types.INTEGER:
        typeInfo.setTypeName("INTEGER");
        break; // 4;
      case Types.BIGINT:
        typeInfo.setTypeName("BIGINT"); //numeric
        break; //-5;
      case Types.FLOAT:
        typeInfo.setTypeName("FLOAT");
        break; // 6;
      case Types.REAL:
        typeInfo.setTypeName("DOUBLE PRECISION"); //real
        break; // 7;
      case Types.DOUBLE:
        typeInfo.setTypeName("DOUBLE PRECISION"); //double
        break; // 8;
      case Types.NUMERIC:
        typeInfo.setTypeName("NUMERIC");
        break; // 2;
      case Types.DECIMAL:
        typeInfo.setTypeName("DECIMAL");
        break; // 3;
      case Types.CHAR:
        typeInfo.setTypeName("CHAR");
        break; // 1;
      case Types.VARCHAR:
        typeInfo.setTypeName("VARCHAR");
        break; //12
      case Types.LONGVARCHAR:
        typeInfo.setTypeName("BLOB SUB_TYPE 1");
        break; //-1;
      case Types.DATE:
        typeInfo.setTypeName("DATE");
        break; // //91;
      case Types.TIME:
        typeInfo.setTypeName("TIME");
        break; // //92;
      case Types.TIMESTAMP:
        typeInfo.setTypeName("TIMESTAMP");
        break; // //93;
      case Types.BINARY:
      case Types.VARBINARY: //-3;
      case Types.LONGVARBINARY:
        typeInfo.setTypeName("BLOB SUB_TYPE 0");
        break; //-4;
      case Types.OTHER:
        typeInfo.setTypeName("BLOB SUB_TYPE 0");
        break; //1111;
      case Types.BLOB:
        typeInfo.setTypeName("BLOB SUB_TYPE 0 "); //blob
        break; //2004;
      case Types.CLOB:
        typeInfo.setTypeName("BLOB SUB_TYPE 0"); //sub type1
        break; //2005;
      case Types.REF: //2006;
      case Types.JAVA_OBJECT: //2000;
      case Types.DISTINCT: //2001;
      case Types.NULL: // 0;
      case Types.DATALINK: //70;
      case Types.BOOLEAN: //16;
      case Types.STRUCT:
      case Types.ARRAY:
      default:
        throw new RepException("REP011", new Object[] {typeInfo.getTypeName()});
    }
  }

  public AbstractColumnObject getColumnObject(TypeInfo typeInfo) throws
      RepException {
    int sqlType = typeInfo.getSqlType();
    switch (sqlType) {
      case -7: // number
      case -6: // number
      case 4: // number
      case 5: // number
        return new IntegerObject(sqlType,this);
      case -5: // bigint
        return new LongObject(sqlType,this);
      case 1: // char
      case 12: // varchar
        return new StringObject(sqlType,this);
      case 6: // float
      case 7: // real
        return new FloatObject(sqlType,this);
      case 8:
      case 2: // number
      case 3:
        return new DoubleObject(sqlType,this);
      case 91: // date
        return new DateObject(sqlType,this);
      case 92: // date
        return new TimeObject(sqlType,this);
      case 93: // date
        return new TimeStampObject(sqlType,this);
      case 2004: // blob
      case -1: //BLOB SUB_TYPE 1
        return new BlobObject(sqlType,this);
      case 2005: // clob
      case -4: //BLOB SUB_TYPE 0
        return new ClobObject(sqlType,this);
      default:
        throw new RepException("REP011", new Object[] {typeInfo.getTypeName()});
    }
  }

  public String getRepTableInsertQuery(String pubsubName, RepTable repTable) {
    StringBuffer sb = new StringBuffer();
    String filter = repTable.getFilterClause();

    if (filter != null) {
      if (!filter.equalsIgnoreCase("")) {
        sb.append("insert into RepTable ").append(" values ( '")
            .append(pubsubName).append("',GEN_ID(")
            .append(RepConstants.gen_Name("RepTable"))
            .append(", 1),'")
            .append(repTable.getSchemaQualifiedName()).append("','")
            .append(repTable.getFilterClause()).append(",")
            .append(repTable.getConflictResolver()).append(" ) ");
      }
    }
    else {
      sb.append("insert into RepTable ").append(" ( ")
          .append(RepConstants.repTable_pubsubName1).append(" , Table_Id , ")
          .append(RepConstants.repTable_tableName2).append(" , ")
          .append(RepConstants.repTable_conflict_resolver4).append(" ) ")
          .append(" values ( '")
          .append(pubsubName).append("',GEN_ID(")
          .append(RepConstants.gen_Name("RepTable"))
          .append(", 1),'")
          .append(repTable.getSchemaQualifiedName()).append("','")
          .append(repTable.getConflictResolver()).append("') ");
    }
//System.out.println(" Query  = "+sb.toString());
    return sb.toString();
  }

  public void dropGenerator(Connection con, String sequenceName) throws
      SQLException {
    Statement stt = con.createStatement();
    try {
      String dropgeneratorquery = " drop generator " + sequenceName;
//       Seq_Shadow_TEST1
      stt.execute(dropgeneratorquery);
    }
    catch (SQLException ex) {
    }
    stt.close();
  }

  /**
   * CLOB and BLOB type columns do not added in trigger definition
   * So this type columns has been removed fom the arraylist
   * @param dataTypeMap ArrayList
   */

  public void makeProvisionForLOBDataTypes(ArrayList dataTypeList) {
    /**
     * Proper handling required here
     */

    /*   ArrayList removeKeysList = null;
         for (int i = 0, size = dataTypeList.size(); i < size; i++) {
      ColumnsInfo ci = (ColumnsInfo) dataTypeList.get(i);
      String dataType = ci.getDataTypeDeclaration();
      if (dataType.indexOf("long") != -1) {
        if (removeKeysList == null) {
          removeKeysList = new ArrayList();
        }
        removeKeysList.add(ci);
      }
         }
         if (removeKeysList != null) {
      for (int i = 0, length = removeKeysList.size(); i < length; i++) {
        dataTypeList.remove(removeKeysList.get(i));
      }
         } */
  }

  // references to columns of type LONG are not allowed in triggers

  public boolean isColumnSizeExceedMaximumSize(TypeInfo typeInfo) throws
      SQLException, RepException {
    boolean flag = false;
    int sqlType = typeInfo.getSqlType();
    int columnsize = typeInfo.getcolumnSize();
    switch (sqlType) {
      case 12: //varchar
        if (columnsize > 4000) {
          flag = true;
          break;
        }
    }
    return flag;
  }

  public void setColumnPrecisionInTypeInfo(TypeInfo typeInfo,
                                           ResultSetMetaData rsmt,
                                           int columnIndex) throws SQLException {
  }

  public int getAppropriatePrecision(int columnSize, String datatypeName) {
    return columnSize;
  }

  public boolean checkSchema(String schemaName) {
    if (schemaName == null) {
      return true;
    }
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

  //if precison is less than scale ,Exception will be thrown to user depending on subscriber databse
  //---as suggested by Parveen Sir
  public int getAppropriateScale(int columnScale) throws RepException {
    return columnScale;
  }

  protected void createIndex(String pubsubName, String tableName) throws
      RepException {
    StringBuffer createIndexQuery = new StringBuffer();
//      create index ind on cmsadm2.R_S_Bank(Rep_sync_id);
    createIndexQuery.append("create index  ")
        .append(RepConstants.Index_Name(tableName))
        .append(" on ")
        .append(tableName)
        .append("(")
        .append(RepConstants.shadow_sync_id1)
        .append(")");
//System.out.println(" createIndexQuery : "+createIndexQuery.toString());
    try {
      runDDL(pubsubName, createIndexQuery.toString());
    }
    catch (RepException ex) {
      // Ignore the Exception
    }
    catch (SQLException ex) {
      // Ignore the Exception
    }
  }

  protected void createRepTable(String pubName) throws SQLException,
      RepException {
    StringBuffer repTableQuery = new StringBuffer();
    repTableQuery.append(" Create Table ").append(rep_TableName)
        .append(" ( " + RepConstants.repTable_pubsubName1 +" varchar(50) not null,")
        .append(RepConstants.repTable_tableId2 +" integer , ")
        .append(" " + RepConstants.repTable_tableName2 +" varchar(50) not null, ")
        .append(RepConstants.repTable_filter_clause3 + " varchar(255) , ")
        .append(RepConstants.repTable_createshadowtable6).append("  char(1) Default 'Y', ")
        .append(RepConstants.repTable_cyclicdependency7).append("  char(1) Default 'N', ")
        .append(" " + RepConstants.repTable_conflict_resolver4 +" varchar(255) , Primary Key (" + RepConstants.repTable_pubsubName1 +", " + RepConstants.repTable_tableName2 + " ) ) ");
    StringBuffer genOnrepTableQuery = new StringBuffer();
    genOnrepTableQuery.append(" Create GENERATOR ")
        .append(RepConstants.gen_Name(rep_TableName));
    runDDL(pubName, repTableQuery.toString());
    runDDL(pubName, genOnrepTableQuery.toString());
  }

  protected void createSuperLogTable(String pubName) throws SQLException,
      RepException {
    StringBuffer logTableQuery = new StringBuffer();

    logTableQuery.append(" Create Table ")
        .append(log_Table).append(" (")
        .append(RepConstants.logTable_commonId1).append(" integer ,")
        .append(RepConstants.logTable_tableName2).append(" varchar(255) ) ");
    StringBuffer GeneratorOnlogTableQuery = new StringBuffer();
    GeneratorOnlogTableQuery.append(" Create GENERATOR ")
        .append(RepConstants.gen_Name(log_Table));

    StringBuffer indexQuery =new StringBuffer();
     indexQuery.append("CREATE INDEX ")
           .append(RepConstants.log_Index)
           .append(" ON "+getLogTableName())
           .append("(")
           .append(RepConstants.logTable_commonId1)
           .append(")");
//System.out.println(" Create Index on LogTable : "+indexQuery.toString());
    runDDL(pubName, logTableQuery.toString());
    runDDL(pubName, GeneratorOnlogTableQuery.toString());
    runDDL(pubName, indexQuery.toString());
  }

  public void createScheduleTable(String subName) throws SQLException,
      RepException {
    StringBuffer ScheduleTableQuery = new StringBuffer();
    ScheduleTableQuery.append(" Create Table ")
        .append(getScheduleTableName())
        .append(" ( " + RepConstants.schedule_Name + " varchar(50) NOT NULL , " +
                RepConstants.subscription_subName1 + " varchar(50) NOT NULL, ")
        .append("  " + RepConstants.schedule_type + " varchar(255) , ")
        .append(" " + RepConstants.publication_serverName3 + " varchar (255) ," +
                RepConstants.publication_portNo + " varchar(255) ,")
        .append(" " + RepConstants.recurrence_type + " varchar(255) , " +
                RepConstants.replication_type + " varchar(255) ,")
        .append(" " + RepConstants.schedule_time + " Integer , ")
        .append(" " + RepConstants.schedule_counter +
                " integer , constraint rep_sch_unq unique(" +
                RepConstants.subscription_subName1 + ") , Primary Key (" +
                RepConstants.schedule_Name + " , " +
                RepConstants.subscription_subName1 + ") ) ");
    runDDL(subName, ScheduleTableQuery.toString());
  }

  public String getScheduleTableName() {
    return Schedule_TableName;
  }

  public String saveRepTableData(Connection connection, String pubsubName,
                                 RepTable repTable) throws SQLException,
      RepException {
    StringBuffer sb = new StringBuffer();
    PreparedStatement repPreparedStatement = null;
    String filter = repTable.getFilterClause();
    if (filter != null) {
      if (!filter.equalsIgnoreCase("")) {

        sb.append("insert into  " + rep_TableName +
                  "  ").append(" values ( ?,GEN_ID(")
            .append(RepConstants.gen_Name(rep_TableName)).append(",1),?,?,?)");
        repPreparedStatement = connection.prepareStatement(sb.toString());

        repPreparedStatement.setString(1, pubsubName);
        repPreparedStatement.setString(2,
                                       repTable.getSchemaQualifiedName().
                                       toString());
        repPreparedStatement.setString(3, repTable.getFilterClause());
        repPreparedStatement.setString(4, repTable.getConflictResolver());
        repPreparedStatement.execute();
//System.out.println("QUERY EXECUTED SUCCESSFULLY");
      }
    }
    else {
      sb.append("insert into  " + rep_TableName + "  (")
          .append(RepConstants.repTable_pubsubName1)
          .append(" , " + RepConstants.repTable_tableId2 + " , ")
          .append(RepConstants.repTable_tableName2).append(" , ")
          .append(RepConstants.repTable_conflict_resolver4).append(" ) ")
          .append(" values ( ?,GEN_ID(")
          .append(RepConstants.gen_Name(rep_TableName)).append(",1),?,?)");
//System.out.println(" sb.toString() ="+sb.toString().toUpperCase());
      repPreparedStatement = connection.prepareStatement(sb.toString());
      repPreparedStatement.setString(1, pubsubName);
      repPreparedStatement.setString(2,
                                     repTable.getSchemaQualifiedName().toString());
      repPreparedStatement.setString(3, repTable.getConflictResolver());
      repPreparedStatement.execute();
//           System.out.println("QUERY EXECUTED SUCCESSFULLY");

    }
    try {
      repPreparedStatement.close();
    }
    catch (SQLException ex) {
      //sqlexception must be ignored
    }
    return sb.toString();
  }

  public void dropPublisherSystemTables(Connection con) {
    try {
      fireDropQuery(con, " drop table " + publication_TableName);
      fireDropQuery(con, " drop table " + bookmark_TableName);
      fireDropQuery(con, " drop table " + rep_TableName);
      fireDropQuery(con, " drop table " + log_Table);
      fireDropQuery(con, " drop table " + getIgnoredColumns_Table());
      fireDropQuery(con, " drop table " + getTrackReplicationTablesUpdation_Table());
      //drop generators on reptable and logtable
      fireDropQuery(con,
                    " drop generator " + RepConstants.gen_Name(rep_TableName));
      fireDropQuery(con, " drop generator " + RepConstants.gen_Name(log_Table));
    }
    catch (Exception ex) {
      //exception must be ignored
    }
  }

  public void dropSubscriberSystemTables(Connection con) {
    try {
      fireDropQuery(con, " drop table " + subscription_TableName);
      fireDropQuery(con, " drop table " + bookmark_TableName);
      fireDropQuery(con, " drop table " + rep_TableName);
      fireDropQuery(con, " drop table " + log_Table);
      fireDropQuery(con, " drop table " + Schedule_TableName);
      fireDropQuery(con, " drop table " + getIgnoredColumns_Table());
      fireDropQuery(con, " drop table " + getTrackReplicationTablesUpdation_Table());
      //drop generators on reptable and logtable
      fireDropQuery(con,
                    " drop generator " + RepConstants.gen_Name(rep_TableName));
      fireDropQuery(con, " drop generator " + RepConstants.gen_Name(log_Table));
    }
    catch (Exception ex) {
      //exception must be ignored
    }
  }

  public void deleteRecordsFromSuperLogTable(Statement subStatment) throws
      SQLException {
    // insert one record in superLogTable

    StringBuffer query = new StringBuffer();
    query.append("insert into ").append(log_Table).append(
        " values  (GEN_ID(")
        .append(RepConstants.gen_Name(rep_TableName)).append(",1),'$$$$$$')");
    subStatment.execute(query.toString());

    query = new StringBuffer();
    // deleting all but one last record from super log table where commonid is maximum
    query.append("Select max (").append(RepConstants.logTable_commonId1).
        append(") from ").append(log_Table);
    ResultSet rs = subStatment.executeQuery(query.toString());
    rs.next();
    long maxCID = rs.getLong(1);

    query = new StringBuffer();

    query.append("delete from ").append(log_Table).append(
        " where ")
        .append(RepConstants.logTable_commonId1).append(" !=").append(maxCID);
    log.debug(query.toString());
    subStatment.executeUpdate(query.toString());
    log.debug("Query executed succssfully");
  }

  public void dropGenerators(Connection con, String generatorName) throws
      SQLException {
    Statement stt = con.createStatement();
    try {
      String dropGeneratorquery = " drop generator " + generatorName;
      log.debug(dropGeneratorquery);
      stt.execute(dropGeneratorquery);
      log.debug("Query executed succssfully");
    }
    catch (SQLException ex) {
    }
    finally {
      if (stt != null)
        stt.close();
    }
  }

  public PreparedStatement makePrimaryPreperedStatement(String[]
      primaryColumns, String shadowTable, String local_pub_sub_name) throws
      SQLException, RepException {
    StringBuffer query = new StringBuffer();
    query.append(" select FIRST 1 SKIP 0 * from ");
    query.append(shadowTable);
    query.append(" where ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" > ");
    query.append("? ");
    for (int i = 0; i < primaryColumns.length; i++) {
      query.append(" and ");
      query.append(primaryColumns[i]);
      query.append("= ? ");
    }
    query.append(" order by " + RepConstants.shadow_sync_id1);
//System.out.println("OracleHandler.makePrimaryPreperedStatement(primaryColumns, shadowTable, local_pub_sub_name) : "+query.toString());
    Connection pub_sub_Connection = connectionPool.getConnection(
        local_pub_sub_name);
    return pub_sub_Connection.prepareStatement(query.toString());
  }



  /**
   * isPrimaryKeyException
   *
   * @param ex SQLException
   * @return boolean
   */
  public boolean isPrimaryKeyException(SQLException ex) {
//  System.out.println("FireBird Hanlder ERRRRRRRORR CODE ::::: "+ex.getErrorCode());
      return  ex.getErrorCode()==335544665 ? true:false;
  }

  /**
   * isForiegnKeyException
   *
   * @param ex SQLException
   * @return boolean
   */
  public boolean isForiegnKeyException(SQLException ex) {
  return false;
  }



  protected void createTrackReplicationTablesUpdationTable(String pubSubName) throws
        RepException, SQLException {
      StringBuffer trackRepTablesUpdationQuery = new StringBuffer();
      trackRepTablesUpdationQuery.append(" CREATE  TABLE ").append(getTrackReplicationTablesUpdation_Table()).append(" ( " +
          RepConstants.trackUpdation + " SMALLINT NOT NULL PRIMARY KEY) ");
      runDDL(pubSubName, trackRepTablesUpdationQuery.toString());
      runDDL(pubSubName,"Insert into "+getTrackReplicationTablesUpdation_Table()+" values(1)" );
    }


  protected void createIgnoredColumnsTable(String pubName) throws SQLException, RepException {
      StringBuffer ignoredColumnsQuery = new StringBuffer();
      ignoredColumnsQuery.append(" Create Table ").append(getIgnoredColumns_Table()).append(" ( ")
      .append(RepConstants.ignoredColumnsTable_tableId1).append("  INTEGER NOT NULL , ")
      .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2).append("  varchar(150) NOT NULL , ")
      .append("   Primary Key (").append(RepConstants.ignoredColumnsTable_tableId1).append(" , ")
      .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2).append(" ) ) ");
      runDDL(pubName, ignoredColumnsQuery.toString());
  }
  //implement this method for providing provision to stop updations done on shadow table
     protected  void createTriggerForTrackReplicationTablesUpdationTable(String
          pubSubName) throws RepException, SQLException {
    /*    StringBuffer trackRepTablesUpdationTriggerQuery = new StringBuffer();
        trackRepTablesUpdationTriggerQuery.append(" CREATE  TRIGGER TRI_")
            .append(getTrackReplicationTablesUpdation_Table()).append(
                " ON " + getTrackReplicationTablesUpdation_Table())
            .append(" AFTER INSERT AS  DELETE FROM " +
                    getTrackReplicationTablesUpdation_Table() + " WHERE ")
            .append(RepConstants.trackUpdation + " NOT IN(SELECT * FROM inserted)");
        runDDL(pubSubName, trackRepTablesUpdationTriggerQuery.toString());*/
      }

      public Object getMinValOfSyncIdTodeleteRecordsFromShadowTable(String
           tableName, Statement stmt) throws SQLException {
         ResultSet rsSyncId = null, rsConsideredId = null;
         // selecting min of syncid or concideredId  from bookmarks table for one table
         try {
           StringBuffer query = new StringBuffer();
           query.append("Select min(").append(RepConstants.
                                              bookmark_lastSyncId4).append(") from ").
               append(
               getBookMarkTableName()).append(" where ").append(
               RepConstants.
               bookmark_TableName3).append(" = '").append(tableName).append("'");
           rsSyncId = stmt.executeQuery(query.toString());
           rsSyncId.next();
           int minSyncId = rsSyncId.getInt(1);

           StringBuffer queryForMinConsideredId = new StringBuffer();
           queryForMinConsideredId.append("Select min(").append(RepConstants.
               bookmark_ConisderedId5).append(") from ").append(
               getBookMarkTableName()).append(" where ").append(
               RepConstants.
               bookmark_TableName3).append(" = '").append(tableName).append("'");
           rsConsideredId = stmt.executeQuery(queryForMinConsideredId.toString());
           rsConsideredId.next();
           int minConsideredId = rsConsideredId.getInt(1);

           if (minSyncId < minConsideredId) {
             return new Integer(minSyncId);
           }
           else {
             return new Integer(minConsideredId);
           }
         }
         finally {
           if (rsSyncId != null)
             try {
               rsSyncId.close();
             }
             catch (SQLException ex) {
               //ignore SQLException
             }
           if (rsConsideredId != null)
             try {
               rsConsideredId.close();
             }
             catch (SQLException ex) {
               //ignore SQLException
             }
         }
       }

       public PreparedStatement makePrimaryPreperedStatementBackwardTraversing(String[] primaryColumns, long lastId, String local_pub_sub_name,String shadowTable) throws SQLException, RepException {
         StringBuffer query = new StringBuffer();
         query.append(" select FIRST 1 SKIP 0 * from ")
         .append(shadowTable)
         .append(" where ")
         .append(RepConstants.shadow_sync_id1)
         .append(" < ?  ")
         .append(" and ")
         .append(RepConstants.shadow_sync_id1)
         .append(" > ")
         .append(lastId);
         for (int i = 0; i < primaryColumns.length; i++) {
           query.append(" and ")
           .append(primaryColumns[i])
           .append(" = ?  ");
         }
         query.append(" order by ")
         .append(RepConstants.shadow_sync_id1)
         .append(" desc ");
         log.debug(query.toString());
// System.out.println("FireBirdDatabaseHandler  makePrimaryPreperedStatementDelete  ::  " +query.toString());
         Connection pub_sub_Connection = connectionPool.getConnection(local_pub_sub_name);
         return pub_sub_Connection.prepareStatement(query.toString());
       }

  /**
   * isSchemaSupported
   * Returing false because FireBird database does not support schema
   * @return boolean
   */
  public boolean isSchemaSupported() {
    return false;
  }

}
