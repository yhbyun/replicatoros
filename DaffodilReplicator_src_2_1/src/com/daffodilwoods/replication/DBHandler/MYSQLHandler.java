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

public class MYSQLHandler
    extends AbstractDataBaseHandler  {

  protected static Logger log = Logger.getLogger(DaffodilDBHandler.class.
                                                 getName());

  public MYSQLHandler() {}

  public MYSQLHandler(ConnectionPool connectionPool0) {
    connectionPool = connectionPool0;
    vendorType = Utility.DataBase_MySQL;

  }

  protected void createSuperLogTable(String pubName) throws SQLException,
      RepException {
    StringBuffer logTableQuery = new StringBuffer();
    logTableQuery.append(" Create Table ")
        .append(log_Table)
        .append(" ( " ).append( RepConstants.logTable_commonId1).append(" bigint auto_increment , ")
        .append(RepConstants.logTable_tableName2 )
        .append("  varchar(255)  ")
        .append(" , ")
        .append(" Primary Key(").append(RepConstants.logTable_commonId1).append("))");
    runDDL(pubName, logTableQuery.toString());

    StringBuffer indexQuery = new StringBuffer();
    indexQuery.append("CREATE INDEX ")
        .append(RepConstants.log_Index)
        .append(" ON " + getLogTableName())
        .append("(")
        .append(RepConstants.logTable_commonId1)
        .append(")");
//System.out.println(" Create Index on LogTable : "+indexQuery.toString());
    runDDL(pubName, indexQuery.toString());

  }

  /**
   * Because changes has been made in structure of RepTable
   * by Hisar team.So old method has been commented. After
   * proper testing with all data base it should be deleted.
   */


  /*  protected void createRepTable(String pubName) throws SQLException,
        RepException
    {
        StringBuffer repTableQuery = new StringBuffer();
        repTableQuery.append(" Create Table ").append(getRepTableName())
            .append("(  "+RepConstants.repTable_pubsubName1+"  varchar(255) , "+RepConstants.repTable_tableId2+"  int auto_increment, ")
            .append(" "+RepConstants.repTable_tableName2+"  varchar(255) , "+RepConstants.repTable_filter_clause3+"  varchar(255), ")
            .append(" "+RepConstants.repTable_conflict_resolver4+"  varchar(255) , Primary Key ("+RepConstants.repTable_pubsubName1+" , "+RepConstants.repTable_tableName2+" ) ) ");
        runDDL(pubName, repTableQuery.toString());
    }*/

  protected void createRepTable(String pubName) throws SQLException,
      RepException {
    StringBuffer repTableQuery = new StringBuffer();
    repTableQuery.append(" Create Table ").append(getRepTableName()).append(
        " ( ")
        .append(RepConstants.repTable_pubsubName1).append(" varchar(155) , ")
        .append(RepConstants.repTable_tableId2).append("  int auto_increment , ")
        .append(RepConstants.repTable_tableName2).append("  varchar(155) , ")
        .append(RepConstants.repTable_filter_clause3).append(
        "  varchar(155) , ")
        .append(RepConstants.repTable_createshadowtable6).append(
        "  char(1) Default 'Y', ")
        .append(RepConstants.repTable_cyclicdependency7).append(
        "  char(1) Default 'N', ")
        .append(RepConstants.repTable_conflict_resolver4).append(
        "  varchar(155), ")
        .append("   Primary Key (").append(RepConstants.repTable_pubsubName1).
        append(" , ")
        .append(RepConstants.repTable_tableName2).append(" , ").append(RepConstants.repTable_tableId2).append(" ) ) ");
    runDDL(pubName, repTableQuery.toString());
  }

  public void createShadowTable(String pubsubName, String tableName,
                                String allColseq,String[] primaryColumns) throws RepException {
    StringBuffer shadowTableQuery = new StringBuffer();
    shadowTableQuery.append(" Create Table ")
        .append(RepConstants.shadow_Table(tableName))
        .append(" ( " + RepConstants.shadow_sync_id1 +"  bigint auto_increment , ")
        .append("   " + RepConstants.shadow_common_id2 + "  bigint , ")
        .append("   " + RepConstants.shadow_operation3 + "  char(1) , ")
        .append("   " + RepConstants.shadow_status4 + "  char(1) ")
        .append(allColseq)
        .append(" , " + RepConstants.shadow_serverName_n + " varchar(255)  ")
        .append(" , " + RepConstants.shadow_PK_Changed + " char(1)  ")
        .append(" , ")
        .append(" Primary Key( ").append(RepConstants.shadow_sync_id1).append(" ))");
    try {
      runDDL(pubsubName, shadowTableQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      // Ignore the Exception
    }
    createIndex(pubsubName, RepConstants.shadow_Table(tableName));
  }

  protected void createScheduleTable(String subName) throws SQLException,
      RepException {
    StringBuffer ScheduleTableQuery = new StringBuffer();
    ScheduleTableQuery.append(" Create Table ")
        .append(Schedule_TableName)
        .append(" ( " + RepConstants.schedule_Name + " varchar(255) , " +
                RepConstants.subscription_subName1 + " varchar(255) unique , ")
        .append("  " + RepConstants.schedule_type + " varchar(255) , ")
        .append(" " + RepConstants.publication_serverName3 + " varchar (255) ," +
                RepConstants.publication_portNo + " varchar(255) ,")
        .append(" " + RepConstants.recurrence_type + " varchar(255) , " +
                RepConstants.replication_type + " varchar(255) ,")
        .append(" " + RepConstants.schedule_time + " bigint , ")
        .append(" " + RepConstants.schedule_counter + " bigint , Primary Key (" +
                RepConstants.schedule_Name + " , " +
                RepConstants.subscription_subName1 + ") ) ");
    runDDL(subName, ScheduleTableQuery.toString());
//        System.out.println(ScheduleTableQuery.toString());
  }

  //DaffodilDB
  //"  create trigger abc2 after insert on a referencing new n for each row  "
  //"  begin declare va int ; Select max(a)  into va from b;   "
  //"  insert into d values ( va , n.a) ; end " ;
  public void createShadowTableTriggers(String pubsubName, String tableName,
                                        ArrayList colInfoList,
                                        String[] primCols) throws RepException {

    String serverName = getLocalServerName();
//    RepPrinter.print(" Columns are :::::: "  + java.util.Arrays.asList(columnTypeInfoMap.keySet().toArray(new String[0])));
//    String[] colNames = (String[]) columnTypeInfoMap.keySet().toArray(new String[0]);
    int size = colInfoList.size();
    String[] colNames = new String[size];
    for (int i = 0; i < size; i++) {
      colNames[i] = ( (ColumnsInfo) colInfoList.get(i)).getColumnName();
    }
    //RepPrinter.print(" Columns are :::::: "  + java.util.Arrays.asList(colNames));
    String colNameSeq = getColumnNameSequence(colNames, "").toString();
    String colNameSeqPrefixOldRow = getColumnNameSequence(colNames, "OLd.").
        toString();
    String colNameSeqPrefixNewRow = getColumnNameSequence(colNames, "NEW.").
        toString();
    String shadowTableName = RepConstants.shadow_Table(tableName);
    String primColumnNamesSeq = getColumnNameSequence(primCols, "rep_old_");
    String primColNameSeqPrefixOldRow = getColumnNameSequence(primCols,
        "OLD.").toString();
    String primColNameSeqPrefixNewRow = getColumnNameSequence(primCols,
        "NEW.").toString();
    String[] primColsOld= getColumnNameWithOldOrNewPrefix(primCols,"OLD.");
    String[] primColsNew= getColumnNameWithOldOrNewPrefix(primCols,"NEW.");
    StringBuffer insertLogTable = new StringBuffer();
    insertLogTable.append(" Insert into ")
        .append(log_Table)
        .append(" ( ").append(RepConstants.logTable_tableName2)
        .append(" ) values ( '")
        .append(tableName).append("'); ");

    StringBuffer insTriggerQuery = new StringBuffer();
    insTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getInsertTriggerName(tableName))
        .append(" after insert on ").append(tableName)
//      .append(" Referencing new as newRow For each Row begin ")
        .append("  For each Row begin ")
        .append(insertLogTable).append(" Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( null , 'I' , null , ")
        .append(colNameSeqPrefixNewRow).append(primColNameSeqPrefixNewRow)
        .append("'").append(serverName).append("') ; end ");

    StringBuffer delTriggerQuery = new StringBuffer();
    delTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getDeleteTriggerName(tableName))
        .append(" after delete on ").append(tableName)
//        .append(" Referencing old as oldRow For each Row begin ")
        .append("  For each Row begin ")
        .append(insertLogTable).append(" Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( null , 'D' , null , ")
        .append(colNameSeqPrefixOldRow).append(primColNameSeqPrefixOldRow)
        .append("'").append(serverName).append("') ; end ");

    StringBuffer updTriggerQuery = new StringBuffer();
    updTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getUpdateTriggerName(tableName))
        .append(" after update on ").append(tableName)
//        .append(" Referencing new as newRow old as oldRow For each Row ")
        .append("  For each Row ")
        .append(" begin declare maxlogid bigint; declare pkchanged char(1); ").append(insertLogTable)
        .append(" Select max(" + RepConstants.logTable_commonId1 +
                ") into maxlogid from ")
        .append(log_Table).append("; ")
        //-------------
        .append(" if( ");
         for (int i = 0; i < primColsOld.length; i++) {
           if (i != 0)
             updTriggerQuery.append(" and ");
           updTriggerQuery.append(primColsOld[i] )
                                   .append("!=" )
                                   .append(primColsNew[i]);
         }
         updTriggerQuery.append(" ) Then")
         .append(" Set pkchanged ='Y' ; end if;")
        //--------
        .append( "Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( maxlogid , 'U' , 'B' , ")
        .append(colNameSeqPrefixOldRow).append(primColNameSeqPrefixOldRow)
        .append("'").append(serverName).append("') ; Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n).append(" , ")
        .append(RepConstants.shadow_PK_Changed)
        .append(" ) Values ( maxlogid , 'U' , 'A' , ")
        .append(colNameSeqPrefixNewRow).append(primColNameSeqPrefixOldRow)
        .append("'").append(serverName).append("',pkchanged) ; end ");
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


  public boolean isDataTypeOptionalSizeSupported(TypeInfo typeInfo) {
    int sqlType = typeInfo.getSqlType();
    switch (sqlType) {
      case -4:
      case -1:
      case -7:
      case 91:
      case 92:
      case 93:
      case 8:
        return false;
      default:
        return true;
    }
  }

  public void setTypeInfo(TypeInfo typeInfo, ResultSet rs) throws RepException,
      SQLException {
    switch (typeInfo.getSqlType()) {
      case Types.BOOLEAN:  //16;
      case Types.BIT:       //-7;
          typeInfo.setTypeName("bool");
        break;
      case Types.TINYINT:
        if (isColumnSizeExceedMaximumSize(typeInfo)) {
         typeInfo.setSqlType(Types.INTEGER);
         typeInfo.setTypeName("int"); //4
         return;
       }
          typeInfo.setTypeName("TinyInt");
        break;  //-6;
      case Types.SMALLINT:
        typeInfo.setTypeName("smallint");
        break;  // 5;
      case Types.INTEGER:
        typeInfo.setTypeName("integer");
        break; // // 4;
      case Types.BIGINT:
        typeInfo.setTypeName("bigint");
        break; // -5;
      case Types.FLOAT:
        typeInfo.setTypeName("float");
        break; //  6;
      case Types.REAL:
        typeInfo.setTypeName("real");
        break; //  7;
      case Types.DOUBLE:
        typeInfo.setTypeName("double precision");
        break; //  8;
      case Types.NUMERIC:
        typeInfo.setTypeName("numeric");
        break; //  2;
      case Types.DECIMAL:  // 3;
        if (typeInfo.getTypeName().equalsIgnoreCase("NUMERIC")) {
          typeInfo.setTypeName("numeric");
          return;
        }
        typeInfo.setTypeName("decimal");
        break; // 3;
      case Types.CHAR:
        if (isColumnSizeExceedMaximumSize(typeInfo)) {
          typeInfo.setSqlType(Types.CLOB);
          typeInfo.setTypeName("TEXT"); //2005
          return;
        }
        typeInfo.setTypeName("char");
        break; // // 1;
      case Types.VARCHAR:
        // Added by sube May 5 2004 to handle SQl_Variant
        // (||) check added by Nancy to handle postgresql text
        if (isColumnSizeExceedMaximumSize(typeInfo) ||
            typeInfo.getTypeName().equalsIgnoreCase("text")) {
          typeInfo.setSqlType(Types.CLOB);
          typeInfo.setTypeName("text"); //2005
          return;
        }
        else if (typeInfo.getTypeName().trim().equalsIgnoreCase("sql_variant")) {
          typeInfo.setSqlType(Types.BLOB);
          typeInfo.setTypeName("blob");
          return; //2004
        }
        typeInfo.setTypeName("varchar");
        break; // //12;
      case Types.LONGVARCHAR: //typeInfo.setTypeName("clob");break;// //-1;
        typeInfo.setSqlType(Types.CLOB);
        typeInfo.setTypeName("text");
        break; //2005
      case Types.DATE:
        typeInfo.setTypeName("date");
        break; // //91;
      case Types.TIME:
        typeInfo.setTypeName("time");
        break; // //92;
      case Types.TIMESTAMP:
        if (typeInfo.getTypeName().equalsIgnoreCase("Time")) {
          typeInfo.setSqlType(Types.TIME);
          typeInfo.setTypeName("time");
          break;
        }
        else if (typeInfo.getTypeName().equalsIgnoreCase("Date")) {
          typeInfo.setSqlType(Types.DATE);
          typeInfo.setTypeName("date");
          break;

        }
        typeInfo.setTypeName("timestamp");
        break; // //93;
      case Types.BINARY:

        //added by Nancy to handle postgresql bytea
        if (typeInfo.getTypeName().equalsIgnoreCase("bytea")) {
          typeInfo.setSqlType(Types.BLOB);
          typeInfo.setTypeName("blob");
          return;
        }
        typeInfo.setTypeName("binary");
        break; // //-2;
      case Types.VARBINARY:
        if (typeInfo.getTypeName().equalsIgnoreCase("bit varying")) {
          break;
        }
        typeInfo.setTypeName("varbinary");
        break; // //-3;
        // bit varying is changed into varbinary and causing incompatibility - 3rd april 04
        //case Types.VARBINARY 	:      typeInfo.setTypeName("varbinary");break;// //-3;
      case Types.LONGVARBINARY:
        typeInfo.setTypeName("long varbinary");
        break; // //-4;
      case Types.JAVA_OBJECT:
        typeInfo.setTypeName("long varbinary");
        break; //2000;
      case Types.BLOB:
        typeInfo.setTypeName("blob");
        break; //2004;
      case Types.CLOB:
        typeInfo.setTypeName("clob");
        break; //2005;

      case Types.REF:
        typeInfo.setTypeName("clob");
        break; //2006;
      case Types.OTHER:
        typeInfo.setTypeName("clob");
        break; //1111;
      case Types.NULL:
      case Types.DISTINCT: //2001;
      case Types.STRUCT: //2002;
      case Types.ARRAY: //2003;
      case Types.DATALINK: //70;
      default:
        throw new RepException("REP031", new Object[] {typeInfo.getTypeName()});
    }
  }

  public AbstractColumnObject getColumnObject(TypeInfo typeInfo) throws
      RepException {
    int sqlType = typeInfo.getSqlType();
// RepPrinter.print(" DaffodilDBHandler typeInfo = " + typeInfo);
    switch (sqlType) {
      case 1: // char
      case 12: // char varying
        return new StringObject(sqlType, this);
      case -3: // bitvarying
        String typeName = typeInfo.getTypeName();
        if (typeName.equalsIgnoreCase("bit varying")) {
          return new BytesObject(sqlType, this);
        }
        return new BlobObject(sqlType, this);
      case -1: // long varchar
      case 2005: // clob
        return new ClobObject(sqlType, this);
      case 2004: // blob
      case -2: // binary
      case -4: // long varbiary
        return new BlobObject(sqlType, this);
      case 4: // int
      case 5: // small int
      case -6: // tinyint
        return new IntegerObject(sqlType, this);
//          case 2: // numeric
      case -5: // bigint
//        Latest change by sachin - march 04   Deleted
//          case -4: // long varbiary
      case 2000: // long varbinary
        return new LongObject(sqlType, this);
      case 3: // decimal
        if ( (typeInfo.getTypeName()).equalsIgnoreCase("NUMERIC")) {
          return new BigDecimalObject(sqlType, this);
        }
      case 8: // double precision
      case 2: // numeric
      case 6: // float
        return new DoubleObject(sqlType, this);
      case 7: // real
        return new FloatObject(sqlType, this);
      case -7: // bit
      case 16: // boolean
        return new BooleanObject(sqlType, this);
      case 91: // date
        return new DateObject(sqlType, this);
      case 92: // time
        return new TimeObject(sqlType, this);
      case 93: // time stamp
        return new TimeStampObject(sqlType, this);
      default:
        throw new RepException("REP031", new Object[] {new Integer(sqlType)});
    }
  }

  public void createSchemas(String pubName, ArrayList schemas) throws
      SQLException, RepException {
    int num = schemas.size();
    if (num == 0) {
      return;
    }
    Connection con = connectionPool.getConnection(pubName);
    String authorization = connectionPool.getUserName();
    for (int i = 0; i < num; i++) {
      String schemaName = (String) schemas.get(i);
      if (schemaName.equalsIgnoreCase("users")) {
        continue;
      }
      StringBuffer sb = new StringBuffer();
      sb.append(" Create Schema ").append(schemaName)
          .append(" authorization ").append(authorization);
      try {
        runDDL(pubName, sb.toString());
      }
      catch (SQLException ex) {
        RepConstants.writeERROR_FILE(ex);
        // Ignore the Exception
      }
      catch (RepException ex) {
        throw ex;
      }
    }
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

  public boolean isColumnSizeExceedMaximumSize(TypeInfo typeInfo) throws
      SQLException, RepException {
    boolean flag = false;
    int sqlType = typeInfo.getSqlType();
    int columnsize = typeInfo.getcolumnSize();
    switch (sqlType) {
      case 1: // char
      case 12: //varchar
        if (columnsize > 4192) {
          flag = true;
        }
        break;
      case -6: // tinyint
        if (columnsize > 127) {
          flag = true;

        }
        break;
    }
    return flag;
  }

  public boolean isColumnSizeExceedMaximumSize1(ResultSet rs, TypeInfo typeInfo) throws
      SQLException, RepException {
    boolean flag = false;
    int sqlType = typeInfo.getSqlType();
    int columnsize = rs.getInt("COLUMN_SIZE");
    switch (sqlType) {
      case 1: // char
      case 12: //varchar
        if (columnsize > 4192) {
          flag = true;

        }
        break;
      case -6: // tinyint
        if (columnsize > 127) {
          flag = true;

        }
        break;
    }
    return flag;
  }

  public void setDefaultSchema(Connection connection) throws RepException {
//    Statement st = null;
//    try {
//      st = connection.createStatement();
//      String query = "set schema users ";
//      st.execute(query);
//    }
//    catch (SQLException ex) {
//      RepConstants.writeERROR_FILE(ex);
//      throw new RepException("REP103", new Object[] {ex.getMessage()});
//    }
//    finally {
//
//      try {
//        st.close();
//      }
//      catch (SQLException ex2) {
//      }
//    }

  }

  public void setColumnPrecisionInTypeInfo(TypeInfo typeInfo,
                                           ResultSetMetaData rsmt,
                                           int columnIndex) throws SQLException {
    int columnPrecion = rsmt.getPrecision(columnIndex);
    typeInfo.setColumnSize(columnPrecion);

  }

  public boolean isPrimaryKeyException(SQLException ex) throws SQLException {
    // 1276 for update
    // 1275 for insert
// System.out.println("DaffodilDB Handler  = "+ex.getErrorCode()+"  ex.getMessage() = "+ex.getMessage());
    if (ex.getMessage().indexOf("PRIMARY KEY") != -1 || ex.getErrorCode() == 1062 ) {
      return true;
    }
//      else if(ex.getMessage().indexOf("column does not allow nulls")!=-1){
//        return false;
//      }

    return false;
  }

  public int getAppropriatePrecision(int columnSize, String datatypeName) {

    if (datatypeName.equalsIgnoreCase("numeric") && columnSize > 38) {
      columnSize = 38;
    }
    else if ( (datatypeName.equalsIgnoreCase("decimal") ||
               datatypeName.equalsIgnoreCase("dec")) && columnSize > 38) {
      columnSize = 38;
    }
    return columnSize;
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
// System.out.println(" createIndexQuery : "+createIndexQuery.toString());
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

  public int getAppropriateScale(int columnScale) throws RepException {
    if (columnScale < 0) {
      throw new RepException("REP026", new Object[] {"1", "38"});
    }
    else if (columnScale >= 38) {
      columnScale = 38;
    }
    else if (columnScale >= 0 && columnScale < 38)
      columnScale = columnScale;
    log.debug("returning columnScale:: " + columnScale);
    return columnScale;
  }

  public PreparedStatement makePrimaryPreperedStatement(String[]
      primaryColumns, String shadowTable, String local_pub_sub_name) throws
      SQLException, RepException {
    StringBuffer query = new StringBuffer();
    query.append(" select  * from ");
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
    query.append(" order by " + RepConstants.shadow_sync_id1)
        .append( " limit 1");
    Connection pub_sub_Connection = connectionPool.getConnection(
        local_pub_sub_name);
//System.out.println("MYSQLHandler.makePrimaryPreperedStatement() :: "+query.toString().toUpperCase());
    return pub_sub_Connection.prepareStatement(query.toString());
  }

  public boolean isForiegnKeyException(SQLException ex) throws SQLException {
    if (ex.getErrorCode() == 1277)
      return true;
    else
      return false;
  }

  public String getIgnoredColumns_Table() {
    return ignoredColumns_Table;
  }

  public String getTrackReplicationTablesUpdation_Table() {
    return trackReplicationTablesUpdation_Table;
  }

  public String getTrackPrimayKeyUpdation_Table() {
      return trackPrimaryKeyUpdation_Table;
    }

  protected void createIgnoredColumnsTable(String pubName) throws SQLException,
      RepException {
    StringBuffer ignoredColumnsQuery = new StringBuffer();
    ignoredColumnsQuery.append(" Create Table ").append(getIgnoredColumns_Table()).
        append(" ( ")
        .append(RepConstants.ignoredColumnsTable_tableId1).append("  int , ")
        .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2).append(
        "  varchar(255) , ")
        .append("   Primary Key (").append(RepConstants.
                                           ignoredColumnsTable_tableId1).append(
        " , ")
        .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2).append(
        " ) ) ");
    runDDL(pubName, ignoredColumnsQuery.toString());
  }

  protected void createTrackReplicationTablesUpdationTable(String pubSubName) throws
      RepException, SQLException {
    StringBuffer trackRepTablesUpdationQuery = new StringBuffer();
    trackRepTablesUpdationQuery.append(" CREATE  TABLE ").append(
        getTrackReplicationTablesUpdation_Table()).append(" ( " +
        RepConstants.trackUpdation + " bit  PRIMARY KEY) ");
    runDDL(pubSubName, trackRepTablesUpdationQuery.toString());
    runDDL(pubSubName,
           "Insert into " + getTrackReplicationTablesUpdation_Table() +
           " values(1)");
  }

  //implement this method for providing provision to stop updations done on shadow table
  protected void createTriggerForTrackReplicationTablesUpdationTable(String
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


  public PreparedStatement makePrimaryPreperedStatementBackwardTraversing(String[] primaryColumns, long lastId, String local_pub_sub_name, String shadowTable) throws SQLException, RepException {
    StringBuffer query = new StringBuffer();
    query.append(" select top(1) * from ");
    query.append(shadowTable);
    query.append(" where ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" < ?  ");
    query.append(" and ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" > ");
    query.append(lastId);
    for (int i = 0; i < primaryColumns.length; i++) {
      query.append(" and ");
      query.append(primaryColumns[i]);
      query.append(" = ?  ");
    }
    query.append(" order by ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" desc ");
    log.debug(query.toString());
//System.out.println("DaffodilDBHandler makePrimaryPreperedStatementDelete  ::  " +query.toString());
    Connection pub_sub_Connection = connectionPool.getConnection(local_pub_sub_name);
    return pub_sub_Connection.prepareStatement(query.toString());
  }

//public static void main(String[] args) {
//try {
//  Class.forName("com.mysql.jdbc.Driver");
//  Connection conn =  DriverManager.getConnection("jdbc:mysql://localhost:3306/replicator","root","");
//System.out.println("MYSQLHandler.main(args) ::   "+conn.getMetaData().getDatabaseProductName());
//  Util.showResultSet(conn.getMetaData().getTypeInfo());

//  System.out.println("Connection Established");
//}
//catch (Exception ex) {ex.printStackTrace();}
//}

  /**
   * isSchemaSupported
   * Returning false because MYSQL database does not support schema.
   * @return boolean
   */
  public boolean isSchemaSupported() {
    return false;
  }

}
