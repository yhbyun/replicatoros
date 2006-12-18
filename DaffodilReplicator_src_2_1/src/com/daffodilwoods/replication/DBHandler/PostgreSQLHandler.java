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

public class PostgreSQLHandler
    extends AbstractDataBaseHandler  {

  private String pg_publication_TableName;
  private String pg_subscription_TableName;
  private String pg_bookmark_TableName;
  private String pg_rep_TableName;
  private String pg_log_Table;
  private String pg_schedule_Table;
  private String pg_ignoredColumns_Table;
  private String pg_trackReplicationTablesUpdation_Table;
  private String pg_trackPrimaryKeyUpdation_Table;
  protected static Logger log = Logger.getLogger(PostgreSQLHandler.class.
                                                 getName());
  public PostgreSQLHandler() {}

  public PostgreSQLHandler(ConnectionPool connectionPool0) {
    connectionPool = connectionPool0;
    vendorType = Utility.DataBase_PostgreSQL;
    pg_publication_TableName = "public." + publication_TableName;
    pg_subscription_TableName = "public." + subscription_TableName;
    pg_bookmark_TableName = "public." + bookmark_TableName;
    pg_rep_TableName = "public." + rep_TableName;
    pg_log_Table = "public." + log_Table;
    pg_schedule_Table = "public." + Schedule_TableName;
    pg_ignoredColumns_Table="public."+ignoredColumns_Table;
    pg_trackReplicationTablesUpdation_Table="public."+trackReplicationTablesUpdation_Table;
    pg_trackPrimaryKeyUpdation_Table ="public."+trackPrimaryKeyUpdation_Table;
  }

  protected void createSuperLogTable(String pubName) throws SQLException,
      RepException {
    StringBuffer logTableQuery = new StringBuffer();
    logTableQuery.append(" Create Table ")
        .append(pg_log_Table)
        .append(" ( " + RepConstants.logTable_commonId1 + " bigserial , " +
                RepConstants.logTable_tableName2 + " varchar(255) ) ");
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
   .append("(  "+RepConstants.repTable_pubsubName1+" varchar(255) ,  "+RepConstants.repTable_tableId2+"  bigserial, ")
            .append("  "+RepConstants.repTable_tableName2+"  varchar(255) ,  "+RepConstants.repTable_filter_clause3+"  varchar(255), ")
            .append("  "+RepConstants.repTable_conflict_resolver4+"  varchar(255) , Primary Key ( "+RepConstants.repTable_pubsubName1+" ,  "+RepConstants.repTable_tableName2+" ) ) ");

        runDDL(pubName, repTableQuery.toString());
    } */

  protected void createRepTable(String pubName) throws SQLException,
      RepException {
    StringBuffer repTableQuery = new StringBuffer();
    repTableQuery.append(" Create Table ").append(getRepTableName()).append(
        " ( ")
        .append(RepConstants.repTable_pubsubName1).append(" varchar(255) , ")
        .append(RepConstants.repTable_tableId2).append("  bigserial, ")
        .append(RepConstants.repTable_tableName2).append("  varchar(255) , ")
        .append(RepConstants.repTable_filter_clause3).append(
        "  varchar(255) , ")
        .append(RepConstants.repTable_createshadowtable6).append(
        "  char(1) Default 'Y', ")
        .append(RepConstants.repTable_cyclicdependency7).append(
        "  char(1) Default 'N', ")
        .append(RepConstants.repTable_conflict_resolver4).append(
        "  varchar(255), ")
        .append("   Primary Key (").append(RepConstants.repTable_pubsubName1).
        append(" , ")
        .append(RepConstants.repTable_tableName2).append(" ) ) ");
    runDDL(pubName, repTableQuery.toString());
  }

  protected void createBookMarkTable(String pubName) throws SQLException,
      RepException {
    StringBuffer bookmarkTableQuery = new StringBuffer();
    bookmarkTableQuery.append(" Create Table ")
        .append(getBookMarkTableName())
        .append(" (  " + RepConstants.bookmark_LocalName1 +
                "  varchar(255) ,  " + RepConstants.bookmark_RemoteName2 +
                "  varchar(255) , ")
        .append("  " + RepConstants.bookmark_TableName3 + "  varchar(255) ,  " +
                RepConstants.bookmark_lastSyncId4 + "  bigint , ")
        .append(
        "  " + RepConstants.bookmark_ConisderedId5 + "  bigint ," +
        RepConstants.bookmark_IsDeletedTable +
        " char(1) default 'N' , Primary Key ( " +
        RepConstants.bookmark_LocalName1 + " ,  " +
        RepConstants.bookmark_RemoteName2 + " ,  " +
        RepConstants.bookmark_TableName3 + " ) ) ");
    runDDL(pubName, bookmarkTableQuery.toString());
  }

  public void createShadowTable(String pubsubName, String tableName,
                                String allColseq,String[] primaryColumns) throws RepException {
    StringBuffer shadowTableQuery = new StringBuffer();
    shadowTableQuery.append(" Create Table ")
        .append(RepConstants.shadow_Table(tableName))
        .append(" (  " + RepConstants.shadow_sync_id1 + "  bigserial , ")
        .append("    " + RepConstants.shadow_common_id2 + "   bigint , ")
        .append("    " + RepConstants.shadow_operation3 + "   character(1) , ")
        .append("    " + RepConstants.shadow_status4 + "   character(1) ")
        .append(allColseq)
        .append(" ,  " + RepConstants.shadow_serverName_n + "  varchar(255) ")
        .append(" ,  " + RepConstants.shadow_PK_Changed + "  char(1) ) ");
    try {
//System.out.println(" shadowTableQuery.toString() ="+shadowTableQuery.toString());
      runDDL(pubsubName, shadowTableQuery.toString());

    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
//          ex.printStackTrace();
      // Ignore the Exception
    }
    createIndex(pubsubName,RepConstants.shadow_Table(tableName));
  }

  protected void createScheduleTable(String subName) throws SQLException,
      RepException {
    StringBuffer ScheduleTableQuery = new StringBuffer();

    ScheduleTableQuery.append(" Create Table ")
        .append(pg_schedule_Table)
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
  }

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
    String colNameSeqPrefixOldRow = getColumnNameSequence(colNames, "old.").
        toString();
    String colNameSeqPrefixNewRow = getColumnNameSequence(colNames, "new.").
        toString();
    String shadowTableName = RepConstants.shadow_Table(tableName);
    String primColumnNamesSeq = getColumnNameSequence(primCols, "rep_old_");
    String primColNameSeqPrefixOldRow = getColumnNameSequence(primCols, "old.").toString();
    String primColNameSeqPrefixNewRow = getColumnNameSequence(primCols, "new.").toString();

    String[] primColsOld = getColumnNameWithOldOrNewPrefix(primCols, "old.");
    String[] primColsNew = getColumnNameWithOldOrNewPrefix(primCols, "new.");

//create trigger abc after delete on t2 for each row  execute procedure delete_insert();
    String table = tableName.substring(tableName.indexOf('.') + 1);
    StringBuffer insTriggerQuery = new StringBuffer();
    insTriggerQuery.append(" Create trigger ")
        .append(getInsertTriggerName(table))
        .append(" after insert on ").append(tableName)
        .append(" For each Row execute procedure  insert_" + table + "()");

    StringBuffer delTriggerQuery = new StringBuffer();
    delTriggerQuery.append(" Create trigger ")
        .append(getDeleteTriggerName(table))
        .append(" after delete on ").append(tableName)
        .append(" For each Row execute  procedure  delete_" + table + "()");

    StringBuffer updTriggerQuery = new StringBuffer();
    updTriggerQuery.append(" Create trigger ")
        .append(getUpdateTriggerName(table))
        .append(" after update on  ").append(tableName)
        .append(" For each Row  execute  procedure  update_" + table + "()");

    try {
      runDDL(pubsubName, createFunctionHandler());
    }
    catch (RepException ex1) {
    }
    catch (SQLException ex1) {
    }

    try {
      runDDL(pubsubName, createFunctionLanguage());
    }
    catch (RepException ex2) {
    }
    catch (SQLException ex2) {

    }

    try {
      runDDL(pubsubName,
             functionForInsertTrigger(tableName, shadowTableName,
                                      primColumnNamesSeq,
                                      colNameSeq, colNameSeqPrefixNewRow,
                                      primColNameSeqPrefixNewRow, serverName));
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
//          ex.printStackTrace();
      // Ignore Exception
    } try {
           runDDL(pubsubName,
             functionForDeleteTrigger(tableName, colNameSeqPrefixOldRow,
                                      primColNameSeqPrefixOldRow,
                                      shadowTableName, colNameSeq,
                                      primColumnNamesSeq, serverName));

    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
//          ex.printStackTrace();
      // Ignore Exception
    }
    try {
             runDDL(pubsubName,
                functionForUpdateTrigger(tableName, colNameSeqPrefixOldRow,
                                         primColNameSeqPrefixOldRow,
                                         shadowTableName, colNameSeq,
                                         primColumnNamesSeq, serverName,
                                         colNameSeqPrefixNewRow,primColsOld,primColsNew));

         }
       catch (RepException ex) {
         throw ex;
       }
       catch (SQLException ex) {
//          ex.printStackTrace();
         // Ignore Exception
       }
       try {
                runDDL(pubsubName, insTriggerQuery.toString());
          }
          catch (RepException ex) {
            throw ex;
          }
          catch (SQLException ex) {
//          ex.printStackTrace();
            // Ignore Exception
          }
          try {
               runDDL(pubsubName, delTriggerQuery.toString());
             }
             catch (RepException ex) {
               throw ex;
             }
             catch (SQLException ex) {
//          ex.printStackTrace();
               // Ignore Exception
             }
             try {
                  runDDL(pubsubName, updTriggerQuery.toString());
                }
                catch (RepException ex) {
                  throw ex;
                }
                catch (SQLException ex) {
//          ex.printStackTrace();
                  // Ignore Exception
                }
}

  public String createFunctionHandler() {
    StringBuffer sb = new StringBuffer();
    sb.append("CREATE FUNCTION plpgsql_call_handler() RETURNS language_handler")
        .append("  AS '$libdir/plpgsql' ")
        .append(" LANGUAGE C ");
    return sb.toString();
  }

  public String createFunctionLanguage() {
    StringBuffer sb = new StringBuffer();
    sb.append("CREATE LANGUAGE plpgsql ")
        .append(" HANDLER plpgsql_call_handler");
    return sb.toString();
  }

  public String functionForInsertTrigger(String tableName,
                                         String shadowTableName,
                                         String primColumnNamesSeq,
                                         String colNameSeq,
                                         String colNameSeqPrefixNewRow,
                                         String primColNameSeqPrefixNewRow,
                                         String serverName) {

    String table = tableName.substring(tableName.indexOf('.') + 1);
    StringBuffer sb = new StringBuffer();
    sb.append("CREATE FUNCTION " + "\"" + "insert_" + table + "\"" +
              "() RETURNS Trigger AS '")
        .append(" BEGIN ")
        .append(insertRecordIntoLogTable(tableName)).append(" Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( null , ''I'' , null , ")
        .append(colNameSeqPrefixNewRow).append(primColNameSeqPrefixNewRow)
        .append(" ''").append(serverName).append("'') ; ")
        .append("  RETURN NULL;")
        .append(" END;")
        .append(" ' LANGUAGE 'plpgsql' VOLATILE ");
    return sb.toString();
  }

  public String functionForUpdateTrigger(String tableName,
                                         String colNameSeqPrefixOldRow,
                                         String primColNameSeqPrefixOldRow,
                                         String shadowTableName,
                                         String colNameSeq,
                                         String primColumnNamesSeq,
                                         String serverName,
                                         String colNameSeqPrefixNewRow,String[] primColsOld,String[] primColsNew) {
    String table = tableName.substring(tableName.indexOf('.') + 1);
    StringBuffer sb = new StringBuffer();
    sb.append("CREATE FUNCTION " + "\"" + "update_" + table + "\"" +
              "() RETURNS Trigger AS '")
        .append(" Declare ")
        .append(" maxlogid bigint; pkchanged char(1); ")
        .append(" BEGIN ")
        .append(insertRecordIntoLogTable(tableName))
        .append(" Select max( " + RepConstants.logTable_commonId1 +
                " ) into maxlogid from ")
        .append(pg_log_Table).append(" ; ")
        .append(" if( ");
        for (int i = 0; i < primColsOld.length; i++) {
          if (i != 0)
            sb.append(" and ");
            sb.append(primColsOld[i] )
              .append("!=" )
              .append(primColsNew[i]);
        }
        sb.append(" ) then ")
        .append(" pkchanged =''Y''; end if;")
        .append(" Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( maxlogid , ''U'' , ''B'' , ")
        .append(colNameSeqPrefixOldRow).append(primColNameSeqPrefixOldRow)
        .append(" ''").append(serverName).append("'') ; Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n).append(" , ")
        .append(RepConstants.shadow_PK_Changed)
        .append(" ) Values ( maxlogid , ''U'' , ''A'' , ")
        .append(colNameSeqPrefixNewRow).append(primColNameSeqPrefixOldRow)
        .append(" ''").append(serverName).append("'',pkchanged) ;")
        .append(" RETURN NULL;")
        .append(" END;")
        .append(" ' LANGUAGE 'plpgsql' VOLATILE ");
    return sb.toString();
  }

  public String functionForDeleteTrigger(String tableName,
                                         String colNameSeqPrefixOldRow,
                                         String primColNameSeqPrefixOldRow,
                                         String shadowTableName,
                                         String colNameSeq,
                                         String primColumnNamesSeq,
                                         String serverName) {

    String table = tableName.substring(tableName.indexOf('.') + 1);
    StringBuffer sb = new StringBuffer();
    sb.append("CREATE FUNCTION  " + "\"" + "delete_" + table + "\"" +
              "() RETURNS Trigger AS '")
        .append(" BEGIN ")
        .append(insertRecordIntoLogTable(tableName)).append("  Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( null , ''D'' , null , ")
        .append(colNameSeqPrefixOldRow).append(primColNameSeqPrefixOldRow)
        .append("''").append(serverName).append("'') ; ")
        .append(" RETURN NULL ;")
        .append(" END;")
        .append("'  LANGUAGE 'plpgsql' VOLATILE ");
    return sb.toString();
  }

  public String insertRecordIntoLogTable(String tableName) {
    StringBuffer insertLogTable = new StringBuffer();
    insertLogTable.append(" Insert into ")
        .append(pg_log_Table)
        .append(" ( ").append(RepConstants.logTable_tableName2)
        .append(" ) values ( ''")
        .append(tableName).append("''); ");
    return insertLogTable.toString();

  }

  private String getInsertTriggerName(String schematable) {
    String tableInsert;
    int index = schematable.indexOf('.');
    if (index == -1) {
      tableInsert = schematable;
    }
    tableInsert = schematable.substring(schematable.indexOf(".") + 1).trim();
    return getObjectName(tableInsert, "tri_");
  }

  private String getDeleteTriggerName(String schematable) {
    String tableDelete;
    int index = schematable.indexOf('.');
    if (index == -1) {
      tableDelete = schematable;
    }
    tableDelete = schematable.substring(schematable.indexOf(".") + 1).trim();
    return getObjectName(tableDelete, "trd_");
  }

  private String getUpdateTriggerName(String schematable) {
    String tableUpdate;
    int index = schematable.indexOf('.');
    if (index == -1) {
      tableUpdate = schematable;
    }
    tableUpdate = schematable.substring(schematable.indexOf(".") + 1).trim();
    return getObjectName(tableUpdate, "tru_");
  }

  public void dropTriggersAndShadowTable(Connection connection, String table,
                                         String pubsubName) throws SQLException,
      RepException {
    String tableName = table.substring(table.indexOf('.') + 1);
    String insertTriggerName = getInsertTriggerName(tableName);
    String updateTriggerName = getDeleteTriggerName(tableName);
    String DeleteTriggerName = getUpdateTriggerName(tableName);

    fireDropQuery(connection,
                  " drop trigger " + insertTriggerName + " on " + table);

    fireDropQuery(connection,
                  " drop trigger " + updateTriggerName + " on " + table);

    fireDropQuery(connection,
                  " drop trigger " + DeleteTriggerName + " on " + table);

    fireDropQuery(connection, " drop table  " + RepConstants.shadow_Table(table));

    fireDropQuery(connection, " drop function " + "insert_" + tableName + "()");

    fireDropQuery(connection, " drop function " + "delete_" + tableName + "()");

    fireDropQuery(connection, " drop function " + "update_" + tableName + "()");

    fireDropQuery(connection, " delete from " + getLogTableName() + " where " +
                  RepConstants.logTable_tableName2 + " = '" + table + "'");
  }

  private static String getObjectName(String schematable, String prefix) {
    int index = schematable.indexOf('.');
    if (index == -1) {
      return prefix + schematable;
    }
    String schema = schematable.substring(0, index);
    String table = schematable.substring(index + 1);
    return schema + "." + prefix + table;
  }

  public void createSubscribedTablesTriggersAndShadowTables(String subName,
      String[] pubTableQueries, HashMap primCols, int pubVendorType,ArrayList repTables) throws
      RepException,SQLException {
    Connection connection = connectionPool.getConnection(subName);
    MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, subName);
    Statement stt = connection.createStatement();
    try {
      for (int i = 0; i < pubTableQueries.length; i++) {
        try {
          stt.execute(pubTableQueries[i].toLowerCase());
        }
        catch (SQLException ex) {
          SchemaQualifiedName sname = new SchemaQualifiedName(mdi,
              getTableName(pubTableQueries[i]));
          String existing = null;
          try {
            existing = mdi.getExistingTableQuery(this, sname, pubVendorType).
                toLowerCase().replaceAll(" ", "");
          }
          catch (RepException ex1) {
            if (ex1.getRepCode().equalsIgnoreCase("REP033")) {
              throw ex;
            }
          }
          if (pubTableQueries[i].toLowerCase().replaceAll(" ",
              "").indexOf(existing.toLowerCase()) != 0) {
            throw new RepException("REP024", new Object[] {subName,
                                   sname.toString()});
          }
          // Ignore the exception if the same table exists with same colums and primary key
        }
      }
    }
    finally {
      if (stt != null)
        stt.close();
    }
    // Just Get the Column Sequenes from Create Table Queries
    // and create Shadow Table and Triggers on Shadow Table
    for (int i = 0; i < pubTableQueries.length; i++) {
      String tableName = getTableName(pubTableQueries[i]);
      SchemaQualifiedName sname = new SchemaQualifiedName(mdi,
          getTableName(pubTableQueries[i]));
      String colSeqenceWithDataType = getColumnSequenceWithDataTypes(
          pubTableQueries[i]);
      ArrayList colInfoList = getColumnNamesAndDataTypes(colSeqenceWithDataType.
          trim());
      String[] pcols = (String[]) ( (ArrayList) primCols.get(tableName.
          toLowerCase())).toArray(new String[0]);
      String allColSequence = getShadowTableColumnDataTypeSequence(colInfoList,
          pcols,(RepTable)repTables.get(i));
      createShadowTable(subName, sname.toString(), allColSequence,pcols);
      makeProvisionForLOBDataTypes(colInfoList);
      createShadowTableTriggers(subName, sname.toString(), colInfoList, pcols);
      //createShadowTablesAndSubscriptionTableTriggers(subName,tableName,colSeqenceWithDataType,(ArrayList) primCols.get(tableName.toLowerCase()));
    }
  }

  public boolean isDataTypeOptionalSizeSupported(TypeInfo typeInfo) {
    int sqlType = typeInfo.getSqlType();
    String typeName = typeInfo.getTypeName();
    switch (sqlType) {
      case 4:
      case 5:
      case 7:
      case -4:
      case -5:
      case -7:
      case 91:
      case 92:
      case 93:
      case -6:
      case 8:
      case -2:
      case -3:
      case 2000:
      case 16:
      case 2004:
      case 2005:
      case 1111:
      case 2006:
      case -1:

//            case 1 :
        return false;

      case 12:
        if (typeInfo.getTypeName().equalsIgnoreCase("text")) {
          return false;
        }
      default:
        return true;
    }
  }

  public void setTypeInfo(TypeInfo typeInfo, ResultSet rs) throws RepException,
      SQLException {
//System.out.println("PostgreSQLHandler = "+typeInfo);
    switch (typeInfo.getSqlType()) {
      case Types.BOOLEAN:
        typeInfo.setTypeName("boolean");
        break; // //16;
      case Types.BIT:
        if (typeInfo.getTypeName().equalsIgnoreCase("bool")) {
          typeInfo.setTypeName("bool");
          return;
        }
        typeInfo.setTypeName("bit");
        break; // //-7;
      case Types.TINYINT:
        typeInfo.setTypeName("int2");
        break; //-6;
      case Types.SMALLINT:
        typeInfo.setTypeName("int2");
        break; // // 5;
      case Types.INTEGER:
        typeInfo.setTypeName("int4");
        break; // // 4;
      case Types.BIGINT:
        typeInfo.setTypeName("int8");
        break; // //-5;
      case Types.FLOAT:
        typeInfo.setTypeName("numeric");
        break; // // 6;
      case Types.REAL:
        typeInfo.setTypeName("float4");
        break; // // 7;
      case Types.DOUBLE:
        typeInfo.setTypeName("float8");
        break; // // 8;
      case Types.NUMERIC:
        typeInfo.setTypeName("numeric");
        break; // // 2;
      case Types.DECIMAL:
        typeInfo.setTypeName("numeric");
        break; // // 3;
      case Types.CHAR:
        typeInfo.setTypeName(" character");
        break; // // 1;
      case Types.VARCHAR:
        if (typeInfo.getTypeName().equalsIgnoreCase("text")) {
          typeInfo.setTypeName("text");
          return;
        }
        typeInfo.setTypeName("varchar");
        break; // //12;
      case Types.LONGVARCHAR:
        typeInfo.setTypeName("text");
        break; // //-1;
      case Types.DATE:
        typeInfo.setTypeName("date");
        break; // //91;
      case Types.TIME:
        typeInfo.setTypeName("time");
        break; // //92;
      case Types.TIMESTAMP:
        if (typeInfo.getTypeName().equalsIgnoreCase("DATE")) {
          typeInfo.setTypeName("DATE");
          typeInfo.setSqlType(Types.DATE);
          return; //  92
        }
        typeInfo.setTypeName("timestamp");
        break; // //93;
      case Types.BINARY:
        typeInfo.setTypeName("bytea");
        break; // //-2;
      case Types.VARBINARY:

//                typeInfo.setTypeName("bytea");
        // commented by sunil to test the
        // case of RAW data type in oracle
        if (typeInfo.getTypeName().equalsIgnoreCase("RAW")) {
          typeInfo.setTypeName("text");
          typeInfo.setSqlType(Types.VARCHAR);
          return; //  12
        }
        typeInfo.setTypeName("bytea");
        break; // //-3;
      case Types.LONGVARBINARY:
        typeInfo.setTypeName("bytea");
        break; // //-4;
      case Types.BLOB:
        typeInfo.setTypeName("bytea");
        break; // //2004;
      case Types.CLOB:
        typeInfo.setTypeName("text");
        break; //2005;
      case Types.OTHER:
        if (typeInfo.getTypeName().equalsIgnoreCase("FLOAT")) { //1111
          typeInfo.setTypeName("numeric");
          typeInfo.setSqlType(Types.FLOAT);
          return; //  6
        }
        else if (typeInfo.getTypeName().equalsIgnoreCase("clob")) {
          typeInfo.setTypeName("text");
          typeInfo.setSqlType(Types.CLOB); //2005
          return;
        }
        else if (typeInfo.getTypeName().equalsIgnoreCase("blob")) {
          throw new RepException("REP031", new Object[] {typeInfo.getTypeName()});
        }
        typeInfo.setTypeName("text");
        break; //1111;
      case Types.NULL: //0
      case Types.DISTINCT: //2001;
      case Types.STRUCT: //2002;
      case Types.ARRAY: //2003;
      case Types.DATALINK: //70;
      case Types.REF:
      case Types.JAVA_OBJECT:
//      case Types.BLOB           :
      default:
        throw new RepException("REP031", new Object[] {typeInfo.getTypeName()});
    }
  }

  public AbstractColumnObject getColumnObject(TypeInfo typeInfo) throws
      RepException {
//System.out.println("PostgreSQL  typeInfo :: "+typeInfo);
    int sqlType = typeInfo.getSqlType();
    switch (sqlType) {
      case 1: // char
      case -1: // long varchar
        return new StringObject(sqlType,this);
      case 12: // char varying
        if (typeInfo.getTypeName().equalsIgnoreCase("text")) {
          return new ClobStreamObject(sqlType,this);
        }
        else {
          return new StringObject(sqlType,this);
        }
      case 2005: // clob
      case 1111:
      case 2000: // long varbinary
      case 2006: //
        return new ClobStreamObject(sqlType,this);
      case -3: // bitvarying
      case 2004: // blob
      case -2: // binary
      case -4: // long varbiary
        return new BlobObject(sqlType,this);
      case 4: // int
      case 5: // small int
      case -6: // tinyint
        return new IntegerObject(sqlType,this);
      case -5: // long
        return new LongObject(sqlType,this);
      case 3: // decimal
      case 8: // double precision
      case 2: // numeric
      case 6: // float
        return new DoubleObject(sqlType,this);
      case 7: // real
        return new FloatObject(sqlType,this);
      case -7: // bit
      case 16: // boolean
        return new BooleanObject(sqlType,this);
      case 91: // date
        return new DateObject(sqlType,this);
      case 92: // time
        return new TimeObject(sqlType,this);
      case 93: // time stamp
        return new TimeStampObject(sqlType,this);
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
    return pg_publication_TableName;
  }

  public String getSubscriptionTableName() {
    return pg_subscription_TableName;
  }

  public String getRepTableName() {
    return pg_rep_TableName;
  }

  public String getLogTableName() {
    return pg_log_Table;
  }

  public String getBookMarkTableName() {
    return pg_bookmark_TableName;
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
          break;
        }
      case -6: // tinyint
        if (columnsize > 127) {
          flag = true;
          break;
        }
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
          break;
        }
      case -6: // tinyint
        if (columnsize > 127) {
          flag = true;
          break;
        }
    }
    return flag;
  }

  public boolean getPrimaryKeyErrorCode(SQLException ex) throws SQLException {
    if (ex.getMessage().indexOf("_pkey") != -1) {
      return true;
    }
    return false;
  }

  public int getAppropriatePrecision(int columnSize, String datatypeName) {
    if (datatypeName.equalsIgnoreCase("numeric") && columnSize > 38) {
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

  //scale 0 to 23 only
  public int getAppropriateScale(int columnScale) throws RepException {
    if (columnScale < 0) {
      throw new RepException("REP026", new Object[] {"1", "23"});
    }
    else if (columnScale >= 23) {
      columnScale = 23;
    }
    else if (columnScale >= 0 && columnScale < 23)
      columnScale = columnScale;
    log.debug("returning columnScale::" + columnScale);
    return columnScale;
  }

  public PreparedStatement makePrimaryPreperedStatement(String[] primaryColumns, String shadowTable, String local_pub_sub_name) throws SQLException, RepException {
    StringBuffer query = new StringBuffer();
   query.append(" select * from ");
   query.append(shadowTable);
   query.append(" where ");
   query.append(RepConstants.shadow_sync_id1);
   query.append(" > ");
   query.append(" ? ");
   for (int i = 0; i < primaryColumns.length; i++) {
     query.append(" and ");
     query.append(primaryColumns[i]);
     query.append("= ? ");
   }
   query.append(" order by " + RepConstants.shadow_sync_id1);
   query.append(" limit 1 ");
    Connection pub_sub_Connection = connectionPool.getConnection(local_pub_sub_name);
    return pub_sub_Connection.prepareStatement(query.toString());
  }



  public boolean isForiegnKeyException(SQLException ex) throws SQLException {
    //System.out.println(" ex Message  : "+ex.getMessage()+" ex Error code : "+ex.getErrorCode());
    if (ex.getErrorCode() == 0)
      return true;
    else
      return false;
  }

  /**
   * isPrimaryKeyException
   *
   * @param ex SQLException
   * @return boolean
   */
  public boolean isPrimaryKeyException(SQLException ex) throws SQLException
   {
       if (ex.getMessage().indexOf("_pkey") != -1)
       {
           return true;
       }
       return false;
   }

   public String getIgnoredColumns_Table() {
     return pg_ignoredColumns_Table;
   }

   public String getTrackReplicationTablesUpdation_Table() {
     return pg_trackReplicationTablesUpdation_Table;
   }

   public String getTrackPrimayKeyUpdation_Table() {
    return pg_trackPrimaryKeyUpdation_Table;
  }

   protected void createIgnoredColumnsTable(String pubName) throws SQLException,
       RepException {
     StringBuffer ignoredColumnsQuery = new StringBuffer();
     ignoredColumnsQuery.append(" Create Table ").append(getIgnoredColumns_Table()).
         append(" ( ")
         .append(RepConstants.ignoredColumnsTable_tableId1).append("  int , ")
         .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2).append(
         "  varchar(255) , ")
         .append(" Primary Key (").append(RepConstants.
                                            ignoredColumnsTable_tableId1).append(
         " , ")
         .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2).append(
         " ) ) ");
     runDDL(pubName, ignoredColumnsQuery.toString());
   }

   protected void createTrackReplicationTablesUpdationTable(String pubSubName) throws
         RepException, SQLException {
       StringBuffer trackRepTablesUpdationQuery = new StringBuffer();
       trackRepTablesUpdationQuery.append(" CREATE  TABLE ").append(getTrackReplicationTablesUpdation_Table()).append(" ( " +
           RepConstants.trackUpdation + " smallint  PRIMARY KEY) ");
       runDDL(pubSubName, trackRepTablesUpdationQuery.toString());
       runDDL(pubSubName,"Insert into "+getTrackReplicationTablesUpdation_Table()+" values(1)" );
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

      public PreparedStatement makePrimaryPreperedStatementBackwardTraversing(String[] primaryColumns, long lastId, String local_pub_sub_name, String shadowTable) throws SQLException, RepException {
        StringBuffer query = new StringBuffer();
        query.append(" select top 1 * from ")
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
        query.append(" order by limit 1 ")
        .append(RepConstants.shadow_sync_id1)
        .append(" desc ");
        log.debug(query.toString());
//System.out.println("PostgreSQLHandler  makePrimaryPreperedStatementDelete  ::  " +query.toString());
        Connection pub_sub_Connection = connectionPool.getConnection(local_pub_sub_name);
        return pub_sub_Connection.prepareStatement(query.toString());
      }

  /**
   * isSchemaSupported
   *
   * @return boolean
   */
  public boolean isSchemaSupported() {
    return true;
  }

  /*
      CREATE FUNCTION merge_db (key INT, data TEXT) RETURNS VOID AS
       $$
       BEGIN
           LOOP
               UPDATE db SET b = data WHERE a = key;
               IF found THEN
                   RETURN;
               END IF;

               BEGIN
                   INSERT INTO db(a,b) VALUES (key, data);
                   RETURN;
               EXCEPTION WHEN unique_violation THEN
                   -- do nothing
               END;
           END LOOP;
       END;
       $$
   */

 }
