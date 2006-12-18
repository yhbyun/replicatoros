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

public class SybaseHandler
    extends AbstractDataBaseHandler  {

  public SybaseHandler() {}

  public SybaseHandler(ConnectionPool connectionPool0) {
    connectionPool = connectionPool0;
    vendorType = Utility.DataBase_Sybase;
    log.debug("VENDER TYPE " + vendorType);
  }

  protected void createSuperLogTable(String pubName) throws SQLException,
      RepException {
    StringBuffer logTableQuery = new StringBuffer();
    logTableQuery.append(" Create Table ")
        .append(log_Table)
        .append(" ( " + RepConstants.logTable_commonId1 +
                " bigint default autoincrement , " +
                RepConstants.logTable_tableName2 + "  varchar(255) ) ");
    log.debug(logTableQuery.toString());
    runDDL(pubName, logTableQuery.toString());
    StringBuffer indexQuery =new StringBuffer();
        indexQuery.append("CREATE INDEX ")
            .append(RepConstants.log_Index)
            .append(" ON "+getLogTableName())
            .append("(")
            .append(RepConstants.logTable_commonId1)
            .append(")");
        runDDL(pubName, indexQuery.toString());
  }

  protected void createRepTable(String pubName) throws SQLException,
      RepException {
    StringBuffer repTableQuery = new StringBuffer();
    repTableQuery.append(" Create Table ").append(getRepTableName())
        .append("(  " + RepConstants.repTable_pubsubName1 + "  varchar(255) , " +
                RepConstants.repTable_tableId2 +
                "  bigint default autoincrement, ")
        .append(" " + RepConstants.repTable_tableName2 + "  varchar(255) , " +
                RepConstants.repTable_filter_clause3 + "  varchar(255) null, ")
        .append(RepConstants.repTable_createshadowtable6).append("  char(1) Default 'Y', ")
        .append(RepConstants.repTable_cyclicdependency7).append("  char(1) Default 'N', ")
        .append(" " + RepConstants.repTable_conflict_resolver4 +
                "  varchar(255) , Primary Key (" +
                RepConstants.repTable_pubsubName1 + " , " +
                RepConstants.repTable_tableName2 + " ) ) ");

    log.debug(repTableQuery.toString());
    runDDL(pubName, repTableQuery.toString());
  }

  protected void createBookMarkTable(String pubName) throws SQLException,
      RepException {
    StringBuffer bookmarkTableQuery = new StringBuffer();
    bookmarkTableQuery.append(" Create Table ")
        .append(getBookMarkTableName())
        .append(" ( " + RepConstants.bookmark_LocalName1 + " varchar(255) , " +
                RepConstants.bookmark_RemoteName2 + " varchar(255) , ")
        .append(" " + RepConstants.bookmark_TableName3 + " varchar(255) , " +
                RepConstants.bookmark_lastSyncId4 + " bigint , ")
        .append(" " + RepConstants.bookmark_ConisderedId5 +
                " bigint ," + RepConstants.bookmark_IsDeletedTable +
                " char(1) default 'N' , Primary Key (" +
                RepConstants.bookmark_LocalName1 +
                ", " + RepConstants.bookmark_RemoteName2 + ", " +
                RepConstants.bookmark_TableName3 + ") ) ");
    log.debug(bookmarkTableQuery.toString());
    runDDL(pubName, bookmarkTableQuery.toString());
  }

  public void createShadowTable(String pubsubName, String tableName,
                                String allColseq,String[] primaryColumns) throws RepException {
    StringBuffer shadowTableQuery = new StringBuffer();
    shadowTableQuery.append(" Create Table ")
        .append(RepConstants.shadow_Table(tableName))
        .append(" ( " + RepConstants.shadow_sync_id1 +
                "  bigint default autoincrement , ")
        .append("   " + RepConstants.shadow_common_id2 + "  bigint null, ")
        .append("   " + RepConstants.shadow_operation3 + "  char(1) , ")
        .append("   " + RepConstants.shadow_status4 + "  char(1) null")
        .append(allColseq)
        .append(" , " + RepConstants.shadow_serverName_n + " varchar(255)  ")
        .append(" ,  " + RepConstants.shadow_PK_Changed + "  char(1) ) ");
    try {
      log.debug(shadowTableQuery.toString());
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
        .append(Schedule_TableName)
        .append(" ( " + RepConstants.schedule_Name + " varchar(255) , " +
                RepConstants.subscription_subName1 + " varchar(255) unique , ")
        .append("  " + RepConstants.schedule_type + " varchar(255) , ")
        .append(" " + RepConstants.publication_serverName3 + " varchar (255) ," +
                RepConstants.publication_portNo + " varchar(255) ,")
        .append(" " + RepConstants.recurrence_type + " varchar(255) , " +
                RepConstants.replication_type + " varchar(255) ,")
        .append(" " + RepConstants.schedule_time + " bigint , ")
        .append(" " + RepConstants.schedule_counter +
                " numeric , Primary Key (" + RepConstants.schedule_Name + " , " +
                RepConstants.subscription_subName1 + ") ) ");
    log.debug(ScheduleTableQuery.toString());
    runDDL(subName, ScheduleTableQuery.toString());
  }

  /* ALTER TRIGGER "TRD_contact" .TRD_contact
      after delete on DBA.contact Referencing
      old as oldRow For each Row
   begin  Insert into Rep_LogTable ( Rep_table_name ) values ( 'DBA.contact');
      Insert Into DBA.REP_SHADOW_contact ( Rep_common_id, Rep_operationType, Rep_status, id , last_name , first_name , title , street , city , state , zip , phone , fax , old_id , Rep_server_name ) Values ( null , 'D' , null , oldRow.id , oldRow.last_name , oldRow.first_name , oldRow.title , oldRow.street , oldRow.city , oldRow.state , oldRow.zip , oldRow.phone , oldRow.fax , oldRow.id , 'sube_3001') ; end
   */
  /* ALTER TRIGGER "TRI_contact" .TRI_contact after insert on
     DBA.contact Referencing new as newRow For each Row begin
     Insert into Rep_LogTable ( Rep_table_name ) values ( 'DBA.contact');
     Insert Into DBA.REP_SHADOW_contact ( Rep_common_id, Rep_operationType, Rep_status, id , last_name , first_name , title , street , city , state , zip , phone , fax , old_id , Rep_server_name ) Values ( null , 'I' , null , newRow.id , newRow.last_name , newRow.first_name , newRow.title , newRow.street , newRow.city , newRow.state , newRow.zip , newRow.phone , newRow.fax , newRow.id , 'sube_3001') ; end
   */
  /* ALTER TRIGGER "TRU_contact" .TRU_contact after update on
     DBA.contact Referencing new as newRow old as oldRow
     For each Row  begin declare maxlogid numeric;
     Insert into Rep_LogTable ( Rep_table_name ) values ( 'DBA.contact');
    Select max(Rep_cid) into maxlogid from Rep_LogTable; Insert Into DBA.REP_SHADOW_contact ( Rep_common_id, Rep_operationType, Rep_status, id , last_name , first_name , title , street , city , state , zip , phone , fax , old_id , Rep_server_name ) Values ( maxlogid , 'U' , 'B' , oldRow.id , oldRow.last_name , oldRow.first_name , oldRow.title , oldRow.street , oldRow.city , oldRow.state , oldRow.zip , oldRow.phone , oldRow.fax , oldRow.id , 'sube_3001') ; Insert Into DBA.REP_SHADOW_contact ( Rep_common_id, Rep_operationType, Rep_status, id , last_name , first_name , title , street , city , state , zip , phone , fax , old_id , Rep_server_name ) Values ( maxlogid , 'U' , 'A' , newRow.id , newRow.last_name , newRow.first_name , newRow.title , newRow.street , newRow.city , newRow.state , newRow.zip , newRow.phone , newRow.fax , oldRow.id , 'sube_3001') ; end
   */

  public void createShadowTableTriggers(String pubsubName, String tableName,
                                        ArrayList colInfoList,
                                        String[] primCols) throws RepException {

    String serverName = getLocalServerName();
    int size = colInfoList.size();
    String[] colNames = new String[size];
    for (int i = 0; i < size; i++) {
      colNames[i] = ( (ColumnsInfo) colInfoList.get(i)).getColumnName();
    }
    //RepPrinter.print(" Columns are :::::: "  + java.util.Arrays.asList(colNames));
    String colNameSeq = getColumnNameSequence(colNames, "").toString();
    String colNameSeqPrefixOldRow = getColumnNameSequence(colNames, "oldRow.").
        toString();
    String colNameSeqPrefixNewRow = getColumnNameSequence(colNames, "newRow.").
        toString();
    String shadowTableName = RepConstants.shadow_Table(tableName);
    String primColumnNamesSeq = getColumnNameSequence(primCols, "old_");
    String primColNameSeqPrefixOldRow = getColumnNameSequence(primCols,
        "oldRow.").toString();
    String primColNameSeqPrefixNewRow = getColumnNameSequence(primCols,
        "newRow.").toString();
    String[] primOld = getColumnNameWithOldOrNewPrefix(primCols,"OLD.");
    String[] primNew  = getColumnNameWithOldOrNewPrefix(primCols,"NEW.");
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
        .append(" Referencing new as newRow For each Row begin ")
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
        .append(" Referencing old as oldRow For each Row begin ")
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
        .append(" Referencing new as newRow old as oldRow For each Row ")
        .append(" begin declare maxlogid numeric; pkchanged char(1); ").append(insertLogTable)
        .append(" Select max(" + RepConstants.logTable_commonId1 +
                ") into maxlogid from ")
        .append(log_Table).append("; Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( maxlogid , 'U' , 'B' , ")
        .append(colNameSeqPrefixOldRow).append(primColNameSeqPrefixOldRow)
        .append("'").append(serverName).append("') ;")
        .append(" if( ");
        for (int i = 0; i < primOld.length; i++) {
         if (i != 0)
           updTriggerQuery.append(" and ");
         updTriggerQuery.append(primOld[i] )
                          .append("!=" )
                         .append(primNew[i]);
        }
        updTriggerQuery.append(" ) Then ")
        .append("  pkchanged :='Y'; end if; ")
        .append(" Insert Into ")
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
      log.debug(insTriggerQuery.toString());
      runDDL(pubsubName, insTriggerQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      log.error(ex);
      // Ignore Exception
    } try {
      log.debug(delTriggerQuery.toString());
      runDDL(pubsubName, delTriggerQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      log.error(ex);
      // Ignore Exception
    }
    try {
         log.debug(updTriggerQuery.toString());
         runDDL(pubsubName, updTriggerQuery.toString());

       }
       catch (RepException ex) {
         throw ex;
       }
       catch (SQLException ex) {
         log.error(ex);
         // Ignore Exception
       }

  }

  public boolean isDataTypeOptionalSizeSupported(TypeInfo typeInfo) {
    log.debug(typeInfo);
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
      case 16:
      case -3:
        return false;

      default:
        return true;
    }
  }

  public void setTypeInfo(TypeInfo typeInfo, ResultSet rs) throws RepException,
      SQLException {
    log.debug(typeInfo);
    switch (typeInfo.getSqlType()) {

      case Types.BOOLEAN: //16
      case Types.BIT:
        typeInfo.setTypeName("bit");
        break; //-7;
      case Types.TINYINT:
        typeInfo.setTypeName("tinyint");
        break; //-6;
      case Types.SMALLINT:
        typeInfo.setTypeName("smallint");
        break; // 5;
      case Types.INTEGER:
        typeInfo.setTypeName("integer");
        break; // 4;
      case Types.BIGINT:
        typeInfo.setTypeName("bigint");
        break; //-5;
      case Types.REAL:
        typeInfo.setTypeName("real");
        break; // 7;
      case Types.FLOAT: // 6
      case Types.DOUBLE:
        typeInfo.setTypeName("float");
        break; // 8;
      case Types.NUMERIC:
        typeInfo.setTypeName("numeric");
        break; // 2;
      case Types.DECIMAL: // 3;
        if (typeInfo.getTypeName().equalsIgnoreCase("money")) {
          typeInfo.setTypeName("money");
          return;
        }
        else if (typeInfo.getTypeName().equalsIgnoreCase("smallmoney")) {
          typeInfo.setTypeName("smallmoney");
          return;
        }
        typeInfo.setTypeName("decimal");
        break; // 3;
      case Types.CHAR:
        typeInfo.setTypeName("char");
        break; // 1;
      case Types.VARCHAR:
        typeInfo.setTypeName("varchar");
        break; //12;
      case Types.DATE:
        typeInfo.setTypeName("date");
        break; //91;
      case Types.TIME:
        typeInfo.setTypeName("time");
        break; // 92;
      case Types.TIMESTAMP:
        typeInfo.setTypeName("timestamp");
        break; // 93;
      case Types.BINARY:
        typeInfo.setTypeName("binary");
        break; // -2;
      case Types.VARBINARY:
        typeInfo.setTypeName("varbinary");
        break; // -3;
      case Types.LONGVARBINARY:
        if (typeInfo.getTypeName().equalsIgnoreCase("long binary")) {
          typeInfo.setTypeName("long binary");
          return;
        }
        typeInfo.setTypeName("image");
        break; //-4;
      case Types.BLOB:
        typeInfo.setTypeName("image");
        break; //2004;
      case Types.LONGVARCHAR: //-1
      case Types.CLOB:
        typeInfo.setTypeName("text");
        break; //2005;
      case Types.OTHER:
        typeInfo.setTypeName("BLOB");
        break; //1111;
      case Types.REF: //2006;
      case Types.NULL:
      case Types.DISTINCT: //2001;
      case Types.STRUCT: //2002;
      case Types.ARRAY: //2003;
      case Types.DATALINK: //70;
      case Types.JAVA_OBJECT: //2000;
//             case Types.BOOLEAN: //16
      default:
        throw new RepException("REP031", new Object[] {typeInfo.getTypeName()});
    }
  }

  public AbstractColumnObject getColumnObject(TypeInfo typeInfo) throws
      RepException {
    log.debug(typeInfo);
    int sqlType = typeInfo.getSqlType();
    switch (sqlType) {
      case 1: // char
      case 12: // char varying
      case -1: // long varchar
        return new StringObject(sqlType,this);
      case -3: // bitvarying
        return new BlobObject(sqlType,this);
      case 2005: // clob
        return new ClobObject(sqlType,this);
      case 2004: // blob
      case -2: // binary
      case -4: // long varbiary
        return new BlobObject(sqlType,this);
      case 4: // int
      case 5: // small int
      case -6: // tinyint
        return new IntegerObject(sqlType,this);
      case -5: // long
      case 2000: // long varbinary
        return new LongObject(sqlType,this);
      case 3: // decimal
        if ( (typeInfo.getTypeName()).equalsIgnoreCase("numeric")) {
          return new BigDecimalObject(sqlType,this);
        }
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

  public boolean isColumnSizeExceedMaximumSize(TypeInfo typeInfo) throws
      SQLException, RepException {
    log.debug(typeInfo);
    boolean flag = false;
    int sqlType = typeInfo.getSqlType();
    int columnsize = typeInfo.getcolumnSize();
    switch (sqlType) {
      case 1: // char
      case 12: //varchar
        if (columnsize > 255) {
          flag = true;

        }
        break;
      case -6: // tinyint
        if (columnsize > 3) {
          flag = true;

        }
        break;
    }
    return flag;
  }

  public void setColumnPrecisionInTypeInfo(TypeInfo typeInfo,
                                           ResultSetMetaData rsmt,
                                           int columnIndex) throws SQLException {
    int columnPrecion = rsmt.getPrecision(columnIndex);
    typeInfo.setColumnSize(columnPrecion);

  }

  public boolean getPrimaryKeyErrorCode(SQLException ex) throws SQLException {
    if (ex.getMessage().indexOf("PRIMARY KEY") != -1 ||
        ex.getErrorCode() == 2601) {
      return true;
    }
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
    else if (datatypeName.equalsIgnoreCase("varchar") && columnSize > 255) {
      columnSize = 255;
    }
    else if (datatypeName.equalsIgnoreCase("tinyint") && columnSize > 3) {
      columnSize = 3;
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

  //scale 0 to 14 only
  public int getAppropriateScale(int columnScale) throws RepException {
    if (columnScale < 0) {
      throw new RepException("REP026", new Object[] {"1", "14"});
    }
    else if (columnScale >= 14) {
      columnScale = 14;
    }
    else if (columnScale >= 0 && columnScale < 14)
      columnScale = columnScale;
    log.debug("returning columnScale::" + columnScale);
    return columnScale;

  }

  public PreparedStatement makePrimaryPreperedStatement(String[]
      primaryColumns, String shadowTable, String local_pub_sub_name) throws
      SQLException, RepException {
    StringBuffer query = new StringBuffer();
    query.append(" select first * from ")
    .append(shadowTable)
    .append(" where ")
    .append(RepConstants.shadow_sync_id1)
    .append(" > ");
    query.append("? ");
    for (int i = 0; i < primaryColumns.length; i++) {
      query.append(" and ")
      .append(primaryColumns[i])
      .append("= ? ");
    }
    query.append(" order by " + RepConstants.shadow_sync_id1);
    Connection pub_sub_Connection = connectionPool.getConnection(local_pub_sub_name);
//System.out.println("SQLServerHandler.makePrimaryPreperedStatement(primaryColumns, shadowTable, local_pub_sub_name): "+query.toString());
    return pub_sub_Connection.prepareStatement(query.toString());
  }



  /**
   * isPrimaryKeyException
   *
   * @param ex SQLException
   * @return boolean
   */
  public boolean isPrimaryKeyException(SQLException ex) {
    return false;
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
  protected void createIgnoredColumnsTable(String pubName) throws SQLException,
         RepException
     {
         StringBuffer ignoredColumnsQuery = new StringBuffer();
     ignoredColumnsQuery.append(" Create Table ").append(getIgnoredColumns_Table()).append(" ( ")
         .append(RepConstants.ignoredColumnsTable_tableId1).append("  bigint , ")
         .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2).append("  varchar(255) , ")
         .append("   Primary Key (").append(RepConstants.ignoredColumnsTable_tableId1).append(" , ")
         .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2).append(" ) ) ");
         runDDL(pubName, ignoredColumnsQuery.toString());
     }

     protected void createTrackReplicationTablesUpdationTable(String pubSubName) throws RepException, SQLException {
          StringBuffer trackRepTablesUpdationQuery = new StringBuffer();
          trackRepTablesUpdationQuery.append(" CREATE  TABLE ").append(getTrackReplicationTablesUpdation_Table()).append(" ( " +
              RepConstants.trackUpdation + " smallint  PRIMARY KEY) ");
          runDDL(pubSubName, trackRepTablesUpdationQuery.toString());
          runDDL(pubSubName,"Insert into "+getTrackReplicationTablesUpdation_Table()+" values(1)" );
        }

        protected  void createTriggerForTrackReplicationTablesUpdationTable(String pubSubName) throws RepException, SQLException {
//           StringBuffer trackRepTablesUpdationTriggerQuery = new StringBuffer();
//           trackRepTablesUpdationTriggerQuery.append(" CREATE  TRIGGER TRI_")
//               .append(getTrackReplicationTablesUpdation_Table()).append(
//                   " ON " + getTrackReplicationTablesUpdation_Table())
//               .append(" AFTER INSERT AS  DELETE FROM " +
//                       getTrackReplicationTablesUpdation_Table() + " WHERE ")
//               .append(RepConstants.trackUpdation + " NOT IN(SELECT * FROM inserted)");
//           runDDL(pubSubName, trackRepTablesUpdationTriggerQuery.toString());
         }

         public PreparedStatement makePrimaryPreperedStatementBackwardTraversing(String[] primaryColumns, long lastId, String local_pub_sub_name,
             String shadowTable) throws SQLException, RepException {
           StringBuffer query = new StringBuffer();
           query.append(" select * from ")
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
//System.out.println("SybaseHandler  makePrimaryPreperedStatementDelete  ::  " +query.toString());
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

}
