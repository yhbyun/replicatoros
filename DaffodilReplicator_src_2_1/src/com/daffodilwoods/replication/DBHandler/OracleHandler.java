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
import java.io.StringBufferInputStream;

public class OracleHandler
    extends AbstractDataBaseHandler  {
  protected static Logger log = Logger.getLogger(OracleHandler.class.getName());

  public OracleHandler() {}

  public OracleHandler(ConnectionPool connectionPool0) {
    connectionPool = connectionPool0;
    vendorType = Utility.DataBase_Oracle;
  }

  //Table command: CREATE TABLE tablename (ID VARCHAR(10) CONSTRAINT pk_id PRIMARY KEY, ...)
  //Sequence command: CREATE SEQUENCE seq tablename minvalue 1 increment by 1 nomaxvalue
  //Insert command: INSERT INTO tablename (ID,...) VALUES(seq_tablename.nextval,....)


  //create table test (id number, testdata varchar2(255));
  //create sequence test_seq start with 1 increment by 1 nomaxvalue;
  // create trigger test_trigger before insert on test for each row
  // begin select test_seq.nextval into :new.id from dual;  end;

  protected void createSuperLogTable(String pubName) throws SQLException,
      RepException {
    StringBuffer logTableQuery = new StringBuffer();
    logTableQuery.append(" Create Table ")
        .append(log_Table).append(" (")
        .append(RepConstants.logTable_commonId1).append(" number ,")
        .append(RepConstants.logTable_tableName2).append(" varchar(255) ) ");
   try{
    runDDL(pubName, logTableQuery.toString());
  }
 catch (RepException ex) {
   throw ex;
 }
 catch (SQLException ex) {
//   System.out.println("Error Code==::"+ex.getErrorCode());
   if(ex.getErrorCode()!=955)
      throw new RepException("REP999", new String[] {log_Table,
                           ex.getMessage()});

 }

  }

  public void CreateSequenceOnLogTable(String pubsubName) throws SQLException,
      RepException {
    StringBuffer seqOnlogTableQuery = new StringBuffer();
    seqOnlogTableQuery.append(" Create SEQUENCE ")
        .append(RepConstants.seq_Name(log_Table))
        .append(" start with 1 increment by 1 nomaxvalue ");
    runDDL(pubsubName, seqOnlogTableQuery.toString());
    StringBuffer indexQuery = new StringBuffer();
    indexQuery.append("CREATE INDEX ")
        .append(RepConstants.log_Index)
        .append(" ON " + getLogTableName())
        .append("(")
        .append(RepConstants.logTable_commonId1)
        .append(")");
//System.out.println(" Create Index on LogTable : " + indexQuery.toString());
  try{
    runDDL(pubsubName, indexQuery.toString());
  }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {

    }

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
    repTableQuery.append(" Create Table ").append(rep_TableName)
            .append(" (  "+RepConstants.repTable_pubsubName1+"
             varchar(255) ,  "+RepConstants.repTable_tableId2+
                "  int , ")
            .append("  "+RepConstants.repTable_tableName2+"  varchar(255) ,  "+RepConstants.repTable_filter_clause3+"  varchar(255) , ")
        .append(
            "  "+RepConstants.repTable_conflict_resolver4+"  varchar(255) , Primary Key ( "+RepConstants.repTable_pubsubName1+" ,  "+RepConstants.repTable_tableName2+" ) ) ");
        runDDL(pubName, repTableQuery.toString());
    } */

  protected void createRepTable(String pubName) throws SQLException,
      RepException {
    StringBuffer repTableQuery = new StringBuffer();
    repTableQuery.append(" Create Table ").append(getRepTableName()).append(
        " ( ")
        .append(RepConstants.repTable_pubsubName1).append(" varchar(255) , ")
        .append(RepConstants.repTable_tableId2).append("  int , ")
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
    try{
    runDDL(pubName, repTableQuery.toString());
  }
 catch (RepException ex) {
   throw ex;
 }
 catch (SQLException ex) {
//System.out.println("Error Code==::"+ex.getErrorCode());
   if(ex.getErrorCode()!=955)
      throw new RepException("REP999", new String[] {rep_TableName,ex.getMessage()});

 }

  }

  public void CreateSequenceOnRepTable(String pubsubName) throws SQLException,
      RepException {
    StringBuffer seqOnrepTableQuery = new StringBuffer();
    seqOnrepTableQuery.append(" Create SEQUENCE ")
        .append(RepConstants.seq_Name(rep_TableName))
        .append(" start with 1 increment by 1 nomaxvalue ");
    try{
      runDDL(pubsubName, seqOnrepTableQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {

    }

  }

  protected void createBookMarkTable(String pubName) throws SQLException,
      RepException {
    StringBuffer bookmarkTableQuery = new StringBuffer();
    bookmarkTableQuery.append(" Create Table ")
        .append(bookmark_TableName)
        .append(" (  " + RepConstants.bookmark_LocalName1 +
                "  varchar(255) ,  " + RepConstants.bookmark_RemoteName2 +
                "  varchar(255) , ")
        .append("  " + RepConstants.bookmark_TableName3 + "  varchar(255) ,  " +
                RepConstants.bookmark_lastSyncId4 + "  NUMBER , ")
        .append(
        "  " + RepConstants.bookmark_ConisderedId5 + "  NUMBER," +
        RepConstants.bookmark_IsDeletedTable +
        " char(1) default 'N' , Primary Key ( " +
        RepConstants.bookmark_LocalName1 + " ,  " +
        RepConstants.bookmark_RemoteName2 + " ,  " +
        RepConstants.bookmark_TableName3 + " ) ) ");
      try{
        runDDL(pubName, bookmarkTableQuery.toString());
      }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
// System.out.println("Error Code==::"+ex.getErrorCode());
      if(ex.getErrorCode()!=955)
         throw new RepException("REP999", new String[] {bookmark_TableName,
                              ex.getMessage()});

    }

  }

  public void createScheduleTable(String subName) throws SQLException,
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
        .append(" " + RepConstants.schedule_time + " NUMBER , ")
        .append(" " + RepConstants.schedule_counter + " NUMBER , Primary Key (" +
                RepConstants.schedule_Name + " , " +
                RepConstants.subscription_subName1 + ") ) ");
    try {
      runDDL(subName, ScheduleTableQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
//System.out.println("Error Code==::"+ex.getErrorCode());
      if(ex.getErrorCode()!=955)
         throw new RepException("REP999", new String[] {Schedule_TableName,ex.getMessage()});

    }
  }

  public void createShadowTable(String pubsubName, String tableName,
                                String allColSequence,String[] primaryColumns) throws RepException {
    StringBuffer shadowTableQuery = new StringBuffer();
    String shadowTableName = RepConstants.shadow_Table(tableName);
    shadowTableQuery.append(" Create Table ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_sync_id1).append(" number ,")
        .append(RepConstants.shadow_common_id2).append(" number , ")
        .append(RepConstants.shadow_operation3).append(" char(1) , ")
        .append(RepConstants.shadow_status4).append(" char(1) ")
        .append(allColSequence).append(" , ")
        .append(RepConstants.shadow_serverName_n).append(" varchar(255) , ")
        .append(RepConstants.shadow_PK_Changed).append(" char(1) )");

    StringBuffer seqOnShadowTableQuery = new StringBuffer();
    seqOnShadowTableQuery.append(" Create SEQUENCE ")
        .append(RepConstants.seq_ShadowTableName(shadowTableName))
        .append(" start with 1 increment by 1 nomaxvalue ");

    try {
      runDDL(pubsubName, shadowTableQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
// System.out.println("Error Code==::"+ex.getErrorCode());
      if(ex.getErrorCode()!=955)
         throw new RepException("REP999", new String[] {shadowTableName,
                              ex.getMessage()});
    }
    try {
      runDDL(pubsubName, seqOnShadowTableQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      // Ignore Exception
    }
    createIndex(pubsubName, RepConstants.shadow_Table(tableName));
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
   String colNameSeqPrefixOldRow = getColumnNameSequence(colNames, ":oldRow.");
   String colNameSeqPrefixNewRow = getColumnNameSequence(colNames, ":newRow.");
   String primColNameSeqPrefixOldRow = getColumnNameSequence(primCols,":oldRow.");
   String primColNameSeqPrefixNewRow = getColumnNameSequence(primCols,":newRow.");

   String primColWithOldPrefix[] =addPrefixWithColumnName(primCols,":oldRow.");
   String primColWithNewPrefix[] =addPrefixWithColumnName(primCols,":newRow.");

  ////
   StringBuffer insertLogTable = new StringBuffer();
   insertLogTable.append(" Insert into ")
       .append(log_Table).append(" values ( ")
       .append(RepConstants.seq_Name(log_Table) + ".nextVal , '")
       .append(tableName).append("'); ");

   StringBuffer shadowTableQuery = new StringBuffer();
   shadowTableQuery.append(" Insert Into ")
       .append(RepConstants.shadow_Table(tableName)).append(" ( ")
       .append(RepConstants.shadow_sync_id1).append(", ")
       .append(RepConstants.shadow_common_id2).append(", ")
       .append(RepConstants.shadow_operation3).append(", ")
       .append(RepConstants.shadow_status4).append(", ")
       .append(getColumnNameSequence(colNames, ""))
       .append(getColumnNameSequence(primCols, "rep_old_"))
       .append(RepConstants.shadow_serverName_n)
       .append(" , ").append(RepConstants.shadow_PK_Changed ).append(" ) Values ( ")
       .append(RepConstants.seq_ShadowTableName(
       RepConstants.shadow_Table(tableName))).append(".nextVal");

   StringBuffer insTriggerQuery = new StringBuffer();
   insTriggerQuery.append(" Create trigger ")
       .append(RepConstants.getInsertTriggerName(tableName))
       .append(" after insert on ").append(tableName)
       .append(" Referencing new as newRow For each Row begin ")
       .append(insertLogTable).append(shadowTableQuery.toString())
       .append(", null ,'I', null , ").append(colNameSeqPrefixNewRow)
       .append(primColNameSeqPrefixNewRow)
       .append("'").append(serverName).append("',null) ; end ; ");

   StringBuffer delTriggerQuery = new StringBuffer();
   delTriggerQuery.append(" Create trigger ")
       .append(RepConstants.getDeleteTriggerName(tableName))
       .append(" after delete on ").append(tableName)
       .append(" Referencing old as oldRow For each Row begin ")
       .append(insertLogTable).append(shadowTableQuery.toString())
       .append(", null ,'D', null , ").append(colNameSeqPrefixOldRow)
       .append(primColNameSeqPrefixOldRow)
       .append("'").append(serverName).append("',null) ; end ; ");

   StringBuffer updTriggerQuery = new StringBuffer();
   updTriggerQuery.append(" Create trigger ")
       .append(RepConstants.getUpdateTriggerName(tableName))
       .append(" after update on ").append(tableName)
       .append(" Referencing new as newRow old as oldRow For each Row ")
       .append(" declare maxlogid number; pkchanged char(1);  begin ")
       .append(insertLogTable)
       .append(" Select max(" + RepConstants.logTable_commonId1 +
               ") into maxlogid from ")
       .append(log_Table).append("; ")
       .append(" if( ");
        for (int i = 0; i < primColWithOldPrefix.length; i++) {
         if (i != 0)
          updTriggerQuery.append(" and ");
         updTriggerQuery.append(primColWithOldPrefix[i])
         .append("!= ")
         .append(primColWithNewPrefix[i]);
        }
        updTriggerQuery.append(" ) THEN  pkChanged := 'Y';  END IF; ")
       .append(shadowTableQuery.toString()).append(",maxlogid,'U','B',")
       .append(colNameSeqPrefixOldRow).append(primColNameSeqPrefixOldRow)
       .append("'").append(serverName).append("',null) ; ")
       .append(shadowTableQuery.toString()).append(", maxlogid,'U','A',")
       .append(colNameSeqPrefixNewRow).append(primColNameSeqPrefixOldRow)
       .append("'").append(serverName).append("',pkChanged) ; ")
       .append(" end ; ");
//System.out.println(" Oracle update trigger query is  : "+updTriggerQuery.toString());

   try {
     runDDL(pubsubName, insTriggerQuery.toString());
   }
   catch (RepException ex) {
     throw ex;
   }
   catch (SQLException ex) {
//System.out.println("OracleHandler.createShadowTableTriggers() : "+ex.getErrorCode());
     if(ex.getErrorCode()!=4081) {
      throw new RepException("REP999", new String[] {Schedule_TableName,ex.getMessage()});
    }
     // Ignore Exception
   }
   try {
         runDDL(pubsubName, delTriggerQuery.toString());
  }
  catch (RepException ex) {
    throw ex;
  }
  catch (SQLException ex) {
    if(ex.getErrorCode()!=4081) {
   throw new RepException("REP999", new String[] {Schedule_TableName,ex.getMessage()});
  }

    // Ignore Exception
  }
  try {
       runDDL(pubsubName, updTriggerQuery.toString());
     }
     catch (RepException ex) {
       throw ex;
     }
     catch (SQLException ex) {
       if(ex.getErrorCode()!=4081) {
         throw new RepException("REP999", new String[] {Schedule_TableName,ex.getMessage()});
       }
       // Ignore Exception
     }

 }

  public boolean isDataTypeOptionalSizeSupported(TypeInfo typeInfo) {
    int sqlType = typeInfo.getSqlType();
    String typeName = typeInfo.getTypeName();
    switch (sqlType) {
      case -4:
      case -1:
      case 7:
      case 91:
      case 92:
      case 93:
      case 2002:
      case 2003:
      case 2004:
      case 2005:
      case 2006:
      case 1111: // Special Handling for 1111
        return false;
      default:
        return true;
    }
  }

  public void setTypeInfo(TypeInfo typeInfo, ResultSet rs) throws RepException,
      SQLException {
    int sqlType = typeInfo.getSqlType();
    String typeName = typeInfo.getTypeName();
    switch (sqlType) {
      case Types.BIT:
        typeInfo.setTypeName("number");
        break; //-7;
      case Types.TINYINT:
        typeInfo.setTypeName("number");
        break; //-6;
      case Types.SMALLINT:
        typeInfo.setTypeName("number");
        break; // 5;
      case Types.INTEGER:
        typeInfo.setTypeName("number");
        break; // 4;
      case Types.BIGINT:
        typeInfo.setTypeName("number");
        break; //-5;
      case Types.FLOAT:
        typeInfo.setTypeName("float");
        break; // 6;
      case Types.REAL:
        typeInfo.setTypeName("real");
        break; // 7;
      case Types.DOUBLE:
        typeInfo.setTypeName("number");
        break; // 8;
      case Types.NUMERIC:
        typeInfo.setTypeName("number");
        break; // 2;
      case Types.DECIMAL:
        typeInfo.setTypeName("number");
        break; // 3;
      case Types.CHAR:
        typeInfo.setTypeName("char");
        break; // 1;
      case Types.VARCHAR:
        if (typeName.equalsIgnoreCase("sql_variant")) {
          typeInfo.setTypeName("clob");
          typeInfo.setSqlType(2005);
          return;
        }
        else if (typeName.equalsIgnoreCase("text")) {
          typeInfo.setTypeName("clob");
          typeInfo.setSqlType(2005);
          return;
        }
        else {
          typeInfo.setTypeName("varchar2");
          break; //12
        }
      case Types.LONGVARCHAR:
        if (typeName.equalsIgnoreCase("text")) {
          typeInfo.setTypeName("clob");
          typeInfo.setSqlType(2005);
          break;
        }
        typeInfo.setTypeName("long");

        break; //-1;
      case Types.DATE:
        typeInfo.setTypeName("date");
        break; // //91;
      case Types.TIME:
        typeInfo.setTypeName("date");
        break; // //92;
      case Types.TIMESTAMP:
        typeInfo.setTypeName("timestamp");
        break; // //93;
      case Types.BINARY:
        typeInfo.setTypeName("blob");
//                typeInfo.setTypeName("raw");
        break; //-2;
      case Types.VARBINARY:
        typeInfo.setTypeName("blob");
//                typeInfo.setTypeName("raw");
        break; //-3;
      case Types.LONGVARBINARY:
        typeInfo.setTypeName("blob");
        typeInfo.setSqlType(2004);
        break; //-4; // sachin
        //typeInfo.setTypeName("number");break;//-4;
      case Types.OTHER:
        if (typeInfo.getTypeName().equalsIgnoreCase("FLOAT")) {
          typeInfo.setTypeName("FLOAT");
          typeInfo.setSqlType(Types.FLOAT);
          return; //  6
        }
        if (typeInfo.getTypeName().equalsIgnoreCase("BLOB")) {
          typeInfo.setTypeName("BLOB");
          typeInfo.setSqlType(Types.BLOB);
          return;
        }
        typeInfo.setTypeName("clob");
        break; //1111;
      case Types.STRUCT:
        typeInfo.setTypeName("struct");
        break; //2002;
      case Types.ARRAY:
        typeInfo.setTypeName("array");
        break; //2003;
      case Types.BLOB:
        typeInfo.setTypeName("blob");
        break; //2004;
      case Types.CLOB:
        typeInfo.setTypeName("clob");
        break; //2005;
      case Types.REF:
        typeInfo.setTypeName("ref");
        break; //2006;
      case Types.JAVA_OBJECT:
        typeInfo.setTypeName("clob");
        break; //2000;

      case Types.DISTINCT: //2001;
      case Types.NULL: // 0;
      case Types.DATALINK: //70;
      case Types.BOOLEAN: //16;
      default:
        throw new RepException("REP031", new Object[] {typeInfo.getTypeName()});
    }
  }

  public AbstractColumnObject getColumnObject(TypeInfo typeInfo) throws
      RepException {
    int sqlType = typeInfo.getSqlType();
    switch (sqlType) {
      case -7: // number
      case -6: // number
      case -5: // number
      case 4: // number
      case 5: // number
      case 3: // number  /// Added by sachin
        return new IntegerObject(sqlType, this);
      case -4: // Long Raw
      case -3: // raw
      case -1: // long
        return new LongObject(sqlType, this);
      case 1: // char
      case 12: // varchar
      case 2002: // strcut
      case 2003: // array
        return new StringObject(sqlType, this);
      case 6: // float
        return new FloatObject(sqlType, this);
      case 7: // real
      case 8:
      case 2: // number
        return new DoubleObject(sqlType, this);

      case 93: // date
//        if(typeInfo.getTypeName().equalsIgnoreCase("timestamp"))
          return new TimeStampObject(sqlType, this);
//          return new DateObject(sqlType, this);
      case 91:
        return new DateObject(sqlType, this);
      case 2004: // blob
        return new BlobObject(sqlType, this);
      case 2005: // clob
        return new ClobStreamObject(sqlType, this);
//            case 2006: // ref
      default:
        throw new RepException("REP031", new Object[] {typeInfo.getTypeName()});
    }
  }

  public String saveRepTableData(Connection connection, String pubsubName,
                                 RepTable repTable) throws SQLException,
      RepException {
    StringBuffer sb = new StringBuffer();
    PreparedStatement repPreparedStatement = null;
    String filter = repTable.getFilterClause();
    if (filter != null) {
      if (!filter.equalsIgnoreCase("")) {
//                sb.append("insert into  "+RepConstants.rep_TableName+"  ").append(" values ( '")
//                    .append(pubsubName).append("',")
//                    .append(RepConstants.seq_Name(RepConstants.rep_TableName)).append(".nextVal, '")
//                    .append(repTable.getSchemaQualifiedName()).append("','")
//                    .append(repTable.getFilterClause()).append("','")
//                    .append(repTable.getConflictResolver()).append("' ) ");

        sb.append("insert into  " + RepConstants.rep_TableName +
            "  ").append(" values ( ?,")
            .append(RepConstants.seq_Name(RepConstants.rep_TableName)).append(
            ".nextVal,?,?,?)");
//System.out.println(" sb.toString() ="+sb.toString().toUpperCase());
        repPreparedStatement = connection.prepareStatement(sb.toString());

        repPreparedStatement.setString(1, pubsubName);
        repPreparedStatement.setString(2,
                                       repTable.getSchemaQualifiedName().toString());
        repPreparedStatement.setString(3, repTable.getFilterClause());
        repPreparedStatement.setString(4, repTable.getConflictResolver());
        repPreparedStatement.execute();
//System.out.println("QUERY EXECUTED SUCCESSFULLY");
      }
    }
    else {
//            sb.append("insert into "+RepConstants.rep_TableName+" ").append(" ( ")
//                .append(RepConstants.repTable_pubsubName1)
//                .append(" , "+RepConstants.repTable_tableId2+" , ")
//                .append(RepConstants.repTable_tableName2).append(" , ")
//                .append(RepConstants.repTable_conflict_resolver4).append(" ) ")
//                .append(" values ( '").append(pubsubName).append("',")
//                .append(RepConstants.seq_Name(RepConstants.rep_TableName)).append(".nextVal, '")
//                .append(repTable.getSchemaQualifiedName()).append("','")
//                .append(repTable.getConflictResolver()).append("') ");

      sb.append("insert into  " + RepConstants.rep_TableName + "  (")
          .append(RepConstants.repTable_pubsubName1)
          .append(" , " + RepConstants.repTable_tableId2 + " , ")
          .append(RepConstants.repTable_tableName2).append(" , ")
          .append(RepConstants.repTable_conflict_resolver4).append(" ) ")
          .append(" values ( ?,")
          .append(RepConstants.seq_Name(RepConstants.rep_TableName)).append(
          ".nextVal,?,?)");
//System.out.println(" sb.toString() ="+sb.toString().toUpperCase());
      repPreparedStatement = connection.prepareStatement(sb.toString());
      repPreparedStatement.setString(1, pubsubName);
      repPreparedStatement.setString(2,
                                     repTable.getSchemaQualifiedName().toString());
      repPreparedStatement.setString(3, repTable.getConflictResolver());
      repPreparedStatement.execute();
//           System.out.println("QUERY EXECUTED SUCCESSFULLY");

    }
    /** @todo statement has been closed */
    repPreparedStatement.close();
    return sb.toString();
  }

  public void dropSequences(Connection con, String sequenceName) throws
      SQLException {
    Statement stt = con.createStatement();
    try {
      String dropsequencequery = " drop sequence " + sequenceName;
//       Seq_Shadow_TEST1
      stt.execute(dropsequencequery);
    }
    catch (SQLException ex) {
    }
    finally {
      if (stt != null)
        stt.close();
    }
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

  public void makeProvisionForLOBDataTypes(ArrayList dataTypeList) {
    ArrayList removeKeysList = null;
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
    }
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

  public boolean isPrimaryKeyException(SQLException ex) throws SQLException {
    if (ex.getErrorCode() == 1) {
      return true;
    }
    return false;
  }

  public void setColumnPrecisionInTypeInfo(TypeInfo typeInfo,
                                           ResultSetMetaData rsmt,
                                           int columnIndex) throws SQLException {
    if (! (typeInfo.getTypeName().equalsIgnoreCase("CLOB") &&
           typeInfo.getSqlType() == 2005 ||
           typeInfo.getTypeName().equalsIgnoreCase("blob") &&
           typeInfo.getSqlType() == 2004)) {
      int columnPrecion = rsmt.getPrecision(columnIndex);
      typeInfo.setColumnSize(columnPrecion);
    }

  }

  public int getAppropriatePrecision(int columnSize, String datatypeName) {
    if (datatypeName.equalsIgnoreCase("DOUBLE") ||
        datatypeName.equalsIgnoreCase("DECIMAL") ||
        datatypeName.equalsIgnoreCase("numeric") && columnSize > 38) {
      columnSize = 38;
    }
    else if (datatypeName.equalsIgnoreCase("varchar") && columnSize > 4000 ||
             datatypeName.equalsIgnoreCase("varchar2") && columnSize > 4000) {
      columnSize = 4000;
    }
    else if (datatypeName.equalsIgnoreCase("LONG VARCHAR") && columnSize > 4000) {
      columnSize = 4000;
    }

    return columnSize;
  }

  public String getTableColumns(int VendorType, String ColumnName,
                                TypeInfo typeInfo, int columnPrecision,
                                ResultSet rs) throws RepException, SQLException {
    StringBuffer sb = new StringBuffer();
    String nullable = rs.getString("IS_NULLABLE").trim();
    int SQLType = typeInfo.getSqlType();
    switch (VendorType) {
      case Utility.DataBase_DaffodilDB:
        switch (SQLType) {
          case Types.BIT:
            sb.append(ColumnName).append(" ").append(typeInfo.
                getTypeDeclaration(columnPrecision)).
                append(" ").append("check( ").append(ColumnName).append("=0").
                append(" or ").append(ColumnName).append("=1)").toString();
            break;
          case Types.TINYINT:
            sb.append(ColumnName).append(" ").append(typeInfo.
                getTypeDeclaration(columnPrecision)).append(" ").append(
                "check( ").append(ColumnName).append(
                "  between -127 and 127)").toString();
            break;
          default:
            sb.append(ColumnName).append(" ").append(typeInfo.
                getTypeDeclaration(columnPrecision));
        }
        break;
      case Utility.DataBase_SqlServer:
        switch (SQLType) {
          case Types.TINYINT:
            sb.append(ColumnName).append(" ").append(typeInfo.
                getTypeDeclaration(columnPrecision)).append(" ").append(
                "check( ").append(ColumnName).append(
                "  between 0 and 255)").toString();
            break;
          default:
            sb.append(ColumnName).append(" ").append(typeInfo.
                getTypeDeclaration(columnPrecision));
        }
        break;
      default:
        sb.append(ColumnName).append(" ").append(typeInfo.
                                                 getTypeDeclaration(
            columnPrecision));
    }
    return sb.toString();
  }

  //scale -84 to 127 only
  public int getAppropriateScale(int columnScale) throws RepException {
    if (columnScale < -84) {
      throw new RepException("REP026", new Object[] {"-84", "127"});
    }
    else if (columnScale >= 127) {
      columnScale = 127;
    }
    else if (columnScale >= -84 && columnScale < 127)
      columnScale = columnScale;
    log.debug("returning columnScale:: " + columnScale);
    return columnScale;
  }

  public void dropPublisherSystemTables(Connection con) {
    try {
      fireDropQuery(con, " drop table " + getPublicationTableName());
      fireDropQuery(con, " drop table " + getBookMarkTableName());
      fireDropQuery(con, " drop table " + getRepTableName());
      fireDropQuery(con, " drop table " + getLogTableName());
      fireDropQuery(con, " drop table " + getIgnoredColumns_Table());
      fireDropQuery(con,
                    " drop table " + getTrackReplicationTablesUpdation_Table());
      //drop sequences on reptable and logtable
      fireDropQuery(con,
                    " drop sequence " + RepConstants.seq_Name(rep_TableName));
      fireDropQuery(con, " drop sequence " + RepConstants.seq_Name(log_Table));
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
      fireDropQuery(con,
                    " drop table " + getTrackReplicationTablesUpdation_Table());
      //drop sequences on reptable and logtable
      fireDropQuery(con,
                    " drop sequence " + RepConstants.seq_Name(rep_TableName));
      fireDropQuery(con, " drop sequence " + RepConstants.seq_Name(log_Table));

    }
    catch (Exception ex) {
    }
  }

  public void deleteRecordsFromSuperLogTable(Statement subStatment) throws
      SQLException {
    // insert one record in superLogTable

    StringBuffer query = new StringBuffer();
    query.append("insert into ").append(log_Table).append(
        " values  (").append(RepConstants.seq_Name(RepConstants.rep_TableName))
        .append(".nextVal ,'$$$$$$')");

    subStatment.execute(query.toString());

    query = new StringBuffer();
    // deleting all but one last record from super log table where commonid is maximum
    query.append("Select max (").append(RepConstants.logTable_commonId1).
        append(") from ").append(log_Table);
//                     System.out.println(query.toString());
    ResultSet rs = subStatment.executeQuery(query.toString());
    rs.next();
    long maxCID = rs.getLong(1);

    query = new StringBuffer();

    query.append("delete from ").append(log_Table).append(
        " where ")
        .append(RepConstants.logTable_commonId1).append(" !=").append(maxCID);
//                     System.out.println(query.toString());
    subStatment.executeUpdate(query.toString());
    log.debug(query.toString());
  }

  public PreparedStatement makePrimaryPreperedStatement(String[]
      primaryColumns, String shadowTable, String local_pub_sub_name) throws
      SQLException, RepException {
    StringBuffer query = new StringBuffer();
    query.append(" select * from ");
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
    query.append("  and rownum=1  ");
    query.append(" order by " + RepConstants.shadow_sync_id1);
//System.out.println("#########OracleHandler.makePrimaryPreperedStatement(primaryColumns, shadowTable, local_pub_sub_name) : "+query.toString());
    Connection pub_sub_Connection = connectionPool.getConnection(
        local_pub_sub_name);
    return pub_sub_Connection.prepareStatement(query.toString());
  }

  public boolean isForiegnKeyException(SQLException ex) throws SQLException {
    if (ex.getErrorCode() == 2292)
      return true;
    else
      return false;
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
void runDDL(String pubsubName, String query) throws SQLException, RepException {
   Connection connection = connectionPool.getConnection(pubsubName);
   Statement stt = connection.createStatement();
   try {
     log.debug(query);
//System.out.println(" query ="+query);
   stt.execute(query);
   log.info("Query executed "+query);
//System.out.println(" QUERY EXECUTED SUCCESSFULLY ");

   }catch(SQLException ex){
       throw ex;
   //Ignore the exception
   }
   finally {
     connectionPool.removeSubPubFromMap(pubsubName);
     if(stt!=null)
   stt.close();
 }
 }

 public PreparedStatement makePrimaryPreperedStatementBackwardTraversing(String[] primaryColumns, long lastId, String local_pub_sub_name, String shadowTable) throws SQLException, RepException {
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
   query.append("  and rownum=1  ")
   .append(" order by ")
   .append(RepConstants.shadow_sync_id1)
   .append(" desc ");
  log.debug(query.toString());
//System.out.println("OralceHandler  makePrimaryPreperedStatementDelete  ::  " +query.toString());
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
