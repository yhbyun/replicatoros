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

public class CloudScapeHandler extends AbstractDataBaseHandler
{
protected static Logger log =Logger.getLogger(CloudScapeHandler.class.getName());

    public CloudScapeHandler()
    {}

    public CloudScapeHandler(ConnectionPool connectionPool0)
    {
    connectionPool = connectionPool0;
    vendorType = Utility.DataBase_Cloudscape;
  }

  protected void createSuperLogTable(String pubName) throws SQLException,
      RepException {
    StringBuffer logTableQuery = new StringBuffer();
    logTableQuery.append(" Create Table ")
        .append(log_Table)
            .append(" ( "+RepConstants.logTable_commonId1+" bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "+RepConstants.logTable_tableName2+" varchar(255) ) ");
    runDDL(pubName, logTableQuery.toString());

        StringBuffer indexQuery =new StringBuffer();
      indexQuery.append("CREATE INDEX ")
          .append(RepConstants.log_Index)
          .append(" ON "+getLogTableName())
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
            .append("(  "+RepConstants.repTable_pubsubName1 +"
             varchar(255) not null , "+RepConstants.repTable_tableId2 +
        " bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), ")
            .append(" "+RepConstants.repTable_tableName2 +
            " varchar(255) not null, "+
            RepConstants.repTable_filter_clause3+" varchar(255), ")
            .append( " "+RepConstants.repTable_conflict_resolver4+
            " varchar(255) , Primary Key ("+
            RepConstants.repTable_pubsubName1+", "+
            RepConstants.repTable_tableName2+") ) ");
    runDDL(pubName, repTableQuery.toString());
    } */


    protected void createRepTable(String pubName) throws SQLException,
       RepException
   {
       StringBuffer repTableQuery = new StringBuffer();
   repTableQuery.append(" Create Table ").append(getRepTableName()).append(" ( ")
       .append(RepConstants.repTable_pubsubName1).append(" varchar(255) NOT NULL, ")
       .append(RepConstants.repTable_tableId2).append("  bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), ")
       .append(RepConstants.repTable_tableName2).append("  varchar(255) NOT NULL, ")
       .append(RepConstants.repTable_filter_clause3).append("  varchar(255) , ")
       .append(RepConstants.repTable_createshadowtable6).append("  char(1) Default 'Y', ")
       .append(RepConstants.repTable_cyclicdependency7).append("  char(1) Default 'N', ")
       .append(RepConstants.repTable_conflict_resolver4).append("  varchar(255), ")
       .append("   Primary Key (").append(RepConstants.repTable_pubsubName1).append(" , ")
       .append(RepConstants.repTable_tableName2).append(" ) ) ");
       runDDL(pubName, repTableQuery.toString());
  }



  protected void createPublicationTable(String pubName) throws RepException,
        SQLException
    {
    StringBuffer pubsTableQuery = new StringBuffer();
    pubsTableQuery.append(" Create Table ")
        .append(getPublicationTableName())
            .append(" ( "+RepConstants.publication_pubName1+" varchar(255)  NOT NULL Primary Key , "+RepConstants.publication_conflictResolver2+" varchar(255) , ")
            .append(" "+RepConstants.publication_serverName3+" varchar (255) ) ");
    runDDL(pubName, pubsTableQuery.toString());
  }

  protected void createBookMarkTable(String pubName) throws SQLException,
        RepException
    {
    StringBuffer bookmarkTableQuery = new StringBuffer();
    bookmarkTableQuery.append(" Create Table ")
        .append(getBookMarkTableName())
        .append(
            " ( "+RepConstants.bookmark_LocalName1+" varchar(255) not null , "+RepConstants.bookmark_RemoteName2+" varchar(255) not null, ")
            .append(" "+RepConstants.bookmark_TableName3+" varchar(255) not null, "+RepConstants.bookmark_lastSyncId4+" bigint , ")
        .append(
        " " + RepConstants.bookmark_ConisderedId5 + " bigint ," +
        RepConstants.bookmark_IsDeletedTable +
        " char(1) default 'N' , Primary Key (" +
        RepConstants.bookmark_LocalName1 + ", " +
        RepConstants.bookmark_RemoteName2 + ", " +
        RepConstants.bookmark_TableName3 + ") ) ");
    runDDL(pubName, bookmarkTableQuery.toString());
  }

  public void createShadowTable(String pubsubName, String tableName,
                                  String allColseq,String[] primaryColumns) throws RepException
    {
    StringBuffer shadowTableQuery = new StringBuffer();
    shadowTableQuery.append(" Create Table ")
        .append(RepConstants.shadow_Table(tableName))
            .append(" ( "+RepConstants.shadow_sync_id1+" bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) , ")
            .append("   "+RepConstants.shadow_common_id2+"  BIGINT , ")
            .append("   "+RepConstants.shadow_operation3+"  char(1) , ")
            .append("   "+RepConstants.shadow_status4+"  char(1) ")
        .append(allColseq)
            .append(" , "+RepConstants.shadow_serverName_n+" varchar(255)  ")
            .append(" ,  " + RepConstants.shadow_PK_Changed + "  char(1) ) ");

        try
        {
      runDDL(pubsubName, shadowTableQuery.toString());

    }
        catch (RepException ex)
        {
      throw ex;
    }
        catch (SQLException ex)
        {
      // Ignore the Exception
    }
     createIndex(pubsubName,RepConstants.shadow_Table(tableName));
  }

  protected void createSubscriptionTable(String pubName) throws RepException,
        SQLException
    {
    String subsTableQuery = " Create Table  "
        + getSubscriptionTableName()
            + " ( "+RepConstants.subscription_subName1+" varchar(255) NOT NULL, "
            + "   "+RepConstants.subscription_pubName2+" varchar(255)  , "
            + "   "+RepConstants.subscription_conflictResolver3+" varchar(255) , "
            + "   "+RepConstants.subscription_serverName4+" varchar (255) , "
            + "   Primary Key ("+RepConstants.subscription_subName1+") ) ";
    runDDL(pubName, subsTableQuery);
  }
    public void createScheduleTable(String subName) throws SQLException, RepException
   {
    StringBuffer ScheduleTableQuery = new StringBuffer();

    ScheduleTableQuery.append(" Create Table ")
        .append(getScheduleTableName())
        .append(" ( " + RepConstants.schedule_Name + " varchar(255) not null, " +
                RepConstants.subscription_subName1 +
                " varchar(255) unique not null, ")
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
//       System.out.println(ScheduleTableQuery.toString());
  }

  public void createShadowTableTriggers(String pubsubName, String tableName,
                                        ArrayList colInfoList,
                                          String[] primCols) throws RepException
    {

    String serverName = getLocalServerName();
//    RepPrinter.print(" Columns are :::::: "  + java.util.Arrays.asList(columnTypeInfoMap.keySet().toArray(new String[0])));
//    String[] colNames = (String[]) columnTypeInfoMap.keySet().toArray(new String[0]);
    int size = colInfoList.size();
    String[] colNames = new String[size];
        for (int i = 0; i < size; i++)
        {
      colNames[i] = ( (ColumnsInfo) colInfoList.get(i)).getColumnName();
    }
    //RepPrinter.print(" Columns are :::::: "  + java.util.Arrays.asList(colNames));
    String colNameSeq = getColumnNameSequence(colNames, "").toString();
    String colNameSeqPrefixOldRow = getColumnNameSequence(colNames, "oldRow.").
        toString();
    String colNameSeqPrefixNewRow = getColumnNameSequence(colNames, "newRow.").
        toString();
    String shadowTableName = RepConstants.shadow_Table(tableName);
        String primColumnNamesSeq = getColumnNameSequence(primCols, "rep_old_");
        String primColNameSeqPrefixOldRow = getColumnNameSequence(primCols,"oldRow.").toString();
        String primColNameSeqPrefixNewRow = getColumnNameSequence(primCols,"newRow.").toString();
        String[] primColsOld =getColumnNameWithOldOrNewPrefix(primCols,"oldRow.");
        String[] primColsNew =getColumnNameWithOldOrNewPrefix(primCols,"newRow.");
    StringBuffer insertLogTable = new StringBuffer();
    insertLogTable.append(" Insert into ")
        .append(log_Table)
        .append(" ( ").append(RepConstants.logTable_tableName2)
        .append(" ) values ( '")
        .append(tableName).append("') ");

    StringBuffer insTriggerQuery = createInsertTriggerForShadowTable(tableName,
        shadowTableName, colNameSeq, primColumnNamesSeq, colNameSeqPrefixNewRow,
        primColNameSeqPrefixNewRow, serverName);
    StringBuffer delTriggerQuery = createDeleteTriggerForShadowTable(tableName,
        shadowTableName, colNameSeq, primColumnNamesSeq, colNameSeqPrefixOldRow,
        primColNameSeqPrefixOldRow, serverName);
    StringBuffer UpTriggerLogQuery = UpTriggerLogTable(tableName,
        insertLogTable.toString());
    StringBuffer UpBeforeTriggerQuery = createBeforeUpdateTrigger(tableName,
        colNameSeq, primColumnNamesSeq, colNameSeqPrefixOldRow,
        primColNameSeqPrefixOldRow, serverName, shadowTableName,
        colNameSeqPrefixNewRow);
    StringBuffer UpAfterTriggerQuery = createAfterUpdateTrigger(tableName,
        colNameSeq, primColumnNamesSeq, colNameSeqPrefixOldRow,
        primColNameSeqPrefixOldRow, serverName, shadowTableName,
        colNameSeqPrefixNewRow,primColsOld,primColsNew);

    try {
      runDDL(pubsubName, insTriggerQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      // Ignore Exception
    } try {
      runDDL(pubsubName, delTriggerQuery.toString());
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (SQLException ex) {
      // Ignore Exception
    }
    try {
         runDDL(pubsubName, UpTriggerLogQuery.toString());
       }
       catch (RepException ex) {
         throw ex;
       }
       catch (SQLException ex) {
         // Ignore Exception
       }
       try {
            runDDL(pubsubName, UpBeforeTriggerQuery.toString());
          }
          catch (RepException ex) {
            throw ex;
          }
          catch (SQLException ex) {
            // Ignore Exception
          }
          try {
               runDDL(pubsubName, UpAfterTriggerQuery.toString());
             }
             catch (RepException ex) {
               throw ex;
             }
             catch (SQLException ex) {
               // Ignore Exception
             }

  }

  private StringBuffer createInsertTriggerForShadowTable(String tableName,
      String shadowTableName, String colNameSeq, String primColumnNamesSeq,
      String colNameSeqPrefixNewRow, String primColNameSeqPrefixNewRow,
      String serverName) {
    StringBuffer insTriggerQuery = new StringBuffer();
//   Create trigger TRI_test after Insert on test  Referencing new as newRow For each Row MODE DB2SQL Insert Into  Shadowtable(common_id,operationType,col1 ,col2,server_name) Values((select max(cid) from LogTable ),'I',newRow.col1,newRow.col2,'server_name')
    insTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getInsertTriggerName(tableName))
        .append(" after insert on ").append(tableName)
        .append(" Referencing new as newRow For each Row MODE DB2SQL")
        .append(" Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( null , 'I' , null , ")
        .append(colNameSeqPrefixNewRow).append(primColNameSeqPrefixNewRow)
        .append("'").append(serverName).append("')");
    return insTriggerQuery;
  }

  private StringBuffer createDeleteTriggerForShadowTable(String tableName,
      String shadowTableName, String colNameSeq, String primColumnNamesSeq,
      String colNameSeqPrefixOldRow, String primColNameSeqPrefixOldRow,
      String serverName) {
    StringBuffer insTriggerQuery = new StringBuffer();
// String t_D_S = "Create trigger TRD_test after delete on test Referencing old as oldRow For each Row MODE DB2SQL  Insert Into Shadowtable(common_id,operationType,col1 ,col2,server_name) Values( (select max(cid) from LogTable ),'D',oldRow.col1,oldRow.col2,'server_name')";
    StringBuffer delTriggerQuery = new StringBuffer();
    delTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getDeleteTriggerName(tableName))
        .append(" after delete on ").append(tableName)
        .append(" Referencing old as oldRow For each Row MODE DB2SQL ")
        .append(" Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( null , 'D' , null , ")
        .append(colNameSeqPrefixOldRow).append(primColNameSeqPrefixOldRow)
        .append("'").append(serverName).append("')");
    return delTriggerQuery;

  }

  public StringBuffer UpTriggerLogTable(String tableName, String insertLogTable) {
    StringBuffer updTriggerLogTableQuery = new StringBuffer();
// Create trigger TRU_LogTable_test after Update on test For each Row MODE DB2SQL Insert into LogTable(table_name) values('test')
    updTriggerLogTableQuery.append(" Create trigger ")
        .append(RepConstants.getUpdateLogTableTriggerName(tableName))
        .append(" after update on ").append(tableName)
        .append(" For each Row MODE DB2SQL")
        .append(insertLogTable);
    return updTriggerLogTableQuery;
  }

  public StringBuffer createBeforeUpdateTrigger(String tableName,
                                                String colNameSeq,
                                                String primColumnNamesSeq,
                                                String colNameSeqPrefixOldRow,
                                                String
                                                primColNameSeqPrefixOldRow,
                                                String serverName,
                                                String shadowTableName,
                                                String colNameSeqPrefixNewRow) {
    StringBuffer updBeforeTriggerQuery = new StringBuffer();
//Create trigger TRU_B_test after Update on test Referencing old as oldRow For each Row MODE DB2SQL Insert Into Shadowtable(common_id,operationType,status,col1,col2,server_name) Values((Select max(cid) from  LogTable),'U','B',oldRow.col1,oldRow.col2,'server_3001')
    updBeforeTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getUpdateBeforeTriggerName(tableName))
        .append(" after update on ").append(tableName)
        .append(" Referencing old as oldRow For each Row MODE DB2SQL")
        .append(" Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n)
            .append(" ) Values ( (Select max("+RepConstants.logTable_commonId1+
                    ") from  "+RepConstants.log_Table+") , 'U' , 'B' , ")
        .append(colNameSeqPrefixOldRow).append(primColNameSeqPrefixOldRow)
        .append("'").append(serverName).append("')");
    return updBeforeTriggerQuery;

  }

  public StringBuffer createAfterUpdateTrigger(String tableName,
                                               String colNameSeq,
                                               String primColumnNamesSeq,
                                               String colNameSeqPrefixOldRow,
                                               String
                                               primColNameSeqPrefixOldRow,
                                               String serverName,
                                               String shadowTableName,
                                                 String colNameSeqPrefixNewRow,String[] primColsOld,String[] primColsNew)
    {
    StringBuffer updAfterTriggerQuery = new StringBuffer();
//Create trigger TRU_B_test after Update on test Referencing old as oldRow For each Row MODE DB2SQL Insert Into Shadowtable(common_id,operationType,status,col1,col2,server_name) Values((Select max(cid) from  LogTable),'U','B',oldRow.col1,oldRow.col2,'server_3001')
    updAfterTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getUpdateAfterTriggerName(tableName))
        .append(" after update on ").append(tableName)
        .append(
        " Referencing old as oldRow new as newRow For each Row MODE DB2SQL")
        .append(" Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(primColumnNamesSeq)
        .append(RepConstants.shadow_serverName_n).append(" , ")
        .append(RepConstants.shadow_PK_Changed)
            .append(" ) Values ((Select max("+RepConstants.logTable_commonId1+
             ") from  "+RepConstants.log_Table+") , 'U' , 'A' , ")
        .append(colNameSeqPrefixNewRow).append(primColNameSeqPrefixOldRow)
        .append("'").append(serverName).append("',(CASE WHEN ");
        for (int i = 0; i < primColsOld.length; i++) {
            if (i != 0)
              updAfterTriggerQuery.append(" and ");
            updAfterTriggerQuery.append(primColsOld[i] )
                                    .append("!=" )
                                    .append(primColsNew[i]);
          }


    updAfterTriggerQuery.append(" THEN 'Y' ELSE null END))");
    return updAfterTriggerQuery;

  }

    public boolean isDataTypeOptionalSizeSupported(TypeInfo typeInfo)
    {
    int sqlType = typeInfo.getSqlType();
    String typeName = typeInfo.getTypeName();
        switch (sqlType)
        {
      case 4:
      case 5:
      case 7:
      case -4:
      case -5:
      case 91:
      case 92:
      case 93:
      case 8:
      case -1:
      case 2004:
      case 2005:
      case -6:
      case -7:
      case 1111:
        return false;
      default:
        return true;
    }
  }

  public void setTypeInfo(TypeInfo typeInfo, ResultSet rs) throws RepException,
        SQLException
    {
        switch (typeInfo.getSqlType())
        {
//      case Types.BIT 		:      typeInfo.setTypeName("SMALLINT");break;//-7;
      case Types.TINYINT:
        typeInfo.setTypeName("SMALLINT");
        break; //-6;
      case Types.SMALLINT:
        typeInfo.setTypeName("SMALLINT");
        break; // 5;
      case Types.INTEGER:
        typeInfo.setTypeName("INTEGER");
        break; //  4;
      case Types.LONGVARBINARY:
        if (typeInfo.getTypeName().equalsIgnoreCase("image")) {
          typeInfo.setTypeName("BLOB"); // //2005;
          typeInfo.setSqlType(Types.BLOB);
          return;
        }
        typeInfo.setTypeName("LONG VARCHAR FOR BIT DATA");
        break; // -4;
      case Types.BIGINT:
        typeInfo.setTypeName("BIGINT");
        break; // //-5;
      case Types.FLOAT:
        typeInfo.setTypeName("FLOAT");
        break; // // 6;
      case Types.REAL:
        typeInfo.setTypeName("REAL");
        break; // // 7;
      case Types.DOUBLE:
        typeInfo.setTypeName("DOUBLE");
        break; // // 8;
      case Types.NUMERIC:
        typeInfo.setTypeName("NUMERIC");
        break; // // 2;
      case Types.DECIMAL:
        typeInfo.setTypeName("DECIMAL");
        break; // // 3;
      case Types.CHAR:

        if (typeInfo.getTypeName().equalsIgnoreCase("NATIONAL CHAR")) {
          typeInfo.setTypeName("NATIONAL CHAR"); //  1;
          return;
        }
        typeInfo.setTypeName("CHAR");
        break; //  1;
      case Types.VARCHAR:

        if (typeInfo.getTypeName().equalsIgnoreCase("NATIONAL CHAR VARYING")) {
          typeInfo.setTypeName("NATIONAL CHAR VARYING"); // 12;
          return;
        }
        else if (typeInfo.getTypeName().equalsIgnoreCase("VARBINARY")) {
          typeInfo.setTypeName("BLOB"); // //2004;
          typeInfo.setSqlType(Types.BLOB);
          return;
        }
        else if (typeInfo.getTypeName().equalsIgnoreCase("text")) {
          typeInfo.setTypeName("CLOB"); // //2005;
          typeInfo.setSqlType(Types.CLOB);
          return;
        }
        typeInfo.setTypeName("VARCHAR");
        break; // //12;
      case Types.LONGVARCHAR:

        if (typeInfo.getTypeName().equalsIgnoreCase("LONG VARCHAR")) {
          typeInfo.setTypeName("LONG VARCHAR"); // -1;
          return;
        }
        else if (typeInfo.getTypeName().equalsIgnoreCase("text")) {
          typeInfo.setTypeName("CLOB"); // //2005;
          typeInfo.setSqlType(Types.CLOB);
          return;
        }

//        typeInfo.setTypeName("LONG NVARCHAR");break;// //-1;
        typeInfo.setTypeName("LONG VARCHAR");
        break; // //-1;
      case Types.DATE:
        typeInfo.setTypeName("DATE");
        break; // //91;
      case Types.TIME:
        typeInfo.setTypeName("TIME");
        break; // //92;
      case Types.TIMESTAMP:
        typeInfo.setTypeName("TIMESTAMP");
        break; // 93;
//      case Types.BINARY		:
//        if(typeInfo.getTypeName().equalsIgnoreCase("bytea")){
//          typeInfo.setTypeName("BLOB");
//          typeInfo.setSqlType(Types.BLOB);
//          return; // //-2;
//        }
//        typeInfo.setTypeName("BLOB"); break;

      case Types.OTHER:
        typeInfo.setTypeName("CLOB");
        break; //1111;
      case Types.BLOB:
        typeInfo.setTypeName("BLOB");
        break; //2004
      case Types.CLOB:
        typeInfo.setTypeName("CLOB");
        break; //2005
      case Types.BINARY: //-2
      case Types.VARBINARY: //-3
      case Types.REF: //2006;
      case Types.JAVA_OBJECT: //2000
      case Types.NULL: //0
      case Types.DISTINCT: //2001;
      case Types.STRUCT: //2002;
      case Types.ARRAY: //2003;
      case Types.DATALINK: //70;
      case Types.BOOLEAN: //16;
      case Types.BIT: //-7
      default:
        throw new RepException("REP031", new Object[] {typeInfo.getTypeName()});
    }
  }

  public AbstractColumnObject getColumnObject(TypeInfo typeInfo) throws
      RepException {
    int sqlType = typeInfo.getSqlType();
//RepPrinter.print(" CloudSpcapeHandler typeInfo = " + typeInfo);
    switch (sqlType) {
      case 1: // NATIONAL CHAR/CHAR
      case 12: // NATIONAL CHAR VARYING/VARCHAR
      case -1: // LONG NVARCHAR/LONG VARCHAR
        return new StringObject(sqlType,this);
      case -2: // BIT
      case -3: // BIT VARYING
      case -4: // LONG VARCHAR FOR BIT DATA
        return new BytesObject(sqlType,this);
      case 4: // INT
      case 5: // SMALLINT
      case -6: //TINYINT
      case -7: //BIT
        return new IntegerObject(sqlType,this);
      case -5: // BIGINT
        return new LongObject(sqlType,this);
      case 3: // DECIMAL
      case 8: // DOUBLE PRECISION
      case 2: // NUMERIC
      case 6: // FLOAT
        return new DoubleObject(sqlType,this);
      case 7: // REAL
        return new FloatObject(sqlType,this);
      case 91: // DATE
        return new DateObject(sqlType,this);
      case 92: // TIME
        return new TimeObject(sqlType,this);
      case 93: // TIMESTAMP
        return new TimeStampObject(sqlType,this);
      case 2004:
        return new BlobObject(sqlType,this);
      case 2005:
      case 1111: //Other
        return new ClobObject(sqlType,this);
      default:
        throw new RepException("REP031", new Object[] {new Integer(sqlType)});
    }
  }

  public boolean getPrimaryKeyErrorCode(SQLException ex) throws SQLException {
    if (ex.getSQLState().equalsIgnoreCase("23505")) {
      return true;
    }
    else {
      return false;
    }
  }

  public int getAppropriatePrecision(int columnSize, String datatypeName) {
    if (datatypeName.equalsIgnoreCase("numeric") && columnSize > 31) {
      columnSize = 31;
    }
    else if (datatypeName.equalsIgnoreCase("DECIMAL") && columnSize > 31) {
      columnSize = 31;
    }

    return columnSize;
  }

  public void makeProvisionForLOBDataTypes(ArrayList dataTypeMap) {
    ArrayList removeKeysList = null;
    for (int i = 0, size = dataTypeMap.size(); i < size; i++) {
      ColumnsInfo ci = (ColumnsInfo) dataTypeMap.get(i);
      String dataType = ci.getDataTypeDeclaration();
      if (dataType.indexOf("BLOB") != -1 ||
          dataType.indexOf("CLOB") != -1) {
        if (removeKeysList == null) {
          removeKeysList = new ArrayList();
        }
        removeKeysList.add(ci);
      }
    }
    if (removeKeysList != null) {
      for (int i = 0, length = removeKeysList.size(); i < length; i++) {
        dataTypeMap.remove(removeKeysList.get(i));
      }
    }
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
            if (nullable.equalsIgnoreCase("NO")) {
              sb.append(ColumnName).append(" ").append(typeInfo.
                  getTypeDeclaration(columnPrecision)).append(" NOT NULL ").
                  append(", ").append("check( ").append(ColumnName).append(
                  " = 0").
                  append(" or ").append(ColumnName).append(" = 1)").toString();
            }
            else {
              sb.append(ColumnName).append(" ").append(typeInfo.
                  getTypeDeclaration(columnPrecision)).append(
                  " check( ").append(ColumnName).append(" = 0 ").append(" or ").
                  append(ColumnName).append(" = 1 )").toString();
            }
            break;
          case Types.TINYINT:
            if (nullable.equalsIgnoreCase("NO")) {
              sb.append(ColumnName).append(" ").append(typeInfo.
                  getTypeDeclaration(columnPrecision)).append("  NOT NULL ").
                  append(", ").append("check( ").append(ColumnName).append(
                  "  between -127 and 127)").toString();
            }
            else {
              sb.append(ColumnName).append(" ").append(typeInfo.
                  getTypeDeclaration(columnPrecision)).append(" ").append(
                  "check( ").append(ColumnName).append(
                  "  between -127 and 127)").toString();
            }
            break;
          default:
            sb.append(ColumnName).append(" ").append(typeInfo.
                getTypeDeclaration(columnPrecision));
        }
        break;
      case Utility.DataBase_SqlServer:
        switch (SQLType) {
          case Types.BIT:
            if (nullable.equalsIgnoreCase("NO")) {
              sb.append(ColumnName).append(" ").append(typeInfo.
                  getTypeDeclaration(columnPrecision)).append("  NOT NULL ").
                  append(" ,").append("check( ").append(ColumnName).append(
                  " = 0 ").
                  append(" or ").append(ColumnName).append(" = 1)").toString();
            }
            else {
              sb.append(ColumnName).append(" ").append(typeInfo.
                  getTypeDeclaration(columnPrecision)).append(" ").append(
                  "check( ").append(ColumnName).append(" = 0 ").append(" or ").
                  append(ColumnName).append(" = 1)").toString();
            }
            break;
          case Types.TINYINT:
            if (nullable.equalsIgnoreCase("NO")) {
              sb.append(ColumnName).append(" ").append(typeInfo.
                  getTypeDeclaration(columnPrecision)).append("  NOT NULL ").
                  append(" ,").append("check( ").append(ColumnName).append(
                  "  between 0 and 255)").toString();
            }
            else {
              sb.append(ColumnName).append(" ").append(typeInfo.
                  getTypeDeclaration(columnPrecision)).append(" ").append(
                  "check( ").append(ColumnName).append(
                  "  between 0 and 255)").toString();
            }
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
      throw new RepException("REP026", new Object[] {"1", "31"});
    }
    else if (columnScale >= 31) {
      columnScale = 31;
    }
    else if (columnScale >= 0 && columnScale < 31)
      columnScale = columnScale;
    log.debug("returning columnScale::" + columnScale);
    return columnScale;
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
    query.append(" order by " + RepConstants.shadow_sync_id1);
    Connection pub_sub_Connection = connectionPool.getConnection(
        local_pub_sub_name);
    return pub_sub_Connection.prepareStatement(query.toString());
  }



public boolean isForiegnKeyException(SQLException ex) throws SQLException {
           if(ex.getErrorCode()== 30000)
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
        if (ex.getSQLState().equalsIgnoreCase("23505"))
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    protected void createIgnoredColumnsTable(String pubName) throws SQLException,
         RepException
     {
         StringBuffer ignoredColumnsQuery = new StringBuffer();
     ignoredColumnsQuery.append(" Create Table ").append(getIgnoredColumns_Table()).append(" ( ")
         .append(RepConstants.ignoredColumnsTable_tableId1).append("  bigint NOT NULL, ")
         .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2).append("  varchar(255) NOT NULL, ")
         .append("   Primary Key (").append(RepConstants.ignoredColumnsTable_tableId1).append(" , ")
         .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2).append(" ) ) ");
         runDDL(pubName, ignoredColumnsQuery.toString());
     }

     protected void createTrackReplicationTablesUpdationTable(String pubSubName) throws
            RepException, SQLException {
          StringBuffer trackRepTablesUpdationQuery = new StringBuffer();
          trackRepTablesUpdationQuery.append(" CREATE  TABLE ").append(getTrackReplicationTablesUpdation_Table()).append(" ( " +
              RepConstants.trackUpdation + " smallint NOT NULL PRIMARY KEY) ");
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
              query.append(" select * from ");
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
//System.out.println("CloudscapeHandler makePrimaryPreperedStatementDelete  ::  " +query.toString());
              Connection pub_sub_Connection = connectionPool.getConnection(local_pub_sub_name);
              return pub_sub_Connection.prepareStatement(query.toString());
            }

  /**
   * isSchemaSupported
   * Derby database support the schema so it return true.
   * @return boolean
   */
  public boolean isSchemaSupported() {
    return true;
  }

}
