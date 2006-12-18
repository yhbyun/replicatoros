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

public class SQLServerHandler
    extends AbstractDataBaseHandler  {
  protected static Logger log = Logger.getLogger(SQLServerHandler.class.getName());
  public SQLServerHandler() {}

  public SQLServerHandler(ConnectionPool connectionPool0) {
    connectionPool = connectionPool0;
    vendorType = Utility.DataBase_SqlServer;
  }

  protected void createSuperLogTable(String pubName) throws SQLException,
      RepException {
    StringBuffer logTableQuery = new StringBuffer();
    logTableQuery.append(" Create Table ")
        .append(getLogTableName())
        .append(" ( " + RepConstants.logTable_commonId1 + " bigint identity , " +
                RepConstants.logTable_tableName2 + " varchar(255) ) ");
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

  protected void createRepTable(String pubName) throws SQLException,
        RepException
    {
    StringBuffer repTableQuery = new StringBuffer();
    repTableQuery.append(" Create Table ").append(getRepTableName()).append(" ( ")
        .append(RepConstants.repTable_pubsubName1).append(" varchar(255) , ")
        .append(RepConstants.repTable_tableId2).append("  int identity, ")
        .append(RepConstants.repTable_tableName2).append("  varchar(255) , ")
        .append(RepConstants.repTable_filter_clause3).append("  varchar(255) , ")
        .append(RepConstants.repTable_createshadowtable6).append("  char(1) Default 'Y', ")
        .append(RepConstants.repTable_cyclicdependency7).append("  char(1) Default 'N', ")
        .append(RepConstants.repTable_conflict_resolver4).append("  varchar(255), ")
        .append("   Primary Key (").append(RepConstants.repTable_pubsubName1).append(" , ")
        .append(RepConstants.repTable_tableName2).append(" ) ) ");
    runDDL(pubName, repTableQuery.toString());
  }

  public void createShadowTable(String pubsubName, String tableName,
                                String allColSequence,String[] primaryColumns) throws RepException {
    StringBuffer shadowTableQuery = new StringBuffer();
    shadowTableQuery.append(" Create Table ")
        .append(RepConstants.shadow_Table(tableName))
        .append(" ( " + RepConstants.shadow_sync_id1 + "  bigint identity , ")
        .append("   " + RepConstants.shadow_common_id2 + "  bigint , ")
        .append("   " + RepConstants.shadow_operation3 + "  char(1) , ")
        .append("   " + RepConstants.shadow_status4 + "  char(1) ")
        .append(allColSequence)
        .append(" , " + RepConstants.shadow_serverName_n + " varchar(255) ")
        .append(" , " + RepConstants.shadow_PK_Changed + " char(1) ) ");
    try {
      runDDL(pubsubName, shadowTableQuery.toString());
    }
    catch (SQLException ex) {
      // Ignore Exception
    }
    catch (RepException ex) {
      throw ex;
    }
     createIndex(pubsubName,RepConstants.shadow_Table(tableName));

  }

  public void createScheduleTable(String subName) throws SQLException,
      RepException {
    StringBuffer ScheduleTableQuery = new StringBuffer();
    ScheduleTableQuery.append(" Create Table ")
        .append(getScheduleTableName())
            .append(" ( " + RepConstants.schedule_Name + " varchar(255) , " + RepConstants.subscription_subName1 + " varchar(255) unique , ")
            .append("  " + RepConstants.schedule_type + " varchar(255) , " )
            .append( " "+ RepConstants.publication_serverName3 + " varchar (255) ," + RepConstants.publication_portNo + " varchar(255) ,")
            .append(" " + RepConstants.recurrence_type + " varchar(255) , " + RepConstants.replication_type + " varchar(255) ,")
            .append(" " + RepConstants.schedule_time + " bigint , " )
            .append(" " + RepConstants.schedule_counter + " bigint , Primary Key (" + RepConstants.schedule_Name + " , " + RepConstants.subscription_subName1 + ") ) ");
    runDDL(subName, ScheduleTableQuery.toString());
  }

  public void createShadowTableTriggers(String pubName, String tableName,
                                        HashMap colNameDataType,
                                        String[] primCols) throws RepException {
  }

  public void createShadowTableTriggers(String pubName, String tableName,
                                        ArrayList colNameDataType,
                                        String[] primCols) throws RepException {

    //SQLServer
    //  create trigger abc3 on a for insert  as begin
    //  declare @va int ,@vb int;
    //  Select @va = max(a) from b;
    //  select @vb = inserted.a from inserted ;
    //  insert into d values ( @va , @vb) ;   end

    String serverName = getLocalServerName();
    String shadowTableName = RepConstants.shadow_Table(tableName);
    StringBuffer insVars = new StringBuffer();
    StringBuffer delVars = new StringBuffer();
    StringBuffer insVarsDataType = new StringBuffer();
    StringBuffer delVarsDataType = new StringBuffer();
    StringBuffer colNameSeq = new StringBuffer();
    StringBuffer primVars = new StringBuffer();
    StringBuffer primColNameSeq = new StringBuffer();
    StringBuffer primColAssign1 = new StringBuffer();
    StringBuffer primColAssign2 = new StringBuffer();
    StringBuffer primVarsDataType = new StringBuffer();
    StringBuffer delAssignSeq = new StringBuffer(); //getColumnNameSequence(colNames,":oldRow.").toString();
    StringBuffer insAssignSeq = new StringBuffer(); //getColumnNameSequence(colNames,":newRow.").toString();
    StringBuffer insertcursor = new StringBuffer();
    StringBuffer deletecursor = new StringBuffer();
    StringBuffer oldRecordPrimaryKey = new StringBuffer();
    String[] oldPrimaryKeyValues = new String[primCols.length];
    String[] newPrimaryKeyValues = new String[primCols.length];


    //Object[] keys = colNameDataType.keySet().toArray();
    //java.util.Arrays.sort(keys);
    int size = colNameDataType.size();
    for (int i = 0; i < size; ) {
      ColumnsInfo ci = (ColumnsInfo) colNameDataType.get(i);
      String columnName = ci.getColumnName();
      for (int j = 0; j < primCols.length; j++) {
      if(primCols[j].equalsIgnoreCase(columnName))  {
        oldPrimaryKeyValues[j] = " @varx" + i;
        newPrimaryKeyValues[j] = " @vary" + i;
      }

      }
      String dataType = ci.getDataTypeDeclaration();
      insVars.append(" @varx" + i);
      delVars.append(" @vary" + i);
      insVarsDataType.append(" @varx" + i + " " + dataType);
      delVarsDataType.append(" @vary" + i + " " + dataType);
      colNameSeq.append(columnName);
      insAssignSeq.append(" @varx" + i + " = inserted." + columnName);
      insertcursor.append(" inserted." + columnName);
      delAssignSeq.append(" @vary" + i + " = deleted." + columnName);
      deletecursor.append(" deleted." + columnName);
      if (++i != size) {
        insVars.append(" , ");
        delVars.append(" , ");
        insVarsDataType.append(" , ");
        delVarsDataType.append(" , ");
        colNameSeq.append(" , ");
        insAssignSeq.append(" , ");
        insertcursor.append(" , ");
        delAssignSeq.append(" , ");
        deletecursor.append(" , ");
      }
    }

    StringBuffer trackPrimaryKeyUpdation = new StringBuffer();
             int primaryColumnLength = oldPrimaryKeyValues.length;
             trackPrimaryKeyUpdation.append(" if( ");
             for (int i = 0; i < primaryColumnLength; i++) {
               if (i != 0)
               trackPrimaryKeyUpdation.append(" and ");
               trackPrimaryKeyUpdation.append(oldPrimaryKeyValues[i]).append("!=")
                                      .append(newPrimaryKeyValues[i]).append(")");
             }


    for (int j = 0, length = primCols.length; j < length; ) {
      String col = (String) primCols[j];
            primColNameSeq.append("rep_old_" + col);
      primVars.append(" @varz" + j);
      primColAssign1.append(" @varz" + j + " = deleted." + col);
      oldRecordPrimaryKey.append("deleted." + col);
      primColAssign2.append(" @varz" + j + " = inserted." + col);
      primVarsDataType.append(" @varz" + j +" ").append(ColumnsInfo.getDataTypeDeclaration(col));
      if (++j != length) {
        primColNameSeq.append(" , ");
        primVars.append(" , ");
        primColAssign1.append(" , ");
        primColAssign2.append(" , ");
        primVarsDataType.append(" , ");
        oldRecordPrimaryKey.append(" , ");
      }
    }

    StringBuffer insertLogTable = new StringBuffer();
    insertLogTable.append("  Insert into ")
        .append(getLogTableName())
        .append(" ( ").append(RepConstants.logTable_tableName2)
        .append(" ) values ( '")
        .append(tableName).append("'); ");

    StringBuffer insTriggerQuery = new StringBuffer();

    String insertCursorName = RepConstants.getCursorName(tableName, "Cursor_I");
    insTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getInsertTriggerName(tableName))
        .append(" on ").append(tableName)
        .append(" for insert as begin  declare @firetrigger bit; declare ")
        .append(insVarsDataType.toString()).append(" , ")
        .append(primVarsDataType.toString())
        .append(" ; declare " + insertCursorName).append(" cursor for select ")
        .append(insertcursor.toString()).append(" from inserted ")
        .append(" SELECT @firetrigger = "+RepConstants.trackUpdation +" FROM "+getTrackReplicationTablesUpdation_Table())
        .append(" if(@firetrigger = 1) begin")
        .append(" open ").append(insertCursorName)
        .append(" fetch next from  ").append(insertCursorName).append(" into ")
        .append(insVars.toString())
        .append(" while @@fetch_status = 0 ").append(" begin ")

        .append(" Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(",").append(primColNameSeq).append(",")
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( null , 'I' , null , ")
        .append(insVars).append(",").append(primVars)
        .append(",'").append(serverName).append("') ; ")
        .append(" fetch next from  ").append(insertCursorName).append(" into ")
        .append(insVars.toString()).append(" end  ")
        .append(" close ").append(insertCursorName)
        .append(" end; deallocate ").append(insertCursorName).append("  end");

    String deleteCursorName = RepConstants.getCursorName(tableName, "Cursor_D");
    StringBuffer delTriggerQuery = new StringBuffer();
    delTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getDeleteTriggerName(tableName))
        .append(" on ").append(tableName)
        .append(" for delete as begin declare ")
        .append(delVarsDataType.toString()).append(" , ")
        .append(primVarsDataType.toString())

        .append(" declare @firetrigger bit; declare " + deleteCursorName).append(" cursor for select ")
        .append(deletecursor.toString()).append(" from deleted ")
        .append(" SELECT @firetrigger = "+RepConstants.trackUpdation +" FROM "+getTrackReplicationTablesUpdation_Table())
        .append(" if(@firetrigger = 1) begin")
        .append(" open ").append(deleteCursorName)
        .append(" fetch next from  ").append(deleteCursorName).append(" into ")
        .append(delVars.toString())
        .append(" while @@fetch_status = 0 ").append(" begin ")
        .append("  Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(",")
        .append(primColNameSeq).append(",")
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( null , 'D' , null , ")
        .append(delVars).append(",").append(primVars)
        .append(",'").append(serverName).append("')  ")
        .append(" fetch next from  ").append(deleteCursorName).append(" into ")
        .append(delVars.toString() + " end ")

        .append(" close ").append(deleteCursorName)
        .append(" end; deallocate ").append(deleteCursorName).append("  end ");

    String updateCursorNameOld = RepConstants.getCursorName(tableName,
        "Cursor_UO");
    String updateCursorNameNew = RepConstants.getCursorName(tableName,
        "Cursor_UN");
    StringBuffer updTriggerQuery = new StringBuffer();

    updTriggerQuery.append(" Create trigger ")
        .append(RepConstants.getUpdateTriggerName(tableName))
        .append(" on ").append(tableName)
        .append(" for update as begin declare @maxlogid bigint , ")
        .append(delVarsDataType.toString()).append(" , ")
        .append(insVarsDataType.toString()).append(" , ")
        .append(primVarsDataType.toString()).append(" ; ")
        .append(" declare @firetrigger bit;  declare " + updateCursorNameOld).append(" cursor for select ")
        .append(deletecursor.toString()).append(" , ").append(
        oldRecordPrimaryKey)
        .append(" from deleted ")
        .append(" declare " + updateCursorNameNew).append(" cursor for select ")
        .append(insertcursor.toString()).append(" from inserted ")
        .append(" SELECT @firetrigger = "+RepConstants.trackUpdation +" FROM "+getTrackReplicationTablesUpdation_Table())
        .append(" if(@firetrigger = 1) begin")
        .append(" open ").append(updateCursorNameOld)
        .append(" open ").append(updateCursorNameNew)

        .append(" fetch next from  ").append(updateCursorNameOld).append(
        " into ")
        .append(delVars.toString()).append(" , ").append(primVars)

        .append(" fetch next from  ").append(updateCursorNameNew).append(
        " into ")
        .append(insVars.toString())

        .append(" while @@fetch_status = 0 ").append(" begin ")

        .append(insertLogTable.toString())
        .append(" Select @maxlogid = max(").append(RepConstants.
        logTable_commonId1)
        .append(") from ").append(getLogTableName()).append(" ;  ")

//       .append(trackPriamryKeyUpdation(oldPrimaryKeyValues,newPrimaryKeyValues,primCols,tableName))

        .append("   Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(",")
        .append(primColNameSeq).append(",")
        .append(RepConstants.shadow_serverName_n)
        .append(" ) Values ( @maxlogid , 'U' , 'B' , ")
        .append(delVars).append(",").append(primVars)
        .append(",'").append(serverName).append("') ;")
        .append(" declare @pkChanged char(1); ")
        .append(trackPrimaryKeyUpdation.toString())
        .append(" begin set @pkChanged ='Y'; end; ")
        .append(" Insert Into ")
        .append(shadowTableName).append(" ( ")
        .append(RepConstants.shadow_common_id2).append(", ")
        .append(RepConstants.shadow_operation3).append(", ")
        .append(RepConstants.shadow_status4).append(", ")
        .append(colNameSeq).append(",")
        .append(primColNameSeq).append(",")
        .append(RepConstants.shadow_serverName_n).append(",")
        .append(RepConstants.shadow_PK_Changed)
        .append(" ) Values ( @maxlogid , 'U' , 'A' , ")
        .append(insVars).append(",").append(primVars)
        .append(",'").append(serverName).append("',@pkChanged) ;   ")

        .append(" fetch next from  ").append(updateCursorNameOld).append(" into ")
        .append(delVars.toString()).append(" , ").append(primVars)
        .append(" fetch next from  ").append(updateCursorNameNew).append(" into ")
        .append(insVars.toString()).append(" ; end")

        .append(" close ").append(updateCursorNameOld)
        .append(" close ").append(updateCursorNameNew)
        .append(" end; deallocate ")
        .append(updateCursorNameOld)
        .append(" deallocate ").append(updateCursorNameNew).append(" end ");

        try
        {
//System.out.println("  insTriggerQuery.toString()  "+insTriggerQuery.toString());
      runDDL(pubName, insTriggerQuery.toString());
    }
        catch (RepException ex)
        {
      throw ex;
    }
    catch (SQLException ex) {
//          ex.printStackTrace();
      // Ignore Exception
    }  try
        {
//System.out.println("  delTriggerQuery.toString()  "+delTriggerQuery.toString());
      runDDL(pubName, delTriggerQuery.toString());
    }
        catch (RepException ex)
        {
      throw ex;
    }
    catch (SQLException ex) {
//          ex.printStackTrace();
      // Ignore Exception
    }
    try
          {
        runDDL(pubName, updTriggerQuery.toString());
      }
          catch (RepException ex)
          {
        throw ex;
      }
      catch (SQLException ex) {
//          ex.printStackTrace();
        // Ignore Exception
      }

  }

  protected void createBookMarkTable(String pubName) throws SQLException,
      RepException {
    StringBuffer bookmarkTableQuery = new StringBuffer();
    bookmarkTableQuery.append(" Create Table ")
        .append(bookmark_TableName)
        .append(" ( " + RepConstants.bookmark_LocalName1 + " varchar(255) , " +
                RepConstants.bookmark_RemoteName2 + " varchar(255) , ")
        .append(" " + RepConstants.bookmark_TableName3 + " varchar(255) , " +
                RepConstants.bookmark_lastSyncId4 + " bigint , ")
        .append(" " + RepConstants.bookmark_ConisderedId5 + " bigint ," +
                RepConstants.bookmark_IsDeletedTable +
                " char(1) default 'N' , Primary Key (" +
                RepConstants.bookmark_LocalName1 + ", " +
                RepConstants.bookmark_RemoteName2 + ", " +
                RepConstants.bookmark_TableName3 + ") ) ");
    runDDL(pubName, bookmarkTableQuery.toString());
  }

  public boolean isDataTypeOptionalSizeSupported(TypeInfo typeInfo) {
    // sql type 3
    // 1 unique identifier
    //y -3 -2 1 2 3 6 12
    //n 1111 -6 -7 -2 -1 3 4 5 7
    int sqlType = typeInfo.getSqlType();
    String typeName = typeInfo.getTypeName();
    switch (sqlType) {
      case -3:
      case 2:
      case 6:
        return true;
      case -2:
        return!typeName.equalsIgnoreCase("timestamp");
      case 1:
        return!typeName.equalsIgnoreCase("uniqueidentifier");
      case 3:
        if (typeName.equalsIgnoreCase("real")) {
          return false;
        }
        return typeName.indexOf("money") == -1;
      case 12:
        return typeName.toLowerCase().startsWith("varchar") ||
            typeName.toLowerCase().equalsIgnoreCase("nvarchar") ||
            typeName.toLowerCase().equalsIgnoreCase("ID");
      default:
        return false;
    }
  }

  public void setTypeInfo(TypeInfo typeInfo, ResultSet rs) throws RepException,
      SQLException {
//RepPrinter.print("In SQLServerHandler SQL Server setTypeInfo " + typeInfo);
//System.out.println("In SQLServerHandler SQL Server setTypeInfo " + typeInfo );
    String typeName = typeInfo.getTypeName().trim();
    int sqlType = typeInfo.getSqlType();
    switch (sqlType) {
      case Types.BOOLEAN:
      case Types.BIT:
        typeInfo.setTypeName("bit");
        break; //-7;
      case Types.TINYINT:
        typeInfo.setTypeName("tinyint");
        break; //-6;
      case Types.SMALLINT:
        typeInfo.setTypeName("bigint");
        break; //5;
      case Types.INTEGER:
        typeInfo.setTypeName("int");
        break; //4;
      case Types.BIGINT:
        typeInfo.setTypeName("bigint");
        break; //-5;
      case Types.FLOAT:
        typeInfo.setTypeName("float");
        break; //6;
      case Types.REAL:
        typeInfo.setTypeName("real");
        break; //7;
      case Types.NUMERIC:
        typeInfo.setTypeName("numeric");
        break; //2;
      case Types.DECIMAL:

//          typeInfo.setTypeName("real");
//          typeInfo.setSqlType(Types.REAL);
        typeInfo.setTypeName("decimal");

        break; //3;
      case Types.CHAR:
        typeInfo.setTypeName("char");
        break; //1;
      case Types.VARCHAR:
        if (typeInfo.getTypeName().trim().equalsIgnoreCase("sql_variant")) {
          typeInfo.setSqlType(Types.BLOB);
          return;
        }
        typeInfo.setTypeName("varchar");
        break; //12;
      case Types.LONGVARCHAR:
        typeInfo.setTypeName("text");
        break; //-1;
      case Types.DATE:
        typeInfo.setTypeName("datetime");
        break; //91;
      case Types.BINARY:

//          if(typeInfo.getTypeName().equalsIgnoreCase("timestamp"))
//          {
//            typeInfo.setTypeName("timestamp");
//            break;
//          }
        typeInfo.setTypeName("binary");
        break; //-2;
      case Types.VARBINARY:
        typeInfo.setTypeName("varbinary");
        break; //-3;
      case Types.LONGVARBINARY:
        typeInfo.setTypeName("image");
        break; //-4;
      case Types.BLOB:
      case Types.CLOB:
      case Types.OTHER:
        if (typeName.equalsIgnoreCase("CLOB")) {
          typeInfo.setTypeName("text");
          typeInfo.setSqlType(Types.BLOB);
          return;
        }
        else if (typeName.equalsIgnoreCase("BLOB")) {
          typeInfo.setTypeName("image");
          typeInfo.setSqlType(Types.BLOB);
          return;
        }
        typeInfo.setTypeName("sql_variant");
        break; //1111;
      case Types.DOUBLE:
        typeInfo.setTypeName("real");
        typeInfo.setSqlType(Types.REAL);
        break; //8;
      case Types.TIME:
        typeInfo.setTypeName("datetime");
        break; //92;
      case Types.TIMESTAMP:
        if (typeName.equalsIgnoreCase("date")) {
          typeInfo.setSqlType(Types.DATE);
        }
        else if (typeName.equalsIgnoreCase("time")) {
          typeInfo.setSqlType(Types.TIME);
        }
        typeInfo.setTypeName("datetime");
        break; //93;
      case Types.NULL:
      case Types.JAVA_OBJECT:
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.ARRAY:
      case Types.REF:
      case Types.DATALINK:
      default:
        throw new RepException("REP031", new Object[] {typeInfo.getTypeName()});
    }
  }

  public AbstractColumnObject getColumnObject(TypeInfo typeInfo) throws
      RepException {
//RepPrinter.print(" SQLSERVER SqlType called for >>>>>>>>> " + typeInfo);
//System.out.println(" SQLSERVER SqlType called for >>>>>>>>> " + typeInfo);
    String typeName = typeInfo.getTypeName().trim();
    int sqlType = typeInfo.getSqlType();
    switch (sqlType) {
      case 2005: // SQL_vqriant
        return new ClobObject(sqlType,this);
      case 2004:
      case 1111: // SQL_vqriant
        if (typeInfo.getTypeName().equalsIgnoreCase("sql_variant")) {
          return new StringSQL_VariantObject(sqlType,this);
        }
        else {
          return new BlobObject(sqlType,this);
        }
      case -1:
        return new ClobObject(sqlType,this);
      case 12: // nvarchar
        if (typeInfo.getTypeName().equalsIgnoreCase("sql_variant")) {
          return new StringSQL_VariantObject(sqlType,this);
        }
        else {
          if (isColumnSizeExceedMaximumSize(typeInfo)) {
            return new SQLServerVarCharObject(sqlType,this);
          }
          return new StringObject(sqlType,this);
        }
      case 1: // char
        return new StringObject(sqlType,this);
      case 4: // int
      case 5: // smallint
//          case  2: // numeric  Commented By sube May 13
      case -6: // tinyint
        return new IntegerObject(sqlType,this);
      case -5: // bigint
        return new LongObject(sqlType,this);
      case -3: // varbinary
        return new ByteObject(sqlType,this);
//            return new BytesObject(sqlType,this);
      case -4: // image
      case -2: // binary , timestamp

//            if(typeInfo.getTypeName().equalsIgnoreCase("timestamp"))
//              return new SQLTimeStamp(sqlType);
        return new BlobObject(sqlType,this);
      case 2: // numeric
      case 3: // decimal
      case 7: // real
      case 8:
        return new DoubleObject(sqlType,this);
      case 6: // float
        return new FloatObject(sqlType,this);
      case 16:
      case -7: // bit
        return new BooleanObject(sqlType,this);
        //Date and Time add by sube
      case 91: // DATE
        return new DateObject(sqlType,this);
      case 92: //TIME
        return new TimeObject(sqlType,this);
      case 93: // datetime , smalldatetime
        return new TimeStampObject(sqlType,this);
//
//            case 91 :   // DATE
//            case 92 :	//TIME
//            case  93: // datetime , smalldatetime
//              return typeName.equalsIgnoreCase("date") ? (AbstractColumnObject) new DateObject(sqlType,this)
//                  : typeName.equalsIgnoreCase("time") ? (AbstractColumnObject) new TimeObject(sqlType,this)
//                  : new TimeStampObject(sqlType,this);
      default:
        throw new RepException("REP031", new Object[] {typeInfo.getTypeName()});
    }
  }

  public void makeProvisionForLOBDataTypes(HashMap dataTypeMap) {
  }

  public void makeProvisionForLOBDataTypes(ArrayList dataTypeMap) {
    ArrayList removeKeysList = null;
    for (int i = 0, size = dataTypeMap.size(); i < size; i++) {
      ColumnsInfo ci = (ColumnsInfo) dataTypeMap.get(i);
      String dataType = ci.getDataTypeDeclaration();
      if (dataType.indexOf("text") != -1 ||
          dataType.indexOf("image") != -1) {
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

  public String updateDataType(String dataType0) {
    int index = dataType0.indexOf("identity");
    if (index == -1) {
      return dataType0;
    }
    return dataType0.substring(0, index);
  }

  public boolean isColumnSizeExceedMaximumSize(TypeInfo typeInfo) {
    return typeInfo.getSqlType() == 12 ? typeInfo.getcolumnSize() > 4192 ? true : false : false;
  }

  public String getTableColumns(int VendorType, String ColumnName,
                                TypeInfo typeInfo, int columnPrecision,
                                ResultSet rs) throws RepException {
    StringBuffer sb = new StringBuffer();
    int SQLType = typeInfo.getSqlType();
    if (VendorType == Utility.DataBase_DaffodilDB) {
      switch (SQLType) {
        case Types.TINYINT:
          sb.append(ColumnName).append(" ").append(typeInfo.getTypeDeclaration(
              columnPrecision)).append(" ").append("check( ").append(ColumnName).
              append("  between 0 and 255)").toString();
          break;
        default:
          sb.append(ColumnName).append(" ").append(typeInfo.getTypeDeclaration(
              columnPrecision));
      }
    }
    else {
      sb.append(ColumnName).append(" ").append(typeInfo.getTypeDeclaration(
          columnPrecision));
    }
    return sb.toString();
  }

  public boolean getPrimaryKeyErrorCode(SQLException ex) throws SQLException {
    if (ex.getErrorCode() == 2627) {
      return true;
    }
    return false;
  }

  public void setColumnPrecisionInTypeInfo(TypeInfo typeInfo,
                                           ResultSetMetaData rsmt,
                                           int columnIndex) throws SQLException {
    int columnPrecion = rsmt.getPrecision(columnIndex);
    if (typeInfo.getTypeName().equalsIgnoreCase("numeric") ||
        typeInfo.getTypeName().equalsIgnoreCase("decimal") &&
        columnPrecion > 38) {
      typeInfo.setColumnSize(columnPrecion);

    }
    else {
      typeInfo.setColumnSize(columnPrecion);
    }

  }

  protected void createIndex(String pubsubName, String tableName) throws
      RepException {
    StringBuffer createIndexQuery = new StringBuffer();
//           create index ind on cmsadm2.R_S_Bank(Rep_sync_id)
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
//             ex.printStackTrace();
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
//      System.out.println("returning columnScale::" + columnScale);
    log.debug("returning columnScale::" + columnScale);
    return columnScale;
  }

  public int getAppropriatePrecision(int columnSize, String datatypeName) {
    if (datatypeName.equalsIgnoreCase("numeric") ||
        datatypeName.equalsIgnoreCase("decimal") && columnSize > 38) {
      columnSize = 38;
    }
    return columnSize;

  }

  public PreparedStatement makePrimaryPreperedStatement(String[]
      primaryColumns, String shadowTable, String local_pub_sub_name) throws
      SQLException, RepException {
    StringBuffer query = new StringBuffer();
    query.append(" select top 1 * from ");
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
//System.out.println("SQLServerHandler.makePrimaryPreperedStatement(primaryColumns, shadowTable, local_pub_sub_name): "+query.toString());
    return pub_sub_Connection.prepareStatement(query.toString());
  }



         public boolean isForiegnKeyException(SQLException ex) throws SQLException {
//System.out.println(" ex Message  : "+ex.getMessage()+" ex Error code : "+ex.getErrorCode());
           if(ex.getErrorCode()== 547)
           return true;
           else
           return false;
         }

  /**
   * isPrimaryKeyException
   * @param ex SQLException
   * @return boolean
   */
  public boolean isPrimaryKeyException(SQLException ex) throws SQLException {
     if (ex.getErrorCode() == 2627) {
             return true;
         }
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
       trackRepTablesUpdationQuery.append(" CREATE  TABLE ").append(
           getTrackReplicationTablesUpdation_Table()).append(" ( " +
           RepConstants.trackUpdation + " bit  PRIMARY KEY) ");
       runDDL(pubSubName, trackRepTablesUpdationQuery.toString());
       runDDL(pubSubName,
              "Insert into " + getTrackReplicationTablesUpdation_Table() + " values(1)");
     }

     protected void createTriggerForTrackReplicationTablesUpdationTable(String
         pubSubName) throws RepException, SQLException {
       StringBuffer trackRepTablesUpdationTriggerQuery = new StringBuffer();
       trackRepTablesUpdationTriggerQuery.append(" CREATE  TRIGGER TRI_")
           .append(getTrackReplicationTablesUpdation_Table()).append(
           " ON " + getTrackReplicationTablesUpdation_Table())
           .append(" AFTER INSERT AS  DELETE FROM " +
                   getTrackReplicationTablesUpdation_Table() + " WHERE ")
           .append(RepConstants.trackUpdation + " NOT IN(SELECT * FROM inserted)");
       runDDL(pubSubName, trackRepTablesUpdationTriggerQuery.toString());
     }

     public PreparedStatement makePrimaryPreperedStatementBackwardTraversing(
         String[] primaryColumns, long lastId, String local_pub_sub_name,
         String shadowTable) throws SQLException, RepException {
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
       query.append(" order by ")
           .append(RepConstants.shadow_sync_id1)
           .append(" desc ");
       log.debug(query.toString());
   //System.out.println("SQlServerHandler  makePrimaryPreperedStatementDelete  ::  "+query.toString());
       Connection pub_sub_Connection = connectionPool.getConnection(
           local_pub_sub_name);
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
