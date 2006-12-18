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

package com.daffodilwoods.replication;

import java.io.*;
import java.sql.*;
import org.apache.log4j.Logger;
import java.util.Properties;

/**
 * This class holds all the constants used throughout the project.
 * In case of some manipulation only this class needs to be manipulated
 * in spite of changing all classes.
 *
 */

public class RepConstants
{

    static {
       initialiseRepConstants();
     }

     // Below given are used to get prefix from user
     private static String InsertTriggerPrefix, UpdateTriggerPrefix,
         DeleteTriggerPrefix, ShadowTablePrefix, SequencePrefix,indexPrefix;

    public final static String publication_TableName = "Rep_Publications";
    public final static String subscription_TableName = "Rep_Subscriptions";
    public final static String bookmark_TableName = "Rep_BookMarkTable";
    public final static String rep_TableName = "Rep_RepTable";
    public final static String log_Table = "Rep_LogTable";
    public final static String log_Index = "Rep_LogIndex";
    public final static String schedule_Table="Rep_ScheduleTable";
    public final static String ignoredColumns_Table = "Rep_IgnoredColumnsTable";
    public final static String trackReplicationTablesUpdation_Table = "Rep_TrackRepTabUpdation";


    public final static String otherOjectQueries_table = "RepOtherObjQueriesTab";

    public final static String publication_pubName1 = "Rep_pub_name";
    public final static String publication_conflictResolver2 = "Rep_conflict_resolver";
    public final static String publication_serverName3 = "Rep_server_Name";

    public final static String subscription_subName1 = "Rep_Sub_Name";
    public final static String subscription_pubName2 = "Rep_pub_Name";
    public final static String subscription_conflictResolver3 = "Rep_conflict_resolver";
    public final static String subscription_serverName4 = "Rep_server_Name";

    public final static String bookmark_LocalName1 = "Rep_LocalName";
    public final static String bookmark_RemoteName2 = "Rep_RemoteName";
    public final static String bookmark_TableName3 = "Rep_Table_Name";
    public final static String bookmark_lastSyncId4 = "Rep_Last_Sync_ID";
    public final static String bookmark_ConisderedId5 = "Rep_ConsideredId";
    public final static String bookmark_IsDeletedTable="Rep_IsDeletedTable";

    public final static String repTable_pubsubName1 = "Rep_PubSub_Name";
    public final static String repTable_tableId2 = "Rep_Table_Id";
    public final static String repTable_tableName2 = "Rep_Table_Name";
    public final static String repTable_filter_clause3 = "Rep_filter_clause";
    public final static String repTable_createshadowtable6 = "Rep_createshadowtable";
    public final static String repTable_cyclicdependency7 = "Rep_cyclicdependency";
    public final static String repTable_conflict_resolver4 = "Rep_conflict_resolver";

    public final static String shadow_sync_id1 = "Rep_sync_id";
    public final static String shadow_common_id2 = "Rep_common_id";
    public final static String shadow_operation3 = "Rep_operationType";
    public final static String shadow_status4 = "Rep_status";
    public final static String shadow_serverName_n = "Rep_server_name";
    public final static String shadow_PK_Changed = "Rep_PK_Changed";

    public final static String logTable_commonId1 = "Rep_cid";
    public final static String logTable_tableName2 = "Rep_table_name";

    public final static String schedule_Name = "Rep_schedule_name";
    public final static String schedule_type = "Rep_scheduleType";
    public final static String schedule_time = "Rep_scheduleTime";
    public final static String recurrence_type = "Rep_recurrenceType";
    public final static String replication_type = "Rep_replicationType";
    public final static String schedule_counter = "Rep_scheduleCounter";
    public final static String publication_portNo ="Rep_publicationPortNo";
    public final static String recurrence_yearType = "Year";
    public final static String recurrence_monthType = "Month";
    public final static String recurrence_dayType = "Day";
    public final static String recurrence_hourType = "Hour";
    public final static String recurrence_minuteType = "Minutes";
    public final static String scheduleType_realTime="realTime";
    public final static String scheduleType_nonRealTime="nonRealTime";

    public final static String replication_snapshotType = "Snapshot";
    public final static String replication_synchronizeType = "Synchronize";
    public final static String replication_pullType = "Pull";
    public final static String replication_pushType = "Push";

  	public final static String create_Publication="Create";
    public final static String addTable_Publication="AddTable";
    public final static String dropTable_Publication="DropTable";

  /**
   * columns for IgnoredColumns Table
   */
  public final static String ignoredColumnsTable_tableId1 = "Rep_Table_Id";
  public final static String ignoredColumnsTable_ignoredcolumnName2 = "Rep_ignoredcolumnName";
  /**
   * columns for trackReplicationTablesUpdation Table
   */
    public final static String trackUpdation = "TrackUpdation";
  //this will be the fully qualified name of the object
  public final static String otherObjectQueriesTable_pubName = "Rep_pubName";
  public final static String otherObjectQueriesTable_objectName = "Rep_objectName";
  public final static String otherObjectQueriesTable_objectType = "Rep_objectType";

    public final static String insert_operation = "I";
    public final static String update_operation = "U";
    public final static String delete_operation = "D";

    public final static String afterUpdate = "A";
    public final static String beforeUpdate = "B";

  public final static String YES = "Y";
  public final static String NO = "N";

  public final static String loadRepTableQuery = " Select  * From " + rep_TableName + " where " + repTable_pubsubName1 + " = ? order by " + RepConstants.repTable_tableId2;

  public final static String loadPublicationQuery = " Select  * From " + publication_TableName + " where " + publication_pubName1 + " = ? ";
  public final static String loadBookMarkTableQuery = "Select * From " + bookmark_TableName;
  public final static String loadSubscriptionQuery = " Select  * From " + subscription_TableName + " where " + subscription_subName1 + " = ? ";

    public final static String subscriber_wins = "subscriber_wins";
    public final static String publisher_wins = "publisher_wins";
    public final static String subscriber = "subscriber";
    public final static String publisher = "publisher";

  public static String shadow_Table(String schematable) {
    return getObjectName(schematable, ShadowTablePrefix.trim());
//          return getObjectName(schematable, "REP_SHADOW_");
     }

  public static String seq_ShadowTableName(String schematable) {
    return getObjectName(schematable, SequencePrefix.trim());
     }

  public static String seq_Name(String tableName) {
//    return "Seq_" + tableName;
        return SequencePrefix.trim() + tableName;
     }


  public static String getInsertTriggerName(String schematable) {
    return getObjectName(schematable, InsertTriggerPrefix.trim());
       }

  public static String getDeleteTriggerName(String schematable) {
    return getObjectName(schematable, DeleteTriggerPrefix);
     }

  public static String getUpdateTriggerName(String schematable) {
    return getObjectName(schematable, UpdateTriggerPrefix);
     }

  public static String getUpdateBeforeTriggerName(String schematable) {
        return getObjectName(schematable, "TRU_B_");
    }

  public static String getUpdateAfterTriggerName(String schematable) {
        return getObjectName(schematable, "TRU_A_");
    }

  public static String getUpdateLogTableTriggerName(String schematable) {
        return getObjectName(schematable, "TRU_LT_");
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

   public static void writeERROR_FILE(Exception ex) {
     try {
      FileOutputStream errorLogFile = new FileOutputStream(PathHandler.getErrorFilePath(), true);
       Timestamp dt = new Timestamp(System.currentTimeMillis());
       errorLogFile.write("\n\n".getBytes());
       errorLogFile.write( (dt + "\n").getBytes());
      errorLogFile.write(ex.getMessage() == null ? "null".getBytes() : ex.getMessage().getBytes());
       errorLogFile.write("\n\n".getBytes());
       StackTraceElement[] traceElement = ex.getStackTrace();
       for (int i = 0; i < traceElement.length; i++) {
         errorLogFile.write(traceElement[i].toString().getBytes());
       }
       errorLogFile.close();
     }
     catch (IOException ex2) {
       // dump the error
     }
   }

   public static void writeMessage_FILE(String ex) {
       try {
         FileOutputStream errorLogFile = new FileOutputStream(PathHandler.
             getErrorFilePath(), true);
         Timestamp dt = new Timestamp(System.currentTimeMillis());
         errorLogFile.write("\n\n".getBytes());
         errorLogFile.write( (dt + "\n").getBytes());
         errorLogFile.write(ex.getBytes());
         errorLogFile.write("\n\n".getBytes());
         errorLogFile.close();
       }
       catch (IOException ex2) {
         // dump the error
       }
     }


   public static String getCursorName(String schematable, String prefix) {
     int index = schematable.indexOf('.');
     if (index == -1) {
       return schematable + prefix;
     }

     String table = schematable.substring(index + 1);
     return table + prefix;
   }

   public static String Index_Name(String tableName)
          {
            int index = tableName.indexOf('.');
            if (index != -1)
            tableName= tableName.substring(index + 1);

          return indexPrefix + tableName;
          }

          private static void initialiseRepConstants(){
            try {
              File f = new File("." + File.separator + "config.ini");
              if (!f.exists())
                f.createNewFile();
              Properties p = new Properties();
              p.load(new FileInputStream(f)); // Try to load props
              InsertTriggerPrefix = p.getProperty("TRI_Prefix", "TRI_").trim();
//        System.out.println(" InsertTriggerPrefix : "+InsertTriggerPrefix);
              UpdateTriggerPrefix = p.getProperty("TRU_Prefix", "TRU_").trim();
//        System.out.println(" UpdateTriggerPrefix : "+UpdateTriggerPrefix);
              DeleteTriggerPrefix = p.getProperty("TRD_Prefix", "TRD_").trim();
//        System.out.println(" DeleteTriggerPrefix : "+DeleteTriggerPrefix);
              ShadowTablePrefix = p.getProperty("ShadowTablePrefix", "REP_SHADOW_").trim();
//        System.out.println(" ShadowTablePrefix : "+ShadowTablePrefix);
              SequencePrefix = p.getProperty("Seq_Prefix", "Seq_").trim();
//        System.out.println(" SequencePrefix : "+SequencePrefix);
              indexPrefix = p.getProperty("Index_Prefix", "I_").trim();
//        System.out.println(" SequencePrefix : "+SequencePrefix);
            }
            catch (Exception ex) {
              RepConstants.writeERROR_FILE(ex);
            }
          }


    public static String gen_Name(String tableName) {
         return "Gen_" + tableName;
       }


       public static String gen_ShadowTableName(String schematable) {
          return getObjectName(schematable, "Gen_");
        }




}
