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
import java.util.Properties;
import java.sql.*;
import java.util.*;
import com.daffodilwoods.replication.RepException;
import com.daffodilwoods.replication.RepConstants;

public class UpdatePreviousVersion {

  private  static String DaffodilDB="DaffodilDB";
  private  static String SQLServer="SQLServer";
  private  static String Oracle="Oracle";
  private  static String Postgres="Postgres";
  private  static String CloudScape="CloudScape";
  private  static String DB2="DB2";

  private  static String VendorType="VendorType";
  private  static String DataBaseName="DataBaseName";
  private  static String Driver="Driver";
  private  static String Url="Url";
  private  static String UserName="UserName";
  private  static String Password="Password";

  public UpdatePreviousVersion() throws Exception{
    try {
      getDatabaseVendorType();
       }
    catch (Exception ex) {}
  }


   private void getDatabaseVendorType(){
                 try {
                   File f = new File("." + File.separator + "config.ini");
                   if (!f.exists())
                    f.createNewFile();
                   Properties p = new Properties();
                   p.load(new FileInputStream(f));
                   VendorType = p.getProperty("VendorType");
 System.out.println(" VENDOR TYPE  : "+VendorType);
                   DataBaseName=p.getProperty("DataBaseName");
 System.out.println(" DATABASE NAME : "+DataBaseName);
                   Driver=p.getProperty("Driver");
 System.out.println(" DATABASE DRIVER  :"+Driver);
                   Url=p.getProperty("Url");
 System.out.println(" DATABASE URL  :"+Url);
                   UserName=p.getProperty("UserName");
 System.out.println(" USER NAME : "+UserName);
                   Password=p.getProperty("Password");
 System.out.println(" PASSWORD :"+Password);
                   createTableQuery(VendorType);
                 }   catch (Exception ex) {
                 }
        }
        private void createTableQuery(String dbServer) throws Exception {
          dbServer =dbServer.trim();
          if (dbServer.equalsIgnoreCase(SQLServer)) {
            Connection SQLconn = getConnection(Driver, Url, UserName, Password);
//Creating trackReplicationTablesUpdation_Table Table
            StringBuffer trackRepTabUpdation_Table = new StringBuffer();
            trackRepTabUpdation_Table.append(" Create Table ")
                .append(RepConstants.trackReplicationTablesUpdation_Table)
                .append(" ( ")
                .append(RepConstants.trackUpdation)
                .append(" bit PRIMARY KEY default 1) ");
            runDDL(SQLconn, trackRepTabUpdation_Table.toString());
            runDDL(SQLconn,"Insert into "+RepConstants.trackReplicationTablesUpdation_Table+" values(1)" );
//Creating Index on Rep_LogTable
            StringBuffer indexQuery = new StringBuffer();
            indexQuery.append(" CREATE INDEX ")
                .append(RepConstants.log_Index)
                .append(" ON " + RepConstants.log_Table)
                .append(" ( ")
                .append(RepConstants.logTable_commonId1)
                .append(" ) ");
            runDDL(SQLconn, indexQuery.toString());
//  Creating Rep_ignoredcolumnName Table
            StringBuffer ignoredColumnsQuery = new StringBuffer();
            ignoredColumnsQuery.append(" Create Table ")
                .append(RepConstants.ignoredColumns_Table).
                append(" ( ")
                .append(RepConstants.ignoredColumnsTable_tableId1).append("  int , ")
                .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
                .append("  varchar(255) , ")
                .append("   Primary Key (")
                .append(RepConstants.ignoredColumnsTable_tableId1)
                .append(" , ")
                .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
                .append(" ) ) ");
            runDDL(SQLconn, ignoredColumnsQuery.toString());
// Alter Rep_RepTable
            StringBuffer alterRepTable = new StringBuffer();
            alterRepTable.append(" Alter table " + RepConstants.rep_TableName)
                .append(" add " + RepConstants.repTable_createshadowtable6)
                .append(" char(1) NOT NULL default 'Y' , ")
                .append(RepConstants.repTable_cyclicdependency7)
                .append(" char(1) NOT NULL default 'N' ");
           runDDL(SQLconn,alterRepTable.toString());

           StringBuffer alterBookMarkTable = new StringBuffer();
           alterBookMarkTable.append(" Alter table " + RepConstants.bookmark_TableName)
               .append(" add " + RepConstants.bookmark_IsDeletedTable)
               .append(" CHAR(1) DEFAULT 'N'");
           runDDL(SQLconn,alterBookMarkTable.toString());
          }



          else  if (dbServer.equalsIgnoreCase(Postgres)) {
            Connection SQLconn = getConnection(Driver, Url, UserName, Password);
//Creating trackReplicationTablesUpdation_Table Table
            StringBuffer trackRepTabUpdation_Table = new StringBuffer();
            trackRepTabUpdation_Table.append(" Create Table ")
                .append(RepConstants.trackReplicationTablesUpdation_Table)
                .append(" ( ")
                .append(RepConstants.trackUpdation)
                .append(" bit PRIMARY KEY  default 1 ) ");
            runDDL(SQLconn, trackRepTabUpdation_Table.toString());
            runDDL(SQLconn,"Insert into "+RepConstants.trackReplicationTablesUpdation_Table+" values(1)" );
//Creating Index on Rep_LogTable
            StringBuffer indexQuery = new StringBuffer();
            indexQuery.append(" CREATE INDEX ")
                .append(RepConstants.log_Index)
                .append(" ON " + RepConstants.log_Table)
                .append(" ( ")
                .append(RepConstants.logTable_commonId1)
                .append(" ) ");
            runDDL(SQLconn, indexQuery.toString());
//  Creating Rep_ignoredcolumnName Table
            StringBuffer ignoredColumnsQuery = new StringBuffer();
            ignoredColumnsQuery.append(" Create Table ")
                .append(RepConstants.ignoredColumns_Table).
                append(" ( ")
                .append(RepConstants.ignoredColumnsTable_tableId1).append("  int , ")
                .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
                .append("  varchar(255) , ")
                .append("   Primary Key (")
                .append(RepConstants.ignoredColumnsTable_tableId1)
                .append(" , ")
                .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
                .append(" ) ) ");
            runDDL(SQLconn, ignoredColumnsQuery.toString());
// Alter Rep_RepTable
            StringBuffer alterRepTable = new StringBuffer();
            alterRepTable.append(" Alter table " + RepConstants.rep_TableName)
                .append(" add " + RepConstants.repTable_createshadowtable6)
                .append(" char(1) NOT NULL default 'Y' , add  ")
                .append(RepConstants.repTable_cyclicdependency7)
                .append(" char(1) NOT NULL default 'N' ");
           runDDL(SQLconn,alterRepTable.toString());
           StringBuffer alterBookMarkTable = new StringBuffer();
          alterBookMarkTable.append(" Alter table " + RepConstants.bookmark_TableName)
              .append(" add " + RepConstants.bookmark_IsDeletedTable)
              .append(" CHAR(1) DEFAULT 'N'");
          runDDL(SQLconn,alterBookMarkTable.toString());

          }
          else  if (dbServer.equalsIgnoreCase(DaffodilDB)) {
           Connection SQLconn = getConnection(Driver, Url, UserName, Password);
//Creating trackReplicationTablesUpdation_Table Table
           StringBuffer trackRepTabUpdation_Table = new StringBuffer();
           trackRepTabUpdation_Table.append(" Create Table ")
               .append(RepConstants.trackReplicationTablesUpdation_Table)
               .append(" ( ")
               .append(RepConstants.trackUpdation)
               .append(" bit PRIMARY KEY default 1 ) ");
           runDDL(SQLconn, trackRepTabUpdation_Table.toString());
           runDDL(SQLconn,"Insert into "+RepConstants.trackReplicationTablesUpdation_Table+" values(1)" );
//Creating Index on Rep_LogTable
           StringBuffer indexQuery = new StringBuffer();
           indexQuery.append(" CREATE INDEX ")
               .append(RepConstants.log_Index)
               .append(" ON " + RepConstants.log_Table)
               .append(" ( ")
               .append(RepConstants.logTable_commonId1)
               .append(" ) ");
           runDDL(SQLconn, indexQuery.toString());
//  Creating Rep_ignoredcolumnName Table
           StringBuffer ignoredColumnsQuery = new StringBuffer();
           ignoredColumnsQuery.append(" Create Table ")
               .append(RepConstants.ignoredColumns_Table).
               append(" ( ")
               .append(RepConstants.ignoredColumnsTable_tableId1).append("  int , ")
               .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
               .append("  varchar(255) , ")
               .append("   Primary Key (")
               .append(RepConstants.ignoredColumnsTable_tableId1)
               .append(" , ")
               .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
               .append(" ) ) ");
           runDDL(SQLconn, ignoredColumnsQuery.toString());
// Alter Rep_RepTable
           StringBuffer alterRepTable = new StringBuffer();
           alterRepTable.append(" Alter table " + RepConstants.rep_TableName)
               .append(" add " + RepConstants.repTable_createshadowtable6)
               .append(" char(1) default 'Y' ");
          runDDL(SQLconn,alterRepTable.toString());

          StringBuffer alterRepTable1 = new StringBuffer();
           alterRepTable1.append(" Alter table " + RepConstants.rep_TableName)
               .append(" add " + RepConstants.repTable_cyclicdependency7)
               .append(" char(1) default 'N' ");
          runDDL(SQLconn,alterRepTable1.toString());

          StringBuffer alterBookMarkTable = new StringBuffer();
          alterBookMarkTable.append(" Alter table " + RepConstants.bookmark_TableName)
              .append(" add " + RepConstants.bookmark_IsDeletedTable)
              .append(" CHAR(1) DEFAULT 'N'");
          runDDL(SQLconn,alterBookMarkTable.toString());


         }


         else  if (dbServer.equalsIgnoreCase(Oracle)) {
           Connection SQLconn = getConnection(Driver, Url, UserName, Password);
//Creating trackReplicationTablesUpdation_Table Table
           StringBuffer trackRepTabUpdation_Table = new StringBuffer();
           trackRepTabUpdation_Table.append(" Create Table ")
               .append(RepConstants.trackReplicationTablesUpdation_Table)
               .append(" ( ")
               .append(RepConstants.trackUpdation)
               .append(" smallint PRIMARY KEY  default 1 ) ");
           runDDL(SQLconn, trackRepTabUpdation_Table.toString());
           runDDL(SQLconn,"Insert into "+RepConstants.trackReplicationTablesUpdation_Table+" values(1)" );
//Creating Index on Rep_LogTable
           StringBuffer indexQuery = new StringBuffer();
           indexQuery.append(" CREATE INDEX ")
               .append(RepConstants.log_Index)
               .append(" ON " + RepConstants.log_Table)
               .append(" ( ")
               .append(RepConstants.logTable_commonId1)
               .append(" ) ");
           runDDL(SQLconn, indexQuery.toString());
//  Creating Rep_ignoredcolumnName Table
           StringBuffer ignoredColumnsQuery = new StringBuffer();
           ignoredColumnsQuery.append(" Create Table ")
               .append(RepConstants.ignoredColumns_Table).
               append(" ( ")
               .append(RepConstants.ignoredColumnsTable_tableId1).append("  int , ")
               .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
               .append("  varchar(255) , ")
               .append("   Primary Key (")
               .append(RepConstants.ignoredColumnsTable_tableId1)
               .append(" , ")
               .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
               .append(" ) ) ");
           runDDL(SQLconn, ignoredColumnsQuery.toString());
// Alter Rep_RepTable
           StringBuffer alterRepTable = new StringBuffer();
           alterRepTable.append(" Alter table " + RepConstants.rep_TableName)
               .append(" add ( " + RepConstants.repTable_createshadowtable6)
               .append(" char(1) default 'Y' , ")
               .append(RepConstants.repTable_cyclicdependency7)
               .append(" char(1) default 'N' ) ");
          runDDL(SQLconn,alterRepTable.toString());
          StringBuffer alterBookMarkTable = new StringBuffer();
          alterBookMarkTable.append(" Alter table " + RepConstants.bookmark_TableName)
              .append(" add " + RepConstants.bookmark_IsDeletedTable)
              .append(" CHAR(1) DEFAULT 'N'");
          runDDL(SQLconn,alterBookMarkTable.toString());

         }


         else  if (dbServer.equalsIgnoreCase(CloudScape)) {
           Connection SQLconn = getConnection(Driver, Url, UserName, Password);
//Creating trackReplicationTablesUpdation_Table Table
           StringBuffer trackRepTabUpdation_Table = new StringBuffer();
           trackRepTabUpdation_Table.append(" Create Table ")
               .append(RepConstants.trackReplicationTablesUpdation_Table)
               .append(" ( ")
               .append(RepConstants.trackUpdation)
               .append(" smallint not null PRIMARY KEY default 1 ) ");
           runDDL(SQLconn, trackRepTabUpdation_Table.toString());
           runDDL(SQLconn,"Insert into "+RepConstants.trackReplicationTablesUpdation_Table+" values(1)" );
//Creating Index on Rep_LogTable
           StringBuffer indexQuery = new StringBuffer();
           indexQuery.append(" CREATE INDEX ")
               .append(RepConstants.log_Index)
               .append(" ON " + RepConstants.log_Table)
               .append(" ( ")
               .append(RepConstants.logTable_commonId1)
               .append(" ) ");
           runDDL(SQLconn, indexQuery.toString());
//  Creating Rep_ignoredcolumnName Table
           StringBuffer ignoredColumnsQuery = new StringBuffer();
           ignoredColumnsQuery.append(" Create Table ")
               .append(RepConstants.ignoredColumns_Table).
               append(" ( ")
               .append(RepConstants.ignoredColumnsTable_tableId1).append("   bigint NOT NULL , ")
               .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
               .append("  varchar(255) NOT NULL, ")
               .append("   Primary Key (")
               .append(RepConstants.ignoredColumnsTable_tableId1)
               .append(" , ")
               .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
               .append(" ) ) ");
           runDDL(SQLconn, ignoredColumnsQuery.toString());
// Alter Rep_RepTable
           StringBuffer alterRepTable = new StringBuffer();
           alterRepTable.append(" Alter table dbo." + RepConstants.rep_TableName)
               .append(" add column " + RepConstants.repTable_createshadowtable6)
               .append(" char(1) NOT NULL default 'Y' ");
          runDDL(SQLconn,alterRepTable.toString());

          StringBuffer alterRepTable1 = new StringBuffer();
           alterRepTable1.append(" Alter table dbo." + RepConstants.rep_TableName)
               .append(" add column " + RepConstants.repTable_cyclicdependency7)
               .append(" char(1) NOT NULL default 'N' ");
          runDDL(SQLconn,alterRepTable1.toString());
          StringBuffer alterBookMarkTable = new StringBuffer();
          alterBookMarkTable.append(" Alter table " + RepConstants.bookmark_TableName)
          .append(" add " + RepConstants.bookmark_IsDeletedTable)
          .append(" CHAR(1) DEFAULT 'N'");
          runDDL(SQLconn,alterBookMarkTable.toString());
         }
         else  if (dbServer.equalsIgnoreCase(DB2)) {
           Connection SQLconn = getConnection(Driver, Url, UserName, Password);
//Creating trackReplicationTablesUpdation_Table Table
           StringBuffer trackRepTabUpdation_Table = new StringBuffer();
           trackRepTabUpdation_Table.append(" Create Table ")
               .append(RepConstants.trackReplicationTablesUpdation_Table)
               .append(" ( ")
               .append(RepConstants.trackUpdation)
               .append(" bit PRIMARY KEY default 1 ) ");
           runDDL(SQLconn, trackRepTabUpdation_Table.toString());
//Creating Index on Rep_LogTable
           StringBuffer indexQuery = new StringBuffer();
           indexQuery.append(" CREATE INDEX ")
               .append(RepConstants.log_Index)
               .append(" ON " + RepConstants.log_Table)
               .append(" ( ")
               .append(RepConstants.logTable_commonId1)
               .append(" ) ");
           runDDL(SQLconn, indexQuery.toString());
           runDDL(SQLconn,"Insert into "+RepConstants.trackReplicationTablesUpdation_Table+" values(1)" );
//  Creating Rep_ignoredcolumnName Table
           StringBuffer ignoredColumnsQuery = new StringBuffer();
           ignoredColumnsQuery.append(" Create Table ")
               .append(RepConstants.ignoredColumns_Table).
               append(" ( ")
               .append(RepConstants.ignoredColumnsTable_tableId1).append("  int , ")
               .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
               .append("  varchar(255) , ")
               .append("   Primary Key (")
               .append(RepConstants.ignoredColumnsTable_tableId1)
               .append(" , ")
               .append(RepConstants.ignoredColumnsTable_ignoredcolumnName2)
               .append(" ) ) ");
           runDDL(SQLconn, ignoredColumnsQuery.toString());
// Alter Rep_RepTable
           StringBuffer alterRepTable = new StringBuffer();
           alterRepTable.append(" Alter table " + RepConstants.rep_TableName)
               .append(" add " + RepConstants.repTable_createshadowtable6)
               .append(" char(1) NOT NULL default 'Y' , ")
               .append(RepConstants.repTable_cyclicdependency7)
               .append(" char(1) NOT NULL default 'N' ");
          runDDL(SQLconn,alterRepTable.toString());

          StringBuffer alterBookMarkTable = new StringBuffer();
          alterBookMarkTable.append(" Alter table " + RepConstants.bookmark_TableName)
              .append(" add " + RepConstants.bookmark_IsDeletedTable)
              .append(" CHAR(1) DEFAULT 'N'");
          runDDL(SQLconn,alterBookMarkTable.toString());

         }
        }
 private Connection getConnection(String driver0, String url0,
                                         String user0, String pwd0) throws
            Exception {
          try {
            Class.forName(driver0);
            return DriverManager.getConnection(url0, user0, pwd0);
          }
          catch (ClassNotFoundException ex) {
            System.out.println(" INVALID DRIVER ");
            throw ex;
          }
          catch (SQLException ex) {
            System.out.println(" INVALID URL ");
            throw ex;
          }
        }
       private void runDDL(Connection conn, String query) throws SQLException, RepException {
          Statement stt = conn.createStatement();
          try {
//            System.out.println(" query =" + query);
            stt.execute(query);
//            System.out.println(" QUERY EXECUTED SUCCESSFULLY ");
          }
          catch (SQLException ex) {
        }
      }

 public static void main(String[] args)throws Exception {
  UpdatePreviousVersion upv=new UpdatePreviousVersion();
}
}
