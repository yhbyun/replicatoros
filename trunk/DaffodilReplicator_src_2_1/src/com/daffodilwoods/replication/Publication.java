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
import java.net.*;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.server.*;
import java.sql.*;
import java.util.*;
import javax.xml.parsers.*;

import org.xml.sax.*;
import org.xml.sax.ContentHandler;
import com.daffodilwoods.replication.DBHandler.*;
import com.daffodilwoods.replication.xml.*;
import com.daffodilwoods.replication.zip.*;
import java.util.ArrayList;
import java.io.FileOutputStream;
import com.daffodilwoods.replication.synchronize.AbstractSynchronize;
import org.apache.log4j.Logger;

/**
 * Publication class holds all the methods which are required for the physical
 * creation of the publication(i.e system tables) and which are used at the time
 * of synchronization at publisher's end. It implements three interfaces,
 *
 * _PubImpl : It makes this class to implement all the remote methods which are
 * called by the subscriber, at the time of subscribing, taking snapshot,
 * synchronizing and for different tasks related to synchronization.
 *
 * _Publication : It makes this class to implement all the methods which are
 * called by the user, at the time of publishing, and for different tasks
 * related to publishing.
 *
 * _Replicator : It makes this class to implement all the methods which are called
 * at the time of synchronization to get the publisher's information.
 *
 */

public class Publication
    extends UnicastRemoteObject implements _PubImpl, _Publication, _Replicator {
  /**
   * name of publication
   */
  private String pubName;

  /**
   * connection of server side
   */
  private ConnectionPool connectionPool;

  /**
   * conflict resolver
   */
  private String conflictResolver;

  /**
   * tables included in publication
   */
  private ArrayList pubRepTables;

  /**
   * server side database handler
   */
  private AbstractDataBaseHandler dbh;

  /**
   * number of tables included in publication
   */
  public int noOfPubTables;

  /**
   * connection of server side
   */
//  public Connection pubConnection;

  /**
   * handler for syncronisation
   */
  SyncXMLCreator syncXMLCreator;
  /**
   * Tables in which updations are required during synchronisation.
   */
  ArrayList usedActualTables;
  // for locking synchronization or snapshot operations
  // for thread safety
  private static boolean isLocked = false;
  private ReplicationServer localServer;
  private static final boolean LOCK = true;
  private static final boolean UNLOCK = false;
  //Kept for multiple subsriber case trying to get lock
  ArrayList listOfWaitingSubcription = new ArrayList();
  HashMap syncIdMap;
  protected static Logger log = Logger.getLogger(Publication.class.getName());
  private boolean isPublicationCyclic;
  String localAddress = null;

  public Publication() throws RemoteException {
  }

  public Publication(ConnectionPool connectionPool0, String pubName0,
                     String serverName0, ReplicationServer localServer0) throws
      RemoteException, RepException {
    pubName = pubName0;
    connectionPool = connectionPool0;
    // make sure whether you need this connection statement or not
//    Connection pubConnection = connectionPool.getConnection(pubName);
    dbh = Utility.getDatabaseHandler(connectionPool, pubName);
    dbh.setLocalServerName(serverName0);
    //String databaseName = pubConnection.getMetaData().getDatabaseProductName();
    syncXMLCreator = new SyncXMLCreator(pubName, connectionPool, dbh);
    localServer = localServer0;
    try{
       localAddress = InetAddress.getLocalHost().getHostAddress();
     }
     catch (UnknownHostException ex) {
//      isLocked = UNLOCK;
       RepConstants.writeERROR_FILE(ex);
       RepException rex = new RepException("REP056", new Object[] {pubName,ex.getMessage()});
       rex.setStackTrace(ex.getStackTrace());
       throw rex;
     }

  }

  /**
   * Sets the conflict resolver for publication
   * @param conflictReolver0 conflict resolver
   * @throws RepException
   */
  public void setConflictResolver(String conflictReolver0) throws RepException {
      if (! (conflictReolver0.equalsIgnoreCase(_Publication.publisher_wins) ||
             conflictReolver0.equalsIgnoreCase(_Publication.subscriber_wins))) {
        throw new RepException("REP016", null);
      }
      conflictResolver = conflictReolver0;
      log.info("conflictResolver " + conflictResolver);
  }

  /**
   * Set the replication tables for the publication
   * @param repTables0 list of tables
   */
  public void setPublicationTables(ArrayList repTables0) {
    pubRepTables = repTables0;
    noOfPubTables = repTables0.size();
  }

  public void setCyclic(boolean isCyclic){
    isPublicationCyclic = isCyclic;
    for (int i = 0; i < pubRepTables.size() ; i++) {
      ((RepTable)pubRepTables.get(i) ).setCyclicDependency(isPublicationCyclic ? "Y" : "N") ;
    }
  }

  public boolean isPublicationCyclic(){
   return isPublicationCyclic;
  }

  public String getConflictResolver() throws RemoteException {
      log.info("Returning conflictResolver " + conflictResolver);
      return conflictResolver;
  }

  /**
   * Set the given filter clause for the given table
   * @param tableName0 for which filter clause is to be added
   * @param filterClause0 filter clause to be set
   * @throws RepException if table does not exist
   * if filter clause is invalid
   */
  public void setFilter(String tableName0, String filterClause0) throws RepException {
    setFilter(tableName0, filterClause0, 0);
  }

  public void setFilter(String tableName0, String filterClause0, int paramCount0) throws RepException {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
      SchemaQualifiedName sname = new SchemaQualifiedName(mdi, tableName0);
      if (tableName0.indexOf('.') == -1) {
        mdi.checkTableExistance(sname);
      }
      log.debug("Table name : " + tableName0);
      log.debug("pubRepTables : " + pubRepTables);
      log.info("Set the filter : " + filterClause0 + " for table " + tableName0);
      Connection pubConnection = connectionPool.getConnection(pubName);
      for (int i = 0, size = pubRepTables.size(); i < size; i++) {
        RepTable repTable = (RepTable) pubRepTables.get(i);
        String sqname = repTable.getSchemaQualifiedName().toString();
        if (sqname.equalsIgnoreCase(sname.toString())) {
          String query = "Select * from " + sqname + " where " + filterClause0;
         try {
          PreparedStatement st = pubConnection.prepareStatement(query);
          if (paramCount0 > 0) {
            for (int j = 1; j <= paramCount0; j++) {
            st.setString(j, null);
            }
          }
          rs = st.executeQuery();
            rs.next();
          }
          catch (SQLException ex1) {
            RepConstants.writeERROR_FILE(ex1);
            RepException rex = new RepException("REP019",new Object[] {filterClause0});
            rex.setStackTrace(ex1.getStackTrace());
            throw rex;
          }
          repTable.setFilterClause(filterClause0);
          log.debug("Filter clause set successfully ");
          return;
        }
      }
      throw new RepException("REP017", new Object[] {tableName0});
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
      try {
        if (rs != null) {
          rs.close();
          if (stmt != null)
            stmt.close();
        }
      }
      catch (SQLException ex) {
        //Ignore the Exception
      }
    }
  }


  /**
   * This method come in use if tables in a publications are refering to
   * each other and It is required to
   * @param tableName0 String
   * @param createShadowTable boolean
   * @throws RepException
   */
  public void setCreateShadowTable(String tableName0, boolean createShadowTable) throws
      RepException {
    MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
    SchemaQualifiedName sname = new SchemaQualifiedName(mdi, tableName0);
    if (tableName0.indexOf('.') == -1) {
      mdi.checkTableExistance(sname);
    }
    RepTable repTable = getRepTable(sname.toString());
    repTable.setCreateShadowTable(createShadowTable ? RepConstants.YES :RepConstants.NO);
  }

  /*public void updateFilter(String tableName0, String filterClause0) throws RepException {
    SchemaQualifiedName sname = new SchemaQualifiedName(tableName0);
    if(tableName0.indexOf('.') == -1) {
      MetaDataInfo mdi = new MetaDataInfo(connectionPool,pubName);
      mdi.checkTableExistance(sname);
    }
    for (int i = 0, size = pubRepTables.size(); i < size ; i++) {
      RepTable repTable = (RepTable) pubRepTables.get(i);
      String sqname = repTable.getSchemaQualifiedName().toString();
      if(sqname.equalsIgnoreCase(sname.toString())) {
        repTable.setFilterClause(filterClause0);
        Connection conn = connectionPool.getConnection(pubName);
        Statement stt = conn.createStatement();
        StringBuffer upd = new StringBuffer();
        upd.append(" update ").append(dbh.getRepTableName())
            .append(" set ").append(RepConstants.repTable_filter_clause3)
            .append(" = '").append(filterClause0)
            .append("' where ").append(RepConstants.repTable_tableName2)
            .append(" = '").append(tableName0).append("'");
//        RepPrinter.print(" Query == " + upd.toString());
        stt.execute(upd.toString());
        stt.close();
        return;
      }
    }
     }*/


  /**
   * This method is responsible to create required system tables on server side
   * and shadow tables and triggers on tables to be published, required for replication
   * and saves data into system tables of the replication corresponding to the publication.
   * @throws RepException
   */
  public void publish() throws RepException {
   Statement stmt =null;
    try {
      //Creates Publication Table
      //Creates BookMark Table
      //Creates Super Log Table
      //Creates Rep Table
      Connection connection = connectionPool.getConnection(pubName);
      stmt =connection.createStatement();
      dbh.createRemoteSystemTables(pubName);
      SchemaQualifiedName[] tableNames = getSchemaQualifiedTableNames();
      MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
      for (int i = 0; i < tableNames.length; i++) {
        RepTable repTable = getRepTable(tableNames[i].toString());
        if (repTable.getCreateShadowTable().equalsIgnoreCase(RepConstants.NO))
          continue;
        String schema = tableNames[i].getSchemaName();
        String table = tableNames[i].getTableName();
        ArrayList colInfoList = mdi.getColumnDataTypeInfo(dbh, schema, table);
        //Searches pubRepTables by the tablename and returns the corresponding object of RepTable and calls function
        String[] primCols = getRepTable(tableNames[i].toString()).getPrimaryColumns();
        //We will get a sequence of  columns and there datatypes along with primary columns and there datatypes.
        //as    " col1 number(10),col2 varchar2(24),old_col1 number(10) "
        String allColSequence = dbh.getShadowTableColumnDataTypeSequence(colInfoList, primCols,repTable);
        //Creates Shadow Tables For all the participating tables
        dbh.createShadowTable(pubName, tableNames[i].toString(), allColSequence,primCols);
        dbh.makeProvisionForLOBDataTypes(colInfoList);
        dbh.createShadowTableTriggers(pubName, tableNames[i].toString(),colInfoList, primCols);
      }
      //Add an entry in Publication table And Rep Table
      savePublicationData(dbh,connection,stmt);
    }
    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP020", new Object[] {ex.getMessage()});
    }
    catch (Throwable ex) {
      log.error(ex, ex);
      throw new RepException("REP020", new Object[] {ex.getMessage()});
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
      try {
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex1) {
      }
    }
  }

  /**
   * Saves the publication name and its tables into server side replication
   * system tables.
   * @param dbh database handler for server side
   * @throws RepException
   */
  private void savePublicationData(AbstractDataBaseHandler dbh,Connection connection,Statement stmt) throws
      RepException {
    try {
      log.info("conflictResolver " + conflictResolver);
      if (conflictResolver == null) {
        log.info("setting conflictResolver " + publisher_wins + " by default");
        conflictResolver = RepConstants.publisher_wins;
        // Insert Into Pulication Table
      }
      StringBuffer insertquery = new StringBuffer();
      insertquery.append("insert into " + dbh.getPublicationTableName() +" values ( '")
          .append(pubName).append("' ,'").append(conflictResolver)
          .append("','").append(dbh.getLocalServerName()).append("' )");
      stmt.execute(insertquery.toString());
      log.info("Query exceuted:" + insertquery.toString());

      // Insert Into Rep Table, for all tables in publication
      for (int i = 0; i < noOfPubTables; i++) {
        RepTable repTable = (RepTable) pubRepTables.get(i);
        repTable.setConflictResolver(conflictResolver);
//      stt.execute(dbh.getRepTableInsertQuery(pubName, repTable));
        dbh.saveRepTableData(connection, pubName, repTable);
        // NEW REP TABLE VIOLATION SACHIN
      }
    }
    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP035", new Object[] {pubName});
    }

  }

  private void appendValue(StringBuffer sb, Object value) {
    if (value == null) {
      sb.append(value);
    }
    else {
      sb.append("'" + value + "'");
    }
  }

  /**
   * This method is responsible to assist to subscribe a subscription. This
   * method creates a XML file to get all the tables involved in the publication
   * and makes an entry in bookmark table of serverside to save the publication
   * name with the subscription.
   * @param clientVendorType0 client database type i.e. oracle or sql server
   * @param subName sub name
   * @param address i p address
   * @param portNo port number
   * @throws RemoteException
   * @throws RepException
   */
  public void createStructure(int clientVendorType0, String subName,String address, boolean isSchemaSupported,_FileUpload fileUpload,String remoteMachineAddress) throws
      RemoteException, RepException {
    FileOutputStream fos = null;
    OutputStreamWriter os = null;
    try {
      if (pubRepTables == null) {
        return;
      }
      //Do entry in the BookMark Table of publication  for each
      //participatig tables in the publication with subscription detail
      //saveSubscriptionData(subName);
      int srcVendorType = Utility.getVendorType(connectionPool, pubName);
      try {
        String filePath = PathHandler.getDefaultFilePathForCreateStructure("struct_" + pubName + "_" +subName);
        fos = new FileOutputStream(filePath);
        os = new OutputStreamWriter(fos);
        os.write("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>");
        os.write("<Database>\r\n");
        TreeMap primCons = new TreeMap(String.CASE_INSENSITIVE_ORDER);
        MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
        AbstractDataBaseHandler dbh = Utility.getDatabaseHandler(connectionPool,pubName);
        ArrayList foreignKeyQueries = new ArrayList();
        for (int i = 0; i < noOfPubTables; i++) {
          RepTable repTable = (RepTable) pubRepTables.get(i);
          SchemaQualifiedName sname = repTable.getSchemaQualifiedName();
          StringBuffer createTableQuery = new StringBuffer();
          String schema = sname.getSchemaName();
          String table = sname.getTableName();
          if(isSchemaSupported){
            createTableQuery.append(" Create Table ").append(schema + "." +table)
                .append(" ( ").append(mdi.generateColumnsQueryForClientNode(dbh,srcVendorType, schema, table, clientVendorType0))
                .append(mdi.getAppliedConstraints(schema, table, primCons)).append(")");
          }else{
            createTableQuery.append(" Create Table ").append(table)
                .append(" ( ").append(mdi.generateColumnsQueryForClientNode(dbh,srcVendorType, schema, table, clientVendorType0))
                .append(mdi.getAppliedConstraints(schema, table, primCons)).append(")");
          }
          if (isSchemaSupported) {
              os.write("<SchemaName>");
              os.write(schema);
              os.write("</SchemaName>");
           }
          os.write("<Table>");
          os.write("<Query>");
          os.write(createTableQuery.toString());
          os.write("</Query>\n");
          os.write("<TableName>");
          if (isSchemaSupported) {
            os.write(sname.toString());
          } else {
             os.write(table);
          }
          os.write("</TableName>\r\n");
          os.write("<PrimaryColumns>\r\n");
          String[] primCols = repTable.getPrimaryColumns();
          for (int j = 0; j < primCols.length; j++) {
            os.write("<ColumnName>");
            os.write(primCols[j]);
            os.write("</ColumnName>");
          }
          os.write("</PrimaryColumns>\r\n");
          os.write("<ForeignKeyColumns>\r\n");
          String[] foreignKeyCols = repTable.getForeignKeyCols();
          if (foreignKeyCols != null) {
            for (int j = 0, size = foreignKeyCols.length; j < size; j++) {
              os.write("<FKColumnName>");
              os.write(foreignKeyCols[j]);
              os.write("</FKColumnName>");
            }
          }
          os.write("</ForeignKeyColumns>\r\n");
          String[] columnstoBeIgnored = repTable.getColumnsToBeIgnored();
          os.write("<IgnoredColumns>\r\n");
          if (columnstoBeIgnored != null) {
//System.out.println(" Columns to beignored" +Arrays.asList(columnstoBeIgnored));
            for (int j = 0, size = columnstoBeIgnored.length; j < size; j++) {
              os.write("<IgnoredColumnName>");
              os.write(columnstoBeIgnored[j]);
              os.write("</IgnoredColumnName>");
            }
          }
          os.write("</IgnoredColumns>\r\n");
          os.write("<FilterClause>");
          //<![CDATA[ rollno > 30 and rollno < 60 ]]>
          if (repTable.getFilterClause() == null) {
            os.write("NO_DATA");
          }
          else {
            os.write("<![CDATA[");
            os.write(repTable.getFilterClause());
            os.write("]]>");
          }
          os.write("</FilterClause>");

          os.write("<CreateShadowTable>");
          os.write(repTable.getCreateShadowTable());
          os.write("</CreateShadowTable>");
          os.write("<CyclicDependency>");
          os.write(repTable.getCyclicDependency());
          os.write("</CyclicDependency>");
          os.write("</Table>\r\n");
        }

        for (int i = 0; i < noOfPubTables; i++) {
          RepTable repTable = (RepTable) pubRepTables.get(i);
          SchemaQualifiedName sname = repTable.getSchemaQualifiedName();
          ArrayList alterTabelStatements = mdi.getForiegnKeyConstraints(sname.getSchemaName(), sname.getTableName());
          if (alterTabelStatements != null && alterTabelStatements.size() > 0) {
            foreignKeyQueries.addAll(alterTabelStatements);
          }
        }
        for (int i = 0, size = foreignKeyQueries.size(); i < size; i++) {
          os.write("<AlterTableForeignKeyStatement>");
          os.write( (String) foreignKeyQueries.get(i));
          os.write("</AlterTableForeignKeyStatement>");
        }

        os.write("</Database>\r\n");
        os.close();
        String localMachineAddress = InetAddress.getLocalHost().getHostAddress();
        if(!localMachineAddress.equalsIgnoreCase(remoteMachineAddress)) {
          String zipFilePath = PathHandler.getDefaultZIPFilePathForCreateStructure("struct_" + pubName + "_" +subName);
          ZipHandler.makeStructZip(zipFilePath, filePath,"struct_" + pubName + "_" + subName);
          WriteOnSocket writeOnSocket = new WriteOnSocket(zipFilePath, filePath,Publication.xmlAndShadow_entries,"struct_" + pubName + "_" + subName, fileUpload, false);
          writeOnSocket.start();
          writeOnSocket.join();
        }
//        writeXMLFileOnClientSocket(address, portNo, zipFilePath);
        log.debug("wrote XMLFile  ClientSocket ");
      }
      catch (IOException ex) {
        RepConstants.writeERROR_FILE(ex);
        RemoteException rem = new RemoteException(ex.getMessage());
        rem.setStackTrace(ex.getStackTrace());
        RepConstants.writeERROR_FILE(rem);
        throw rem;
      }
      catch (InterruptedException ex) {
      RepConstants.writeERROR_FILE(ex);
      RemoteException rem = new RemoteException(ex.getMessage());
      rem.setStackTrace(ex.getStackTrace());
      RepConstants.writeERROR_FILE(rem);
      throw rem;

      }
    }
    finally {
      try {
        os.close();
        fos.close();
      }
      catch (IOException ex1) {
        // Ignore the Exception
      }
    }
  }

  public Object[] getPublisherAddressAndPort() throws
      RemoteException, RepException {
    try {
    Integer portNumber = null;
    ServerSocket serverSocket = connectionPool.startServerSocket();
      portNumber = new Integer(serverSocket.getLocalPort());
      syncXMLCreator.serverSocket = serverSocket;
      return new Object[] {localAddress, portNumber};
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
    }

  }


  /**
   * Inserts a row into bookmark table of server side to put a matching pair of
   * publication name and subscription name.
   * @param subName sub name
   * @throws RepException
   */
  public void saveSubscriptionData(String subName) throws RemoteException {
    Statement stt = null;
    try {
      Connection connection = connectionPool.getConnection(pubName);
      stt = connection.createStatement();
      for (int i = 0; i < noOfPubTables; i++) {
        RepTable repTable = (RepTable) pubRepTables.get(i);
        StringBuffer sb0 = new StringBuffer();
        sb0.append(" Insert into ").append(dbh.getBookMarkTableName()).append(
            " values ( '").append(pubName).append("','").append(subName).append(
            "','").append(repTable.getSchemaQualifiedName()).append("',0,0,'N')");
        stt.execute(sb0.toString());
      }
    }
    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RemoteException(ex.getMessage());
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RemoteException(ex.getMessage());
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
      try {
        if (stt != null)
          stt.close();
      }
      catch (SQLException ex1) {
        // Ignore the exception
      }
    }
  }

  /**
   * creates an XMl file for getting Snapshot on clients side.
   * Do entry in the server side bookmark table.
   * Deletes all records from the server side log table.
   *
   * @param address
   * @param portNo
   * @param subName
   * @throws SQLException
   * @throws RemoteException
   * @throws RepException
   */
  public void createSnapShot(String subName,boolean isSchemaSupported,_FileUpload fileUpload,String remoteMachineAddress) throws SQLException, RemoteException, RepException {
    Statement stmt = null;
    ResultSet rows = null, shadowResult = null;
    try {
      FileOutputStream fos = new FileOutputStream(PathHandler.getDefaultFilePathForClient("snapshot_" + pubName + "_" +subName));
      OutputStreamWriter os = new OutputStreamWriter(fos);
      BufferedWriter bw = new BufferedWriter(os);
      Connection pubConnection = connectionPool.getConnection(pubName);
      stmt =pubConnection.createStatement();
//      long startTime=System.currentTimeMillis();
      XMLWriter xmlWriter = new XMLWriter(bw, dbh, pubConnection);
      bw.write("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>");
      bw.write("<root_snapshot>");
      String tableName;
      SchemaQualifiedName sname;
      String[] primaryKeyColumns;
      stmt = pubConnection.createStatement();
      for (int i = 0; i < noOfPubTables; i++) {
        RepTable repTable = (RepTable) pubRepTables.get(i);
        primaryKeyColumns =repTable.getPrimaryColumns();
        if (repTable.getCreateShadowTable().equalsIgnoreCase(RepConstants.NO))
          continue;
        sname = (repTable).getSchemaQualifiedName();
        tableName = isSchemaSupported ? sname.toString() : sname.getTableName();
        ArrayList encodedCols = PathHandler.getEncodedColumns(tableName);
        bw.write("<tableName>");
        bw.write(tableName);
        String query;
        String[] parameters = null;
        if (repTable.getFilterClause() == null || repTable.getFilterClause().equalsIgnoreCase("")) {
          query = "Select * from " + repTable.getRepTableQualifiedIdentifier();
        }
        else {
          query = "Select * from " + repTable.getRepTableQualifiedIdentifier() + " where " + repTable.getFilterClause();
        }

        /** @todo comment the method getResultSet and create the statement
         *  outside the loop as anywhere as require.*/

        rows = getResultSet(query, parameters); // Query
        ResultSetMetaData rsmt = rows.getMetaData();
        int noOfColumns = rsmt.getColumnCount();
        ResultSet primayKeyResultSet =getPrimaryKeyResultSet(stmt,tableName,repTable.getPrimaryColumns());
        while (rows.next()) {
          bw.write("<operation>");
          bw.write("I");
          bw.write("<row>");
          for (int c = 1; c <= noOfColumns; c++) {
            String columnName = rsmt.getColumnName(c);
            if (!encodedCols.contains(columnName.toUpperCase())) {
              bw.write("<c" + c + ">");
//              bw.write("<" + columnName + ">");
            }
            else {
              bw.write("<\"" + columnName + "\"  Encode=\"y\">");
            }
            // Special Handelling for Blob Clob Case
            xmlWriter.write(rows, c, encodedCols, columnName);
            bw.write("</c" + c + ">\r\n");
//            bw.write("</" + columnName + ">\r\n");
          }
          bw.write("</row>\r\n");
          xmlWriter.writePrimaryKeyElement(primaryKeyColumns,primayKeyResultSet,encodedCols);
          bw.write("</operation>\r\n");
          primayKeyResultSet.next();
          bw.flush();
        }
        // Close the resultset because new instance of resultset has been created for each table.
        rows.close();
        bw.write("</tableName>\r\n");
        // updating values for bookmark table
        String selectMaxSyncID= " select max(" + RepConstants.shadow_sync_id1 + ") from " +
        RepConstants.shadow_Table(tableName);
        log.debug(selectMaxSyncID);
        shadowResult = stmt.executeQuery(selectMaxSyncID);
        shadowResult.next();
        Object maxvalue = shadowResult.getObject(1);
        if (maxvalue == null) {
        maxvalue = new Long(0);
        }
        log.debug("max(" + RepConstants.shadow_sync_id1 + ")=" + maxvalue);
        StringBuffer updateBookmarkQuery = new StringBuffer("update " +dbh.getBookMarkTableName() + " set ");
        updateBookmarkQuery.append(RepConstants.bookmark_lastSyncId4 + " = " +maxvalue + ", ");
        updateBookmarkQuery.append(RepConstants.bookmark_ConisderedId5 + " = " +maxvalue + " ");
        updateBookmarkQuery.append(" where " + RepConstants.bookmark_LocalName1 +" ='" + pubName + "' ");
        updateBookmarkQuery.append(" and  " + RepConstants.bookmark_RemoteName2 +" ='" + subName + "' ");
        updateBookmarkQuery.append(" and  " + RepConstants.bookmark_TableName3 +" ='" + tableName + "'");
        stmt.executeUpdate(updateBookmarkQuery.toString());
        log.debug(updateBookmarkQuery.toString());
      }
      bw.write("</root_snapshot>\r\n");
//      System.out.println("TIME TAKEN IN CREATING XML FOR SNAPSHOT:::"+(System.currentTimeMillis()-startTime));
      if(bw!=null)
      bw.close();
      fos.close();

      localAddress = InetAddress.getLocalHost().getHostAddress();
      if(!localAddress.equalsIgnoreCase(remoteMachineAddress)) {
        // making zip file from xml file
        ZipHandler.makeZip(PathHandler.getDefaultZIPFilePathForClient("snapshot_" +pubName + "_" + subName),
                           PathHandler.getDefaultFilePathForClient("snapshot_" +pubName + "_" + subName),
                           "snapshot_" + pubName + "_" + subName /*+ ".xml"*/);
        // writing zip file on socket
        WriteOnSocket writeOnSocket = new WriteOnSocket(PathHandler.getDefaultZIPFilePathForClient("snapshot_" + pubName + "_" +subName),
            PathHandler.getDefaultFilePathForClient("snapshot_" + pubName + "_" + subName),_Publication.xmlAndShadow_entries,
               "snapshot_" + pubName + "_" + subName, fileUpload, true);
        writeOnSocket.start();
        writeOnSocket.join();
      }
       dbh.deleteRecordsFromSuperLogTable(stmt);
    }

    catch (FileNotFoundException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP085", null);
    } //Temporary Snapshot File Can not created.
    catch (IOException ex) {
      RepConstants.writeERROR_FILE(ex);
      RepException rex = new RepException("REP086", new Object[] {ex.getMessage()});
      rex.setStackTrace(ex.getStackTrace());
      throw rex;
    }
    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      RepException rex = new RepException("REP055", new Object[] {pubName,ex.getMessage()});
      rex.setStackTrace(ex.getStackTrace());
      throw rex;
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    catch (InterruptedException ex) {
     RepConstants.writeERROR_FILE(ex);
     RepException rex = new RepException("REP055", new Object[] {pubName,ex.getMessage()});
     rex.setStackTrace(ex.getStackTrace());
     throw rex;
      }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
      /** @todo  Close the statement */
      if (shadowResult != null)
        shadowResult.close();
      if (stmt != null)
        stmt.close();
        isLocked = UNLOCK;
      }

  }

  /**
   * Creates an XML file for synchronization purpose and send it to subscriber's
   * socket.
   * Creates a server socket and send's it's information to the subscriber,
   * so that subscriber can transfer file over it.
   *
   * @param address
   * @param portNo
   * @param subName
   * @param clientServerName
   * @return
   * @throws RemoteException
   * @throws RepException
   */

  public Object[] createXMLForClient(String subName, String clientServerName,boolean isSchemaSupported, _FileUpload fileUpload,String remoteMachineAddress) throws
      RemoteException, RepException {
//      checkForLock();
      Object[] usedActualTablesLastSyncId;
      Object[] LastSyncId = null;
      String localAddress = null;
      Integer portNumber = null;
      try {
        localAddress =InetAddress.getLocalHost().getHostAddress();
        if (noOfPubTables > 0) {
          usedActualTablesLastSyncId = syncXMLCreator.createXMLFile(PathHandler.
              getDefaultFilePathForClient("server_" + pubName + "_" +subName) // xml file path
              , PathHandler.getDefaultZIPFilePathForClient("server_" + pubName + "_" +subName) // zip file path
              , "server_" + pubName + "_" +subName//+ ".xml" // xml file name
              , subName, pubRepTables, clientServerName,
              noOfPubTables,_Publication.xmlAndShadow_entries, pubName,isSchemaSupported,fileUpload,localAddress,remoteMachineAddress);

          usedActualTables = (ArrayList) usedActualTablesLastSyncId[0];
          LastSyncId = (Object[]) usedActualTablesLastSyncId[1];
        }

        ServerSocket serverSocket = connectionPool.startServerSocket();
        localAddress = null;
        try {
          localAddress = InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException ex) {
//        isLocked = UNLOCK;
          RepConstants.writeERROR_FILE(ex);
          RepException rex = new RepException("REP056", new Object[] {pubName,ex.getMessage()});
          rex.setStackTrace(ex.getStackTrace());
          throw rex;
        }
        portNumber = new Integer(serverSocket.getLocalPort());
        syncXMLCreator.serverSocket = serverSocket;
      }
      catch (RepException ex1) {
         ex1.printStackTrace();
//      isLocked = UNLOCK;
        RepConstants.writeERROR_FILE(ex1);
        throw ex1;
      }
      catch (Exception ex1) {
         ex1.printStackTrace();
//      isLocked = UNLOCK;
        RepConstants.writeERROR_FILE(ex1);
        throw new RepException("REP054", new Object[] {subName, ex1.getMessage()});
      }
      catch (Throwable ex1) {
        ex1.printStackTrace();
      //      isLocked = UNLOCK;
        log.error(ex1.getMessage(), ex1);
        throw new RepException("REP054", new Object[] {subName, ex1.getMessage()});
      }finally {
      connectionPool.removeSubPubFromMap(pubName);
    }
      return new Object[] {localAddress, portNumber, LastSyncId};
  }




  private ResultSet getResultSet(String preparedQuery, String[] parameters) throws
      SQLException,RepException {
    try
    {
      Connection pubConnection = connectionPool.getConnection(pubName);
      PreparedStatement pst = pubConnection.prepareStatement(preparedQuery);
      if (parameters != null) {
        for (int i = 0, size = parameters.length; i < size; i++) {
          pst.setString(i + 1, parameters[i]);
        }
      }
      return pst.executeQuery();
    } finally {
       connectionPool.removeSubPubFromMap(pubName);
    }
  }

  /**
   * merges the data between the client side and server side.
   * @param subName
   * @param remoteServerName
   * @throws RemoteException
   * @throws RepException
   */
  public synchronized void synchronize(String subName, String remoteServerName,boolean isCreateTransactionLogFile,String remoteMachineAddress) throws
      RemoteException, RepException {
    BufferedWriter bw = null;
    Statement stmt = null;
    ResultSet rs = null;
    boolean isCurrentTableCyclic =false;
    String localMachineAddress=null;
    try {
      Utility.createTransactionLogFile = isCreateTransactionLogFile;
      localMachineAddress =InetAddress.getLocalHost().getHostAddress();

//      String transactionLogURL = PathHandler.getDefaultTransactionLogFilePathForPublisher(pubName);
//      FileOutputStream fos1 = new FileOutputStream(transactionLogURL, true);
//      OutputStreamWriter os = new OutputStreamWriter(fos1);
//      bw = new BufferedWriter(os);

      // unzipping zip file from client
      if(!localMachineAddress.equalsIgnoreCase(remoteMachineAddress)) {
        ZipHandler.unZip(PathHandler.getDefaultZIPFilePathForClient("client_" +subName + "_" + pubName),PathHandler.getDefaultFilePathForClient("client_" +subName + "_" + pubName));
      }
      SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
      XMLReader reader = saxParser.getXMLReader();
      MergeHandler mg = new MergeHandler(true,connectionPool.getConnection(subName), this,
                                         remoteServerName, dbh, bw,
                                         "MERGE REPLICATION",
                                         PathHandler.
                                         fullOrPartialTransactionLogFile(),Utility.getDatabaseMataData(connectionPool, pubName));
      mg.setLocalName(pubName);
      mg.setRemoteName(subName);
      ContentHandler ch = mg;
      reader.setContentHandler(ch);
//      AbstractSynchronize.writeDateInTransactionLogFile(bw);

      //initializing hashmap for maxSyncId for updating consideredId of bookMarkTable further
      syncIdMap = new HashMap();
      Connection pubConnection = connectionPool.getConnection(pubName);
      RepTable repTable=null;
      for (int i = 0; i < pubRepTables.size(); i++) {
        repTable =(RepTable) pubRepTables.get(i);
        String tableName = repTable.getSchemaQualifiedName().toString();
        isCurrentTableCyclic = repTable.getCyclicDependency().equalsIgnoreCase(RepConstants.YES);
        StringBuffer query = new StringBuffer();
        stmt = pubConnection.createStatement();
        query.append(" select max(").append(RepConstants.shadow_sync_id1).
            append(") from ").append(RepConstants.shadow_Table(tableName));
        rs = stmt.executeQuery(query.toString());
        rs.next();
        syncIdMap.put(tableName, new Long(rs.getLong(1)));
        long maxSynId = rs.getLong(1);
        syncIdMap.put(tableName, new Long(maxSynId));
      }
      reader.parse(PathHandler.getDefaultFilePathForClient("client_" + subName + "_" +pubName));
//      AbstractSynchronize.writeOperationInTransactionLogFile(bw, mg.insert,mg.update, mg.delete, "MERGE");
      makePublicationTransactionLogFile(pubName,mg,bw);
      mg.closeAllStatementAndResultset();

      /**
       * Following code has been written to handle the case of cyclic  table and update the value of all the columns that are set to null in first pass.
       */
     if(isCurrentTableCyclic){

     MergeHandler mg1 = new MergeHandler(false,connectionPool.getConnection(subName), this,
                                        remoteServerName, dbh, bw,
                                        "MERGE REPLICATION",
                                        PathHandler.
                                        fullOrPartialTransactionLogFile(),Utility.getDatabaseMataData(connectionPool, pubName));
     mg1.setLocalName(pubName);
     mg1.setRemoteName(subName);
     ContentHandler ch1 = mg1;
     reader.setContentHandler(ch1);
     reader.parse(PathHandler.getDefaultFilePathForClient("client_" + subName + "_" +pubName));
     makePublicationTransactionLogFile(pubName,mg,bw);
     mg1.closeAllStatementAndResultset();
   }

      if (_Publication.xmlAndShadow_entries) {
        // deleting XML file
        deleteFile(PathHandler.getDefaultFilePathForClient("client_" + subName + "_" +pubName));
        // deleting zip file
        deleteFile(PathHandler.getDefaultZIPFilePathForClient("client_" +subName + "_" +pubName));
      }
      deleteRecordsFromSuperLogTable(stmt);
      for (int i = 0; i < pubRepTables.size(); i++) {
        repTable = (RepTable) pubRepTables.get(i);
        if (repTable.getCreateShadowTable().equalsIgnoreCase(RepConstants.NO))
          continue;
        String tableName = ( (RepTable) pubRepTables.get(i)).getSchemaQualifiedName().toString();
        // we have updated last sync id at the time of creating xml file too beacause of any power failure or other reasons
        // we are also updating Coincederd Id as well as Last Sync Id so that we do not
        // get last record while making xml file previously from shadow table in next cycle of synchronisation
        // as client changes can also occur at server whihc are not to be sent back to client.
        UpdateConisdered_LastsyncIdForBookMarksTable(pubName, subName,tableName, ((Number) syncIdMap.get(tableName)).toString(),stmt);
      }

      syncIdMap.clear();
      if (_Publication.xmlAndShadow_entries) {
        deleteRecordsFromShadowTable(stmt);
      }

    }
    catch (Exception ex) {
//      isLocked = UNLOCK;
      if(Utility.createTransactionLogFile)  {
        AbstractSynchronize.writeUnsuccessfullOperationInTransaction(bw);
      }
//      RepPrinter.print(" EXCEPTION IN SAX-PARSER ");
      RepConstants.writeERROR_FILE(ex);
      RepException rex = null;
      if (ex instanceof SAXException) {
        Exception e = ( (SAXException) ex).getException();
        if (e instanceof RepException) {
          throw (RepException) e;
        }
        else {
          rex = new RepException("REP056", new Object[] {pubName, e.getMessage()});
          rex.setStackTrace(e.getStackTrace());
          throw rex;
        }
      }
      else {
        rex = new RepException("REP056", new Object[] {pubName, ex.getMessage()});
        rex.setStackTrace(ex.getStackTrace());
      }
      throw rex;
    }
    finally {
       connectionPool.removeSubPubFromMap(pubName);
      try {
        if (rs != null) {
          rs.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex1) {
      }
//      System.out.println("in finally of publisher for realeasing lock");
//      isLocked = UNLOCK;
      try {
        if(bw!=null)
        bw.close();
      }
      catch (IOException ex2) {
      }
    }
  }

  /**
   * It handle the push replication operation in publication data source.
   * @param subName String
   * @param remoteServerName String
   * @throws RemoteException
   * @throws RepException
   */

  public synchronized void push(String subName, String remoteServerName,boolean isCreateTransactionLogFile,String remoteMachineAddress) throws
      RemoteException, RepException {
    BufferedWriter bw = null;
    Statement stmt = null;
    ResultSet rs = null;
    Connection pubConnection =null;
     boolean isCurrentTableCyclic =false;
     Utility.createTransactionLogFile = isCreateTransactionLogFile;
     String localMachineAddress =null;
    try {

//      String transactionLogURL = PathHandler.getDefaultTransactionLogFilePathForPublisher(pubName);
//      FileOutputStream fos1 = new FileOutputStream(transactionLogURL, true);
//      OutputStreamWriter os = new OutputStreamWriter(fos1);
//      bw = new BufferedWriter(os);

      // unzipping zip file from client
      localMachineAddress = InetAddress.getLocalHost().getHostAddress();
      if(!localMachineAddress.equalsIgnoreCase(remoteMachineAddress)) {
        ZipHandler.unZip(PathHandler.getDefaultZIPFilePathForClient("client_" +subName + "_" + pubName),PathHandler.getDefaultFilePathForClient("client_" +subName + "_" + pubName));
      }
      SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
      XMLReader reader = saxParser.getXMLReader();
      MergeHandler mg = new MergeHandler(true,connectionPool.getConnection(subName), this,
                                         remoteServerName, dbh, bw,
                                         "PUSH REPLICATION",PathHandler.fullOrPartialTransactionLogFile(),
                                         Utility.getDatabaseMataData(connectionPool, pubName));
      mg.setLocalName(pubName);
      mg.setRemoteName(subName);
      ContentHandler ch = mg;
      reader.setContentHandler(ch);
//     AbstractSynchronize.writeDateInTransactionLogFile(bw);
//     initializing hashmap for maxSyncId for updating consideredId of bookMarkTable further
      syncIdMap = new HashMap();


      syncIdMap = new HashMap();
      pubConnection = connectionPool.getConnection(pubName);
      for (int i = 0; i < pubRepTables.size(); i++) {
        RepTable repTable  =( (RepTable) pubRepTables.get(i));
       String tableName = repTable.getSchemaQualifiedName().toString();

         isCurrentTableCyclic = repTable.getCyclicDependency().equalsIgnoreCase(RepConstants.YES);
        StringBuffer query = new StringBuffer();

        stmt = pubConnection.createStatement();
        query.append(" select max(").append(RepConstants.shadow_sync_id1).append(") from ")
        .append(RepConstants.shadow_Table(tableName));
        rs = stmt.executeQuery(query.toString());
        rs.next();
        syncIdMap.put(tableName,new Long(rs.getLong(1)));
        long maxSynId =rs.getLong(1);
        syncIdMap.put(tableName,new Long(maxSynId));
      }
      reader.parse(PathHandler.getDefaultFilePathForClient("client_" + subName + "_" +pubName));
      mg.closeAllStatementAndResultset();

//     AbstractSynchronize.writeOperationInTransactionLogFile(bw, mg.insert,mg.update, mg.delete, "MERGE");
      makePublicationTransactionLogFile(pubName,mg,bw);
       if(isCurrentTableCyclic){
          MergeHandler mg1 = new MergeHandler(false,connectionPool.getConnection(subName), this,
                                         remoteServerName, dbh, bw,
                                         "PUSH REPLICATION",
                                         PathHandler.fullOrPartialTransactionLogFile(),
                                         Utility.getDatabaseMataData(connectionPool, pubName));
          mg1.setLocalName(subName);
          mg1.setRemoteName(pubName);
          ContentHandler ch1 = mg1;
          reader.setContentHandler(ch1);
          AbstractSynchronize.writeDateInTransactionLogFile(bw);
          reader.parse(PathHandler.getDefaultFilePathForClient("client_" +subName));
          makePublicationTransactionLogFile(pubName,mg,bw);
          mg1.closeAllStatementAndResultset();
        }
      if (_Publication.xmlAndShadow_entries) {
        // deleting XML file
        deleteFile(PathHandler.getDefaultFilePathForClient("client_" + subName + "_" +pubName));
        // deleting zip file
        deleteFile(PathHandler.getDefaultZIPFilePathForClient("client_" +subName + "_" +pubName));
      }

      deleteRecordsFromSuperLogTable(stmt);
      for (int i = 0; i < pubRepTables.size(); i++) {
        String tableName = ( (RepTable) pubRepTables.get(i)).getSchemaQualifiedName().toString();
        // we have updated last sync id at the time of creating xml file too beacause of any power failure or other reasons
        // we are also updating Coinsiderd Id as well as Last Sync Id so that we do not
        // get last record while making xml file previously from shadow table in next cycle of synchronisation
        // as client changes can also occur at server whihc are not to be sent back to client.
        UpdateConisdered_LastsyncIdForBookMarksTable(pubName, subName,tableName, ((Number) syncIdMap.get(tableName)).toString(),stmt);
      }
      syncIdMap.clear();
      if (_Publication.xmlAndShadow_entries) {
        deleteRecordsFromShadowTable(stmt);
      }
//      isLocked = UNLOCK;
    }
    catch (Exception ex) {
//      isLocked = UNLOCK;
      if(Utility.createTransactionLogFile)
      AbstractSynchronize.writeUnsuccessfullOperationInTransaction(bw);
      RepConstants.writeERROR_FILE(ex);
      RepException rex = null;
      if (ex instanceof SAXException) {
        Exception e = ( (SAXException) ex).getException();
        if (e instanceof RepException) {
          throw (RepException) e;
        }
        else {
          rex = new RepException("REP056", new Object[] {pubName, e.getMessage()});
          rex.setStackTrace(e.getStackTrace());
          throw rex;
        }
      }
      else {
        rex = new RepException("REP056", new Object[] {pubName, ex.getMessage()});
        rex.setStackTrace(ex.getStackTrace());
      }
      throw rex;
    }
    finally {
       //      isLocked = UNLOCK;
       connectionPool.removeSubPubFromMap(pubName);
      try {
        if(bw!=null)
        bw.close();
        if (rs != null) {
          rs.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex1) {
      }
      catch (IOException ex2) {
      }
    }
  }

  private ArrayList getUsedActualtable() {
    ArrayList includedTables = new ArrayList();
    for (int i = 0; i < noOfPubTables; i++) {
      SchemaQualifiedName sname = ( (RepTable) pubRepTables.get(i)).getSchemaQualifiedName();
      //commented on 2_may_2005 as there was problem in daffodil to
//      daffodil push()of schemaqualified name
//      includedTables.add(sname.getTableName());
       includedTables.add(sname.toString());
    }
    return includedTables;
  }

  /**
   * writes the zip file on client socket
   * @param address
   * @param portNo
   * @param xmlFilePath
   * @throws IOException
   */
  private void writeXMLFileOnClientSocket(String address, int portNo,String xmlFilePath) throws IOException {
    log.debug("write XMLFile  ClientSocket on address " + address + ",portNo " + portNo + " xmlFilePath " + xmlFilePath);
    Socket s = new Socket(address, portNo);
    FileInputStream fis = new FileInputStream(xmlFilePath);
    s.setSendBufferSize(Integer.MAX_VALUE);
    OutputStream sos = s.getOutputStream();
    BufferedOutputStream bos = new BufferedOutputStream(sos);
    byte[] buf = new byte[1024];
    int len = 0;
    while ( (len = fis.read(buf)) > 0) {
      bos.write(buf, 0, len);
    }
    bos.flush();
    bos.close();
    sos.close();
    fis.close();
    s.close();
  }

  private SchemaQualifiedName[] getSchemaQualifiedTableNames() {
    SchemaQualifiedName[] tableNames = new SchemaQualifiedName[noOfPubTables];
    for (int i = 0; i < noOfPubTables; i++) {
      RepTable repTable = (RepTable) pubRepTables.get(i);
      tableNames[i] = repTable.getSchemaQualifiedName();
    }
    return tableNames;
  }

  public String getServerName() throws RemoteException {
      return dbh.getLocalServerName();
  }

  public String getFilterClause(SchemaQualifiedName tableName) throws RemoteException {
      for (int i = 0; i < noOfPubTables; i++) {
        RepTable repTable = (RepTable) pubRepTables.get(i);
        if (repTable.getSchemaQualifiedName().equals(tableName)) {
          return repTable.getFilterClause();
        }
      }
      throw new RemoteException("Invalid table name " + tableName);
    }

  public AbstractDataBaseHandler getDBDataTypeHandler() {
    return dbh;
  }

  public String getPub_SubName() {
    return pubName;
  }

  public RepTable getRepTable(String tableName) throws RepException {
      log.debug("Number of tables " + pubRepTables.size());
      SchemaQualifiedName sname;
      String tabNames;
      for (int i = 0; i < pubRepTables.size(); i++) {
      log.debug(" Qualified Names  " +( (RepTable) pubRepTables.get(i)).getSchemaQualifiedName());
      sname =( (RepTable) pubRepTables.get(i)).getSchemaQualifiedName();
      tabNames = tableName.indexOf(".") !=-1 ? sname.toString() : sname.getTableName();
      if (tabNames.equalsIgnoreCase(tableName)) {
        return (RepTable) pubRepTables.get(i);
      }
    }
    RepException rep = new RepException("REP033", new Object[] {tableName});
    RepConstants.writeERROR_FILE(rep);
    throw rep;
  }

  private void deleteFile(String fileName) {
    File f = new File(fileName);
    boolean deleted = f.delete();
  }

  private void deleteRecordsFromSuperLogTable(Statement stmt) throws
      SQLException {
    // insert one record in superLogTable
    ResultSet rs=null;
    try {
      StringBuffer query = new StringBuffer();
      query.append("insert into ").append(dbh.getLogTableName()).append(" (").
          append(RepConstants.logTable_tableName2)
          .append(") values  ('$$$$$$')");
      stmt.execute(query.toString());

      query = new StringBuffer();
      query.append("Select max(").append(RepConstants.logTable_commonId1).
          append(") from ").append(dbh.getLogTableName());
      rs= stmt.executeQuery(query.toString());
      rs.next();
      long maxCID = rs.getLong(1);

      query = new StringBuffer();
      query.append("delete from ").append(dbh.getLogTableName()).append(" where ")
          .append(RepConstants.logTable_commonId1).append(" !=").append(maxCID);
      stmt.executeUpdate(query.toString());
      log.debug(query.toString());
    }
    finally {
      if(rs!=null)
        rs.close();
      }
  }

  /**
   * deletes the unwanted reocrds from  shadow table.
   * @throws SQLException
   */
  private void deleteRecordsFromShadowTable(Statement stmt) throws SQLException, RepException {
     if (usedActualTables == null) {
       usedActualTables = getUsedActualtable();
     }
     int noofTables = usedActualTables.size();
     if (noofTables > 0) {
       StringBuffer query;
       for (int i = 0; i < noofTables; i++) {
//         // selecting min of syncid or concideredId  from bookmarks table for one table
         Object minValue= dbh.getMinValOfSyncIdTodeleteRecordsFromShadowTable((String)
             usedActualTables.get(i),stmt);
         if (minValue instanceof Number) {
           minValue = new Long(((Number)minValue).longValue());
         } else {
           minValue = new Long(Long.parseLong((String)minValue));
         }
         // deleting records from shadow table for that table
         query = new StringBuffer();
         query.append("delete from ").append(RepConstants.shadow_Table( (String)
             usedActualTables.get(i)))
             .append(" where  ").append(RepConstants.shadow_sync_id1).append(
             " < ").append(minValue);
//               System.out.println("Delete Query==::"+query.toString());
         stmt.executeUpdate(query.toString());
         log.debug(query.toString());
       }
     }
  }


  /**
   * updates the bookmarks table with the considered Id for all tables.
   * @param pubName
   * @param subName
   * @param tableName
   * @throws SQLException
   */

  private void UpdateConisdered_LastsyncIdForBookMarksTable(String pubName,
      String subName, String tableName, Object syncId,Statement stmt) throws SQLException,
      RepException {
      StringBuffer query = new StringBuffer();
      Connection pubConnection = connectionPool.getConnection(pubName);
      stmt = pubConnection.createStatement();

      /* query.append(" select max(").append(RepConstants.shadow_sync_id1).append(") from ")
            .append(RepConstants.shadow_Table(tableName));
       ResultSet rs = stmt.executeQuery(query.toString());
        rs.next();
        long maxSyncId = rs.getLong(1);
       query = new StringBuffer();*/

      Object maxSyncId = syncId;

      log.debug("maxSyncId" + maxSyncId);

      // appened updation of LastSyncId in Bookmarks Table
      query.append(" UPDATE  ").append(dbh.getBookMarkTableName()).append(
          " set  ").append(RepConstants.bookmark_ConisderedId5)
          .append(" = ").append(maxSyncId).append("  where ").append(
          RepConstants.bookmark_LocalName1).append(
          " = '").append(pubName).append("' and ").append(RepConstants.
          bookmark_RemoteName2)
          .append(" = '").append(subName).append("' and ")
          .append(RepConstants.bookmark_TableName3).append(" = '").append(
          tableName).append("'");
      stmt.executeUpdate(query.toString());
      log.debug(query.toString());
  }

  private void UpdateConisdered_LastsyncIdForBookMarksTable_old(String pubName,
      String subName, String tableName) throws SQLException, RepException {
    Statement stmt = null;
    try {
      StringBuffer query = new StringBuffer();
      Connection pubConnection = connectionPool.getConnection(pubName);
      stmt = pubConnection.createStatement();
      query.append(" select max(").append(RepConstants.shadow_sync_id1).append(") from ")
          .append(RepConstants.shadow_Table(tableName));
      ResultSet rs = stmt.executeQuery(query.toString());
      rs.next();
      long maxSyncId = rs.getLong(1);

      query = new StringBuffer();
      // appened updation of LastSyncId in Bookmarks Table
      query.append(" UPDATE  ").append(dbh.getBookMarkTableName()).append(
          " set  ").append(RepConstants.bookmark_ConisderedId5)
          .append(" = ").append(maxSyncId).append(" , ").append(RepConstants.
          bookmark_lastSyncId4).append(" = ").append(maxSyncId)
          .append("  where ").append(RepConstants.bookmark_LocalName1).append(
          " = '").append(pubName).append("' and ").append(RepConstants.
          bookmark_RemoteName2)
          .append(" = '").append(subName).append("' and ")
          .append(RepConstants.bookmark_TableName3).append(" = '").append(
          tableName).append("'");
      stmt.executeUpdate(query.toString());
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
      try {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ex) {
      }
    }
  }

  public void unpublish() throws RepException {
    Statement stmt = null;
    try {
      Connection con = connectionPool.getConnection(pubName);
      stmt = con.createStatement();
      checkDependingSubscriptions(stmt);
      try {
        String query = " delete from " + dbh.getPublicationTableName() +
                    " where " + RepConstants.publication_pubName1 + " = '" +pubName + "'";
        stmt.execute(query);
        log.debug("Unpublish query :"+query);
      }
      catch (SQLException ex) {
        RepConstants.writeERROR_FILE(ex);
        throw new RepException("REP036", new Object[] {pubName});
      }
      deleteNonSharedPublishedSubscribedTables(con, pubName,stmt);
      localServer.refershPublication(pubName);
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      RepConstants.writeERROR_FILE(ex);
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
      try {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ex1) {
      }
    }
  }

  private void deleteNonSharedPublishedSubscribedTables(Connection con, String pubsubName,Statement stmt) throws SQLException, RepException {

    //" Select Table_Name from RepTable Where PubSub_Name = '"+pubName+"' and Table_Name Not In "
    //"( Select Distinct Table_Name from RepTable Where PubSub_Name <> '"+pubName+"') ";

    ResultSet rs = null;
    ResultSet rsSub=null;
    try {
      StringBuffer tablesToDelete = new StringBuffer();
      tablesToDelete.append(" Select ").append(RepConstants.repTable_tableName2)
          .append(" from ").append(dbh.getRepTableName()).append(" where ")
          .append(RepConstants.repTable_pubsubName1).append(" = '")
          .append(pubsubName).append("' and ").append(RepConstants.
          repTable_tableName2)
          .append(" not in ( Select Distinct ").append(RepConstants.
          repTable_tableName2).append(" from ").append(dbh.getRepTableName())
          .append(" Where ").append(RepConstants.repTable_pubsubName1)
          .append(" <> '").append(pubsubName).append("') ");
  //    RepPrinter.print(" Query Is : == " + tablesToDelete.toString());
      rs = stmt.executeQuery(tablesToDelete.toString());
      while (rs.next()) {
        MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubsubName);
        SchemaQualifiedName sname = new SchemaQualifiedName(mdi,
            rs.getString(RepConstants.repTable_tableName2));
        deleteAll(con, pubsubName, sname.toString());
      }

      String deleteQuery = " delete from " + dbh.getRepTableName() + " where " +RepConstants.repTable_pubsubName1 + " = '" + pubsubName + "'";
      stmt.execute(deleteQuery);
      log.debug(deleteQuery);
      deleteQuery =" delete from " + dbh.getBookMarkTableName() + " where " +RepConstants.bookmark_LocalName1 + " = '" + pubsubName + "'";
      stmt.execute(deleteQuery);
      log.debug(deleteQuery);
      rs = stmt.executeQuery("select * from " + dbh.getPublicationTableName());
      boolean ispubTableExist = rs != null ? rs.next() ? true : false : false;
      try {
        rsSub = stmt.executeQuery("select * from " +dbh.getSubscriptionTableName());
        String query = " delete from " + dbh.getRepTableName() + " where " +RepConstants.repTable_pubsubName1 + " = '" + pubsubName + "'";
        stmt.execute(query);
        log.debug(query);
        query = " delete from " + dbh.getBookMarkTableName() + " where " +RepConstants.bookmark_LocalName1 + " = '" + pubsubName + "'";
        stmt.execute(query);
        log.debug(query);
        rs = stmt.executeQuery("select * from " + dbh.getPublicationTableName());
        if (!rs.next()) {
          dbh.dropPublisherSystemTables(con);
        }
      }
      catch (SQLException ex) {
        /**
        *  Ignore the exception becuase Sublication table
        *  does not exist in Publisher Database. It is the
        *  case when table is published and subscribed in
        *  same database.
        */
      }
       boolean issubTableExist = rsSub != null ? rsSub.next() ? true : false : false;
       if(!ispubTableExist&& !issubTableExist)
       dbh.dropPublisherSystemTables(con);
    }
    finally {
      if (rs != null)
        rs.close();
      if (rsSub != null)
        rsSub.close();
    }

  }

  private void deleteAll(Connection connection, String pubsubName, String table) throws SQLException, RepException {
    dbh.dropTriggersAndShadowTable(connection, table, pubsubName);
  }



  public void dropSubscription(String subName) throws RemoteException, RepException {
    Statement stmt = null;
    try {
      Connection con = connectionPool.getConnection(pubName);
      stmt = con.createStatement();
      String query = "Delete from " + dbh.getBookMarkTableName() +
          " where " + RepConstants.bookmark_RemoteName2 + " = '" + subName +"'";
      stmt.execute(query);
      log.debug(query);
    }
    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP034", new Object[] {ex.getMessage()});
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
      try {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ex1) {
      }
    }
  }


  synchronized public void checkForLock(String pubSubName) throws RepException, RemoteException {
   if(  listOfWaitingSubcription.size() > 0){
    if(listOfWaitingSubcription.contains(pubSubName) ){
       if(isLocked) {
         throw new RepException("REP052", null);
       }
       log.debug("locking for subscription::"+pubSubName);
      listOfWaitingSubcription.remove(pubSubName);
      isLocked = LOCK;
      return;
    }
      listOfWaitingSubcription.add(pubSubName);
      throw new RepException("REP052", null);
   }
    if (isLocked) {
      listOfWaitingSubcription.add(pubSubName);
     throw new RepException("REP052", null);
    }
    log.debug("locking for subscription::"+pubSubName);
     isLocked = LOCK;
  }

  private void checkDependingSubscriptions(Statement stmt) throws RepException {
    StringBuffer sb = new StringBuffer();
    sb.append(" Select * From ").append(dbh.getBookMarkTableName())
        .append(" where ").append(RepConstants.bookmark_LocalName1)
        .append(" = '").append(pubName).append("'");
    ResultSet rs = null;
    try {
      rs = stmt.executeQuery(sb.toString());
      if (rs.next()) {
        do {
     log.debug("Subscriber " + rs.getString(2) + " exists");
      }  while (rs.next());
        throw new RepException("REP047", new Object[] {pubName});
      }
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP045", new Object[] {pubName, ex.getMessage()});
    }
    finally {
      try {
        if(rs!=null)
          rs.close();
      }
      catch (SQLException ex1) {
      }
    }
  }

  public  void releaseLOCK() throws RemoteException {
    isLocked = UNLOCK;
    log.debug("isLocked :" + isLocked);
  }

  public int getPubVendorName() throws RepException {
      int pubVendor = Utility.getVendorType(connectionPool, pubName);
      return pubVendor;
  }


  public void updateBookMarkLastSyncId(String remote_Pub_Sub_Name, Object[] lastId) throws RemoteException,
      SQLException, RepException  {
    Statement stmt = null;
    try {
      Connection pubConnection = connectionPool.getConnection(pubName);
      stmt = pubConnection.createStatement();
//    Object lastId = getLastUIDFromShadowTable(shadowTable);
      for (int i = 0; i < pubRepTables.size(); i++) {
        String tableName = ( (RepTable) pubRepTables.get(i)).getSchemaQualifiedName().toString();
        String updateQuery = "update " + dbh.getBookMarkTableName() + " set " +
            RepConstants.bookmark_lastSyncId4 + "=" + lastId[i] + " where  " +
            RepConstants.bookmark_LocalName1 + " = '" + pubName + "' and " +
            RepConstants.bookmark_RemoteName2 + " = '" + remote_Pub_Sub_Name +"' and " +
            RepConstants.bookmark_TableName3 + " = '" + tableName + "' ";
        stmt.executeUpdate(updateQuery);
      }
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
      if (stmt != null)
        stmt.close();
    }
  }


  public void addTableToPublication(String[] newTableList,String[] filterClauses) throws
      RepException {
    Connection pubConnection=null;
    Statement stmt=null;
     boolean islockedTaken=false;
    try {
      checkForLock(pubName);
      islockedTaken=true;
       pubConnection = connectionPool.getConnection(pubName);
       stmt=pubConnection.createStatement();
      checkTableNameIfNull(newTableList);
      //check if user is passing null instead of string array else check filter clause syntax
      if (filterClauses == null) {
        filterClauses = new String[newTableList.length];
        for (int i = 0; i < filterClauses.length; i++)
          filterClauses[i] = null;
      }
      else  {
        checkFilter(stmt,newTableList, filterClauses);
      }
      localServer.addTableToPublication(pubName, newTableList,filterClauses,pubRepTables, this);
      SchemaQualifiedName[] tableNames = getSchemaQualifiedTableNames();
      MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
      for (int i = 0; i < tableNames.length; i++) {
       String schema = tableNames[i].getSchemaName();
       String table = tableNames[i].getTableName();
       ArrayList colInfoList = mdi.getColumnDataTypeInfo(dbh, schema, table);
       //Searches pubRepTables by the tablename and returns the corresponding object of RepTable and calls function
       RepTable repTable=getRepTable(tableNames[i].toString());
       String[] primCols =repTable.getPrimaryColumns();
       //We will get a sequence of  columns and there datatypes along with primary columns and there datatypes.
       //as    " col1 number(10),col2 varchar2(24),old_col1 number(10) "
       String allColSequence = dbh.getShadowTableColumnDataTypeSequence(colInfoList, primCols,repTable);
       //Creates Shadow Tables For all the participating tables
       dbh.createShadowTable(pubName, tableNames[i].toString(), allColSequence,primCols);
       dbh.makeProvisionForLOBDataTypes(colInfoList);
       dbh.createShadowTableTriggers(pubName, tableNames[i].toString(),colInfoList, primCols);
        }
        savePublicationNewTables(pubConnection,stmt,pubRepTables);
    }
    catch (SQLException ex) {
//      ex.printStackTrace();
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP311", new Object[] {pubName, ex.getMessage()});
    }
    catch (Throwable ex) {
//     ex.printStackTrace();
      throw new RepException("REP311", new Object[] {pubName, ex.getMessage()});
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
        if (islockedTaken)
         isLocked = UNLOCK;
       if(stmt!=null)
        try {
          stmt.close();
        }
        catch (SQLException ex1) {
        }
    }

  }

  /**
   * Saves the publication name and its tables into server side replication
   * system tables.
   * @param dbh database handler for server side
   * @throws RepException
   */
  private void savePublicationNewTables( Connection connection ,Statement stmt,ArrayList tablesList) throws
      RepException {
    try {
      //Delete all entries from Rep_table
      StringBuffer sb = new StringBuffer();
      sb.append("delete from ").append(dbh.getRepTableName()).append(" where ")
          .append(RepConstants.repTable_pubsubName1).append(" = '")
          .append(pubName).append("'");
log.debug(sb.toString());
      stmt.execute(sb.toString());
      // Insert Into Rep Table, for all tables in publication
      for (int i = 0; i < tablesList.size(); i++) {
        RepTable repTable = (RepTable) tablesList.get(i);
        repTable.setConflictResolver(conflictResolver);
        dbh.saveRepTableData(connection, pubName, repTable);
      }
    }
    catch (SQLException ex) {
//      ex.printStackTrace();
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP0202", new Object[] {pubName,ex.getMessage()});
    }
  }

  /**
   * Inserts a row into bookmark table of server side to put a matching pair of
   * publication name and subscription name.
   * @param subName sub name
   * @throws RepException
   */
  public void saveSubscriptionNewData(String subName) throws RemoteException {
    Statement stt = null;
    ResultSet rs = null;
    try {
      Connection connection = connectionPool.getConnection(pubName);
      MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
      ArrayList dropTableFromBookMarkList = new ArrayList();
      ArrayList insertTableToBookMarkList = new ArrayList();
      ArrayList wrongPubRepTableList = new ArrayList();
      ArrayList correctPubRepTableList = new ArrayList();
      stt = connection.createStatement();
      for (int i = 0; i < noOfPubTables; i++) {
        RepTable repTable = (RepTable) pubRepTables.get(i);
        StringBuffer sb0 = new StringBuffer();
        sb0.append(" Insert into ").append(dbh.getBookMarkTableName()).append(
            " values ( '").append(pubName).append("','").append(subName).append(
            "','").append(repTable.getSchemaQualifiedName()).append("',0,0,'N')");
        try {
           log.debug(sb0.toString());
          stt.execute(sb0.toString());
        }
        catch (SQLException ex2) {
          //ignore the Exception
        }
      }
      StringBuffer  updatewithN = new StringBuffer();
        updatewithN.append("Update ")
            .append(dbh.getBookMarkTableName()).append(" set ")
            .append(RepConstants.bookmark_IsDeletedTable).append(" ='N' " );
        log.debug("upadting IsDeletedTable to 'N' :"+updatewithN.toString());
        stt.execute(updatewithN.toString());

      //get correct pubReptable list
      for (int i = 0; i < pubRepTables.size(); i++) {
        RepTable repTable = (RepTable) pubRepTables.get(i);
        correctPubRepTableList.add(repTable.getSchemaQualifiedName());
      }
      //get wrong pubReptable list
      StringBuffer sb = new StringBuffer();
      sb.append("select ").append(RepConstants.bookmark_TableName3).append(
          " from ").append(dbh.getBookMarkTableName()).append(" where ")
          .append(RepConstants.bookmark_LocalName1).append(" = '")
          .append(pubName).append("'").append(" and ").append(RepConstants.
          bookmark_RemoteName2).append(" = '")
          .append(subName).append("'"); ;
      rs = stt.executeQuery(sb.toString());

      while (rs.next()) {
        SchemaQualifiedName sname = new SchemaQualifiedName(mdi, rs.getString(1));
        String schema = sname.getSchemaName();
        String table = sname.getTableName();
        RepTable repTable = new RepTable(sname, RepConstants.publisher);
        //Sets Sorted primary key columns in repTable.
        mdi.setPrimaryColumns(repTable, schema, table);
        wrongPubRepTableList.add(repTable.getSchemaQualifiedName());
      }
      //get list of tables whose entry to be dropped from bookmarktable
      for (int j = 0; j < correctPubRepTableList.size(); j++) {
        if (!wrongPubRepTableList.contains(correctPubRepTableList.get(j))) {
          insertTableToBookMarkList.add(correctPubRepTableList.get(j));
        }
      }
      //get list of tables whose entry to be inserted into bookmarktable
      for (int j = 0; j < wrongPubRepTableList.size(); j++) {
        if (!correctPubRepTableList.contains(wrongPubRepTableList.get(j))) {
          dropTableFromBookMarkList.add(wrongPubRepTableList.get(j));
        }
      }
      for (int j = 0; j < dropTableFromBookMarkList.size(); j++) {
        StringBuffer deleteQuery = new StringBuffer();
        deleteQuery.append(" delete from ").append(dbh.getBookMarkTableName()).
            append(" where ").append(RepConstants.bookmark_LocalName1).append(
            " = '").append(pubName)
            .append("'").append(" and ").append(RepConstants.bookmark_TableName3).append(
            " ='").append(dropTableFromBookMarkList.get(j).toString()).append(
            " '").append(
            " and ").append(RepConstants.bookmark_RemoteName2).append(" ='").
            append(subName).append(" '");
log.debug("dropTableFromBookMarkList ::"+deleteQuery.toString());
        stt.execute(deleteQuery.toString());
      }
      for (int j = 0; j < insertTableToBookMarkList.size(); j++) {
        StringBuffer insertQuery = new StringBuffer();
        insertQuery.append(" Insert into ").append(dbh.getBookMarkTableName()).
            append(" values ( '").append(pubName).append("','").append(subName).append(
            "','").append(insertTableToBookMarkList.get(j).toString()).append(
            "',0,0,'N')");
log.debug("insertTableToBookMarkList ::"+insertQuery.toString());
        stt.execute(insertQuery.toString());
      }
    }
    catch (SQLException ex) {
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RemoteException(ex.getMessage());
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
      try {
        if(rs!=null)
            rs.close();
        if (stt != null)
          stt.close();
      }
      catch (SQLException ex1) {
        // Ignore the exception
      }
    }
  }

  public void dropTableFromPublication(String[] dropTableList) throws RepException {
    Statement stt = null;
     boolean islockedTaken=false;
    try {
      checkForLock(pubName);
      islockedTaken=true;
      checkTableNameIfNull(dropTableList);
      ArrayList dropRepTableList = new ArrayList();
      ArrayList repTableList = localServer.dropTableFromPublication(pubName,dropTableList, this, pubRepTables);
      for (int i = 0; i < repTableList.size(); i++) {
        RepTable repTable = (RepTable) repTableList.get(i);
        dropRepTableList.add(repTable.getSchemaQualifiedName());
      }
      Connection connection = connectionPool.getConnection(pubName);
      stt = connection.createStatement();
      //drop triggers,shadow tables,delete from logtable
      for (int i = 0; i < dropRepTableList.size(); i++) {

         StringBuffer query = new StringBuffer();
       //delete entry from reptable
        query.append(" DELETE " +" FROM  " +dbh.getRepTableName()+ " WHERE ");
        query.append(RepConstants.repTable_pubsubName1);
        query.append(" = '");
        query.append(pubName);
        query.append("' and ");
        query.append(RepConstants.repTable_tableName2);
        query.append(" ='");
        query.append(dropRepTableList.get(i).toString());
        query.append("'");
        log.debug(query.toString());
        stt.execute(query.toString());
        StringBuffer updateQuery = new StringBuffer();
                        updateQuery.append(" update ").append(dbh.getBookMarkTableName()).
                            append(" set ").append(RepConstants.bookmark_ConisderedId5).
                            append(" = 0 ,").append(RepConstants.bookmark_lastSyncId4).
                            append(" = 0 ,").append(RepConstants.bookmark_IsDeletedTable).append(" = 'Y' ").
                            append(" where ").append(RepConstants.bookmark_LocalName1).append(" = '").
                            append(pubName).append("'").append(" and ").append(RepConstants.bookmark_TableName3).
                            append(" ='").append(dropRepTableList.get(i).toString()).append("'");
        log.debug("updating bookmarktable lstsyncid,consideredId and isDeletedTable : "+updateQuery.toString());
        stt.execute(updateQuery.toString());
        dbh.dropTriggersAndShadowTable(connection,dropRepTableList.get(i).toString(),pubName);
      }
      //modifying the pubRepTablesList
      for (int i = 0; i < repTableList.size(); i++) {
        RepTable repTable = (RepTable) repTableList.get(i);
        for (int j = 0; j < pubRepTables.size(); j++) {
          RepTable pRepTable = (RepTable) pubRepTables.get(j);
          if (pRepTable.getRepTableQualifiedIdentifier().toString().equals(
              repTable.getRepTableQualifiedIdentifier().toString())) {
            pubRepTables.remove(pubRepTables.get(j));
            noOfPubTables--;
          }
        }
      }
    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
      if (ex instanceof RepException) {
        throw (RepException) ex;
      }
      RepException rex = new RepException("REP311", new Object[] {ex.getMessage()});
      rex.setStackTrace(ex.getStackTrace());
      throw rex;
    }
    finally {
      connectionPool.removeSubPubFromMap(pubName);
      if (islockedTaken)
         isLocked = UNLOCK;
      try {
        if (stt != null) {
          stt.close();
        }
      }
      catch (SQLException ex1) {
      }
    }
  }

  public ConnectionPool getConnectionPool() {
    return connectionPool;
  }
//check filter in case of add tables to publisher.
//  This method does early checking of filter clauses
  public void checkFilter(Statement stmt,String[] tableName0, String[] filterClause0) throws
        RepException {
      ResultSet rs = null;
      try {
        if(tableName0.length!=filterClause0.length){
       throw new RepException("REP319",null);
        }
        MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
        for (int i = 0; i < tableName0.length; i++) {
          SchemaQualifiedName sname = new SchemaQualifiedName(mdi, tableName0[i]);
          if (tableName0[i].indexOf('.') == -1) {
            mdi.checkTableExistance(sname);
          }
         log.debug("Table name : " + tableName0[i]);
         log.info("check the filter : " + filterClause0[i] + " for table " +tableName0[i]);
         Connection pubConnection = connectionPool.getConnection(pubName);
          if (filterClause0[i]!=null) {
            try {
              String query = "Select * from " + sname + " where " +filterClause0[i];
             log.debug("check filter query  : " + query);
              rs = stmt.executeQuery(query);
              rs.next();
            }
            catch (SQLException ex1) {
              RepConstants.writeERROR_FILE(ex1);
              RepException rex = new RepException("REP019",new Object[] {filterClause0[i]});
              rex.setStackTrace(ex1.getStackTrace());
              throw rex;
            }
          }
        }
      }
      finally {
        try {
          if (rs != null) {
            rs.close();
          }
        }
        catch (SQLException ex) {
        }
      }
    }


    public void checkTableNameIfNull(String[] tableNames) throws RepException {
      if (tableNames == null) {
        throw new RepException("REP316", null);
      }
      for (int i = 0; i < tableNames.length; i++) {
        if (tableNames[i] == null) {
          throw new RepException("REP320", null);
        }
      }
    }
    /**
       * creates an XMl file for getting Snapshot on clients side.
       * Do entry in the server side bookmark table.
       * Deletes all records from the server side log table.
       *
       * @param address
       * @param portNo
       * @param subName
       * @throws SQLException
       * @throws RemoteException
       * @throws RepException
       */
      public void createSnapShotAfterUpdateSub(String address, int portNo, String subName,ArrayList tablesForSnapShot,boolean isSchemaSupported,_FileUpload fileUpload,String remoteMachineAddress) throws
          SQLException, RemoteException, RepException {
        Statement stmt = null;
        ResultSet rows = null, shadowResult = null;
        try {
//          checkForLock();
          FileOutputStream fos = new FileOutputStream(PathHandler.getDefaultFilePathForClient("snapshot_" + pubName+ "_" +subName));
          OutputStreamWriter os = new OutputStreamWriter(fos);
          BufferedWriter bw = new BufferedWriter(os);
          Connection pubConnection = connectionPool.getConnection(pubName);
          XMLWriter xmlWriter = new XMLWriter(bw, dbh, pubConnection);
          bw.write("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>");
          bw.write("<root_snapshot>");
          String tableName;
          SchemaQualifiedName sname;
          stmt = pubConnection.createStatement();
          for (int i = 0; i < noOfPubTables; i++) {
            RepTable repTable = (RepTable) pubRepTables.get(i);
            sname =(repTable).getSchemaQualifiedName();
            tableName = isSchemaSupported ? sname.toString() : sname.getTableName() ;
            //compare for new table list
            if(tablesForSnapShot.contains(tableName.toLowerCase())){
            ArrayList encodedCols = PathHandler.getEncodedColumns(tableName);
            bw.write("<tableName>");
            bw.write(tableName);
            String query;
            if (repTable.getFilterClause() == null || repTable.getFilterClause().equalsIgnoreCase("")) {
              query = "Select * from " + repTable.getRepTableQualifiedIdentifier();
//                    tableName
            }
            else {
//                  tableName
              query = "Select * from " + repTable.getRepTableQualifiedIdentifier() +" where " + repTable.getFilterClause();
            }
            rows = stmt.executeQuery(query);
            ResultSetMetaData rsmt = rows.getMetaData();
            int noOfColumns = rsmt.getColumnCount();
            while (rows.next()) {
              bw.write("<row>");
              for (int c = 1; c <= noOfColumns; c++) {
                String columnName = rsmt.getColumnName(c);
                if (!encodedCols.contains(columnName.toUpperCase())) {
                  bw.write("<" + columnName + ">");
                }
                else {
                  bw.write("<" + columnName + "  Encode=\"y\">");
                }
                // Special Handelling for Blob Clob Case
                xmlWriter.write(rows, c, encodedCols, columnName);
                bw.write("</" + columnName + ">\r\n");
              }
              bw.write("</row>\r\n");
              bw.flush();
            }
            bw.write("</tableName>\r\n");
            // updating values for bookmark table
            stmt.execute(" select max(" + RepConstants.shadow_sync_id1 + ") from " +RepConstants.shadow_Table(tableName));
            log.debug(" select max(" + RepConstants.shadow_sync_id1 + ") from " +RepConstants.shadow_Table(tableName));
            shadowResult = stmt.getResultSet();
            boolean value = shadowResult.next();
            Object maxvalue = shadowResult.getObject(1);
            if (maxvalue == null) {
              maxvalue = new Long(0);
            }
            log.debug("max(" + RepConstants.shadow_sync_id1 + ")=" + maxvalue);
            StringBuffer updateBookmarkQuery = new StringBuffer("update " +dbh.getBookMarkTableName() + " set ");
            updateBookmarkQuery.append(RepConstants.bookmark_lastSyncId4 + " = " +maxvalue + ", ");
            updateBookmarkQuery.append(RepConstants.bookmark_ConisderedId5 + " = " +maxvalue + " ");
            updateBookmarkQuery.append(" where " + RepConstants.bookmark_LocalName1 +" ='" + pubName + "' ");
            updateBookmarkQuery.append(" and  " + RepConstants.bookmark_RemoteName2 +" ='" + subName + "' ");
            updateBookmarkQuery.append(" and  " + RepConstants.bookmark_TableName3 +" ='" + tableName + "'");
            stmt.executeUpdate(updateBookmarkQuery.toString());
            log.debug(updateBookmarkQuery.toString());
           }
          }
          bw.write("</root_snapshot>\r\n");
          if(bw!=null)
          bw.close();
          fos.close();

          // making zip file from xml file
          if(!localAddress.equalsIgnoreCase(remoteMachineAddress)) {
            ZipHandler.makeZip(PathHandler.getDefaultZIPFilePathForClient("snapshot_" + pubName + "_" + subName),PathHandler.getDefaultFilePathForClient("snapshot_" + pubName +"_" + subName), "snapshot_" + pubName + "_" + subName + ".xml");
            // writing zip file on socket
    //         writeXMLFileOnClientSocket(address, portNo,PathHandler.getDefaultZIPFilePathForServer("snapshot_" + pubName));
            WriteOnSocket writeOnSocket = new WriteOnSocket(PathHandler.getDefaultZIPFilePathForClient("snapshot_" + pubName),PathHandler.getDefaultFilePathForClient("snapshot_" + pubName),_Publication.xmlAndShadow_entries, "snapshot_" + pubName,fileUpload, true);
            writeOnSocket.start();
            writeOnSocket.join();
          }
          /*   if (_Publication.xmlAndShadow_entries) {
            // deleting zip file
            deleteFile(PathHandler.getDefaultZIPFilePathForServer("snapshot_" +pubName));
            // deleting xml file
            deleteFile(PathHandler.getDefaultFilePathForServer("snapshot_" +pubName));
          }
        */
          deleteRecordsFromSuperLogTable(stmt);
        }
        catch (FileNotFoundException ex) {
          RepConstants.writeERROR_FILE(ex);
          throw new RepException("REP085", null);
        } //Temporary Snapshot File Can not created.
        catch (IOException ex) {
          RepConstants.writeERROR_FILE(ex);
          RepException rex = new RepException("REP086", new Object[] {ex.getMessage()});
          rex.setStackTrace(ex.getStackTrace());
          throw rex;
        }
        catch (SQLException ex) {
          RepConstants.writeERROR_FILE(ex);
          RepException rex = new RepException("REP055", new Object[] {pubName,ex.getMessage()});
          rex.setStackTrace(ex.getStackTrace());
          throw rex;
        }
        catch (InterruptedException ex) {
          RepConstants.writeERROR_FILE(ex);
          RepException rex = new RepException("REP055", new Object[] {pubName,ex.getMessage()});
          rex.setStackTrace(ex.getStackTrace());
          throw rex;

          }
        catch (RepException ex) {
          RepConstants.writeERROR_FILE(ex);
          throw ex;
        }
        finally {
          connectionPool.removeSubPubFromMap(pubName);
          /** @todo  Close the statement */
          if (rows != null)
            rows.close();
          if (shadowResult != null)
            shadowResult.close();
          if (stmt != null)
            stmt.close();
//          isLocked = UNLOCK;
        }
      }
      public ArrayList dropTableListForSub(String subName) throws RepException,
      SQLException ,RemoteException{
        ResultSet rs=null;
        Statement stt=null;
        ArrayList dropTableFromSub=new ArrayList();
        try{
          stt=connectionPool.getConnection(pubName).createStatement();
          StringBuffer sb = new StringBuffer();
          sb.append("select ").append(RepConstants.bookmark_TableName3)
              .append(" from ").append(dbh.getBookMarkTableName()).append(" where ")
              .append(RepConstants.bookmark_LocalName1).append(" = '")
              .append(pubName).append("'").append(" and ")
              .append(RepConstants.bookmark_RemoteName2).append(" = '")
              .append(subName).append("'").append(" and ")
              .append(RepConstants.bookmark_IsDeletedTable).append(" ='Y'");
          rs = stt.executeQuery(sb.toString());
          while(rs.next()){
            dropTableFromSub.add(rs.getString(RepConstants.bookmark_TableName3));
          }
          return dropTableFromSub;
        }finally{
          connectionPool.removeSubPubFromMap(pubName);
          try {
            if (rs != null) {
              rs.close();
            }
            if (stt != null) {
              stt.close();
            }
          }
          catch (SQLException ex) {
          }
        }
      }

      public void updatePublisherShadowAndBookmarkTableAfterPullOnSubscriber(String remote_Pub_Sub_Name,
          Object[] lastId) throws RemoteException, SQLException, RepException {
        Statement stmt = null;
        try {
          updateBookMarkLastSyncId(remote_Pub_Sub_Name, lastId);
          Connection pubConnection = connectionPool.getConnection(pubName);
          stmt = pubConnection.createStatement();
          for (int i = 0; i < pubRepTables.size(); i++) {
            String tableName = ( (RepTable) pubRepTables.get(i)).getSchemaQualifiedName().toString();
            UpdateConisdered_LastsyncIdForBookMarksTable(pubName,remote_Pub_Sub_Name,tableName, lastId[i], stmt);
          }
          deleteRecordsFromShadowTable(stmt);
        }
        finally {
          connectionPool.removeSubPubFromMap(pubName);
          try {
            if (stmt != null) {
              stmt.close();
            }
          }
          catch (SQLException ex) {
          }
        }
      }

    public void setIgnoredColumns(String tableName,
                String[] columnNamesToBeIgnored) throws RepException {
      MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
      SchemaQualifiedName sname = new SchemaQualifiedName(mdi, tableName);
      if (tableName.indexOf('.') == -1) {
        mdi.checkTableExistance(sname);
      }
      getRepTable(tableName).setColumnsToBeIgnored(columnNamesToBeIgnored);
    }

    /**
     * To get the result of primary columns to write its value in XML
     * file during snapshot.
     * @param connection Connection
     * @param tableName String
     * @param primaryColumnNames String[]
     * @throws SQLException
     * @return ResultSet
     */
    private ResultSet getPrimaryKeyResultSet(Statement stmt,String tableName, String[] primaryColumnNames) throws SQLException{
     StringBuffer sb =new StringBuffer();
           sb.append("SELECT  ");
           for (int i = 0; i < primaryColumnNames.length; i++) {
             if(i!=0)
             sb.append(",");
             sb.append(primaryColumnNames[i]);
           }
           sb.append(" FROM ").append(tableName);
           ResultSet resultSetPrimaryKey = stmt.executeQuery(sb.toString());
           resultSetPrimaryKey.next();
           return resultSetPrimaryKey;
   }

  private void makePublicationTransactionLogFile(String subName,MergeHandler mg ,BufferedWriter bw ) throws Exception {
      if(Utility.createTransactionLogFile) {
        String transactionLogURL = PathHandler.getDefaultTransactionLogFilePathForPublisher(pubName);
        FileOutputStream fos1 = new FileOutputStream(transactionLogURL, true);
        OutputStreamWriter os = new OutputStreamWriter(fos1);
        bw = new BufferedWriter(os);
        AbstractSynchronize.writeDateInTransactionLogFile(bw);
        AbstractSynchronize.writeOperationInTransactionLogFile(bw, mg.insert,mg.update, mg.delete, "MERGE");
        bw.flush();
      }
}

    private boolean isSchemaSupported() {
      return  dbh.isSchemaSupported();
    }

  /**
   * getFileUploader
   *
   * @param subName String
   * @return ArrayList
   */
  public _FileUpload getFileUploader()throws RepException,
      SQLException,RemoteException {
    return new FileUpload();
  }

}
