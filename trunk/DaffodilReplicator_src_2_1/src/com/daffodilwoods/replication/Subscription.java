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
import com.daffodilwoods.replication.zip.ZipHandler;
import com.daffodilwoods.replication.xml.SnapshotHandler;
import com.daffodilwoods.replication.xml.DDLHandler;
import org.apache.log4j.Logger;
import com.daffodilwoods.replication.synchronize.AbstractSynchronize;
import java.sql.ResultSet;
import java.math.BigDecimal;

/**
 * subscription class holds all the methods which are required for the physical
 * creation of the subscription(i.e system tables) and which are used at the time
 * of synchronization at subscriber's end. It implements three interfaces,
 *
 * _subscription : It makes this class to implement all the methods which are
 * called by the user, at the time of subscribig, and for different tasks
 * related to synchronization.
 *
 * _SubImpl : It makes this class to implement the method getServerName which
 * is called remotely by the publisher to get the local server name at the time
 * of synchronization.
 *
 * _Replicator : It makes this class to implement all the methods which are called
 * at the time of synchronization to get the subscriber's information.
 *
 */

public class Subscription
    extends UnicastRemoteObject
    implements _SubImpl, _Subscription, _Replicator {
  public String subName;
  private int remotePort;
  private String remoteUrl;
  private ConnectionPool connectionPool;
  private ArrayList subRepTables;
  private String conflictResolver;
  private String pubName;
  private _ReplicationServerImpl remoteServer;
  protected static Logger log = Logger.getLogger(Subscription.class.getName());
  private ArrayList alterTableAddFKQueries;

  // To write the file on client socket.
  private _FileUpload fileUpload;

  public AbstractDataBaseHandler dbHandler;

  SyncXMLCreator syncXMLCreator;
  private ReplicationServer localServer;
  HashMap syncIdMap;

  public Subscription() throws RemoteException {
  }

  public Subscription(ConnectionPool connectionPool0, String subName0,
                      String serverName0, ReplicationServer localServer0) throws
      RemoteException,
      RepException {
    subName = subName0;
    connectionPool = connectionPool0;
    //localAddress = connectionPool.getLocalAddress();
    // make sure whether you need this connection statement or not
    Connection subConnection = connectionPool.getConnection(subName);
    dbHandler = Utility.getDatabaseHandler(connectionPool, subName);
    dbHandler.setLocalServerName(serverName0);
    //String databaseName = connectionPool.getConnection(subName).getMetaData().getDatabaseProductName();
    dbHandler.setLocalServerName(serverName0);
    syncXMLCreator = new SyncXMLCreator(subName, connectionPool, dbHandler);
    localServer = localServer0;
    fileUpload = new FileUpload();


  }

  /**
   * This method is called after parsing to set the repTables with the structures
   * same as written on XML file.
   * @param tableNames0
   */

  public void setSubscriptionTables(ArrayList tableNames0) {
    subRepTables = tableNames0;
  }

  /** @todo Added by hisar team. */

  public void setAlterTableAddFKStatements(ArrayList alterTableAddFKQueries0) {
    alterTableAddFKQueries = alterTableAddFKQueries0;
  }

  /**
   * This method is called at the time of creating the subscriber. This method
   * sets the url(i p address.) of the publsher's m/c for the subscriber, i.e
   * on which m/c the publisher exists. And on which m/c the subscriber will
   * lookup for this publisher.
   *
   * @param remoteUrl0
   * @throws RepException
   */

  public void setRemoteServerUrl(String remoteUrl0) throws RepException {
    if (remoteUrl0.equalsIgnoreCase("localhost")) {
      throw new RepException("REP004", null);
    }
    remoteUrl = remoteUrl0;
  }

  /**
   * This method is called at the time of creating the subscriber. This method
   * sets the port no. of the publsher for the subscriber, i.e on which port
   * the publisher is running. And on which port the subscriber will lookup
   * for the replication server of the publisher.
   *
   * @param remotePort0
   */

  public void setRemoteServerPortNo(int remotePort0) {
    remotePort = remotePort0;
  }

  public String getRemoteServerUrl() {
    return remoteUrl;
  }

  public int getRemoteServerPortNo() {
    return remotePort;
  }

  public ConnectionPool getConnectionPool() {
    return connectionPool;
  }

  public void setPublicatonName(String pubName0) {
    pubName = pubName0;
  }

  /**
   * This method is responsible for the physical creation of the subscriber.
   * This method creates replication system tables for subscriber at the
   * subscriber's end. For getting the publication's replication tables'
   * structures It looks up the remote replication server, gets the object
   * of the publication's class, and then calls remote methods which
   * creates XML file on the publisher's end. This file holds the queries
   * needed to create the same structures over client system. Now this file
   * is transferred to the client socket. And by parsing this file, replication
   * tables are generated at the sub side. Then proper triggers are created.
   * And subscription data is saved.
   *
   * @throws RepException
   */

  public void subscribe() throws RepException {
    // Create an Instance of DBHandler With Respect to Client Connection
    Statement stmt = null;
    try {
      //Creates Subscription Table
      //Creates BookMark Table
      //Creates Super Log Table
      //Creates Rep Table
      Connection connection = connectionPool.getConnection(subName);
      log.debug("connection :::   "+connection);
      stmt = connection.createStatement();
      dbHandler.createClientSystemTables(subName);
      HashMap primCols = new HashMap();
      _PubImpl pub = getPublication();
      log.debug("pub :::   "+pub);
      int pubVendorType = pub.getPubVendorName();
      log.debug("pubVendorType :::   "+pubVendorType);
      //Returns [0] array of schemas [1]publicationtablequeries.
      ArrayList[] schemaTables = getPublicationTableQueries(primCols, pub);
       log.debug("schemaTables :::   "+schemaTables);
      dbHandler.createSchemas(pubName, schemaTables[0]);
      String[] alterTableQueries = alterTableAddFKQueries == null ? null : (String[]) alterTableAddFKQueries.toArray(new String[0]);
       log.debug("alterTableQueries :::   "+alterTableQueries);
      dbHandler.createSubscribedTablesTriggersAndShadowTables(subName,(String[]) (schemaTables[1]).toArray(new String[0]), alterTableQueries, primCols, pubVendorType, subRepTables);
      //Do Entry in the Subscription Table , BookMark Table, Rep Table.
      saveSubscriptionData(dbHandler, connection, stmt);
      pub.saveSubscriptionData(subName);
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP025", new Object[] {ex.getMessage()});
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
      if (stmt != null)
        try {
          stmt.close();
        }
        catch (SQLException ex1) {
        }
    }
  }

  /**
   * Make an entry in the subscriptions table. And inserts all the records
   * corresponding to each replication table in to the reptabe and Bookmark table.
   *
   * @param dbHandler
   * @throws SQLException
   * @throws RepException
   */

  private void saveSubscriptionData(AbstractDataBaseHandler dbHandler,
                                    Connection connection, Statement stmt) throws
      SQLException, RepException {

    try {

      //Inserts into Subscription table
      stmt.execute("insert into " + dbHandler.getSubscriptionTableName() +" values ( '" + subName + "', '"
                   + pubName + "' , '" + conflictResolver + "' , '" +dbHandler.getLocalServerName() + "' )");
      log.info("Query exceuted insert into " +
               dbHandler.getSubscriptionTableName() +
               " values ( '" + subName + "', '" + pubName + "' , '" +
               conflictResolver + "' , '" +
               dbHandler.getLocalServerName() + "' )");
      //For each table makes an entry into the BookMark Table and Rep Table
      for (int i = 0, size = subRepTables.size(); i < size; i++) {
        RepTable repTable = (RepTable) subRepTables.get(i);
        repTable.setConflictResolver(conflictResolver);
        StringBuffer sb2 = new StringBuffer();
        sb2.append(" Insert into ").append(dbHandler.getBookMarkTableName())
            .append(" values ( '").append(subName).append("','")
            .append(pubName).append("','").append(repTable.getSchemaQualifiedName())
            .append("',").append("0,0,'N')");
//          stt.execute(dbHandler.getRepTableInsertQuery(subName, repTable));
        dbHandler.saveRepTableData(connection, subName, repTable);
        stmt.execute(sb2.toString());
        log.debug(sb2.toString());
      }
    }
    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP022", new Object[] {subName, pubName});
    }
    finally {
      if (stmt != null)
        stmt.close();
    }
  }

  /**
   * remove all the reocrds from published tables at subscriber
   * and updates the client side with the latest records of server.
   * @throws RepException
   */
  public synchronized void getSnapShot() throws RepException {
    Connection subConnection = null;
    Statement subStatment = null;
    boolean islockedTaken = false,isCurrentTableCyclic = false;
    _PubImpl publication = null;
    try {
      String localAddress = null,remoteServerName=null,remoteAddress=null;
      try {
        _ReplicationServerImpl remoteServer = getRemoteReplicationServer();
        remoteServerName = remoteServer.getServerName();
        publication = remoteServer.getRemotePublication(pubName);
        publication.checkForLock(subName);
        islockedTaken = true;
        localAddress = InetAddress.getLocalHost().getHostAddress();
         remoteAddress =remoteServer.getRemoteAddress();
         publication.createSnapShot(subName,isSchemaSupported(),fileUpload,localAddress );
      }
      catch (RepException ex) {
        log.error(ex.getMessage(), ex);
        throw ex;
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
        RepException rex = new RepException("REP053", new Object[] {subName,ex.getMessage()});
        rex.setStackTrace(ex.getStackTrace());
        throw rex;
      }
      try {
        //unzipping the zip file
        if(!localAddress.equalsIgnoreCase(remoteAddress)) {
          ZipHandler.unZip(PathHandler.getDefaultZIPFilePathForClient("snapshot_" + pubName + "_" + subName),
                           PathHandler.getDefaultFilePathForClient("snapshot_" + pubName +"_" + subName));
        }
      }
      catch (IOException ex) {
        RepConstants.writeERROR_FILE(ex);
        RepException rex = new RepException("REP052", new Object[] {subName,ex.getMessage()});
        rex.setStackTrace(ex.getStackTrace());
        throw rex;
      }

      try {
        subConnection = connectionPool.getConnection(subName);
        subStatment = subConnection.createStatement();
        for (int i = 0; i < subRepTables.size(); i++) {
          RepTable repTable = ( (RepTable) subRepTables.get(i));
          isCurrentTableCyclic = repTable.getCyclicDependency().equalsIgnoreCase(RepConstants.YES);
          if(isCurrentTableCyclic)
          break;
        }
        SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
        XMLReader reader = saxParser.getXMLReader();
        // Delete records from all tables
        deleteAllRecordsFromMainTables(subStatment);
        //Instance for content handler
        SnapshotHandler ch = new SnapshotHandler(true, subConnection, this,dbHandler, remoteServerName); //  instance for content hanedler
        //  instance for content hanedler
        ch.setPubName(pubName);
        ch.setSubName(subName);
        reader.setContentHandler(ch);
        reader.parse(PathHandler.getDefaultFilePathForClient("snapshot_" + pubName + "_" +subName));
        ch.closeAllStatementAndResultset();
        /**
         * To handle the cyclic table case referenced
         * columns value is updated in second pass.
         */
        if(isCurrentTableCyclic){
        SnapshotHandler ch1 = new SnapshotHandler(false, subConnection, this,dbHandler, remoteServerName); //  instance for content hanedler
        ch1.setPubName(pubName);
        ch1.setSubName(subName);
        reader.setContentHandler(ch1);
        reader.parse(PathHandler.getDefaultFilePathForClient("snapshot_" + pubName + "_" +subName));
        ch1.closeAllStatementAndResultset();
        }
        if (_Subscription.xmlAndShadow_entries) {
          // deleting xml file
          deleteFile(PathHandler.getDefaultFilePathForClient("snapshot_" + pubName + "_" +subName));
          // deleting zip file
          deleteFile(PathHandler.getDefaultZIPFilePathForClient("snapshot_" + pubName + "_" +subName));
        }
        dbHandler.deleteRecordsFromSuperLogTable(subStatment);
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
        RepException rex = new RepException("REP053", new Object[] {subName,ex.getMessage()});
        rex.setStackTrace(ex.getStackTrace());
        throw rex;
      }
    }
    catch (RepException rex) {
      RepConstants.writeERROR_FILE(rex);
      throw rex;
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
      try {
        if (islockedTaken)
          publication.releaseLOCK();
      }
      catch (RemoteException ex2) {
        RepConstants.writeERROR_FILE(ex2);
      }

      try {
        if (subStatment != null) {
          subStatment.close();

        }
      }
      catch (SQLException ex1) {
      }
    }
  }

  /**
   * Delete records from all main tables
   * @throws SQLException
   */
  private void deleteAllRecordsFromMainTables(Statement subStatment) throws SQLException,  RepException {
    for (int i = subRepTables.size() - 1; i >= 0; i--) {
      RepTable repTable = (RepTable) subRepTables.get(i);
      subStatment.execute("delete from " + repTable.getSchemaQualifiedName());
    }
  }

  /**
   * This method first gets the object of the publications object.
   * Do entry in the server's bookmark table.
   * Creates struct_pubname.xml,struct_pubname.zip file , blob.lob, clob.lob
   * on the publisher and writes tablequery file i.e XMLfile on the subscribers
   * socket, Then parses this file.
   *
   * @param primColMap
   * @return
   * @throws SQLException
   * @throws RemoteException
   * @throws RepException
   */

  private ArrayList[] getPublicationTableQueries(HashMap primColMap, _PubImpl pub) throws
      SQLException, RemoteException, RepException {

    if (pub == null) {
      throw new RepException("REP036", new Object[] {pubName});
    }
    conflictResolver = pub.getConflictResolver();
    String address = null,remoteMachineAddresss =null;
      _ReplicationServerImpl replicationServer = getRemoteReplicationServer();
      remoteMachineAddresss =   replicationServer.getRemoteAddress();

    try {
      address = InetAddress.getLocalHost().getHostAddress();
    }
    catch (UnknownHostException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP025", new Object[] {subName, ex.getMessage()});
    }
    //Do entry in the server's bookmark table.
    //Creates struct_pubname.xml,struct_pubname.zip file , blob.lob, clob.lob on the publisher.
    //Writes tablequery file i.e XMLfile on the subscribers socket
    pub.createStructure(Utility.getVendorType(connectionPool, subName), subName,address, isSchemaSupported(),fileUpload,address);
    ArrayList schemas = null;
    ArrayList queries = null;
    MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, subName);
    try {
      String filePath = PathHandler.getDefaultFilePathForCreateStructure("struct_" + pubName + "_" + subName);
      //Creates xml,blob.lob,clob.lob files over the subscriber
      if(!address.equalsIgnoreCase(remoteMachineAddresss)) {
        String zipFilePath = PathHandler.getDefaultZIPFilePathForCreateStructure("struct_" + pubName + "_" + subName);
        ZipHandler.unStructZip(zipFilePath, filePath);
      }
      SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
      //Returns XMLReaderImpl class' object in XMLReader interface's reference.
      XMLReader reader = saxParser.getXMLReader();
      schemas = new ArrayList();
      queries = new ArrayList();
      ContentHandler ddh = new DDLHandler(this, schemas, queries,conflictResolver, primColMap, mdi);
      reader.setContentHandler(ddh);
      //Actually called method is XMLReaderImpl->parse.
      //internally calls startDicument,endDocument methods of DDLHandler.
      reader.parse(filePath);
    }
    catch (SAXException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP081", null);
    }
    catch (ParserConfigurationException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP081", null);
    }
    catch (FileNotFoundException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP082", null);
    }
    catch (IOException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP083", null);
    }
    return new ArrayList[] {schemas, queries};
  }

  /**
   * Gets the object of the replication server class by looking up the
   * rmi registry of the specified port and url.
   *
   * @return
   * @throws NotBoundException
   * @throws MalformedURLException
   * @throws RemoteException
   * @throws RepException
   */

  private _ReplicationServerImpl getRemoteReplicationServer() throws
      RepException {
    //remoteUrl & remotePort are set by the user it comes from the functions setRemoteServerPortNo.
    //& setRemoteServerURL...
    try {
      if (remoteServer != null) {
        return remoteServer;
      }
      String ipadd = null;
      try {
        ipadd = InetAddress.getByName(remoteUrl).getHostAddress();
      }
      catch (UnknownHostException ex) {
        RepConstants.writeERROR_FILE(ex);
        throw new RepException("REP001", new Object[] {ex.getMessage()});
      }
      String name = "rmi://" + ipadd + ":" + Integer.toString(remotePort) + "/" +ipadd + "_" + remotePort;
      log.debug("Naming.lookup(" + name + ")");
      remoteServer = (_ReplicationServerImpl) Naming.lookup(name);
      return remoteServer;
    }
    catch (RemoteException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP025", new Object[] {subName, ex.getMessage()});
    }
    catch (MalformedURLException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP025", new Object[] {subName, ex.getMessage()});
    }

    catch (NotBoundException ex1) {
      RepConstants.writeERROR_FILE(ex1);
      throw new RepException("REP025", new Object[] {subName, ex1.getMessage()});
    }
    catch (RepException ex1) {
      throw ex1;
    }
  }

  /**
   * Merges the data between both publishers side and subscribers side as per
   * the conflict resolver is set.
   *
   * @throws RepException
   */
  public synchronized void synchronize() throws RepException {
    _PubImpl publication = null;
    Object[] serverInfo = null;
    Object[] pubLastSyncId = null;
    BufferedWriter bw = null;
    Statement stmt = null;
    ResultSet rs = null;
    boolean isCurrentTableCyclic = false, islockedTaken = false;
    String localMachineAddress=null,remoteMachineAddress=null;
    try {
      try {
        _ReplicationServerImpl remoteRepServer =  getRemoteReplicationServer();
        publication = remoteRepServer.getRemotePublication(pubName);
       remoteMachineAddress = remoteRepServer.getRemoteAddress();
        publication.checkForLock(subName);
        islockedTaken = true;
        //Creates synchronization related files over server and transfers it over client socket. Besides it creates server socket over server
        //and returns serversocket related information.
        localMachineAddress =InetAddress.getLocalHost().getHostAddress();
        serverInfo = publication.createXMLForClient(subName, getServerName(),isSchemaSupported(),fileUpload,localMachineAddress);
        pubLastSyncId = ( (Object[]) serverInfo[2]);
      }
      catch (RemoteException ex1) {
        RepConstants.writeERROR_FILE(ex1);
        RepException rex = new RepException("REP001",new Object[] {ex1.getMessage()});
        rex.setStackTrace(ex1.getStackTrace());
        throw rex;
      }
      catch (Exception ex1) {
        RepConstants.writeERROR_FILE(ex1);
        RepException rex = new RepException("REP057", new Object[] {ex1.getMessage()});
        rex.setStackTrace(ex1.getStackTrace());
        throw rex;
      }

      try {
        // unzipping zip file
       if(!localMachineAddress.equalsIgnoreCase(remoteMachineAddress)) {
         ZipHandler.unZip(PathHandler.getDefaultZIPFilePathForClient("server_" +pubName + "_" + subName),PathHandler.getDefaultFilePathForClient("server_" +pubName + "_" + subName));
       }
      }
      catch (IOException ex) {
        RepConstants.writeERROR_FILE(ex);
        throw new RepException("REP084", null);
      }

      try {
        Connection subConnection = connectionPool.getConnection(subName);
        stmt = subConnection.createStatement();
//        String transactionLogURL = PathHandler.getDefaultTransactionLogFilePathForSubscriber(subName);
//        FileOutputStream fos = new FileOutputStream(transactionLogURL, true);
//        OutputStreamWriter os = new OutputStreamWriter(fos);
//        bw = new BufferedWriter(os);
        SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
        //saxParser.setProperty("validation",new Boolean(false));
        XMLReader reader = saxParser.getXMLReader();

        MergeHandler mg = new MergeHandler(true,
                                           connectionPool.getConnection(subName), this,
                                           publication.getServerName(),
                                           dbHandler, bw, "MERGE REPLICATION",
                                           PathHandler.
                                           fullOrPartialTransactionLogFile(),
                                           Utility.getDatabaseMataData(connectionPool, subName));
        mg.setLocalName(subName);
        mg.setRemoteName(pubName);
        ContentHandler ch = mg;
        reader.setContentHandler(ch);
//        AbstractSynchronize.writeDateInTransactionLogFile(bw);

        //initializing hashmap for maxSyncId for updating consideredId of bookMarkTable further
        syncIdMap = new HashMap();
        for (int i = 0; i < subRepTables.size(); i++) {
        RepTable repTable  =( (RepTable) subRepTables.get(i));
        String tableName = repTable.getSchemaQualifiedName().toString();
        StringBuffer query = new StringBuffer();
        isCurrentTableCyclic = repTable.getCyclicDependency().equalsIgnoreCase(RepConstants.YES);
        query.append(" select max(").append(RepConstants.shadow_sync_id1).append(") from ").append(RepConstants.shadow_Table(tableName));
        rs = stmt.executeQuery(query.toString());
        rs.next();
        syncIdMap.put(tableName, new Long(rs.getLong(1)));
        log.debug("tableName:" + tableName + " syncid: " + rs.getLong(1));
        }
        reader.parse(PathHandler.getDefaultFilePathForClient("server_" + pubName + "_" + subName));
        mg.closeAllStatementAndResultset();
//        AbstractSynchronize.writeOperationInTransactionLogFile(bw, mg.insert,mg.update, mg.delete, "MERGE");
         makeSubscriberTransactionLgFile(subName,mg,bw,"MERGE");

        // Second pass
        if (isCurrentTableCyclic) {
          MergeHandler mg1 = new MergeHandler(false,
                                              connectionPool.getConnection(subName), this,
                                              publication.getServerName(),
                                              dbHandler, bw,
                                              "MERGE REPLICATION",
                                              PathHandler.
                                              fullOrPartialTransactionLogFile(),
                                              Utility.getDatabaseMataData(connectionPool, subName));
          mg1.setLocalName(subName);
          mg1.setRemoteName(pubName);
          ContentHandler ch1 = mg1;
          reader.setContentHandler(ch1);
//          AbstractSynchronize.writeDateInTransactionLogFile(bw);
          reader.parse(PathHandler.getDefaultFilePathForClient("server_" +pubName+ "_" + subName));
//        AbstractSynchronize.writeOperationInTransactionLogFile(bw, mg.insert,mg.update, mg.delete, "MERGE");
          makeSubscriberTransactionLgFile(subName,mg,bw,"MERGE");
          mg1.closeAllStatementAndResultset();
        }

        //updating consideredId on subscriber side
        for (int i = 0; i < subRepTables.size(); i++) {
          String tableName = ( (RepTable) subRepTables.get(i)).getSchemaQualifiedName().toString();
          if ( ( (RepTable) subRepTables.get(i)).getCreateShadowTable().equalsIgnoreCase(RepConstants.NO))
            continue;
          UpdateConisderedForBookMarksTable(pubName, subName, tableName,(Long) syncIdMap.get(tableName),stmt);
        }
        syncIdMap.clear();
      }
      catch (Exception ex) {
        if(Utility.createTransactionLogFile) {
          AbstractSynchronize.writeUnsuccessfullOperationInTransaction(bw);
        }
        RepConstants.writeERROR_FILE(ex);
        RepException rep = null;
        if (ex instanceof SAXException) {
          Exception e = ( (SAXException) ex).getException();
          if (e instanceof RepException) {
            throw (RepException) e;
          }
          else {
            rep = new RepException("REP057", new Object[] {e.getMessage()});
            rep.setStackTrace(e.getStackTrace());
            throw rep;
          }
        }
        else {
          rep = new RepException("REP057", new Object[] {ex.getMessage()});
          rep.setStackTrace(ex.getStackTrace());
        }
        throw rep;
      }

      if (_Subscription.xmlAndShadow_entries) {
        // deleting xml file
        deleteFile(PathHandler.getDefaultFilePathForClient("server_" +pubName + "_" + subName));
        // deleting zip file
        deleteFile(PathHandler.getDefaultZIPFilePathForClient("server_" +pubName + "_" + subName));
      }
      //going to update lastsyncId in bookmark table on publisher side

      try {
        publication.updateBookMarkLastSyncId(subName, pubLastSyncId);
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
        throw new RepException("REP057", new Object[] {subName, ex.getMessage()});
      }

// CREATE n Write XML File from Client Side to Socket for server Side.
      try {
        if (subRepTables.size() > 0) {
          // creating and writing XML file ON SOCKET for server
         Object[] clientTablesAndLastId =
         syncXMLCreator.createXMLFile(PathHandler.getDefaultFilePathForClient("client_" + subName + "_" + pubName),
                                    PathHandler.getDefaultZIPFilePathForClient("client_" + subName +"_" + pubName),
                                    "client_" + subName + "_" + pubName /*+ ".xml"*/, pubName,subRepTables,publication.getServerName(),subRepTables.size(),
         _Subscription.xmlAndShadow_entries,subName,isSchemaSupported(),publication.getFileUploader(),localMachineAddress,remoteMachineAddress);
          ArrayList usedActualTables = (ArrayList) clientTablesAndLastId[0];
          Object[] subLastSyncId = (Object[]) clientTablesAndLastId[1];
          dbHandler.deleteRecordsFromSuperLogTable(stmt);
          //This method let publisher to accept the data written on the publisher's socket by the subscriber. And parse it.
          //And deletes records from pub's log table.And perform other table operations.
          publication.synchronize(subName, getServerName(),Utility.createTransactionLogFile,localMachineAddress);
          for (int i = 0; i < subRepTables.size(); i++) {
            RepTable repTable = (RepTable) subRepTables.get(i);
            if (repTable.getCreateShadowTable().equalsIgnoreCase(RepConstants.NO))
              continue;
            String tableName = ( (RepTable) subRepTables.get(i)).getSchemaQualifiedName().toString();
            updateBookMarkLastSyncId(tableName, pubName, subLastSyncId[i],stmt);
          }
          if (_Subscription.xmlAndShadow_entries) {
            deletingRecordsFromShadowTable(usedActualTables, stmt);
          }
        }
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
        if (ex instanceof RepException) {
          throw (RepException) ex;
        }
        RepException rex = new RepException("REP054", new Object[] {subName,ex.getMessage()});
        rex.setStackTrace(ex.getStackTrace());
        throw rex;
      }
    }
    catch (RepException rex) {
      RepConstants.writeERROR_FILE(rex);
      throw rex;
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
      try {
        if (islockedTaken)
          publication.releaseLOCK();
      }
      catch (RemoteException ex2) {
        RepConstants.writeERROR_FILE(ex2);
      }
      try {
        if (bw != null)
          bw.close();
        if (rs != null) {
          rs.close();
        }
        if (stmt != null)
          stmt.close();
      }
      catch (IOException ex4) {
      }
      catch (SQLException ex4) {
      }
    }
  }

  /**
   * In Push Replication records are uploaded in publisher database.
   * Subscriber create a XML file of records which  considered for
   * synchronization and write it on client synchronization path.
   * Publisher read the zip file of client records.After parsing
   * Operation are performed on Publisher database as per rule.
   */

  public synchronized void push() throws RepException {
    _PubImpl publication = null;
    Statement stmt = null;
    ResultSet rs = null;
    Connection subConnection = null;
    boolean islockedTaken = false;
    String localMachineAddress=null,remoteMachineAddress=null;
    try {
      subConnection = connectionPool.getConnection(subName);

      try {
        _ReplicationServerImpl remoteRepServer =getRemoteReplicationServer();
        publication = remoteRepServer.getRemotePublication(pubName);
        remoteMachineAddress =remoteRepServer.getRemoteAddress();
         localMachineAddress =InetAddress.getLocalHost().getHostAddress();
      }
      catch (RemoteException ex1) {
        RepConstants.writeERROR_FILE(ex1);
        RepException rex = new RepException("REP001",new Object[] {ex1.getMessage()});
        rex.setStackTrace(ex1.getStackTrace());
        throw rex;
      }
      catch (Exception ex1) {
        RepConstants.writeERROR_FILE(ex1);
        RepException rex = new RepException("REP0106",new Object[] {ex1.getMessage()});
        rex.setStackTrace(ex1.getStackTrace());
        throw rex;
      }
// CREATE n Write XML File from Client Side to Socket for server Side.
      try {
        stmt = subConnection.createStatement();
        publication.checkForLock(subName);
        islockedTaken = true;
        if (subRepTables.size() > 0) {
          // creating and writing XML file ON SOCKET for server
          Object[] clientTablesAndLastId = syncXMLCreator.createXMLFile(
              PathHandler.getDefaultFilePathForClient("client_" + subName + "_" +pubName),
              PathHandler.getDefaultZIPFilePathForClient("client_" + subName +"_" + pubName), "client_" + subName + "_" + pubName /*+ ".xml"*/,
              pubName,subRepTables,publication.getServerName(),subRepTables.size(),
              _Subscription.xmlAndShadow_entries, subName,isSchemaSupported(),publication.getFileUploader(),localMachineAddress,remoteMachineAddress);
          ArrayList usedActualTables = (ArrayList) clientTablesAndLastId[0];
          Object[] subLastSyncId = (Object[]) clientTablesAndLastId[1];
          dbHandler.deleteRecordsFromSuperLogTable(stmt);
          /* This method let publisher to accept the data written on the
             publisher's socket by the subscriber. And parse it.
             And deletes records from pub's log table.
             And perform other table operations.
           */
          //initializing hashmap for maxSyncId for updating consideredId of bookMarkTable further

          syncIdMap = new HashMap();
          for (int i = 0; i < subRepTables.size(); i++) {
            String tableName = ( (RepTable) subRepTables.get(i)).getSchemaQualifiedName().toString();
            StringBuffer query = new StringBuffer();
            query.append(" select max(").append(RepConstants.shadow_sync_id1).append(") from ").append(RepConstants.shadow_Table(tableName));
            rs = stmt.executeQuery(query.toString());
            rs.next();
            syncIdMap.put(tableName, new Long(rs.getLong(1)));
          }
          publication.push(subName, getServerName(),Utility.createTransactionLogFile,localMachineAddress);
          for (int i = 0; i < subRepTables.size(); i++) {
            String tableName = ( (RepTable) subRepTables.get(i)).getSchemaQualifiedName().toString();
            updateBookMarkLastSyncId(tableName, pubName, subLastSyncId[i],stmt);
            UpdateConisderedForBookMarksTable(pubName, subName, tableName, (Long) syncIdMap.get(tableName),stmt);
          }
          syncIdMap.clear();
          if (_Subscription.xmlAndShadow_entries) {
            deletingRecordsFromShadowTable(usedActualTables, stmt);
          }
        }
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
        if (ex instanceof RepException) {
          throw (RepException) ex;
        }
        RepException rex = new RepException("REP0105", new Object[] {subName, ex.getMessage()});
        rex.setStackTrace(ex.getStackTrace());
        throw rex;
      }
    }
    catch (RepException rex) {
      RepConstants.writeERROR_FILE(rex);
      throw rex;
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);

      try {
        if (islockedTaken)
          publication.releaseLOCK();
      }
      catch (RemoteException ex2) {
        RepConstants.writeERROR_FILE(ex2);
      }
    }
  }

  /**
   * In pull replication publisher performed his operation
   * on subscriber database. Subscriber read the XML file and
   * parse it. After that MergerHandler get the  element
   * one by one and performed the requitred DML operation
   * on subscriber database.
   * @throws RepException
   */

  public synchronized void pull() throws RepException {
    _PubImpl publication = null;
    String localAddress = null,remoteMachineAddress=null;
    Object[] pubLastSyncId;
    BufferedWriter bw = null;
    Statement stmt = null;
    ResultSet resultSet = null;
    Connection subConnection = null;
    boolean islockedTaken = false, isCurrentTableCyclic = false;
    try {
      try {
        subConnection = connectionPool.getConnection(subName);
        stmt = subConnection.createStatement();
        _ReplicationServerImpl remoteRepServer = getRemoteReplicationServer();
        publication = remoteRepServer.getRemotePublication(pubName);
        remoteMachineAddress = remoteRepServer.getRemoteAddress();
        publication.checkForLock(subName);
        islockedTaken = true;
        localAddress = InetAddress.getLocalHost().getHostAddress();

        /*
         Creates synchronization related files over server and transfers
         it over client socket. Besides it creates server socket over server
         and returns serversocket related information.
         */
        long startTime = System.currentTimeMillis();
        Object[] serverInfo = publication.createXMLForClient(subName, getServerName(),isSchemaSupported(),fileUpload,localAddress);
        pubLastSyncId = (Object[]) serverInfo[2];
      }
      catch (RemoteException ex1) {
        RepConstants.writeERROR_FILE(ex1);
        RepException rex = new RepException("REP001",new Object[] {ex1.getMessage()});
        rex.setStackTrace(ex1.getStackTrace());
        throw rex;
      }
      catch (Exception ex1) {
        RepConstants.writeERROR_FILE(ex1);
        RepException rex = new RepException("REP0152", new Object[] {ex1.getMessage()});
        rex.setStackTrace(ex1.getStackTrace());
        throw rex;
      }

      try {
        // unzipping zip file
        if(!localAddress.equalsIgnoreCase(remoteMachineAddress)) {
          ZipHandler.unZip(PathHandler.getDefaultZIPFilePathForClient("server_" +pubName + "_" + subName),PathHandler.getDefaultFilePathForClient("server_" +pubName + "_" + subName));
        }
      }
      catch (IOException ex) {
        RepConstants.writeERROR_FILE(ex);
        throw new RepException("REP084", null);
      }
      try {

//        String transactionLogURL = PathHandler.getDefaultTransactionLogFilePathForSubscriber(subName);
//        FileOutputStream fos = new FileOutputStream(transactionLogURL, true);
//        OutputStreamWriter os = new OutputStreamWriter(fos);
//        bw = new BufferedWriter(os);

        SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
        // saxParser.setProperty("validation",new Boolean(false));
        XMLReader reader = saxParser.getXMLReader();
//        long startTime = System.currentTimeMillis();
        MergeHandler mg = new MergeHandler(true,connectionPool.getConnection(subName), this,publication.getServerName(),dbHandler, bw, "PULL REPLICATION",PathHandler.fullOrPartialTransactionLogFile(),Utility.getDatabaseMataData(connectionPool, subName));
        mg.setLocalName(subName);
        mg.setRemoteName(pubName);
        ContentHandler ch = mg;
        reader.setContentHandler(ch);
//        AbstractSynchronize.writeDateInTransactionLogFile(bw);
        //initializing hashmap for maxSyncId for updating consideredId of bookMarkTable further

        syncIdMap = new HashMap();

        for (int i = 0; i < subRepTables.size(); i++) {
          RepTable repTable = ( (RepTable) subRepTables.get(i));
          String tableName = repTable.getSchemaQualifiedName().toString();
          StringBuffer query = new StringBuffer();
          isCurrentTableCyclic = repTable.getCyclicDependency().
              equalsIgnoreCase(RepConstants.YES);
          query.append(" select max(").append(RepConstants.shadow_sync_id1).
              append(") from ").append(RepConstants.shadow_Table(tableName));
          resultSet = stmt.executeQuery(query.toString());
          resultSet.next();
          syncIdMap.put(tableName, new Long(resultSet.getLong(1)));
        }
        reader.parse(PathHandler.getDefaultFilePathForClient("server_" +pubName + "_" + subName));
        mg.closeAllStatementAndResultset();
//System.out.println(" TIME TAKEN IN PERFORMING OPERATION ON DATABASE :: "+(System.currentTimeMillis()-startTime));

//      AbstractSynchronize.writeOperationInTransactionLogFile(bw, mg.insert,mg.update, mg.delete, "PULL");

        makeSubscriberTransactionLgFile(subName,mg,bw,"PULL");


        /**
         * Following code has been written to handler the case
         * of cyclic table and complete the cyclic work in two
         * pass.In second pass value of all the colums is updated
         * by actual values that are set to null in first pass.
         */
        if (isCurrentTableCyclic) {
          MergeHandler mg1 = new MergeHandler(false,connectionPool.getConnection(subName), this,
                                              publication.getServerName(),
                                              dbHandler, bw,
                                              "PULL REPLICATION",
                                              PathHandler.fullOrPartialTransactionLogFile(),
                                              Utility.getDatabaseMataData(connectionPool, subName));
          mg1.setLocalName(subName);
          mg1.setRemoteName(pubName);
          ContentHandler ch1 = mg1;
          reader.setContentHandler(ch1);
          AbstractSynchronize.writeDateInTransactionLogFile(bw);
          reader.parse(PathHandler.getDefaultFilePathForClient("server_" +pubName+ "_" + subName));
          mg1.closeAllStatementAndResultset();
        }
        try {
          for (int i = 0; i < subRepTables.size(); i++) {
            String tableName = ( (RepTable) subRepTables.get(i)).getSchemaQualifiedName().toString();
            UpdateConisderedForBookMarksTable(pubName, subName, tableName,(Long) syncIdMap.get(tableName),stmt);
          }
          syncIdMap.clear();
        }
        catch (SQLException ex) {
          RepConstants.writeERROR_FILE(ex);
          throw new RepException("REP0152", new Object[] {subName,ex.getMessage()});
        }
        makeSubscriberTransactionLgFile(subName,mg,bw,"PULL");
//        AbstractSynchronize.writeOperationInTransactionLogFile(bw, mg.insert,mg.update, mg.delete, "PULL");
      }
      catch (Exception ex) {
        if(Utility.createTransactionLogFile) {
          AbstractSynchronize.writeUnsuccessfullOperationInTransaction(bw);
        }
        RepConstants.writeERROR_FILE(ex);
        RepException rep = null;
        if (ex instanceof SAXException) {
          Exception e = ( (SAXException) ex).getException();
          if (e instanceof RepException) {
            throw (RepException) e;
          }
          else {
            rep = new RepException("REP0152", new Object[] {e.getMessage()});
            rep.setStackTrace(e.getStackTrace());
            throw rep;
          }
        }
        else {
          rep = new RepException("REP0152", new Object[] {ex.getMessage()});
          rep.setStackTrace(ex.getStackTrace());
        }
        throw rep;
      }

      if (_Subscription.xmlAndShadow_entries) {
        // deleting xml file
        deleteFile(PathHandler.getDefaultFilePathForClient("server_" +pubName + "_" + subName));
        // deleting zip file
        deleteFile(PathHandler.getDefaultZIPFilePathForClient("server_" +pubName + "_" + subName));
      }
      try {
        publication.updatePublisherShadowAndBookmarkTableAfterPullOnSubscriber(subName, pubLastSyncId);
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
        throw new RepException("REP0152", new Object[] {subName,ex.getMessage()});
      }
    }
    catch (RepException rex) {
      RepConstants.writeERROR_FILE(rex);
      throw rex;
    }
    finally {
      try {
        if (bw != null)
          bw.close();
        connectionPool.removeSubPubFromMap(subName);
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
      }
      try {
        if (islockedTaken)
          publication.releaseLOCK();
      }
      catch (RemoteException ex2) {
        RepConstants.writeERROR_FILE(ex2);
      }

    }
  }

  /**
   * Added by Hisar Team
   * Return the instance of RepTable corresponding to table name passed
   * @param tableName String
   * @throws RepException
   * @return RepTable
   */
  public RepTable getRepTable(String tableName) throws RepException {
    for (int i = 0; i < subRepTables.size(); i++) {
      if ( ( (RepTable) subRepTables.get(i)).getSchemaQualifiedName().
          toString().equalsIgnoreCase(tableName)) {
        return (RepTable) subRepTables.get(i);
      }
    }
    RepException rep = new RepException("REP017", new Object[] {tableName});
    RepConstants.writeERROR_FILE(rep);
    throw rep;
  }

  public void setFilterClause(String tableName, String filterClause) {
    for (int i = 0; i < subRepTables.size(); i++) {
      if ( ( (RepTable) subRepTables.get(i)).getSchemaQualifiedName().
          toString().equalsIgnoreCase(tableName)) {
        ( (RepTable) subRepTables.get(i)).setFilterClause(filterClause);
      }
    }
  }

  public String getServerName() throws RemoteException {
    return dbHandler.getLocalServerName();
  }

  public AbstractDataBaseHandler getDBDataTypeHandler() {
    return dbHandler;
  }

  public String getPub_SubName() {
    return subName;
  }

  private void deleteFile(String fileName) {
    File f = new File(fileName);
    boolean deleted = f.delete();
  }

  private void deleteALLRecordsFromSuperLogTable(Statement stmt) throws  SQLException {
    stmt.executeUpdate("delete from " + dbHandler.getLogTableName());
  }

  private void deleteRecordsFromSuperLogTable1(Statement subStatment) throws SQLException {
    // insert one record in superLogTable
    StringBuffer query = new StringBuffer();
    query.append("insert into ").append(dbHandler.getLogTableName()).append(" (").
        append(RepConstants.logTable_tableName2).append(") values  ('$$$$$$')");

    subStatment.execute(query.toString());

    query = new StringBuffer();
    // deleting all but one last record from super log table where commonid is maximum
    query.append("Select max (").append(RepConstants.logTable_commonId1).
        append(") from ").append(dbHandler.getLogTableName());
    ResultSet rs = subStatment.executeQuery(query.toString());
    rs.next();
    long maxCID = rs.getLong(1);

    query = new StringBuffer();

    query.append("delete from ").append(dbHandler.getLogTableName()).append(
        " where ")
        .append(RepConstants.logTable_commonId1).append(" !=").append(maxCID);
    int deletedNo = subStatment.executeUpdate(query.toString());
    log.debug(query.toString());
  }

  private void deletingRecordsFromShadowTable(ArrayList usedActualTables,Statement stmt) throws SQLException, RepException {

      int noOFTables = usedActualTables.size();
      StringBuffer query;
      if (noOFTables > 0) {
        for (int i = 0; i < noOFTables; i++) {
          Object minValue= dbHandler.getMinValOfSyncIdTodeleteRecordsFromShadowTable((String)
                     usedActualTables.get(i),stmt);
                 if (minValue instanceof BigDecimal) {
                   minValue = new Long( ( (BigDecimal) minValue).longValue());
                 }
                 else if (minValue instanceof Double) {
                   minValue = new Long( ( (Double) minValue).longValue());
                 }else if (minValue instanceof Long) {
                   minValue = new Long( ( (Long) minValue).longValue());
                 }else if (minValue instanceof Integer) {
                   minValue = new Long( ( (Integer) minValue).longValue());
                 }

          // deleting records from shadow table for that table
          query = new StringBuffer();
          query.append("delete from ").append(RepConstants.shadow_Table( (
              String) usedActualTables.get(i))).append(" where  ").append(
              RepConstants.shadow_sync_id1).append(" < ").append(minValue);
          int count = stmt.executeUpdate(query.toString());
        }
      }
    }

  private void deletingRecordsFromShadowTable_old(ArrayList usedActualTables) {
    Statement stmt = null;
    try {
      int noOFTables = usedActualTables.size();
      StringBuffer query;
      if (noOFTables > 0) {
        Connection conn = connectionPool.getConnection(subName);
        stmt = conn.createStatement();
        for (int i = 0; i < noOFTables; i++) {
          String shadowTable = RepConstants.shadow_Table( (String)
              usedActualTables.get(i));
          query = new StringBuffer();
          query.append("SELECT MAX(").append(RepConstants.shadow_sync_id1).
              append(") from ").append(shadowTable);
          ResultSet rs = stmt.executeQuery(query.toString());
          rs.next();
          long maxSyncId = rs.getLong(1);

          query = new StringBuffer();
          query.append("delete from ").append(shadowTable).append(" where ")
              .append(RepConstants.shadow_sync_id1).append(" != ").append(maxSyncId);
          int count = stmt.executeUpdate(query.toString());
          log.debug(query.toString());
        }
      }
    }
    catch (Exception ex) {
      log.debug(ex);
      RepConstants.writeERROR_FILE(ex);
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
      try {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ex1) {
      }
    }
  }

  private void UpdateConisderedForBookMarksTable(String pubName,
                                                 String subName,
                                                 String tableName,
                                                 Long syncId, Statement stmt) throws
      SQLException, RepException {

    StringBuffer query = new StringBuffer();

    /*query.append(" select max(").append(RepConstants.shadow_sync_id1).append(
         ") from ")
         .append(RepConstants.shadow_Table(tableName));
     ResultSet rs = st.executeQuery(query.toString());
     rs.next();
     long maxSyncId = rs.getLong(1);

         query = new StringBuffer();*/
    Long maxSyncId = syncId;
    log.debug("maxSyncId" + maxSyncId);
    query.append(" UPDATE  ").append(dbHandler.getBookMarkTableName()).append(
        " set  ").append(RepConstants.bookmark_ConisderedId5)
        .append(" = ").append(maxSyncId).append("  where ").append(
        RepConstants.
        bookmark_LocalName1).append(" = '").append(subName).append("' and ").
        append(RepConstants.bookmark_RemoteName2)
        .append(" = '").append(pubName).append("' and ")
        .append(RepConstants.bookmark_TableName3).append(" = '").append(
        tableName).append("'");
    stmt.executeUpdate(query.toString());
    log.debug(query.toString());

  }

  /**
   * This method first gets the object of the replication server by
   * looking up the remote url and port. Then gets the publication object
   * from that remote object.
   *
   * @return
   * @throws RepException
   * @throws RemoteException
   */

  private _PubImpl getPublication() throws RepException, RemoteException {
    _ReplicationServerImpl serverRS = null;
     serverRS = getRemoteReplicationServer();
    _PubImpl pub = serverRS.getRemotePublication(pubName);
    if (pub == null) {
      throw new RepException("REP048", new Object[] {subName, pubName});
    }
    return pub;
  }

  /* It unsubscribe the subscriber as well as drop all related schedules */

  public void unsubscribe() throws RepException {
    Connection con = null;
    Statement stt = null;
    try {
      try {
        getPublication().dropSubscription(subName);
      }
      catch (RepException ex) {
        RepConstants.writeERROR_FILE(ex);
        throw ex;
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
        throw new RepException("REP046", new Object[] {subName, ex.getMessage()});
      }
      try {
//      Connection con = connectionPool.getConnection(subName);
        con = connectionPool.getFreshConnection(subName);
        dbHandler.setDefaultSchema(con);
        stt = con.createStatement();
        try {
          stt.execute(" delete from " + dbHandler.getSubscriptionTableName() +
                      " where " + RepConstants.subscription_subName1 + " = '" +
                      subName + "'");

          log.debug(" delete from " + dbHandler.getSubscriptionTableName() +
                    " where " + RepConstants.subscription_subName1 + " = '" +
                    subName + "'");
        }
        catch (SQLException ex) {
          RepConstants.writeERROR_FILE(ex);
          throw new RepException("REP037", new Object[] {subName});
        }
        try {
          localServer.getScheduleHandler().dropSchedule(subName);
        }
        catch (RepException ex) {
          if (! (ex.getRepCode() == "REP203")) {
            RepConstants.writeERROR_FILE(ex);
            throw ex;
          }
        }
        deleteNonSharedPublishedSubscribedTables(con, subName);
        localServer.refershSubscription(subName);
      }
      catch (SQLException ex) {
        RepConstants.writeERROR_FILE(ex);
      }
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
      try {
        if (stt != null)
          stt.close();
        if (con != null)
          con.close();
      }
      catch (SQLException ex1) {
      }

    }
  }

  private void deleteNonSharedPublishedSubscribedTables(Connection con,
      String pubsubName) throws SQLException, RepException {

    //" Select Table_Name from RepTable Where PubSub_Name = '"+pubName+"' and Table_Name Not In "
    //"( Select Distinct Table_Name from RepTable Where PubSub_Name <> '"+pubName+"') ";

    Statement stt = null;
    ResultSet rs = null;
    ResultSet rsPub = null;

    try {
      StringBuffer tablesToDelete = new StringBuffer();
      tablesToDelete.append(" Select ").append(RepConstants.repTable_tableName2)
          .append(" from ").append(dbHandler.getRepTableName()).append(
          " where ")
          .append(RepConstants.repTable_pubsubName1).append(" = '").append(
          pubsubName)
          .append("' and ").append(RepConstants.repTable_tableName2)
          .append(" not in ( Select Distinct ").append(RepConstants.
          repTable_tableName2)
          .append(" from ").append(dbHandler.getRepTableName())
          .append(" Where ").append(RepConstants.repTable_pubsubName1)
          .append(" <> '").append(pubsubName).append("') ");
      stt = con.createStatement();
      rs = stt.executeQuery(tablesToDelete.toString());
      while (rs.next()) {
        MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool,
            pubsubName);
        SchemaQualifiedName sname = new SchemaQualifiedName(mdi,
            rs.getString(RepConstants.repTable_tableName2));
//            deleteAll(pubsubName, rs.getString(RepConstants.repTable_tableName2));
        deleteAll(pubsubName, sname.toString(), con);

      }
      stt.execute(" delete from " + dbHandler.getRepTableName() + " where " +
                  RepConstants.repTable_pubsubName1 + " = '" + pubsubName +"'");
      log.debug(" delete from " + dbHandler.getRepTableName() + " where " +
                RepConstants.repTable_pubsubName1 + " = '" + pubsubName + "'");
      stt.execute(" delete from " + dbHandler.getBookMarkTableName() +
                  " where " +
                  RepConstants.bookmark_LocalName1 + " = '" + pubsubName +"'");
      log.debug(" delete from " + dbHandler.getBookMarkTableName() +
                " where " +
                RepConstants.bookmark_LocalName1 + " = '" + pubsubName + "'");
      rs = stt.executeQuery("select * from " +
                            dbHandler.getSubscriptionTableName());

      boolean issubTableExist = rs != null ? rs.next() ? true : false : false;

      try {
        rsPub = stt.executeQuery("select * from " +
                                 dbHandler.getPublicationTableName());
      }
      catch (SQLException ex) {
        /**
         *  Ignore the exception becuase Publication table
         *  does not exist in Subscriber Database. It is the
         *  case when table is published and subscribed in
         *  same database.
         */
      }
      boolean ispubTableExist = rsPub != null ? rsPub.next() ? true : false : false;
      if (!ispubTableExist && !issubTableExist) {
        dbHandler.dropSubscriberSystemTables(con);
      }
    }
    finally {
      if (rs != null)
        rs.close();
      if (rsPub != null)
        rsPub.close();
      if (stt != null)
        stt.close();
    }
  }

  private void deleteAll(String pubsubName, String table, Connection con) throws
      SQLException,

      RepException {

    dbHandler.dropTriggersAndShadowTable(con, table, pubsubName);

//     String tableName = table.substring(table.indexOf('.')+1);
//    fireDropQuery(con, " drop trigger "+RepConstants.getInsertTriggerName(table));
//    fireDropQuery(con, " drop trigger "+RepConstants.getDeleteTriggerName(table));
//    fireDropQuery(con, " drop trigger "+RepConstants.getUpdateTriggerName(table));
//    fireDropQuery(con, " drop table "+RepConstants.shadow_Table(table));
//    Utility.getDatabaseHandler(connectionPool,pubsubName)
//        .dropSequences(con,RepConstants.seq_Name(tableName));
//    fireDropQuery(con, " delete from "+dbHandler.getLogTableName()+" where "+RepConstants.logTable_tableName2 + " = '" + table + "'");
  }

  public ArrayList getRepTables() {
    return subRepTables;
  }

  /* It saves the schedule information in the schedule table*/

  private void saveScheduleData(String schName, String subName,
                                String scheduleType,
                                String publicationServerName,
                                String publicationPortNo,
                                String recurrenceType,
                                String repType, long schTime, int counter) throws
      RepException {

    Connection connection = connectionPool.getConnection(subName);
    Statement stt = null;
    ResultSet rs = null;
    try {
      stt = connection.createStatement();
      StringBuffer insertQuery = new StringBuffer();
      insertQuery = insertQuery.append("insert into ").append(dbHandler.
          getScheduleTableName())
          .append(" values ( '").append(schName).append("', '")
          .append(subName).append("' , '").append(scheduleType).append(
          "' , '")
          .append(publicationServerName).append("', '")
          .append(publicationPortNo).append("', '")
          .append(recurrenceType).append("' , '")
          .append(repType).append("' , ").append(schTime).append(" , ").
          append(
          counter).append(" )");
      stt.executeUpdate(insertQuery.toString());
      log.info("add schedule query " + insertQuery.toString());

      localServer.getScheduleHandler().startSchedule(subName, schName,
          scheduleType,
          publicationServerName, publicationPortNo, recurrenceType, repType,
          schTime, counter);
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
//         ex.printStackTrace();
      try {
        StringBuffer query = new StringBuffer();
        query = query.append("select ").append(RepConstants.subscription_subName1).append(
            " from ").append(dbHandler.getScheduleTableName()).append(
            " where ").
            append(RepConstants.subscription_subName1).append(" = '").append(
            subName).append("'");
        rs = stt.executeQuery(query.toString());
        if (rs.next() == true) {
          throw new RepException("REP201", new Object[] {subName});
        }
      }
      catch (SQLException ex1) {
        RepConstants.writeERROR_FILE(ex1);
      }
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
      try {
        if (stt != null)
          stt.close();
        if (rs != null)
          rs.close();
      }
      catch (SQLException ex2) {
      }
    }
  }

  /**
   *
   * @param scheduleName String
   * @param subscriptionName String
   * @param scheduleType String
   * @param publicationServerName String
   * @param publicationPortNo String
   * @param recurrenceType String
   * @param replicationType String
   * @param startDateTime Timestamp
   * @param scheduleCounter int
   * @throws RepException
   */
  public void addSchedule(String scheduleName, String subscriptionName,
                          String scheduleType0, String publicationServerName,
                          String publicationPortNo,
                          String recurrenceType, String replicationType,
                          Timestamp startDateTime, int scheduleCounter) throws
      RepException {
    String scheduleType = null;
    scheduleType = scheduleType0;
    try {
      if (scheduleName == null) {
        throw new RepException("REP215", null);
      }
      _Subscription sub = localServer.getSubscription(subscriptionName);
      if (sub == null) {
        throw new RepException("REP037", new Object[] {subscriptionName});
      }
      if (publicationServerName == null) {
        throw new RepException("REP094", null);
      }
      if (publicationPortNo == null) {
        throw new RepException("REP095", null);
      }
      try {
        sub.setRemoteServerPortNo(Integer.parseInt(publicationPortNo));
      }
      catch (NumberFormatException ex) {
        throw new RepException("REP220", new Object[] {null});
      }
      if (replicationType == null) {
        throw new RepException("REP212", null);
      }
      if (scheduleType == null) {
        throw new RepException("REP222", new Object[] {null});
      }
      if (! ( (scheduleType.equalsIgnoreCase(RepConstants.scheduleType_nonRealTime)) ||
             (scheduleType.equalsIgnoreCase(RepConstants.scheduleType_realTime)))) {
        throw new RepException("REP223", new Object[] {null});
      }
      if (! (replicationType.equalsIgnoreCase(RepConstants.replication_snapshotType) ||
             replicationType.equalsIgnoreCase(RepConstants.replication_synchronizeType) ||
             replicationType.equalsIgnoreCase(RepConstants.replication_pullType) ||
             replicationType.equalsIgnoreCase(RepConstants.replication_pushType))) {
        throw new RepException("REP216", null);
      }
      long startTime = 0;
      if (scheduleType.equalsIgnoreCase(RepConstants.scheduleType_nonRealTime)) {
        startTime = startDateTime.getTime();
        if (startTime <= System.currentTimeMillis()) {
          throw new RepException("REP202", null);
        }
        if (recurrenceType == null) {
          throw new RepException("REP213", null);
        }
        if (! (recurrenceType.equalsIgnoreCase(RepConstants.recurrence_yearType) ||
               recurrenceType.equalsIgnoreCase(RepConstants.recurrence_monthType) ||
               recurrenceType.equalsIgnoreCase(RepConstants.recurrence_dayType) ||
               recurrenceType.equalsIgnoreCase(RepConstants.recurrence_hourType)||
               recurrenceType.equalsIgnoreCase(RepConstants.recurrence_minuteType))) {
          throw new RepException("REP218", null);
        }

        if (startDateTime == null) {
          throw new RepException("REP214", null);
        }
        if (! (scheduleCounter >= 0)) {
          throw new RepException("REP217", null);
        }
      }

      saveScheduleData(scheduleName, subscriptionName, scheduleType,
                       publicationServerName,
                       publicationPortNo, recurrenceType, replicationType,
                       startTime, scheduleCounter);
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
    }
  }

  /**
   * removeSchedule
   * @param scheduleName String
   * @param subscriptionName String
   */
  public void removeSchedule(String scheduleName0, String subName0) throws
      RepException {
    try {
      if (scheduleName0 == null) {
        throw new RepException("REP215", null);
      }
      _Subscription sub = localServer.getSubscription(subName0);
      if (sub == null) {
        throw new RepException("REP037", new Object[] {subName0});
      }

      localServer.getScheduleHandler().dropSchedule(subName0);
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
    }
  }

  /**
   * editSchedule
   *
   * @param scheduleName0 String
   * @param subName0 String
   * @param PubServerName String
   * @param PubPortNo String
   */
  public void editSchedule(String scheduleName0, String subName0,
                           String newPubServerName, String newPubPortNo) throws
      RepException {
    try {
      if (scheduleName0 == null) {
        throw new RepException("REP215", null);
      }
      _Subscription sub = localServer.getSubscription(subName0);
      if (sub == null) {
        throw new RepException("REP037", new Object[] {subName0});
      }
      if (newPubServerName == null) {
        throw new RepException("REP094", null);
      }
      if (newPubPortNo == null) {
        throw new RepException("REP095", null);
      }
      try {
        sub.setRemoteServerPortNo(Integer.parseInt(newPubPortNo));
      }
      catch (NumberFormatException ex) {
        throw new RepException("REP220", new Object[] {null});
      }
      localServer.getScheduleHandler().editSchedule(scheduleName0, subName0,
          newPubServerName, newPubPortNo);
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
    }
  }

  private void updateBookMarkLastSyncId(String tableName,
                                        String remote_Pub_Sub_Name,
                                        Object lastId, Statement stmt) throws
      Exception {
//        Object lastId = getLastUIDFromShadowTable(shadowTable);
    String updateQuery = "update " + dbHandler.getBookMarkTableName() +
        " set " +
        RepConstants.bookmark_lastSyncId4 + "=" + lastId + " where  " +
        RepConstants.bookmark_LocalName1 + " = '" + subName + "' and " +
        RepConstants.bookmark_RemoteName2 + " = '" + remote_Pub_Sub_Name +
        "' and " + RepConstants.bookmark_TableName3 + " = '" + tableName +
        "' ";
    stmt.executeUpdate(updateQuery);
  }

  public synchronized void updateSubscription() throws RepException {
    Statement stt = null;
    ResultSet rs = null;
    try {
      MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, subName);
      HashMap primCols = new HashMap();
      _PubImpl pub = getPublication();
      int pubVendorType = pub.getPubVendorName();
      Connection subConnection = connectionPool.getConnection(subName);
      //getting list of tables which after subscribing were dropped(even if dropped and added again by pub)
      ArrayList dropTableListFromSub = pub.dropTableListForSub(subName);
      //dropping table and unsubscribing them from replicator tables
      if (!dropTableListFromSub.isEmpty()){
        UnsubscribeAndDropTablesDroppedFromPub(subConnection,dropTableListFromSub);
      }
        //Returns [0] array of schemas [1]publicationtablequeries.
      ArrayList[] schemaTables = getPublicationTableQueries(primCols, pub);
      ArrayList dropTableList = new ArrayList();
      ArrayList oldSubRepTableList = new ArrayList();
      ArrayList newSubRepTableList = new ArrayList();
      stt = subConnection.createStatement();
      StringBuffer sb = new StringBuffer();
      //getting all the old subscribed tables list
      sb.append("select ").append(RepConstants.repTable_tableName2).append(
          " from ").append(dbHandler.getRepTableName()).append(" where ")
          .append(RepConstants.repTable_pubsubName1).append(" = '")
          .append(subName).append("'");
      rs = stt.executeQuery(sb.toString());
      while (rs.next()) {
        SchemaQualifiedName sname = new SchemaQualifiedName(mdi, rs.getString(1));
        String schema = sname.getSchemaName();
        String table = sname.getTableName();
        RepTable repTable = new RepTable(sname, RepConstants.publisher);
        //Sets Sorted primary key columns in repTable.
        mdi.setPrimaryColumns(repTable, schema, table);
        oldSubRepTableList.add(repTable.getSchemaQualifiedName());
      }

      String[] alterTableQueries = alterTableAddFKQueries == null ? null :
          (String[]) alterTableAddFKQueries.toArray(new String[0]);

      dbHandler.createSubscribedTablesTriggersAndShadowTables(subName,
          (String[]) (schemaTables[1]).toArray(new String[0]),
          alterTableQueries, primCols, pubVendorType, subRepTables);

      //Do Entry in the Subscription Table , BookMark Table, Rep Table.
      saveSubscriptionNewData(dbHandler);
      //get new subReptable list
      for (int i = 0; i < subRepTables.size(); i++) {
        RepTable repTable = (RepTable) subRepTables.get(i);
        newSubRepTableList.add(repTable.getSchemaQualifiedName());
      }
      //find if any table if was dropped from publisher.If any found,add to dropTableList
      for (int j = 0; j < oldSubRepTableList.size(); j++) {
        if (!newSubRepTableList.contains(oldSubRepTableList.get(j))) {
          dropTableList.add(oldSubRepTableList.get(j));
        }
      }
      StringBuffer deleteQuery = new StringBuffer();
      for (int j = 0; j < dropTableList.size(); j++) {
        //drop triggers,shadow tables,delete from logtable
        dbHandler.dropTriggersAndShadowTable(subConnection,
                                             dropTableList.get(j).toString(),subName);
        //delete entry from bookmarkTable
        deleteQuery.append(" delete from ").append(dbHandler.
            getBookMarkTableName()).append(" where ").append(RepConstants.
            bookmark_LocalName1).append(" = '").append(subName)
            .append("'").append(" and ").append(RepConstants.repTable_tableName2).append(
            " ='").append(dropTableList.get(j).toString()).append(" '").
            append(" and ").append(RepConstants.bookmark_RemoteName2).append(" ='").
            append(pubName).append(" '");
        stt.execute(deleteQuery.toString());
      }
      pub.saveSubscriptionNewData(subName);
    }

    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP318", new Object[] {ex.getMessage()});
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
      try {
        if (rs != null)
          rs.close();
      }
      catch (SQLException ex1) {
      }
      try {
        if (stt != null) {
          stt.close();
        }
      }
      catch (SQLException ex2) {
      }
    }
  }

  /**
   * Make an entry in the subscriptions table. And inserts all the records
   * corresponding to each replication table into the reptabe and Bookmark table.
   *
   * @param dbHandler
   * @throws SQLException
   * @throws RepException
   */

  private void saveSubscriptionNewData(AbstractDataBaseHandler dbHandler) throws
      SQLException, RepException {
    Connection connection = connectionPool.getConnection(subName);
    Statement stt = null;
    try {
      stt = connection.createStatement();
      //Delete all entries from Rep_table
      StringBuffer sb = new StringBuffer();
      sb.append("delete from ").append(dbHandler.getRepTableName()).append(" where ")
          .append(RepConstants.repTable_pubsubName1).append(" = '")
          .append(subName).append("'");
      log.debug("Delete all entries from Rep_table::" + sb.toString());
      stt.execute(sb.toString());

      //For each table makes an entry into the BookMark Table and Rep Table
      for (int i = 0, size = subRepTables.size(); i < size; i++) {
        RepTable repTable = (RepTable) subRepTables.get(i);
        repTable.setConflictResolver(conflictResolver);
        StringBuffer sb2 = new StringBuffer();
        sb2.append(" Insert into ").append(dbHandler.getBookMarkTableName())
            .append(" values ( '").append(subName).append("','")
            .append(pubName).append("','").append(repTable.getSchemaQualifiedName())
            .append("',").append("0,0,'N')");
        try {
          log.debug("Table Entry in BookMarkTable::" + sb2.toString());
          stt.execute(sb2.toString());
        }
        catch (SQLException ex1) {
        }
        dbHandler.saveRepTableData(connection, subName, repTable);
      }
    }
    catch (SQLException ex) {
      throw ex;
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
      try {
        if (stt != null) {
          stt.close();
        }
      }
      catch (SQLException ex2) {
      }
    }
  }

  /**
   * Getting snapshot of newly added tables or tables
   *  on which no replication operation was done
   *
   * @throws RepException
   */
  public synchronized void getSnapShotAfterUpdatingSubscriber() throws
      RepException {
    Connection subConnection = null;
    Statement stmt = null;
    _PubImpl publication = null;
    boolean islockedTaken = false,isCurrentTableCyclic = false;
    try {
      ServerSocket serverSocket = null;
      Socket socket = null;
      serverSocket = connectionPool.startServerSocket();
      String remoteServerName;

      String localAddress = null,remoteMachineAddress=null;
      try {
        ArrayList tablesForSnapShot = getTableListForSnapShotAfterUpdatingSub();
        _ReplicationServerImpl remoteServer = getRemoteReplicationServer();
        remoteServerName = remoteServer.getServerName();
        remoteMachineAddress = remoteServer.getRemoteAddress();
        publication = remoteServer.getRemotePublication(pubName);
        publication.checkForLock(subName);
        islockedTaken = true;
        localAddress = InetAddress.getLocalHost().getHostAddress();
        publication.createSnapShotAfterUpdateSub(localAddress,serverSocket.getLocalPort(),subName, tablesForSnapShot,isSchemaSupported(),fileUpload,localAddress);
      }
      catch (RepException ex) {
        log.error(ex.getMessage(), ex);
        throw ex;
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
        RepException rex = new RepException("REP053", new Object[] {subName,ex.getMessage()});
        rex.setStackTrace(ex.getStackTrace());
        throw rex;
      }
      try {
//        socket = serverSocket.accept();
//        InputStream is = socket.getInputStream();
//        FileOutputStream fos = new FileOutputStream(PathHandler.
//            getDefaultZIPFilePathForClient("snapshot_" + pubName+ "_" +subName));
//        byte[] buf = new byte[1024];
//        int len = 0;
//        while ( (len = is.read(buf)) > 0) {
//          fos.write(buf, 0, len);
//        }
//        fos.close();
//        is.close();
//        serverSocket.close();
//        socket.close();
        //unzipping the zip file
        if(!localAddress.equalsIgnoreCase(remoteMachineAddress)) {
          ZipHandler.unZip(PathHandler.getDefaultZIPFilePathForClient("snapshot_" + pubName + "_" + subName),PathHandler.getDefaultFilePathForClient("snapshot_" +pubName + "_" + subName));
        }

      }
      catch (IOException ex) {
        RepConstants.writeERROR_FILE(ex);
        RepException rex = new RepException("REP052", new Object[] {subName,ex.getMessage()});
        rex.setStackTrace(ex.getStackTrace());
        throw rex;
      }

      try {

        for (int i = 0; i < subRepTables.size(); i++) {
          RepTable repTable = ( (RepTable) subRepTables.get(i));
          isCurrentTableCyclic = repTable.getCyclicDependency().equalsIgnoreCase(RepConstants.YES);
          if(isCurrentTableCyclic)
          break;
        }
        SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
        XMLReader reader = saxParser.getXMLReader();
        subConnection = connectionPool.getConnection(subName);
        stmt = subConnection.createStatement();
        //Instance for content handler
        SnapshotHandler ch = new SnapshotHandler(true, subConnection, this,dbHandler, remoteServerName);
        //  instance for content hanedler
        ch.setPubName(pubName);
        ch.setSubName(subName);
        reader.setContentHandler(ch);
        reader.parse(PathHandler.getDefaultFilePathForClient("snapshot_" +pubName+ "_" +subName));
        ch.closeAllStatementAndResultset();

        if(isCurrentTableCyclic){
        SnapshotHandler ch1 = new SnapshotHandler(false, subConnection, this,dbHandler, remoteServerName); //  instance for content hanedler
        ch1.setPubName(pubName);
        ch1.setSubName(subName);
        reader.setContentHandler(ch1);
        reader.parse(PathHandler.getDefaultFilePathForClient("snapshot_" + pubName + "_" +subName+ "_" +subName));
        ch1.closeAllStatementAndResultset();
        }


        if (_Subscription.xmlAndShadow_entries) {
          // deleting xml file
          deleteFile(PathHandler.getDefaultFilePathForClient("snapshot_" +pubName+ "_" +subName));
          // deleting zip file
          deleteFile(PathHandler.getDefaultZIPFilePathForClient("snapshot_" +pubName+ "_" +subName));
        }
        dbHandler.deleteRecordsFromSuperLogTable(stmt);
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
        RepException rex = new RepException("REP053", new Object[] {subName,ex.getMessage()});
        rex.setStackTrace(ex.getStackTrace());
        throw rex;
      }
    }
    catch (RepException rex) {
      RepConstants.writeERROR_FILE(rex);
      throw rex;
    }
    finally {
      connectionPool.removeSubPubFromMap(subName);
      try {
        if (islockedTaken)
          publication.releaseLOCK();
      }
      catch (RemoteException ex2) {
        RepConstants.writeERROR_FILE(ex2);
      }

    }
  }

  private ArrayList getTableListForSnapShotAfterUpdatingSub() throws RepException {
    ResultSet rs = null;
    Connection connection = null;
    Statement stt = null;
    ArrayList tablesForSnapShot = new ArrayList();
    StringBuffer selectTableNames = new StringBuffer();
    selectTableNames.append("select ").append(RepConstants.bookmark_TableName3)
        .append(" from ").append(RepConstants.bookmark_TableName).append(" where ")
        .append(RepConstants.bookmark_lastSyncId4).append(" = 0 ")
        .append(" and ").append(RepConstants.bookmark_ConisderedId5).append(" = 0 ");
    try {
      connection = connectionPool.getConnection(subName);
      stt = connection.createStatement();
      rs = stt.executeQuery(selectTableNames.toString());
      while (rs.next()) {
        tablesForSnapShot.add(rs.getString(RepConstants.bookmark_TableName3).toLowerCase());
      }
    }
    catch (SQLException ex) {

    }
    catch (RepException ex1) {
      throw ex1;
    }
    finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (stt != null) {
          stt.close();
        }
      }
      catch (SQLException ex2) {
      }
      connectionPool.removeSubPubFromMap(subName);
    }
    return tablesForSnapShot;
  }

  private void UnsubscribeAndDropTablesDroppedFromPub(Connection
      subConnection, ArrayList dropTableList) throws RepException,
      RepException, SQLException {
    Statement stt = null;
    try {
      stt = subConnection.createStatement();
      for (int i = 0; i < dropTableList.size(); i++) {
        String tableName = dropTableList.get(i).toString();
//drop triggers,shadow tables,delete from logtable
        dbHandler.dropTriggersAndShadowTable(subConnection, tableName,subName);
        StringBuffer deleteQuery = new StringBuffer();
        //delete entry from bookmarkTable
        deleteQuery.append(" delete from ").append(dbHandler.
            getBookMarkTableName()).append(
            " where ").append(RepConstants.bookmark_LocalName1).append(" = '").append(
            subName).append("'").append(" and ").append(RepConstants.repTable_tableName2).
            append(" ='").append(tableName).append("'").append(
            " and ").append(RepConstants.bookmark_RemoteName2).append(" ='").
            append(pubName).append("'");
        log.debug(deleteQuery.toString());
        try {
          stt.execute(deleteQuery.toString());
        }
        catch (SQLException ex1) {
        }

        StringBuffer deleteRepTableQuery = new StringBuffer();
      //delete entry from bookmarkTable
      deleteRepTableQuery.append(" delete from ").append(dbHandler.getRepTableName())
         .append( " where ").append(RepConstants.repTable_pubsubName1).append( " = '").append(subName)
        .append( "'").append(" and ").append(RepConstants.repTable_tableName2).append(" ='")
        .append(tableName).append("'");
      log.debug(deleteRepTableQuery.toString());
      try {
        stt.execute(deleteRepTableQuery.toString());
      }
      catch (SQLException ex1) {
        throw ex1;
//        ex1.printStackTrace();
      }

        //dropping table from database
        String dropTable = "Drop table " + tableName;
        log.debug("dropTable::" + dropTable);
        try {
          stt.execute(dropTable);
        }
        catch (SQLException ex) {

//          ex.printStackTrace();
        }
      }
    }
    finally {
      try {
        if (stt != null)
          stt.close();
      }
      catch (Exception ex) {
      }
    }
  }

  private void makeSubscriberTransactionLgFile(String subName,MergeHandler mg,BufferedWriter bw,String replicationType) throws  Exception {
    if(Utility.createTransactionLogFile) {
      String transactionLogURL = PathHandler.getDefaultTransactionLogFilePathForSubscriber(subName);
      FileOutputStream fos = new FileOutputStream(transactionLogURL, true);
      OutputStreamWriter os = new OutputStreamWriter(fos);
      bw = new BufferedWriter(os);
      AbstractSynchronize.writeDateInTransactionLogFile(bw);
      AbstractSynchronize.writeOperationInTransactionLogFile(bw, mg.insert,mg.update, mg.delete, replicationType);
      bw.flush();
    }
 }

 private boolean isSchemaSupported() {
 return  dbHandler.isSchemaSupported();
 }




}
