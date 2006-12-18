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

import java.net.*;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.sql.*;
import java.util.*;
import com.daffodilwoods.replication.zip.ImportedTablesInfo;
import com.daffodilwoods.graph.DirectedGraph;
import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;
import com.daffodilwoods.replication.schedule.ScheduleHandler;
import org.apache.log4j.Logger;
import java.io.File;
import org.apache.log4j.PropertyConfigurator;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import com.daffodilwoods.graph.Vertex;
import javax.sql.*;

/**
 * This is the main class of Repliactor, any side whether publisher or
 * subscriber has to get instance of this class by method getInstance and
 * then connect to the required database by using method setDataSource.
 * Publisher can create publication by executing the method:
 * createPublication (get the instance of Publication class),
 * Subscriber can create subscription by executing the method:
 * createSubscription (get the instance of Subscription class).
 * After that the control is transferred to the publisher and subscriber.
 * and they start communicating to each other.
 * It also implements _ReplicationServerImpl interface. So it gives publication's
 * object when needed remotely(At the time of subscribing, snapshot, synchronize).
 *
 */

public class ReplicationServer
    extends UnicastRemoteObject implements _ReplicationServer, _ReplicationServerImpl {

  /**
   * Replication server name and url
   */
  private String name, url;

  /**
   * Port on which Replication server is running
   */
  private int port;

  /**
   * Mapping of connections
   */
  private ConnectionPool connectionPool;

  /**
   * Mapping of pulications
   */
  private TreeMap pubMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);

  /**
   * Mapping of subscriptions
   */

  public ScheduleHandler scheduleHandler;

  private TreeMap subMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
  private Connection defaultConnection;
  public String driver = "",URL = "",user,
                           password,
                           portNo,
                           databaseName,
                           databaseServerName,
                           dataBaseName,
                           dBPortNo,
                           vendorName;


  ArrayList tablesInCycle=null;

  protected static Logger log = Logger.getLogger(ReplicationServer.class.getName());

  private ReplicationServer(int port0, String url0) throws RemoteException {
    port = port0;
    url = url0;
  }

  /**
   * This method is responsible to get the Replication Server instance.
   * It starts a replication server at the specified port and m/c by starting
   * the rmi registry over the specified port and m/c and binding this object.
   *
   * @param     port0          Port no where we want to start the replication server.
   * @param     url0           System's name or ipaddress where to start replication server.
   * @return    rep            instance of replication server
   * @throws    RepException   if port is busy or m/c name is passed as localhost
   */
  public static ReplicationServer getInstance(int port0, String url0) throws RepException {
   //initialize and load log4j.properties file
    initializeLog4j();
    if (url0.equalsIgnoreCase("localhost")) {
      log.error("Url cannot be localhost");
      throw new RepException("REP004", null);
    }
    ReplicationServer rep;
    try {
      String ipadd = null;
      try {
        ipadd = InetAddress.getByName(url0).getHostAddress();
      }
            catch (UnknownHostException ex)
            {
//ex.printStackTrace();
        log.error(ex.getMessage(), ex);
        throw new RepException("REP004", new Object[]{ex.getMessage()});
      }
      rep = new ReplicationServer(port0, url0);
      Registry r = LocateRegistry.createRegistry(port0);

      //String url = "rmi://"+ url0 +":"+ Integer.toString(port0)+"/"+url0+"_"+port0;
      String url = "rmi://" + ipadd + ":" + Integer.toString(port0) + "/" +ipadd + "_" + port0;
      Naming.rebind(url, rep);
      log.info("URL " + url + " PORT " + port0);
    }
    catch (RemoteException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (MalformedURLException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP004", new Object[] {ex.getMessage()});
    }
    log.debug("ReplicationServer started");
    return (ReplicationServer) rep;
  }

  /**
   * This method is called to get the publication's object even after
   * creating it. It is well implemented in the method getPriPublication.
   *
   * @param    pubName         Name of the publication whose object is required.
   * @return   pub             Publication class' object.
   * @throws RemoteException
   * @throws RepException
   */

  public _Publication getPublication(String pubName) throws RemoteException, RepException {
    return getPriPublication(pubName);
  }

  /**
   * Returns the publication object. When called by the subscriber remotely.
   *
   * @param pubName
   * @return
   * @throws RemoteException
   * @throws RepException
   */

  public _PubImpl getRemotePublication(String pubName) throws RemoteException, RepException {
    return getPriPublication(pubName);
  }

  /**
   * This method returns the publications object, It is implementd for the
   * cases in which because of some reasons the publication's object can not
   * be gotten directly. As if the replication server is restarted,so this
   * method searches publications and reptable tables for gathering the required
   * information and to rebuild the publication's object.
   *
   * @param    pubName          Name of the publication
   * @return   pub              Publication's object.
   * @throws   RemoteException
   * @throws   RepException
   */

  private Publication getPriPublication(String pubName) throws RemoteException, RepException {
    Publication pub = (Publication) pubMap.get(pubName);
    //pub will be null if the remote replication server has been restarted
    if (pub != null) {
      return pub;
    }
    //connectionPool will be null if could not get connection from datasource
    if (connectionPool == null) {
      throw new RepException("REP002", null);
    }
    //Here it comes if replication server has been restarted
    Connection con = connectionPool.getConnection(pubName);
    MetaDataInfo mdis = Utility.getDatabaseMataData(connectionPool, pubName);
    ResultSet rs = null;
    PreparedStatement prStt1 = null;
    try {
      //searches in publication table
      prStt1 = con.prepareStatement(RepConstants.loadPublicationQuery);
      prStt1.setString(1, pubName);
      rs = prStt1.executeQuery();
      boolean f1 = rs.next();
//      if (!rs.next()) {
        if (!f1) {
        return null;
      }
      String conflictReolver = rs.getString(RepConstants.publication_conflictResolver2);
      String serverName = rs.getString(RepConstants.publication_serverName3);
      MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
      AbstractDataBaseHandler dataBaseHandler = Utility.getDatabaseHandler(connectionPool, pubName);
      //searches in reptable
      ArrayList tableNames = new ArrayList();
            prStt1 = con.prepareStatement(RepConstants.loadRepTableQuery);
            prStt1.setString(1, pubName);
            rs = prStt1.executeQuery();
      while (rs.next()) {
        String tableName = rs.getString(RepConstants.repTable_tableName2);
        String filterClause = rs.getString(RepConstants.repTable_filter_clause3);
        SchemaQualifiedName sname = new SchemaQualifiedName(mdis, tableName);
        RepTable repTable = new RepTable(sname, filterClause,RepConstants.publisher);
        repTable.setServerType(RepConstants.publisher);
        String createShadowTable = rs.getString(RepConstants.repTable_createshadowtable6);
        String cyclicDependency = rs.getString(RepConstants.repTable_cyclicdependency7);
        String rep_CR = rs.getString(RepConstants.repTable_conflict_resolver4);
        repTable.setConflictResolver(rep_CR == null ? conflictReolver : rep_CR);
        repTable.setCreateShadowTable(createShadowTable);
        repTable.setCyclicDependency(cyclicDependency);
        mdi.setPrimaryColumns(repTable, sname.getSchemaName(),sname.getTableName());
        mdi.setForeignKeyColumns(repTable, sname.getSchemaName(),sname.getTableName());
        mdi.setAllColumns(repTable, sname.getSchemaName(),sname.getTableName());
        dataBaseHandler.setIgnoredColumns(con, pubName, repTable);
        tableNames.add(repTable);
      }
            /** @todo   rs has been closed  */
            rs.close();
      try {
        name = InetAddress.getLocalHost().getHostName() + "_" + port;
      }
      catch (UnknownHostException ex) {
        ex.printStackTrace();
        RepConstants.writeERROR_FILE(ex);
        throw new RepException(ex.getMessage(), null);
      }
      pub = new Publication(connectionPool, pubName, name, this);
      pub.setConflictResolver(conflictReolver);
      pub.setPublicationTables(tableNames);
      pubMap.put(pubName, pub);
      log.info("Publisher name " + pubName + " conflictResolver " +conflictReolver + " TableNames " + tableNames.toString());
      return pub;
    }
    catch (SQLException ex) {
      log.debug("Returning Null Publicaiton");
      return null;
    }
    finally {
          try {
        if (prStt1 != null)
            prStt1.close();
          }
          catch (SQLException ex1) {
          }
    }
  }

  /**
   * It is responsible to get the default connection with the database.
   * Default connection implies the connection of the replication server.
   *
   * @param   driver0       The driver name of the concerned databse.
   * @param   url0          The URL of the cocerned database.
   * @param   user0         The username for the concerned databse.
   * @param   password0     The password for the concerned databse.
   * @throws  RepException  In case it can not get connection with
   *                        the database.
   */

  public void setDataSource(String driver0, String url0, String user0,
                            String password0) throws RepException {
    try {
      driver = driver0;
      URL = url0;
      connectionPool = new ConnectionPool(url0, driver0, user0, password0);
      connectionPool.setLocalAddress(url);
      connectionPool.setLocalPortNo(port);
      //Gets connection.
      defaultConnection = connectionPool.getDefaultConnection();
      AbstractDataBaseHandler adh = Utility.getDatabaseHandler(connectionPool,defaultConnection);
      scheduleHandler = new ScheduleHandler(this, adh);
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP0", new Object[] {ex.getMessage()});
    }
  }


  public void setDataSource(String  dataBaseName0,String user0,String password0, String dBPortNo0, String databaseServerName0, String vendorName0) throws RepException {
    dataBaseName =dataBaseName0;
    user =user0;
    password =password0;
    dBPortNo =dBPortNo0;
    databaseServerName =databaseServerName0;
    vendorName =vendorName0;
   try {
     DBDataSource dbs = new DBDataSource(dataBaseName0, user0, password0,databaseServerName0, dBPortNo0,vendorName0);
     DataSource datasource = dbs.getDataSource();
     defaultConnection = datasource.getConnection(user0,password0);
     connectionPool = new ConnectionPool(datasource, user0, password0);
//    defaultConnection = connectionPool.getDefaultConnection();
     AbstractDataBaseHandler adh = Utility.getDatabaseHandler(connectionPool,defaultConnection);
     scheduleHandler = new ScheduleHandler(this, adh);
   }
   catch (SQLException ex) {
     RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP0", new Object[] {ex.getMessage()});
   }
   catch (RepException ex) {
    throw ex;
   }
  }

  /**
   * Checks the following :
   * Publication alredy exists.
   * Specified Table(s) exists in database.
   * Each table must have primary key.
   * Sequence of tables specified ( Primary Table than Foreign Table )
   * Then, Create the instance of Publication, set the instance of RepTable [] to pub
   * Put it in Map
   *
   * @param   pubName        Name of the Publication
   * @param   tableNames     Names of the tables to be published.
   * @return  pub1           Publication class' object.
   * @throws  RepException   In case of violation of above checked conditions.
   */

  public _Publication createPublication(String pubName, String[] tableNames) throws RepException {
    log.info("Create Publication " + pubName);
    return createPublication(pubName,tableNames,null);
      }

  /**
   * Check the Dublicacy of tables in  given list
   * If dublicate table name occure then it through
   * the RepException because it further create the
   * problemes in Replication process.
   * @param pubTables ArrayList
   * @param sname SchemaQualifiedName
   * @param pubName String
   * @throws RepException
   */
  private void checkTablesDuplicyInPublication(ArrayList pubTables, SchemaQualifiedName sname, String pubName) throws RepException {
    if (pubTables.size() > 0) {
      for (int j = 0; j < pubTables.size(); j++) {
        RepTable repTable = (RepTable) pubTables.get(j);
        String sqname = repTable.getSchemaQualifiedName().toString();
        log.debug("Checking Table " + sqname +" if already existing in database");
        if (sqname.equals(sname.toString())) {
            throw new RepException("REP013", new Object[] {sname.toString(),pubName});
        }
      }
    }
  }

  public _Publication createPublicationBK(String pubName, String[] tableNames) throws RepException {
    //If database is not binded
    if (connectionPool == null) {
      throw new RepException("REP002", null);
    }
    //Checks in publication table in database

    if (checkPublicationExistance(pubName)) {
      //if(pubMap.containsKey(pubName))
      throw new RepException("REP014", new Object[] {pubName});
    }
    //Checks the properness of given table names
    if (tableNames == null || tableNames.length == 0) {
      throw new RepException("REP012", new Object[] {pubName});
    }

    TreeMap repTableMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    ArrayList pubTableList = new ArrayList();

    try {

      MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
      for (int i = 0; i < tableNames.length; i++) {
        //Converts Table names  to schema qualified name.
        SchemaQualifiedName sname = new SchemaQualifiedName(mdi, tableNames[i]);
        if (repTableMap.containsKey(sname.toString())) {
          throw new RepException("REP013", new Object[] {sname.toString(),pubName});
        }
        String schema = sname.getSchemaName();
        String table = sname.getTableName();
        //checks whether the table exists in the database or not.
        mdi.checkTableExistance(sname);
        schema = sname.getSchemaName();
        RepTable repTable = new RepTable(sname, RepConstants.publisher);
        //repTable.setConflictResolver(RepConstants.publisher_wins);
        //Sets Sorted primary key columns in repTable.
        mdi.setPrimaryColumns(repTable, schema, table);
        mdi.setForeignKeyColumns(repTable, schema, table);
        //Checks for First Primary key then foreign key tables else exception.
        mdi.checkTableSequenceAccordingForeignKey(schema, table, pubTableList);
        pubTableList.add(sname.toString().toUpperCase());
        repTableMap.put(sname.toString(), repTable);
      }
      //name = InetAddress.getLocalHost().getHostName()+"_"+pubName+"_"+port;
      name = InetAddress.getLocalHost().getHostName() + "_" + port;
      //Creates publication by setting it's DataBaseHandler &  XMLCreator
      Publication publ = new Publication(connectionPool, pubName, name, this);
      //Sets publication's repTable elements
      publ.setPublicationTables(new ArrayList(repTableMap.values()));
      pubMap.put(pubName, publ);
      return publ;
    }
    catch (RepException ex) {
      throw ex;
    }
    catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP031", new Object[] {pubName, ex.getMessage()});
    }
  }

  /**
   * This method returns the subscription's object, It is implementd for the
   * cases in which because of some reasons the subscription's object can not
   * be gotten directly. As if the replication server is restarted,so this
   * method searches subscriptions and reptable tables for gathering the required
   * information and to rebuild the subscription's object.
   *
   * @param      subName         Subscription's name
   * @return     sub             Subscription class' object
   * @throws     RepException    If could not set up connection with the database.
   */

  public _Subscription getSubscription(String subName) throws RepException {
    Subscription sub = (Subscription) subMap.get(subName);
    if (sub != null) {
      return sub;
    }
    if (connectionPool == null) {
      throw new RepException("REP002", null);
    }
    MetaDataInfo mds = Utility.getDatabaseMataData(connectionPool, subName);
    AbstractDataBaseHandler dataBaseHandler = Utility.getDatabaseHandler(connectionPool, subName);
    //Here it comes if the replication server has been restarted.
    PreparedStatement prStt1 = null;
    ResultSet rs = null;
    try {
      Connection con = connectionPool.getFreshConnection(subName);
      prStt1 = con.prepareStatement(RepConstants.loadSubscriptionQuery);
      prStt1.setString(1, subName);
       rs = prStt1.executeQuery();
      if (!rs.next()) {
        return null;
      }

      //Searches in Subscription table.
      String pubName = rs.getString(RepConstants.subscription_pubName2);
      String conflictReolver = rs.getString(RepConstants.subscription_conflictResolver3);
      String serverName = rs.getString(RepConstants.subscription_serverName4);
      ArrayList tableNames = new ArrayList();

      //Searches in the Rep Table
            prStt1 = con.prepareStatement(RepConstants.loadRepTableQuery);
            prStt1.setString(1, subName);
            rs = prStt1.executeQuery();
      if (!rs.next()) {
        return null;
      }

      sub = new Subscription(connectionPool, subName, serverName, this);
      ArrayList repTables = new ArrayList();
      do {
        String tableName = rs.getString(RepConstants.repTable_tableName2);
        String filterClause = rs.getString(RepConstants.repTable_filter_clause3);
        String createShadowTable = rs.getString(RepConstants.repTable_createshadowtable6);
        String cyclicDependency = rs.getString(RepConstants.repTable_cyclicdependency7);
        SchemaQualifiedName sname = new SchemaQualifiedName(mds, tableName);
        RepTable repTable = new RepTable(sname, filterClause, RepConstants.subscriber);
        repTable.setConflictResolver(conflictReolver);
        repTable.setServerType(RepConstants.subscriber);
        repTable.setCreateShadowTable(createShadowTable);
        repTable.setCyclicDependency(cyclicDependency);
        MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, subName);
        mdi.setPrimaryColumns(repTable, sname.getSchemaName(), sname.getTableName());
        mdi.setForeignKeyColumns(repTable, sname.getSchemaName(), sname.getTableName());
        mdi.setAllColumns(repTable, sname.getSchemaName(),sname.getTableName());
        dataBaseHandler.setIgnoredColumns(con, subName,repTable);
        repTables.add(repTable);
      }
      while (rs.next());
      sub.setPublicatonName(pubName);
      sub.setSubscriptionTables(repTables);
      subMap.put(subName, sub);
      log.info("Subscriber name " + subName + " Publication Name " + pubName);
      return sub;
    }
    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      return null;
    }
    catch (RemoteException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException(ex.getMessage(), null);
    }
    finally {
          try {
        if (rs != null) {
              rs.close();
              prStt1.close();
            }
          }
          catch (SQLException ex1) {
          }
        }
  }

  /**
   * Checks wheter the subscription with the same name already exists
   * in Subscription Table. Creates the subscription's object.
   * Then add an entry in the subscription Map.
   *
   * @param   subName           Name of the subscription.
   * @param   pubName           Name of the corresponind publication.
   * @return  sub               subscription class' object.
   * @throws  RepException      If the above check violets.
   */

  public _Subscription createSubscription(String subName, String pubName) throws RepException {
    log.debug("Create subscription " + subName + " with publication " + pubName);

    //Checks if the subscription with the same name already exists in Subscription Table
    if (checkSubscriptionExistance(subName)) {
      throw new RepException("REP023", new Object[] {subName});
    }
    try {
      name = InetAddress.getLocalHost().getHostName() + "_" + port;
      Subscription sub = new Subscription(connectionPool, subName, name, this);
      sub.setPublicatonName(pubName);
      subMap.put(subName, sub);
      log.info("created subscription " + subName + " with publication " +pubName);
      return sub;
    }
    catch (RepException ex) {
      log.error(ex.getMessage(), ex);
      RepConstants.writeERROR_FILE(ex);
      throw ex;
    }
    catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP042", new Object[] {subName, ex.getMessage()});
    }
  }

  public Connection getConnection(String pub_sub_Name) throws RepException {
    return connectionPool.getConnection(pub_sub_Name);
  }

  public Connection getDefaultConnection() {
    return defaultConnection;
  }

  /**
   * Searches into the publiactions table for the publication name.
   *
   * @param                pubName
   * @return               true if found, else false.
   * @throws RepException
   */

  private boolean checkPublicationExistance(String pubName) throws RepException {
    AbstractDataBaseHandler dbh = Utility.getDatabaseHandler(connectionPool,pubName);
    String pubs = " Select " + RepConstants.publication_pubName1 +
        " from " + dbh.getPublicationTableName() + "  where " +
        RepConstants.publication_pubName1 + " = '" + pubName + "'";
    Statement stt = null;
    try {
      stt = defaultConnection.createStatement();
      ResultSet rsPubs = stt.executeQuery(pubs);
      log.debug("Query checkPublicationExistance " + pubs);
      return rsPubs.next();
    }
    catch (SQLException ex) {
      // Ignore the Exception
    }
    finally {
      try {
        if (stt != null)
        stt.close();
      }
      catch (SQLException ex1) {
        // Ignore the Exception
      }
    }
    return false;
  }

  /**
   * Searches into the subscriptions table for the subscription name.
   *
   * @param                subName
   * @return               true if found, else false.
   * @throws RepException
   */

  private boolean checkSubscriptionExistance(String subName) throws RepException {
    AbstractDataBaseHandler dbh = Utility.getDatabaseHandler(connectionPool,subName);
    String pubs = " Select " + RepConstants.subscription_subName1
        + " from " + dbh.getSubscriptionTableName() + " where " +
        RepConstants.subscription_subName1 + " = '" + subName + "'";
    Statement stt = null;
    try {
      stt = defaultConnection.createStatement();
      ResultSet rsPubs = stt.executeQuery(pubs);
      log.debug("Query checkSubscriptionExistence " + pubs);
      return rsPubs.next();
    }
    catch (SQLException ex) {
      // Ignore the Exception
    }
    finally {
      try {
        if (stt != null)
        stt.close();
      }
      catch (SQLException ex1) {
        // Ignore the Exception
      }
    }
    return false;
  }

  public void refershPublication(String pubName) {
    pubMap.remove(pubName);
  }

  public void refershSubscription(String subName) {
    subMap.remove(subName);
  }

  public String getServerName() throws RemoteException {
    return name;
  }

  /**
   * This method returns the scheduleHandler instance which is used during
   *  add,edit and remove Schedule methods in subscription class.
   * @return ScheduleHandler
   */

  public ScheduleHandler getScheduleHandler() {
    return scheduleHandler;
  }

  /**
   * To add more table in Publication
   * Checks the following :
   * Publication  exists or not.
   * Specified Table(s) exists in database.
   * Each table must have primary key.
   * Sequence of tables specified ( Primary Table than Foreign Table )
   * @param   pubName        Name of the Publication
   * @param   tableNames     Names of the tables to be published.
   * @return  pub1           Publication class' object.
   * @throws  RepException   In case of violation of above checked conditions.
   */

  public void addTableToPublication(String pubName, String[] tableNames0,
                                    String[] filterClauses,
                                    ArrayList pubRepTables,
                                    Publication pub) throws RepException {
    Publication pub1 = pub;
    //If database is not binded
    if (connectionPool == null) {
      throw new RepException("REP002", null);
    }
    //Checks in publication table in database

    if (!checkPublicationExistance(pubName)) {
      //if(!pubMap.containsKey(pubName))
      throw new RepException("REP036", new Object[] {pubName});
    }
    //Checks the properness of given table names
    if (tableNames0 == null || tableNames0.length == 0) {
      throw new RepException("REP012", new Object[] {pubName});
    }
    ArrayList pubTableList = new ArrayList();
    ArrayList repTableList = new ArrayList();
    ArrayList existingPubTableList = new ArrayList();
    ArrayList newRepTableList = new ArrayList();
    String conflictResolver = null;
    try {
      conflictResolver = pub1.getConflictResolver();

      //get already existing tables in publisher
      for (int i = 0; i < pubRepTables.size(); i++) {
        RepTable repTable = (RepTable) pubRepTables.get(i);
        existingPubTableList.add(repTable.getSchemaQualifiedName().toString());
// System.out.println("existingPubTableList " +repTable.getSchemaQualifiedName().toString());
      }
        MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
        String[]  tableNames = mdi.getTablesHierarchy(tableNames0);

//      initialising newRepTableList with new tables in publisher
      for (int j = 0; j < tableNames.length; j++) {
        if (tableNames[j].equalsIgnoreCase("")) {
          throw new RepException("REP317", null);
        }
        SchemaQualifiedName sname = new SchemaQualifiedName(mdi, tableNames[j]);
        mdi.checkTableExistance(sname);
        if (existingPubTableList.contains(sname.toString())) {
          throw new RepException("REP310", new Object[] {tableNames[j]});
        }
        else
         existingPubTableList.add(sname.toString());
// System.out.println("new tables adding to existing " + sname.toString());
      }

      String[] allTables = new String[existingPubTableList.size()];
      existingPubTableList.toArray( (Object[]) allTables);
      allTables = mdi.getTablesHierarchy(allTables);

      for (int i = 0; i < allTables.length; i++) {
        //Converts Table names  to schema qualified name.
        SchemaQualifiedName sname = new SchemaQualifiedName(mdi, allTables[i]);
        checkTablesDuplicyInPublication(repTableList, sname, pubName);
        String schema = sname.getSchemaName();
        String table = sname.getTableName();
        //checks whether the table exists in the database or not.
        mdi.checkTableExistance(sname);
        //Checks for First Primary key then foreign key tables else exception.
        mdi.checkTableSequenceAccordingForeignKey(schema, table, pubTableList);

        RepTable repTable = new RepTable(sname, RepConstants.publisher);
        //Sets Sorted primary key columns in repTable.
        mdi.setPrimaryColumns(repTable, schema, table);

        //we will call mdi.setAllColumns in publication.addTableToPublication

        repTable.setConflictResolver(conflictResolver);
        try {
//System.out.println("filter clause for " + sname + " = " +pub1.getFilterClause(sname));
          repTable.setFilterClause(pub1.getFilterClause(sname));
        }
        catch (RemoteException ex1) {
          ex1.getMessage();
          //ignore the exception in case of new tables
        }
        pubTableList.add(sname.toString().toUpperCase());
        repTableList.add(repTable);
//System.out.println("repTableList " + repTableList.get(i));
      }
       pub.setPublicationTables(repTableList);
       //tableNames0-without hierarchy set
       for (int i = 0; i < tableNames0.length; i++) {
        if (filterClauses[i] != null)
          pub.setFilter(tableNames0[i], filterClauses[i]);
       }
}
    catch (RepException ex) {
//      ex.printStackTrace();
      throw ex;
    }
    catch (Exception ex) {
//      ex.printStackTrace();
      throw new RepException("REP031", new Object[] {pubName, ex.getMessage()});
    }
  }

  /**
     * To add more table in Publication
     * Checks the following :
     * Publication  exists or not.
     * Specified Table(s) exists in database.
     * Each table must have primary key.
     * Sequence of tables specified ( Primary Table than Foreign Table )
     * @param   pubName        Name of the Publication
     * @param   tableNames     Names of the tables to be published.
     * @return  pub1           Publication class' object.
     * @throws  RepException   In case of violation of above checked conditions.
     */

  public ArrayList dropTableFromPublication(String pubName, String[] tableNames,
                                            Publication pub, ArrayList pubRepTables) throws RepException {
      //If database is not binded
      if (connectionPool == null) {
        throw new RepException("REP002", null);
      }
      //Checks in publication table in database

      if (!checkPublicationExistance(pubName)) {
        //if(!pubMap.containsKey(pubName))
        throw new RepException("REP036", new Object[] {pubName});
      }
      //Checks the properness of given table names
      if (tableNames == null || tableNames.length == 0) {
        throw new RepException("REP012", new Object[] {pubName});
      }
      ArrayList newPubRepTableList = new ArrayList();

      try {
        MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
        mdi.checkChildTableIncludedInDropTableList(pubRepTables,tableNames);
        tableNames = mdi.getTablesHierarchy(tableNames);
      int noOfAllowedDropTables = 0;
//      initialising newRepTableList with tables to be dropped from publisher
        for (int j = 0; j < tableNames.length; j++) {
          SchemaQualifiedName sname = new SchemaQualifiedName(mdi, tableNames[j]);
          String schema = sname.getSchemaName();
          String table = sname.getTableName();
          mdi.checkTableExistance(sname);
          RepTable repTable = new RepTable(sname, RepConstants.publisher);
        noOfAllowedDropTables = checkTableExistanceInPublication(noOfAllowedDropTables, pubRepTables,repTable.getSchemaQualifiedName(), pubName);
          repTable.setFilterClause(pub.getFilterClause(sname));
          repTable.setConflictResolver(pub.getConflictResolver());
          mdi.setPrimaryColumns(repTable, schema, table);
          mdi.setAllColumns(repTable, sname.getSchemaName(),sname.getTableName());
          newPubRepTableList.add(repTable);
        }
        return newPubRepTableList;
      }
      catch (RepException ex) {
        throw ex;
      }
      catch (Exception ex) {
        throw new RepException("REP031", new Object[] {pubName, ex.getMessage()});
      }
    }


    //check table to be dropped if published or not
    // check if table to be dropped is the only table published ,if yes then throw Exception
    //check if user is trying to drop all the tables,if yes throw Exception
    //if not published throw exception

    private int checkTableExistanceInPublication(int noOfAllowedDropTables,
    ArrayList pubTables,SchemaQualifiedName sname,String pubName) throws RepException {
      if (pubTables.size() > 0) {
      boolean flag = false;
        for (int j = 0; j < pubTables.size(); j++) {
          RepTable repTable = (RepTable) pubTables.get(j);
          String sqname = repTable.getSchemaQualifiedName().toString();
          if (sqname.equals(sname.toString())) {
          flag = true;
            noOfAllowedDropTables++;
          }
          // check if table to be dropped is the only table published ,if yes then throw Exception
        if (pubTables.size() == 1 && flag == true) {
          throw new RepException("REP314", new Object[] {sname.toString(),pubName});
          }
          //check if user is trying to drop all the tables,if yes throw Exception
        if (pubTables.size() == noOfAllowedDropTables) {
           throw new RepException("REP315", new Object[] {pubName});
         }
        }
        //check table to be dropped if published or not
      if (!flag) {
        throw new RepException("REP313", new Object[] {sname.toString(),pubName});
        }
      }
      return noOfAllowedDropTables;
  }

  private static void initializeLog4j() {
    try {
    File f1 = new File("." + File.separator + "log4j.properties");
  if (!f1.exists()) {
        f1.createNewFile();
        Writer output = null;
        if (f1.canWrite()) {
          output = new BufferedWriter(new FileWriter(f1));
          output.write("log4j.rootCategory=Off");
          output.write("\n");
          output.write("# log4j.rootCategory=DEBUG,A1");
          output.write("\n");
          output.write("log4j.appender.A1=org.apache.log4j.ConsoleAppender");
          output.write("\n");
          output.write("log4j.appender.A1.layout=org.apache.log4j.PatternLayout");
          output.write("\n");
          output.write("log4j.appender.A1.layout.ConversionPattern=%d{ABSOLUTE} %p [%c{1}] [%M %L] %m%n");
        }
         if (output != null)
        output.close();
      }
      PropertyConfigurator.configureAndWatch("." + File.separator +"log4j.properties");
    }
    catch (Exception ex) {
    RepConstants.writeERROR_FILE(ex);
  }
  }


  /**
   * createPublication
   *
   * @param pubName String
   * @param tableNames String[]
   * @param removeCycleTableNames String[]
   * @return _Publication
   */
  public _Publication createPublication(String pubName, String[] tableNames,
                                        String[] removeCycleTableNames) throws RepException {
    //If database is not binded
      if (connectionPool == null) {
        throw new RepException("REP002", null);
      }

      //Checks in publication table in database
      if (checkPublicationExistance(pubName)) {
        //if(pubMap.containsKey(pubName))
        throw new RepException("REP014", new Object[] {pubName});
      }

      //Checks the properness of given table names
      if (tableNames == null || tableNames.length == 0) {
        throw new RepException("REP012", new Object[] {pubName});
      }

      //repTableList is a list of all reptable that are created
      //corresponding to which is included in publication.
      ArrayList repTableList = new ArrayList();
      try {
        // pubTableList contain all tables that are included in publication
        ArrayList pubTableList = new ArrayList();
        MetaDataInfo mdi = Utility.getDatabaseMataData(connectionPool, pubName);
        SchemaQualifiedName[] schemaQualifiedNames = new SchemaQualifiedName[tableNames.length];
        DirectedGraph directedGraph = new DirectedGraph(tableNames.length);

        for (int i = 0; i < tableNames.length; i++) {
          //Converts Table names  to schema qualified name.
          SchemaQualifiedName sname = new SchemaQualifiedName(mdi, tableNames[i]);
          checkTablesDuplicyInPublication(repTableList, sname, pubName);
          String schema = sname.getSchemaName();
          String table = sname.getTableName();

          //checks whether the table exists in the database or not.
          mdi.checkTableExistance(sname);
          schema = sname.getSchemaName();
          RepTable repTable = new RepTable(sname, RepConstants.publisher);

          //repTable.setConflictResolver(RepConstants.publisher_wins);
          //Sets Sorted primary key columns in repTable.
          mdi.setPrimaryColumns(repTable, schema, table);
          mdi.setForeignKeyColumns(repTable, schema, table);

          //we are not calling mdi.setAllColumn(),we are doing this while publishing
          pubTableList.add(sname.toString());
          directedGraph.addVertex(sname);
          repTableList.add(repTable);
          schemaQualifiedNames[i] = sname;
        }
        Map importedTableInfoMap = mdi.getImportedTablesInfo(schemaQualifiedNames,directedGraph,removeCycleTableNames);
        if (directedGraph.hasCycle()) {
          tablesInCycle = directedGraph.TablesInCycle();
          throw new RepException("REP0205",new Object[]{tablesInCycle});
        }

        List repTablesNamesOrderdAccToHierarcy=Arrays.asList(directedGraph.topologicalSort());
//System.out.println("Ordered Tables :: " +repTablesNamesOrderdAccToHierarcy);
        int noOfRepTables = repTablesNamesOrderdAccToHierarcy.size();
        RepTable[] tempRep = new RepTable[noOfRepTables];
        for (int i = 0; i < noOfRepTables; i++) {
          RepTable repTable = (RepTable) repTableList.get(i);
          SchemaQualifiedName unOrderedschemaQualifiedName = repTable.getSchemaQualifiedName();
          int orderedIndex = repTablesNamesOrderdAccToHierarcy.indexOf(unOrderedschemaQualifiedName);
          tempRep[orderedIndex] = repTable;
      }
// System.out.println("repTablesOrderdAccToHierarcy::");
        ArrayList repTables = new ArrayList(noOfRepTables);
        for (int i = noOfRepTables - 1; i >= 0; i--) {
          repTables.add(tempRep[i]);
// System.out.println("tempRep[i] ::" + tempRep[i]);
        }
        //name = InetAddress.getLocalHost().getHostName()+"_"+pubName+"_"+port;
        name = InetAddress.getLocalHost().getHostName() + "_" + port;
        //Creates publication by setting it's DataBaseHandler &  XMLCreator
        Publication publ = new Publication(connectionPool, pubName, name, this);
        //Sets publication's repTable elements
//System.out.println(" ACTUAL NO of repTables :: " + repTables.size());
        publ.setPublicationTables(repTables);
        publ.setCyclic(removeCycleTableNames != null ? removeCycleTableNames.length == 0 ? false : true : false);
        pubMap.put(pubName, publ);
        return publ;
      }
      catch (RepException ex) {
        throw ex;
      }
      catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        RepConstants.writeERROR_FILE(ex);
        throw new RepException("REP039", new Object[] {ex.getMessage()});
      }
  }
  //Return ArrayList having table having cycles
//  this Arraylist needed if working through GUI
  public ArrayList getTablesInCycle(){
    ArrayList tablesRelatedInCycle=new ArrayList();
    int listSize=tablesInCycle.size();
    for (int i = 0; i < listSize; i++) {
      if(i!=listSize-1){
        tablesRelatedInCycle.add(tablesInCycle.get(i) + "-" +
                                 tablesInCycle.get(i + 1));
      }
    }
    return tablesRelatedInCycle;
  }

  /**
   * getRemoteAddress
   *
   * @return String
   */
  public String getRemoteAddress() throws RepException {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    }
    catch (UnknownHostException ex) {
      return null;
    }
  }
}
