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
import java.sql.*;
import java.util.*;

import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * This class holds the mappings of all the connections in a perticular
 * replication server. Every replication server has one copy of this class,
 * that helps replication server to get different connections for different
 * publications or subscriptions or default connection.
 * This class stores this connection information in to a Map (connectionMap).
 */

public class ConnectionPool implements java.io.Serializable{

  private DataSource dataSource;
  private String url, driver, user, password, port;
  private HashMap connectionMap;
  String localAddress;
  int localPortNo;
  protected static Logger log =Logger.getLogger(ConnectionPool.class.getName());

  // for debugging
  private HashSet openConnectionNames = new HashSet();

  public ConnectionPool() {
  }

  public ConnectionPool(String url0, String driver0, String user0,
                        String password0) {
    url = url0;
    driver = driver0;
    user = user0;
    password = password0;
    connectionMap = new HashMap();
  }

  public ConnectionPool(DataSource dataSource) {
    this.dataSource = dataSource;
    connectionMap = new HashMap();
  }

  public ConnectionPool(DataSource dataSource, String username, String password) {
      this.dataSource = dataSource;
      this.user       = username;
      this.password   = password;
      connectionMap   = new HashMap();
    }

  private Connection getConnectionFromDataSource() throws RepException
  {
    try {
      Connection connection = (user != null ? dataSource.getConnection(user, password) : dataSource.getConnection());

      if (log.isDebugEnabled()) {
        log.debug("Got connection "+connection.toString()+" from data source");
        openConnectionNames.add(connection.toString());
      }
      return connection;
    }
    catch (SQLException ex) {
      throw new RepException("REP007", null);
    }
  }

  public synchronized Connection getConnection(String pubsubName) throws RepException {
    if (dataSource != null) {
      return getConnectionFromDataSource();
    }
    Connection connection = (Connection) connectionMap.get(pubsubName);
    if (connection != null) {
      return connection;
    }
    try {
      Class.forName(driver);
    }
    catch (ClassNotFoundException ex) {
            log.error(ex.getMessage(),ex);
      throw new RepException("REP005", new Object[] {driver});
    }
    try {
      connection = DriverManager.getConnection(url, user, password);
    }
    catch (SQLException ex1) {
            log.error(ex1.getMessage(),ex1);
      throw new RepException("REP007", null);
    }
    connectionMap.put(pubsubName, connection);
    return connection;
  }

  public synchronized Connection getFreshConnection(String pubsubName) throws RepException {
    if (dataSource != null) {
      return getConnectionFromDataSource();
    }
    try {
      Class.forName(driver);
    }
    catch (ClassNotFoundException ex) {
            log.error(ex.getMessage(),ex);
      throw new RepException("REP005", new Object[] {driver});
    }
    try {

      String urlWithoutSchema = url.toLowerCase();
      int indexscehma = urlWithoutSchema.indexOf("schema");
      if (indexscehma != -1) {
        int indexcolon = urlWithoutSchema.indexOf(';', indexscehma);
        if (indexcolon == -1) {
          urlWithoutSchema = url.substring(0, indexscehma - 1);
        }
        else {
          urlWithoutSchema = url.substring(0, indexscehma);
          urlWithoutSchema = urlWithoutSchema +
              url.substring(indexscehma, indexcolon - 1);

        }
      }
      else {
        urlWithoutSchema = url;
      }

      return DriverManager.getConnection(urlWithoutSchema, user, password);
    }
    catch (SQLException ex1) {
            log.error(ex1.getMessage(),ex1);
      throw new RepException("REP007", null);
    }
  }

  public void setLocalAddress(String localAddress0) {
    localAddress = localAddress0;
  }

  public String getLocalAddress() {
    return localAddress;
  }

  public void setLocalPortNo(int localPort0) {
    localPortNo = localPort0;
  }

  public int getLocalPortNo() {
    return localPortNo;
  }

  public ServerSocket startServerSocket() throws RepException {
    int port = 3457;
    ServerSocket serverSocket = null;
    while (port < 6000) {
      try {
        return new ServerSocket(port);
      }
      catch (IOException ex) {
        port++;
      }
    }
    throw new RepException("REP085", null);
  }

  public String getUserName() {
    return user;
  }

  public synchronized Connection getDefaultConnection() throws RepException {
    if (dataSource != null) {
      return getConnectionFromDataSource();
    }
    try {
      Class.forName(driver);
    }
    catch (ClassNotFoundException ex1) {
      RepConstants.writeERROR_FILE(ex1);
      throw new RepException("REP005", new Object[] {driver});
    }
    try {
      return DriverManager.getConnection(url, user, password);
    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
  }

  public synchronized void returnConnection(Connection connection) throws RepException {
    if ((dataSource != null) && (connection != null)) {
      try {
        openConnectionNames.remove(connection.toString());
        if (!connection.isClosed()) {
          if (log.isDebugEnabled()) {
            StringBuffer logMsg = new StringBuffer();

            logMsg.append("Returning connection ");
            logMsg.append(connection.toString());
            logMsg.append(" to data source.\nRemaining connections:");
            if (openConnectionNames.isEmpty()) {
              logMsg.append(" None");
            }
            else {
              for (Iterator it = openConnectionNames.iterator(); it.hasNext();) {
                logMsg.append("\n    ");
                logMsg.append(it.next().toString());
              }
            }
            log.debug(logMsg.toString());
          }
          connection.close();
        }
      }
      catch (SQLException ex) {
        throw new RepException("REP001", new Object[] {ex.getMessage()});
      }
    }
  }
  public void removeSubPubFromMap(String subPubName){
//System.out.println(" Removed called with "+ subPubName);
//Thread.dumpStack();
    if(connectionMap.containsKey(subPubName)){
     Object object = connectionMap.remove(subPubName);
//     System.out.println("  Object remvoded for "+subPubName+" -- "+ object);
    }
  }


public static void closeStatementAndResultSet(Statement stmt,ResultSet rs) {
   try {
     if (rs != null) {
       rs.close();
     }
   }
   catch (SQLException ex2) {
   }
   try {
     if (stmt != null)
       stmt.close();
   }
   catch (SQLException ex) {
   }

}

}
