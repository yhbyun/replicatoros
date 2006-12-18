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

import javax.sql.*;
import java.lang.reflect.*;
import java.sql.*;

public class DBDataSource {
  private String dataBaseName,
      user,
      password,
      dBServerName,
      dBPortNo,
      vendorName,
      connectionMode,
      databaseName,
      hostName,
      portNo,
      Daffodil_ProductName = "DaffodilDB",
      Oracle_ProductName = "Oracle",
      SqlServer_ProductName = "Microsoft SQL Server",
      PointBase_ProductName = "PointBase",
      Cloudscape_ProductName = "Apache Derby",
      PostgreSQL_ProductName = "PostgreSQL",
      DB2_ProductName = "DB2/NT",
      DB2_6000_ProductName = "DB2/6000",
      DB2_AS_400_ProductName = "DB2 UDB",
      Sybase_ASE = "Adaptive Server Enterprise",
      Sybase_ASA = "Adaptive Server AnyWhere",
      FireBird_ProductName = "Firebird"
      ;
  public DBDataSource(String dataBaseName0, String user0, String password0,
                      String dBServerName0, String dBPortNo0,
                      String vendorName0) {
    dataBaseName = dataBaseName0;
    user = user0;
    password = password0;
    dBServerName = dBServerName0;
    dBPortNo = dBPortNo0;
    vendorName = vendorName0;
  }

  public DataSource getDataSource() throws RepException {
    DataSource dataSource = null;
    Class calass = null;
    try {
      System.out.println(System.getProperty("java.class.path"));
      if (vendorName.equalsIgnoreCase(Daffodil_ProductName)) {
        if (connectionMode.startsWith("Embedded")) {
          calass = Class.forName(
              "in.co.daffodil.db.jdbc.RmiDaffodilDBDataSource");
          dataSource = (DataSource) (calass.newInstance());
          setDataSrcPropDaffoEmbedded(calass, dataSource, hostName,
                                      databaseName, user, password);
        }
        else {
          calass = Class.forName(
              "in.co.daffodil.db.RMI.RMIDaffodilDBDataSource");
          dataSource = (DataSource) (calass.newInstance());
          setDataSrcPropDaffoServer(calass, dataSource, hostName, databaseName,
                                    portNo, user, password);
        }

      }
      else if (vendorName.startsWith("M") || vendorName.startsWith("SQL")) {
        calass = Class.forName(
            "com.microsoft.jdbcx.sqlserver.SQLServerDataSource");
        dataSource = (DataSource) (calass.newInstance());
        System.out.println(" dataSource :: " + dataSource.getClass());
        setDataSourceProperties(calass, dataSource, hostName, dataBaseName,
                                portNo, user, password);
      }
      else if (vendorName.equalsIgnoreCase(Oracle_ProductName)) {
        calass = Class.forName("oracle.jdbc.pool.OracleDataSource");
        dataSource = (DataSource) (calass.newInstance());
        setDataSourceProperties(calass, dataSource, hostName, dataBaseName,
                                portNo, user, password);
      }
      else if (vendorName.startsWith("DB2")) {
        calass = Class.forName("com.ibm.db2.jdbc.db2connectionPoolDataSource");
        dataSource = (DataSource) (calass.newInstance());
        setDataSourceProperties(calass, dataSource, hostName, dataBaseName,
                                portNo, user, password);
      }
      else if (vendorName.equalsIgnoreCase(Cloudscape_ProductName)) {
        calass = Class.forName("org.apache.derby.jdbc.ClientDataSource");
        dataSource = (DataSource) (calass.newInstance());
        setDataSourceProperties(calass, dataSource, hostName, dataBaseName,
                                portNo, user, password);
      }
      else if (vendorName.equalsIgnoreCase(PostgreSQL_ProductName)) {
        calass = Class.forName(
            " org.postgresql.jdbc2.optional.PoolingDataSource");
        dataSource = (DataSource) (calass.newInstance());
        setDataSourceProperties(calass, dataSource, hostName, dataBaseName,
                                portNo, user, password);
      }
      else if (vendorName.equalsIgnoreCase(FireBird_ProductName)) {
        calass = Class.forName(" org.firebirdsql.pool.FBWrappingDataSource");
        dataSource = (DataSource) (calass.newInstance());
        setDataSourceProperties(calass, dataSource, hostName, dataBaseName,
                                portNo, user, password);
      }
      else if (vendorName.equalsIgnoreCase(FireBird_ProductName)) {
        calass = Class.forName("com.evermind.sql.DriverManagerDataSource");
        dataSource = (DataSource) (calass.newInstance());
        setDataSourceProperties(calass, dataSource, hostName, dataBaseName,
                                portNo, user, password);
      }

    }
    catch (Exception e) {
      System.err.println("PROBLE IN MAKING THE INSTANCE OF DATASOURCE CLASS");
      e.printStackTrace();
    }
    return dataSource;
  }

  private void setDataSourcePropertiesSQlServer(Class calass,
                                                DataSource dataSoruce,
                                                String hostName,
                                                String dataBaseName,
                                                String user,
                                                String password) throws
      RepException {
    try {
      // Set  Host Name
      calass.getMethod("setServerName",
          new Class[] {String.class}).invoke(dataSoruce, new Object[] {hostName});
      // Set the Database Name
      calass.getMethod("setDatabaseName",
          new Class[] {String.class}).invoke(dataSoruce,
                                             new Object[] {dataBaseName});
      // Set user Name
      calass.getMethod("setUser", new Class[] {String.class}).invoke(dataSoruce,
          new Object[] {user});
      // Set password
      calass.getMethod("setPassword",
          new Class[] {String.class}).invoke(dataSoruce, new Object[] {password});
    }
    catch (InvocationTargetException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (IllegalArgumentException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (IllegalAccessException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (SecurityException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (NoSuchMethodException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }

  }

  private void setDataSourceProperties(Class calass, DataSource dataSoruce,
                                       String hostName, String dataBaseName,
                                       String portNumber, String user,
                                       String password) throws RepException {
    try {

      Method methodName = calass.getMethod("setServerName", new Class[] {String.class});
      methodName.invoke(dataSoruce, new Object[] {"sube"});
      System.out.println(" Set Server Name ::  " + methodName);

      methodName = calass.getMethod("setDatabaseName", new Class[] {String.class});
      System.out.println(" Database Name :: " + methodName);
      methodName.invoke(dataSoruce, new Object[] {dataBaseName});

      methodName = calass.getMethod("setUser", new Class[] {String.class});
      System.out.println(" setUser : " + methodName);
      methodName.invoke(dataSoruce, new Object[] {user});

      methodName = calass.getMethod("setPassword", new Class[] {String.class});
      System.out.println(" setPassword :: " + methodName);
      methodName.invoke(dataSoruce, new Object[] {password});
    }
    catch (InvocationTargetException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (IllegalArgumentException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (IllegalAccessException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (SecurityException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (NoSuchMethodException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }

  }

  private void setDataSrcPropDaffoServer(Class calass, DataSource dataSoruce,
                                         String hostName, String dataBaseName,
                                         String portNumber, String user,
                                         String password) throws RepException {
    try {
      // Set the HostName
      calass.getMethod("setHostName",
          new Class[] {String.class}).invoke(dataSoruce, new Object[] {hostName});
      //Set DatabaseName
      calass.getMethod("setDatabaseName",
          new Class[] {String.class}).invoke(dataSoruce,
                                             new Object[] {dataBaseName});
      //Set port Number
      calass.getMethod("setPortNumber",
          new Class[] {String.class}).invoke(dataSoruce,
                                             new Object[] {portNumber});
      //Set user
      calass.getMethod("setUser", new Class[] {String.class}).invoke(dataSoruce,
          new Object[] {user});
      //Set password
      calass.getMethod("setPassword",
          new Class[] {String.class}).invoke(dataSoruce, new Object[] {password});
    }
    catch (InvocationTargetException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (IllegalArgumentException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (IllegalAccessException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (SecurityException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (NoSuchMethodException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }

  }

  private void setDataSrcPropDaffoEmbedded(Class calass, DataSource dataSoruce,
                                           String hostName, String dataBaseName,
                                           String user,
                                           String password) throws RepException {
    try {
      // Set Server Name
      calass.getMethod("setServerName",
          new Class[] {String.class}).invoke(dataSoruce, new Object[] {hostName});
      // Set Database Name
      calass.getMethod("setDatabaseName",
          new Class[] {String.class}).invoke(dataSoruce,
                                             new Object[] {dataBaseName});
      // Set user Name
      calass.getMethod("setUser", new Class[] {String.class}).invoke(dataSoruce,
          new Object[] {user});
      // Set password
      calass.getMethod("setPassword",
          new Class[] {String.class}).invoke(dataSoruce, new Object[] {password});
    }
    catch (InvocationTargetException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (IllegalArgumentException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (IllegalAccessException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (SecurityException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }
    catch (NoSuchMethodException ex) {
      throw new RepException("REP001", new Object[] {ex.getMessage()});
    }

  }

  public static void main(String[] args) {

    try {

      DBDataSource dbs = new DBDataSource("dataBaseName0", " user0",
                                          "password0", "dBServerName0",
                                          "dBPortNo0", "vendorName0");
      DataSource ds = dbs.getDataSource();

      System.out.println(" User Name  = " +
                         ds.getConnection().getMetaData().getUserName());
      Connection conn = ds.getConnection();
      System.out.println(" Connection " + conn);
      Statement stmt = conn.createStatement();
      System.out.println(" Statement : " + stmt);
      stmt.execute("Create table t(c1 integer)");
      System.out.println(" Table created successfully ");
      stmt.execute(" Drop table t");
      System.out.println(" Table dropped successfully ");

    }
    catch (Exception ex) {
      ex.printStackTrace();
    }

  }

}
