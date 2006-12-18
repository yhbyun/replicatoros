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

package com.daffodilwoods.replication.xml;

import java.sql.*;
import java.util.*;

import org.xml.sax.*;
import org.xml.sax.helpers.*;
import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.DBHandler.*;
import com.daffodilwoods.replication.synchronize.*;
import java.io.BufferedWriter;
import org.apache.log4j.Logger;

/**
 * This class is the implementation class for ContentHandler for parsing the
 * synchronization XML file. This class implements differnt event methods which automatically
 * are called by the parser. It implements these methods to get and use the different
 * values stored on the XML file.
 */

public class MergeHandler
    extends DefaultHandler {
  XMLElement currentElement;
  Connection connection;
  Statement statement;
  _Replicator replicator;
  TreeMap treeMap;
  String conflictResolver, localName, remoteName;
  PreparedStatement updateConisderedForBookMarksTable;
  AbstractDataBaseHandler dbHandler;
  MetaDataInfo mdi;
  OperationDelete operationDelete;
  OperationInsert operationInsert;
  OperationUpdate operationUpdate;
  String remoteServerName, replicationType, transactionLogType;
  BufferedWriter bw;
  public int insert, update, delete;
  private XMLElement tableElement;
  boolean isFirstPass;
  private boolean isCurrentTableCyclic;
  protected static Logger log = Logger.getLogger(MergeHandler.class.getName());

  /**
   * Default Handler for parsing and reading the contents from XML file
   * @param connection0
   * @param replicator0
   * @param remoteServerName0
   * @throws RepException
   */
  public MergeHandler(boolean isFirstPass0, Connection connection0,
                      _Replicator replicator0,
                      String remoteServerName0,
                      AbstractDataBaseHandler dbHandler0, BufferedWriter bw0,
                      String replicationType0, String transactionLogType0,
                      MetaDataInfo mdi0) throws RepException {
    try {
      currentElement = new XMLElement("root");
      connection = connection0;
      statement = connection.createStatement();
      replicator = replicator0;
      dbHandler = dbHandler0;

      mdi = mdi0;
//    showResultSet(statement.executeQuery(" Select * from RepTable"));
      transactionLogType = transactionLogType0;
      remoteServerName = remoteServerName0;
      bw = bw0;
      replicationType = replicationType0;
      isFirstPass = isFirstPass0;
    }
    catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP057", new Object[] {ex.getMessage()});
    }
  }

  /**
   * Initilazing an XML element and adding its childs.
   * @param namespace
   * @param localname
   * @param qname
   * @param atts
   * @throws SAXException
   */
  public void startElement(String namespace, String localname, String qname, Attributes atts) throws SAXException {
    XMLElement childElement = null;
    if ( (currentElement == null ||
          !currentElement.elementName.equals("operation")) &&
        qname.equals("tableName")) {
      childElement = new XMLElement(qname);
      tableElement = childElement;
    }
    else {
      if (tableElement != null) {
        initializeOperations(tableElement.elementValue);
      }
      childElement = new XMLElement(qname);
      tableElement = null;
    }
    currentElement.addChild(childElement);
    childElement.setParentElement(currentElement);
    childElement.addAtt(atts.getValue("name"));

    // encoded cols
//System.out.println("atts.getValue(Encode) ="+atts.getValue("Encode"));
    childElement.addEncodeAtt(atts.getValue("Encode"));
    currentElement = childElement;
  }

  /**
   * Called after end of an element is reached for calling update/insert/delete on the respective table.
   * @param namespace
   * @param localname
   * @param qname
   * @throws SAXException
   */
  public void endElement(String namespace, String localname, String qname) throws SAXException {
    try {
      currentElement.checkEncoding();
      XMLElement parentElement = currentElement.getParentElement();
      if ( (parentElement == null ||
            parentElement.elementName.equals("tableName")) &&
          qname.equals("operation")) {
//System.out.println("currentElement.elementValue="+currentElement.elementValue);
        if (currentElement.elementValue.equals(RepConstants.insert_operation)) {
          createInsertQuery();
//         Utility.insertCount++;
//System.out.println("Utility.insertCount = "+Utility.insertCount);
        }
        else if (currentElement.elementValue.equals(RepConstants.update_operation)) {
          createUpdateQuery();
        }
        else if (currentElement.elementValue.equals(RepConstants.delete_operation)) {
          createDeleteQuery();
        }
      }
      if ( (parentElement == null ||
            !parentElement.elementName.equals("operation")) &&
          qname.equals("tableName")) {
        try {
          currentElement.elementList.clear();
        }
        catch (Exception ex) {
          RepConstants.writeERROR_FILE(ex);
        }
        if (operationInsert != null) {
          insert += operationInsert.insertCount;
          operationInsert.closeAllStatments();
        }
        if (operationUpdate != null) {
          update += operationUpdate.updateCount;
          operationUpdate.closeAllStatments();
        }
        if (operationDelete != null) {
          delete += operationDelete.deleteCount;
          operationDelete.closeAllStatments();
        }
      }
      currentElement = parentElement;
    }
    catch (RepException ex1) {
//          ex1.printStackTrace();
      throw new SAXException(ex1.getMessage(), ex1);
    }
  }

  /**
   * getting the value for XML element.
   * @param ch
   * @param start
   * @param len
   * @throws SAXException
   */
  public void characters(char[] ch, int start, int len) throws SAXException {
    String elementValue = new String(ch, start, len);
    if (elementValue.equalsIgnoreCase("") || elementValue.equalsIgnoreCase("\n")) {
      return;
    }
    currentElement.setElementValue(elementValue);
  }

  private void initializeOperations(String elementValue) throws SAXException {
    Statement st = null;
    ResultSet conisderedIdRS = null;
    try {
//        Utility.insertCount=0;
      RepTable repTable = replicator.getRepTable(elementValue);
      isCurrentTableCyclic = repTable.getCyclicDependency().equalsIgnoreCase(RepConstants.YES);
      treeMap = repTable.getColumnTreeMap(connection,replicator.getDBDataTypeHandler());
      conflictResolver = repTable.getConflictResolver();
      st = connection.createStatement();
      StringBuffer query = new StringBuffer();
      query.append("SELECT ").append(RepConstants.bookmark_ConisderedId5).
          append(" FROM ").append(dbHandler.getBookMarkTableName()).
          append(" where ").append(RepConstants.bookmark_TableName3).append(
          " ='").append(repTable.getSchemaQualifiedName().toString())
          .append("' and ").append(RepConstants.bookmark_LocalName1).append(
          "='").append(localName).append("' and ")
          .append(RepConstants.bookmark_RemoteName2).append(" = '").append(
          remoteName).append("'");
      conisderedIdRS = st.executeQuery(query.toString());
      boolean f1 = conisderedIdRS.next();
      Object conisderedId = conisderedIdRS.getObject(1);
      operationDelete = new OperationDelete(repTable, connection, treeMap,
                                            conisderedId, remoteServerName,
                                            dbHandler, bw, replicationType,
                                            transactionLogType, mdi,
                                            isFirstPass, isCurrentTableCyclic);
      operationUpdate = new OperationUpdate(repTable, connection, treeMap,
                                            conisderedId, remoteServerName,
                                            dbHandler, bw, replicationType,
                                            transactionLogType, mdi,
                                            isFirstPass, isCurrentTableCyclic);
      operationInsert = new OperationInsert(repTable, connection, treeMap,
                                            conisderedId, remoteServerName,
                                            dbHandler, bw, replicationType,
                                            transactionLogType, mdi,
                                            isFirstPass, isCurrentTableCyclic);

    }
    catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new SAXException(ex.getMessage(), ex);
    }
  }

  /**
   * creating and firing a insert query.
   * @throws RepException
   */
  private void createInsertQuery() throws RepException {
    try {
      log.debug("Current Elemnent  =" + currentElement);
      operationInsert.execute(currentElement);
    }
    catch (Exception ex) {
      log.error(ex, ex);
      if (ex instanceof RepException) {
        if ( ( (RepException) ex).getRepCode().equalsIgnoreCase("REP051")) {
          RepException rex = new RepException("REP050", null);
          ( (RepException) rex).SetStackTrace( (RepException) ex);
          throw rex;
        }
        throw (RepException) ex;
      }
      else {
        throw new RepException("REP057", new Object[] {ex.getMessage()});
      }
    }
  }

  /**
   * creating and firing a update query.
   * @throws RepException
   */
  private void createUpdateQuery() throws RepException {
    try {
      operationUpdate.execute(currentElement);
    }
    catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      if (ex instanceof RepException) {
        if ( ( (RepException) ex).getRepCode().equalsIgnoreCase("REP051")) {
          RepException rex = new RepException("REP050", null);
          rex.SetStackTrace( (RepException) ex);
          throw rex;
        }
        throw (RepException) ex;
      }
      else {
        throw new RepException("REP057", new Object[] {ex.getMessage()});
      }
    }
  }

  /**
   * creating and firing a delete query.
   * @throws RepException
   */
  private void createDeleteQuery() throws RepException {
    try {
      operationDelete.execute(currentElement);
    }
    catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      if (ex instanceof RepException) {
        if ( ( (RepException) ex).getRepCode().equalsIgnoreCase("REP051")) {
          RepException rex = new RepException("REP050", null);
          rex.SetStackTrace( (RepException) ex);
          throw rex;
        }
        throw (RepException) ex;
      }
      else {
        throw new RepException("REP057", new Object[] {ex.getMessage()});
      }
    }
  }

  private PreparedStatement makeUpdateConisderedForBookMarksTable() throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append(" UPDATE  ").append(dbHandler.getBookMarkTableName()).append(
        " set  ").append(RepConstants.bookmark_ConisderedId5)
        .append(" = ? where ").append(RepConstants.bookmark_LocalName1).append(
        " = ? and ").append(RepConstants.bookmark_RemoteName2).append(
        " = ? and ")
        .append(RepConstants.bookmark_TableName3).append(" =? ");
    return connection.prepareStatement(query.toString());
  }

  public void setLocalName(String localName0) {
    localName = localName0;
  }

  public void setRemoteName(String remoteName0) {
    remoteName = remoteName0;
  }

  public void closeAllStatementAndResultset() {
    try {
      if (statement != null)
        statement.close();
    }
    catch (SQLException ex) {
    }
  }

  /**
   * used for debugging. shows values stored in a resultSet.
   * @param rs
   * @throws SQLException
   */
  /* public static void showResultSet(ResultSet rs) throws SQLException {
     ResultSetMetaData metaData = rs.getMetaData();
     int columnCount = metaData.getColumnCount();
     Object[] displayColumn = new Object[columnCount];
     for (int i = 1; i <= columnCount; i++)
       displayColumn[i - 1] = metaData.getColumnName(i);
//    System.out.println(Arrays.asList(displayColumn));
     while (rs.next()) {
       Object[] columnValues = new Object[columnCount];
       for (int i = 1; i <= columnCount; i++)
         columnValues[i - 1] = rs.getObject(i);
//      System.out.println(Arrays.asList(columnValues));
     }
   }*/
}
