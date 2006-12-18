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
import com.daffodilwoods.replication.column.*;
import org.apache.log4j.Logger;

/**
 * This class is the implementation class for ContentHandler for parsing the
 * snapshot XML file. This class implements differnt event methods which automatically
 * are called by the parser. It implements these methods to get and use the different
 * values stored on the XML file.
 * These methods helps at the time of taking snapshot, as when some information is
 * found by the parser these methods stores the relative value and perform different
 * operations like inserting, updating and deleting records from respective tables.
 */

public class SnapshotHandler
    extends DefaultHandler {
  XMLElement currentElement;
  Connection subConnection;
  Statement statement;
  Subscription subscription;
  TreeMap treeMap;
  PreparedStatement preparedStatement, psForUpdateBookMarksTable;
  String remoteServerName,subName, pubName;
  AbstractDataBaseHandler dbHandler;
  boolean setAutoCommitFlag = true;
  protected static Logger log = Logger.getLogger(SnapshotHandler.class.getName());
  private boolean isFirstPass, isCurrentTableCyclic;
  private RepTable currentRepTable;
  private XMLElement tableElement;
  private TreeMap allColumnsMap;

  /**
   * Default Handler for parsing and reading the contents from XML file
   * for getting Snapshot
   * @param subConnection0
   * @param subscription0
   * @throws SQLException
   */
  public SnapshotHandler(boolean isFirstPass0, Connection subConnection0,
                         Subscription subscription0,
                         AbstractDataBaseHandler dbHandler0,
                         String remoteServerName0) throws SQLException {
    currentElement = new XMLElement("root");
    subConnection = subConnection0;
    statement = subConnection.createStatement();
    subscription = subscription0;
    dbHandler = dbHandler0;
    remoteServerName = remoteServerName0;
    isFirstPass = isFirstPass0;
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
    try {
      XMLElement childElement = null;

      /*
        Below given if condition and structure of XML file has been changed
        after merging of branch "Vinesreplication".
        A new element Operation is added to make the
        common element of row and primary key element.
        Note : - to get the copy of old source check out
        it from branch replicator1_9
       */

     if ( (currentElement == null ||
         !currentElement.elementName.equals("operation")) &&
         qname.equals("tableName")) {

      /**
       * In place of "XMLElement" class "XMLTableElement"
       * class is used to control the outofmemory error.
       * If a table have records in lakh then outofmemoryerror
       * occurs because in "XMLElement" class one by one all the
       *  element are added in elementlist.
       */
      childElement = new XMLTableElement(qname);
//       childElement = new XMLElement(qname);
        tableElement = childElement;
      }
      else {
        if (tableElement != null) {
          createStatmement();
        }
        childElement = new XMLElement(qname);
        tableElement = null;
      }
      currentElement.addChild(childElement);
      childElement.setParentElement(currentElement);
      childElement.addAtt(atts.getValue("name"));

      //add Encode
      childElement.addEncodeAtt(atts.getValue("Encode"));
      currentElement = childElement;
    }
    catch (NullPointerException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new SAXException(ex);
    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new SAXException(ex);
    }
  }

  /**
   * Called after end of an element is reached for calling delete and then insert on the respective tables on client side.
   * @param namespace
   * @param localname
   * @param qname
   * @throws SAXException
   */
  public void endElement(String namespace, String localname, String qname) throws SAXException {
    try {
      currentElement.checkEncoding();
      XMLElement parentElement = currentElement.getParentElement();
      log.debug(" parentElement  "+parentElement);
      log.debug("qname::::"+qname);
      /*
        Below given if condition and structure of XML file has been changed
        after merging of branch "Vinesreplication".
        A new element Operation is added to make the
        common element of row and primary key element.
        Note : - to get the copy of old source check out
        it from branch replicator1_9
       */
      if ( (parentElement == null ||
          parentElement.elementName.equals("tableName")) &&
          qname.equals("operation")) {

//      currentElement = parentElement;

        createQuery();
        currentElement.elementList.clear();
      }
      if ( (parentElement == null ||
            !parentElement.elementName.equals("row") ||
            !qname.equals("primary")) &&
          qname.equals("tableName")) {
        try {
          String tableNaam = currentElement.elementValue;
//        String shadowTableName = RepConstants.shadow_Table(tableNaam);
          String shadowTableName = dbHandler.getShadowTableName(tableNaam);
          // delete all entries from Shadow Table as snapshot of table is taken by now
          StringBuffer query = new StringBuffer();
          query.append("SELECT MAX(").append(RepConstants.shadow_sync_id1).append(") from ").append(shadowTableName);
          ResultSet rs = statement.executeQuery(query.toString());
          rs.next();
          long lastSyncId = rs.getLong(1);
          rs.close();

          query = new StringBuffer();
          query.append("delete from ").append(shadowTableName).append(" where ")
              .append(RepConstants.shadow_sync_id1).append(" != ").append(lastSyncId);
          statement.execute(query.toString());

          String updateServerNameQuery = "update " + shadowTableName + " set " +
              RepConstants.shadow_serverName_n + " =  '" + remoteServerName +"'";
          statement.execute(updateServerNameQuery);

          // updating records Lastsyncid and Concidered Id on BookMarks Table
          if (psForUpdateBookMarksTable == null) {
            psForUpdateBookMarksTable = makeUpdateBookMarksTable();
          }
          psForUpdateBookMarksTable.setLong(1, lastSyncId);
          psForUpdateBookMarksTable.setLong(2, lastSyncId);
          psForUpdateBookMarksTable.setString(3, subName);
          psForUpdateBookMarksTable.setString(4, pubName);
          psForUpdateBookMarksTable.setString(5, tableNaam);
          int count = psForUpdateBookMarksTable.executeUpdate();
          currentElement.elementList.clear();

          // inserting and deleting records from log Table..
          insert_dummyRecordInLogTable();
          deleteRecordsFromSuperLogTable(tableNaam);
        }
        catch (Exception ex) {
          RepConstants.writeERROR_FILE(ex);
          throw new SAXException(ex.getMessage(), ex);
        }
        if (preparedStatement != null) {
          try {
            preparedStatement.close();
          }
          catch (SQLException ex2) {
          }
        }
      }
      if (parentElement != null) {
        currentElement = parentElement;
      }
    }
    catch (ClassCastException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new SAXException(ex.getMessage(), ex);
    }
    catch (NullPointerException ex1) {
      ex1.printStackTrace();
      RepConstants.writeERROR_FILE(ex1);
      throw new SAXException(ex1);
    }
    catch (Exception ex1) {
      ex1.printStackTrace();
      RepConstants.writeERROR_FILE(ex1);
      throw new SAXException(ex1);
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
    try {
      String elementValue = new String(ch, start, len);
      if (elementValue.equalsIgnoreCase("") ||
          elementValue.equalsIgnoreCase("\n")) {
        return;
      }
      currentElement.setElementValue(elementValue);
    }
    catch (NullPointerException ex1) {
      throw new SAXException(ex1.getMessage(), ex1);
    }
  }

  private void createStatmement() throws SAXException {
    try {
      String elementValue = currentElement.elementValue;
      XMLElement parentElement = currentElement.getParentElement();
      if ( (parentElement == null ||
            !parentElement.elementName.equals("row")) &&
          currentElement.elementName.equals("tableName")) {
        RepTable repTable = subscription.getRepTable(elementValue);
        isCurrentTableCyclic = repTable.getCyclicDependency().equalsIgnoreCase(RepConstants.YES);
        if (isFirstPass) {
//     statement.execute("DELETE FROM " + elementValue);
//     ResultSet rs = statement.getResultSet();
          treeMap = repTable.getColumnTreeMap(subConnection,subscription.getDBDataTypeHandler());
          String preparedQuery = repTable.createInsertQueryForSnapShot();
          preparedStatement = subConnection.prepareStatement(preparedQuery);
          log.debug(preparedQuery);
        }
        else {
          if (isCurrentTableCyclic) {
            treeMap = repTable.getColumnTreeMap(subConnection,subscription.getDBDataTypeHandler());
            String preparedQuery = repTable.createUpdateQueryForSnapShot();
            preparedStatement = subConnection.prepareStatement(preparedQuery);
          }
        }
        currentRepTable = repTable;
        allColumnsMap=currentRepTable.getAllColumns();
      }
    }
    catch (ClassCastException ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new SAXException(ex.getMessage(), ex);
    }
    catch (NullPointerException ex1) {
      ex1.printStackTrace();
      log.error(ex1.getMessage(), ex1);
      throw new SAXException(ex1.getMessage(), ex1);
    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
      throw new SAXException(ex.getMessage(), ex);
    }

  }

  /**
   * creating and firing Insert Query for client subscribed tables.
   * @throws SAXException
   */
  public void createQuery() throws SAXException, SQLException {

    try {
      ArrayList elements = currentElement.getChildElements();
//     XMLElement  parentElement =currentElement.getParentElement();
//     ArrayList elements = parentElement.getChildElements();
      ArrayList InsertElements = ( (XMLElement) elements.get(0)).getChildElements();
//      ArrayList InsertElements = elements;
      log.debug(" Is First pass  :  " + isFirstPass);

      if (isFirstPass) {
        int j = 0;
        for (int i = 0; i < InsertElements.size(); i++) {
          XMLElement element = (XMLElement) InsertElements.get(i);
          String columnName = element.elementName;
          columnName = (String) allColumnsMap.get(columnName);
          log.debug(" allColumnsMap : "+allColumnsMap);
          String value = element.elementValue;
          log.debug(" XML Element : " + element);
          log.debug(" columnName : " + columnName);
          log.debug(" value :  " + value);
          if (currentRepTable.isIgnoredColumn(columnName)) {
            log.debug("isIgnoredColumn: " + columnName);
            continue;
          }
          log.debug(" isCurrentTableCyclic : " + isCurrentTableCyclic);
          log.debug(columnName + " isForiegnKeyColumn : " +currentRepTable.isForiegnKeyColumn(columnName));
          if (isCurrentTableCyclic && currentRepTable.isForiegnKeyColumn(columnName)) {
            log.debug(" Is isCurrentTableCyclic :  YES");
            log.debug(" currentRepTable.isForiegnKeyColumn(columnName) : "+currentRepTable.isForiegnKeyColumn(columnName));
            AbstractColumnObject columnObject = (AbstractColumnObject) treeMap.get(columnName);
            //once setAutoCommitFlag is set to false,we shouldn't change it to true
            // by checking for other columns for that 'if' check is used
            checkAutocommit(columnObject);
            columnObject.setColumnObject(preparedStatement, "NULL", j + 1);
            log.debug("setting  null to " + columnName);
          }
          else {
            AbstractColumnObject columnObject = (AbstractColumnObject) treeMap.get(columnName);
            //once setAutoCommitFlag is set to false,we shouldn't change it to true
            // by checking for other columns for that 'if' check is used
            checkAutocommit(columnObject);
            columnObject.setColumnObject(preparedStatement, element, j + 1);
            log.debug("setting " + columnName + " to " + element.elementValue);
          }
          j++;
        }

        try {
          preparedStatement.execute();
        }
        catch (SQLException ex1) {
          ex1.printStackTrace();
        }
      }
      else {
        log.debug("IS CURRENT TABLE CYCLIC  "+isCurrentTableCyclic);
        if (isCurrentTableCyclic) {
          int columnIndex = 0;
          String[] foreignKeyCols = currentRepTable.getForeignKeyCols();
          for (int j = 0, size = foreignKeyCols.length; j < size; j++) {
            for (int i = 0; i < InsertElements.size(); i++) {
              XMLElement element = (XMLElement) InsertElements.get(i);
               String ColumnName = element.elementName;
               ColumnName = (String) allColumnsMap.get(ColumnName);
              log.debug("Second Pass " + ColumnName + " isForiegnKeyColumn : " +currentRepTable.isForiegnKeyColumn(ColumnName));
              if (ColumnName.equalsIgnoreCase(foreignKeyCols[j])) {
                String value = element.elementValue;
                ( (AbstractColumnObject) treeMap.get(ColumnName)).setColumnObject(preparedStatement, element, columnIndex + 1);
                log.debug("Second Pass ColumnName::" + ColumnName + ":" + value);
                log.debug("Second Pass columnName : " + ColumnName);
                log.debug("Second Pass value :  " + value);
                break;
              }
              log.debug("Second Pass XML Element : " + element);
            }
            columnIndex++;
          }
          // set object for where cluase

          ArrayList primaryKeyElements = ( (XMLElement) elements.get(1)).getChildElements();
          log.debug("Create Query  Primary Key Element  : "+primaryKeyElements);
          String[] primaryKeyValues = new String[primaryKeyElements.size()];
          for (int i = 0; i < primaryKeyElements.size(); i++) {
            String ColumnName = ( ( (XMLElement) primaryKeyElements.get(i)).getAttribute());
            log.debug("Create Query   Coulmns  :  "+ColumnName);
            XMLElement prKeyValuesElement = (XMLElement) primaryKeyElements.get(i);
            log.debug("prKeyValuesElement.elementValue  SnapshotHandler.createQuery() "+prKeyValuesElement.elementValue);
            primaryKeyValues[i] = prKeyValuesElement.elementValue;
            log.debug("treeMap::::"+treeMap);
            ( (AbstractColumnObject) treeMap.get(ColumnName)).setColumnObject(preparedStatement, prKeyValuesElement, columnIndex + 1);
            columnIndex++;
          }
          preparedStatement.execute();
        }
      }
    }
    catch (SQLException ex) {
      //exception is dumped in case when  parent table is not included in the publisher
      /*     RepConstants.writeERROR_FILE(ex);
           try {
             if (!dbHandler.getPrimaryKeyErrorCode(ex)) {
               throw new SAXException(ex.getMessage(), ex);
             }
          }
          catch (SQLException ex1) {
              RepConstants.writeERROR_FILE(ex1);
           } */
    }
    finally {
      setAutocomitTrueAndCommitRecord();
    }
  }

  private PreparedStatement makeUpdateBookMarksTable() throws Exception {
    StringBuffer query = new StringBuffer();
    query.append(" UPDATE  ").append(dbHandler.getBookMarkTableName()).append(" set   ")
        .append(RepConstants.bookmark_ConisderedId5)
        .append(" = ? , ").append(RepConstants.bookmark_lastSyncId4).append(" = ?   where ( ")
        .append(RepConstants.bookmark_LocalName1).append(" = ? and ")
        .append(RepConstants.bookmark_RemoteName2).append(" = ? and ")
        .append(RepConstants.bookmark_TableName3).append(" = ? ) ");
    return subConnection.prepareStatement(query.toString());
  }

  public void setPubName(String pubName0) {
    pubName = pubName0;
  }

  public void setSubName(String subName0) {
    subName = subName0;
  }

  /**
   * deletes records from super log table
   * @param tableName
   * @throws java.lang.Exception
   */
  private void deleteRecordsFromSuperLogTable(String tableName) throws Exception {
    Statement stmt = subConnection.createStatement();
    try {
      // deleting all records from super log table where tableName is passed one
      // or for whihc xml file has been written.
      StringBuffer query = new StringBuffer();
      query.append("delete from ").append(dbHandler.getLogTableName()).append(" where ")
          .append(RepConstants.logTable_tableName2).append(" = '")
          .append(tableName).append("'");
      stmt.executeUpdate(query.toString());
    }
    finally {
      if (stmt != null)
        stmt.close();
    }
  }

  /**
   * insert a dummy record in super log table for getting a unique id after snapshot for inserting records in shadow Table.
   * @throws SQLException
   */
  private void insert_dummyRecordInLogTable() throws SQLException {
    Statement stmt = subConnection.createStatement();
    StringBuffer query = new StringBuffer();
    query.append("insert into ").append(dbHandler.getLogTableName()).append(" (")
        .append(RepConstants.logTable_tableName2).append(") values  ('$$$$$$')");
    stmt.execute(query.toString());
    query = new StringBuffer();
    query.append("Select max(").append(RepConstants.logTable_commonId1).append(") from ")
        .append(dbHandler.getLogTableName());
    ResultSet rs = stmt.executeQuery(query.toString());
    rs.next();
    long maxCID = rs.getLong(1);
    rs.close();
    query = new StringBuffer();
    query.append("delete from ").append(dbHandler.getLogTableName())
        .append(" where ").append(RepConstants.logTable_tableName2).append(" = '")
        .append("$$$$$$").append("' and ").append(RepConstants.logTable_commonId1)
        .append(" != ").append(maxCID);
    stmt.executeUpdate(query.toString());
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
   * This has been implemented to handle the problem
   * related to CLOB and BLOB data type. Postgre
   * do not to insert LOB object in autocommit mode.
   * @param <any> Abs
   * @return boolean
   */
  private void checkAutocommit(AbstractColumnObject aco) throws SQLException {
    if (setAutoCommitFlag) {
      if (dbHandler instanceof PostgreSQLHandler &&
          (aco instanceof ClobObject || aco instanceof BlobObject)) {
        setAutoCommitFlag = false;
        subConnection.setAutoCommit(false);
      }
    }

  }

  private void setAutocomitTrueAndCommitRecord() throws SQLException {
    if (setAutoCommitFlag == false) {
      subConnection.commit();
      subConnection.setAutoCommit(true);
      setAutoCommitFlag = true;
    }
  }

}
