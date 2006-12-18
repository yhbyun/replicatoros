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

package com.daffodilwoods.replication.synchronize;

import java.math.*;
import java.sql.*;
import java.util.*;

import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.DBHandler.*;
import com.daffodilwoods.replication.column.*;
import com.daffodilwoods.replication.xml.*;
import java.io.BufferedWriter;

/**
 * This is the main class which handles the synchronization operations on main table
 * when merge handler reads Insert operation from the XML file.
 */

public class OperationInsert
    extends AbstractSynchronize {
 private PreparedStatement preparedStatement,preparedStatementForLastRecord,
      preparedStatementForCommonId,
      preparedStatementForDelete, updatePreparedStstement, updateShadowTablePST,
      updateMainTableExceptPKCols, updatePreparedStatementSetNull;
 private Object[] record;

  /**
   * OperationInsert handles th case related to Insert operation i.e. when a record
   * with Insert operation is found in XML File.
   *
   * @param repTable0 -> instance of RepTable for which insert operation is called.
   * @param connection0 -> instance of Connection with local server
   * @param columnObjectHashMap0 -> columnHashMap containing the column objects of respective columns of main table.
   * @param conisderedId0 -> last UID ahead from which records of the shadow table are to be searched .
   * @param remoteServerName0 ->  remoteServer Name.
   */
  public OperationInsert(RepTable repTable0, Connection connection0,
                         TreeMap columnObjectTreeMap0, Object conisderedId0,
                         String remoteServerName0,
                         AbstractDataBaseHandler dbHandler0, BufferedWriter bw0,
                         String replicationType0, String transactionLogType0,
                         MetaDataInfo mdi0, boolean isFirstPass0,
                         boolean isCurrentTableCyclic0) throws SQLException {
    repTable = repTable0;
    allColumnsMap =  repTable.getAllColumns();
    connection = connection0;
    conisderedId = conisderedId0;
    remoteServerName = remoteServerName0;
    dbHandler = dbHandler0;
    mdi = mdi0;
    isFirstPass = isFirstPass0;
    isCurrentTableCyclic = isCurrentTableCyclic0;
    tableName = repTable.getSchemaQualifiedName().toString();
    bw = bw0;
    replicationType = replicationType0;
    transactionLogType = transactionLogType0;
    preparedStatement = connection.prepareStatement(repTable.createInsertQueryForSnapShot());
    columnObjectTreeMap = columnObjectTreeMap0;
    shadowTable = dbHandler.getShadowTableName(tableName);

//    RepConstants.shadow_Table(repTable.getSchemaQualifiedName().toString());
    preparedStatementForLastRecord = makePreparedStatementForLastRecord(shadowTable, repTable.getPrimaryColumns());
    Statement stmt = null;
    ResultSet rs = null;
    Object tempVal;
    try {
      stmt = connection.createStatement();
      rs = connection.createStatement().executeQuery("select max(" +RepConstants.shadow_sync_id1 + ") from " + shadowTable);
      rs.next();

      // so as to include the last record of the shadow table in the search.
      // when traceOriginalRecord is called, then we dont want the last record searched to be included in the search.
      // so we are increasing the Last UID by 1.
      tempVal = rs.getObject(1);
    }
    finally {
      /** @todo  close the statement and resultset sube */
      if (rs != null)
        rs.close();
      if (stmt != null)
        stmt.close();
    }

    if (tempVal == null) {
      tempVal = new Long(0);
    }
    else if (tempVal instanceof Double) {
      tempVal = new Long( ( (Double) tempVal).longValue());
    }
    else if (tempVal instanceof BigDecimal) {
      tempVal = new Long( ( (BigDecimal) tempVal).longValue());
    }
    else if (tempVal instanceof Integer) { //for firebird
      tempVal = new Long( ( (Integer) tempVal).longValue());
    }
    else if (tempVal instanceof String) { //for MySQL
          tempVal = new Long( ( (Long.parseLong((String)tempVal))));
        }

    lastShadowUid = new Long( ( (Long) tempVal).longValue() + 1);

    primaryColumnNames = repTable.getPrimaryColumns(); // getting primary column names for the table.
    preparedStatementForCommonId = makePreparedStatementForCommonId();
    tableName = repTable.getSchemaQualifiedName().toString();
    preparedStatementForDelete = makePreparedStatementForDeleteOnTable(tableName);
    updateShadowTablePST = makeUpdate_remoteServerName_SHADOWTABLE_Statement(); // update remote server case
    xmlElement_NULL = new XMLElement("NULL");
    xmlElement_NULL.elementValue = "NULL";

  }

  /**
   * Inserts a record in the corresponding Table.
   * @param currentElement
   * @throws SQLException
   * @throws RepException
   */
  public void execute(XMLElement currentElement) throws SQLException, RepException {

    ArrayList insertElements = currentElement.getChildElements();
    ArrayList rowElements = ( (XMLElement) insertElements.get(0)).getChildElements();
    ArrayList primaryKeyElements = ( (XMLElement) insertElements.get(1)).getChildElements();
    tableColumnNames = new String[rowElements.size()];

    String[] primaryKeyValues = new String[primaryKeyElements.size()];
    primaryKeyColumnsObject = new AbstractColumnObject[primaryKeyElements.size()];

    for (int i = 0; i < primaryKeyElements.size(); i++) {
      String ColumnName = ( ( (XMLElement) primaryKeyElements.get(i)).getAttribute());
      XMLElement prKeyValuesElement = (XMLElement) primaryKeyElements.get(i);
      primaryKeyValues[i] = prKeyValuesElement.elementValue;
      primaryKeyColumnsObject[i] = ( (AbstractColumnObject) columnObjectTreeMap.get(ColumnName));
      primaryKeyColumnsObject[i].setColumnObject(updateShadowTablePST,prKeyValuesElement, i + 1);
    }
    Object[] conflictingPrimaryKeyValues = getObjectArray(primaryKeyValues);
    Object lastSyncId;
    lastSyncId = getLastSyncId();
    record = new Object[rowElements.size()];
    int columnIndex = 0;
    String columnName;
    // This check has been added to handle the cyclic case.
    if (isFirstPass) {
      for (int i = 0; i < rowElements.size(); i++) {

        XMLElement rowElement = (XMLElement) rowElements.get(i);
        columnName =rowElement.getAttribute();
//System.out.println("Operation Insert Execute columnName : "+columnName);
        columnName = (String)allColumnsMap.get(columnName);
//System.out.println("Operation Insert Execute columnName : "+columnName);
//      tableColumnNames[i] = rowElement.getAttribute();
        tableColumnNames[i] = columnName;
        if (repTable.isIgnoredColumn(tableColumnNames[i])) {
          continue;
        }
        String value = rowElement.elementValue;
        record[columnIndex] = value;
        if (isCurrentTableCyclic && repTable.isForiegnKeyColumn(tableColumnNames[i])) {
          AbstractColumnObject aco = (AbstractColumnObject) columnObjectTreeMap.get(tableColumnNames[i]);
          //Set null in cyclic column
          aco.setColumnObject(preparedStatement, "NULL", columnIndex + 1);
        }
        else {
          AbstractColumnObject aco = (AbstractColumnObject) columnObjectTreeMap.get(tableColumnNames[i]);
          //once setAutoCommitFlag is set to false,we shouldn't change it to true
          // by checking for other columns for that 'if' check is used
          if (setAutoCommitFlag) {
            setAutoCommitFlag = checkAutocommit(dbHandler, aco);
            connection.setAutoCommit(setAutoCommitFlag);
          }

          aco.setColumnObject(preparedStatement, rowElement, columnIndex + 1);

        }
        columnIndex++;
      }
      try {
        //Insert record in datasource
        preparedStatement.execute();
        loggingInsertOperation(tableName, record, replicationType);
        updateServerName(primaryKeyElements, lastSyncId);
        writeInsertOperationInTransactionLogFile(bw, tableName, record,replicationType,transactionLogType);
        insertCount++;
      }
      catch (SQLException ex) {
        // primary key voilation
        if (!dbHandler.isPrimaryKeyException(ex)) {
          RepConstants.writeERROR_FILE(ex);
          throw ex;
        }
        try {
          if (!repTable.isLocalServerWinner()) {
            primaryVoilation(conflictingPrimaryKeyValues);
            lastSyncId = getLastSyncId();
            preparedStatement.execute();
            updateServerName(primaryKeyElements, lastSyncId);
          }
        }
        catch (SQLException ex1) {
          /**
           * Handling of Exception on localServer wins
           * :-DELETE statement conflicted with COLUMN REFERENCE constraint
           * (Inserted on both sides)
           * Publisher:          Subscriber:
           * t1:1,2              t1:1,2
           * t2:1,3              t2:1,3
           * First primary key violation occurs then we try to delete
           * from table then foreign key violation occurred.For this case,
           * we have updated all other columns values except primary column
           **/
          if (dbHandler.isForiegnKeyException(ex1)) {
            lastSyncId = getLastSyncId();
            /**@todo
             * when used lastSyncId,it updated subscriber operation also.So,increment by 1
             * done while testing
             */

            long lastSyncId1 = Long.parseLong(lastSyncId.toString());
            lastSyncId1 = lastSyncId1 + 1;
            updateRecordsInCaseOfParentChildTable(rowElements);
            updateServerName(primaryKeyElements, new Long(lastSyncId1));
          }
          else {
            throw ex1;
          }
        }
      }
      finally {
        if (setAutoCommitFlag == false) {
          setAutocomitTrueAndCommitRecord(connection);
        }
      }

    }
    else if (isCurrentTableCyclic) {
      /*
       To update exported columns which we have not updated in first pass in case
             if primarykey then foreign key violation occurs
       */
      if (!repTable.isLocalServerWinner()) {
        updateRecordsInCaseOfParentChildTableSecondPass(rowElements);
        updateServerName(primaryKeyElements, lastSyncId);
      }
      executeUpdateInCyclic(rowElements, primaryKeyElements, lastSyncId,conflictingPrimaryKeyValues);
    }

  }

  /**
   * Updates the serverName in shadow table corresponding to the pk_key and last
   * synchronisation done.
   *
   *  @param primaryKeyElements
   * @param lastSyncId
   * @throws SQLException
   */
  private void updateServerName(ArrayList primaryKeyElements, Object lastSyncId) throws SQLException {
    // updating the shadow table for Server Name after insert
    updateShadowTablePST.setObject(primaryKeyElements.size() + 1, lastSyncId);
    updateShadowTablePST.executeUpdate();
  }

  /**
   * Handles the case when primary key voilation occurs due to synchronisation.
   * Records may be rollbacked to their original status as before synchronization
   * @param conflictingPrimaryKeyValues
   * @throws SQLException
   * @throws RepException
   */
  private void primaryVoilation(Object[] conflictingPrimaryKeyValues) throws SQLException, RepException {
    Tracer tracer = new Tracer();
    traceOriginalRecord(conflictingPrimaryKeyValues, tracer, lastShadowUid); // trace the orignal record inserted/updated in main table for which the  record
    if (tracer.type.equals(RepConstants.insert_operation)) { // Insert Case
      Object[] primaryValues = tracer.primaryKeyValues;
      for (int i = 0; i < primaryValues.length; i++) {
        preparedStatementForDelete.setObject(i + 1, primaryValues[i]);
      }
      if (isCurrentTableCyclic) {
        setNullInColumnOfChildTableToInsertTheRecord(primaryColumnNames,primaryValues);
      }
      preparedStatementForDelete.execute();
    }
    else if (tracer.type.equals(RepConstants.update_operation)) { // Update Case
      if (updatePreparedStstement == null) {
        updatePreparedStstement = makeUpdatePreparedStstement(tableName);
      }
      ResultSet newRS = tracer.rs;
//    showCurrentRow(newRS);
      ArrayList[] updatePS = null;
      ArrayList updatePSValues = null;
      ArrayList updatePS_NULLColObj = null;
      try {
        updatePS = setParametersInUpdatePS(conflictingPrimaryKeyValues, newRS);
        updatePSValues = updatePS[0];
        updatePS_NULLColObj = updatePS[1];
      }
      finally {
        if (newRS != null)
          newRS.close();
      }
      try {
        updatePreparedStstement.execute();
      }
      catch (SQLException ex) {
        if (!dbHandler.isPrimaryKeyException(ex)) {
          throw ex;
        }
        primaryVoilation(tracer.primaryKeyValues);
        int nullCnt = 0;
        for (int i = 0; i < updatePSValues.size(); i++) {
          if (updatePSValues.get(i) != null) {
            updatePreparedStstement.setObject(i + 1, updatePSValues.get(i));
          }
          else {
            ( (AbstractColumnObject) updatePS_NULLColObj.get(nullCnt++)).
                setColumnObject(updatePreparedStstement, xmlElement_NULL, i + 1);
          }
        }
        updatePreparedStstement.execute();
      }
      //we will not  update the shadown table entries with remote server name in this case
    }
  }

  /**
   * setting patameters for update prepared statement with the primary key values passed.
   * @param conflictingPrimaryKeyValues
   * @param newRS
   * @return
   * @throws SQLException
   */
  private ArrayList[] setParametersInUpdatePS(Object[] conflictingPrimaryKeyValues, ResultSet newRS) throws SQLException {
    int cnt = tableColumnNames.length;
    ArrayList list = new ArrayList();
    ArrayList nulllist = new ArrayList();
    int k = 0;
    for (int i = 0; i < cnt; i++) {
      if (repTable.isIgnoredColumn(tableColumnNames[i])) {
        continue;
      }
      Object obj = newRS.getObject(tableColumnNames[i]);
      if (obj != null) {
//      updatePreparedStstement.setObject(i + 1, obj);
        updatePreparedStstement.setObject(k + 1, obj);
      }
      else {
        AbstractColumnObject colObj = ( (AbstractColumnObject)columnObjectTreeMap.get(tableColumnNames[i]));
//        colObj.setColumnObject(updatePreparedStstement, xmlElement_NULL, i + 1);
        colObj.setColumnObject(updatePreparedStstement, xmlElement_NULL, k + 1);
        nulllist.add(colObj);
      }
      k++;
      list.add(obj);
    }

    for (int j = 0; j < conflictingPrimaryKeyValues.length; j++) {
      Object obj = conflictingPrimaryKeyValues[j];
      updatePreparedStstement.setObject(cnt + j + 1, obj);
      list.add(obj);
    }
    return new ArrayList[] {list, nulllist};
  }

  /**
   * For Debugging purpose only
   * @param rs
   * @throws SQLException
   */
  private void showCurrentRowaa(ResultSet rs) throws SQLException {
    ResultSetMetaData rsmd = rs.getMetaData();
    int count = rsmd.getColumnCount();
    for (int i = 1; i <= count; i++) {
      if (i != 1) {
      }
    }
  }

  /**
   * tarce the record present in shadow table for the primary key passed and having syncId less than UID passed.
   * @param primaryKeyValues
   * @param tracer
   * @param Uid
   * @return
   * @throws SQLException, RepException
   */
  private boolean traceOriginalRecord(Object[] primaryKeyValues, Tracer tracer,Object Uid) throws RepException, SQLException {
    preparedStatementForLastRecord.setObject(1, Uid);
    for (int i = 0; i < primaryKeyValues.length; i++) {
      preparedStatementForLastRecord.setObject(i + 2, primaryKeyValues[i]);
    }
    ResultSet lastRow = preparedStatementForLastRecord.executeQuery();
    if (!lastRow.next()) {
      // done by sube singh
      lastRow.close();
      return false;
    }
//        showCurrentRow(lastRow);
    String operation = lastRow.getString(RepConstants.shadow_operation3);

    if (operation.equals(RepConstants.insert_operation)) {
      tracer.type = RepConstants.insert_operation;
      tracer.rs = lastRow;
      tracer.primaryKeyValues = primaryKeyValues;
      return true;
    }
    else {
      // Operation should be UPDATE
      Object commonId = lastRow.getObject(RepConstants.shadow_common_id2);
      preparedStatementForCommonId.setObject(1, commonId);
      for (int i = 0; i < primaryColumnNames.length; i++) {
        Object obj = lastRow.getObject("REP_OLD_" + primaryColumnNames[i]);
        preparedStatementForCommonId.setObject(i + 2, obj);
      }

      ResultSet rs = preparedStatementForCommonId.executeQuery();
      if (!rs.next()) {
        // if other common record not found for Status Before Update 'B'
        throw new RepException("REP051", new Object[] {commonId, shadowTable});
      }
//        showCurrentRow(rs);
      Object[] primaryColValues = new Object[primaryColumnNames.length];
      for (int j = 0; j < primaryColumnNames.length; j++) {
        primaryColValues[j] = rs.getObject(primaryColumnNames[j]);
      }
      Object UID = rs.getObject(RepConstants.shadow_sync_id1);
      boolean gotRow = traceOriginalRecord(primaryColValues, tracer, UID);
      if (gotRow == false) {
        tracer.rs = rs;
        tracer.type = RepConstants.update_operation;
        tracer.primaryKeyValues = primaryColValues;
      }
      else {
        rs.close();
      }
      return true;
    }
  }

  private PreparedStatement makePreparedStatementForLastRecord(String shadowTable, String[] primaryColumns) throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append(" SELECT * FROM  ");
    query.append(shadowTable);
    query.append("  WHERE  ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(">");
    query.append(conisderedId);
    query.append(" AND ");
    query.append(RepConstants.shadow_sync_id1);
    query.append("< ? ");
    for (int i = 0; i < primaryColumns.length; i++) {
      query.append("  AND  ");
      query.append(primaryColumns[i]);
      query.append(" = ? ");
    }
    query.append(" ORDER BY  ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" DESC ");
    return connection.prepareStatement(query.toString());
  }

  private PreparedStatement makePreparedStatementForCommonId() throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append(" SELECT * FROM  ");
    query.append(shadowTable);
    query.append(" WHERE   ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" >= ");
    query.append(conisderedId);
    query.append(" AND ");
    query.append(RepConstants.shadow_common_id2);
    query.append(" = ? ");

    for (int i = 0; i < primaryColumnNames.length; i++) {
      query.append(" AND ");
      query.append("REP_OLD_" + primaryColumnNames[i]);
      query.append(" = ");
      query.append(" ? ");
    }
    query.append(" AND ");
    query.append(RepConstants.shadow_status4);
    query.append(" = '");
    query.append(RepConstants.beforeUpdate);
    query.append("'");
    return connection.prepareStatement(query.toString());
  }

  private Object[] getObjectArray(String[] primaryKeyValues) throws SQLException {
    Object[] pkValues = new Object[primaryKeyColumnsObject.length];
    for (int i = 0; i < primaryKeyColumnsObject.length; i++) {
      pkValues[i] = primaryKeyColumnsObject[i].getObject(primaryKeyValues[i]);
    }
    return pkValues;
  }

  private PreparedStatement makePreparedStatementForDeleteOnTable(String tableName) throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append(" DELETE FROM ");
    query.append(tableName);
    query.append(" WHERE ");
    for (int i = 0; i < primaryColumnNames.length; i++) {
      if (i != 0) {
        query.append(" AND ");
      }
      query.append(primaryColumnNames[i]);
      query.append("= ?");
    }
    return connection.prepareStatement(query.toString());
  }

//  used for rollback in case primary contraint voilation comes;
  private PreparedStatement makeUpdatePreparedStstement(String tableName) throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append("UPDATE  ");
    query.append(tableName);
    query.append(" SET ");

    for (int i = 0; i < tableColumnNames.length; i++) {
      if (repTable.isIgnoredColumn(tableColumnNames[i])) {
        continue;
      }
//      if (i != 0) {
//        query.append(" , ");
//      }
      query.append(", " + tableColumnNames[i]);
      query.append(" = ?");
    }
    int indexOfFirstComma = query.indexOf(",");
    if (indexOfFirstComma != -1) {
      query.deleteCharAt(indexOfFirstComma);
    }
    query.append(" WHERE ");
    for (int i = 0; i < primaryColumnNames.length; i++) {
      if (i != 0) {
        query.append(" AND ");
      }
      query.append(primaryColumnNames[i]);
      query.append("= ?");
    }
    return connection.prepareStatement(query.toString());
  }

  /**
   * returns last synchronisation id from shadow Table
   * @return
   * @throws SQLException
   */
  public Object getLastSyncId() throws SQLException {
    boolean flag = false;
    Object lastId = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      String[] primaryColumnNames = repTable.getPrimaryColumns();
      StringBuffer query = new StringBuffer();
      query.append("SELECT  max(").append(RepConstants.shadow_sync_id1).
          append(") FROM ").append(shadowTable);
      stmt = connection.createStatement();
      rs = stmt.executeQuery(query.toString());
      flag = rs.next();
      lastId = rs.getObject(1);
    }
    finally {
      /** @todo  close the rs and stmt sube */
      if (rs != null)
        rs.close();
      if (stmt != null)
        stmt.close();
    }
    return flag ? (lastId == null ? new Long(0) : lastId) : new Long(0);
  }

  private PreparedStatement makeUpdate_remoteServerName_SHADOWTABLE_Statement() throws SQLException {
    String[] primaryColumnNames = repTable.getPrimaryColumns();
    StringBuffer query = new StringBuffer();
    query.append(" UPDATE  ");
    query.append(shadowTable);
    query.append(" SET ");
    query.append(RepConstants.shadow_serverName_n);
    query.append("='");
    query.append(remoteServerName);
    query.append("' WHERE ");
    for (int i = 0; i < primaryColumnNames.length; i++) {
      if (i != 0) {
        query.append(" AND ");
      }
      query.append(primaryColumnNames[i]);
      query.append("= ?");
    }
    query.append(" AND ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" >= ? ");
    return connection.prepareStatement(query.toString());
  }

  public void closeAllStatments() {
    if (preparedStatementForLastRecord != null) {
      try {
        preparedStatementForLastRecord.close();
      }
      catch (SQLException ex) {
      }
    }
    if (preparedStatementForCommonId != null) {
      try {
        preparedStatementForCommonId.close();
      }
      catch (SQLException ex1) {
      }

    }
    if (preparedStatementForDelete != null) {
      try {
        preparedStatementForDelete.close();
      }
      catch (SQLException ex2) {
      }
    }
    if (updatePreparedStstement != null) {
      try {
        updatePreparedStstement.close();
      }
      catch (SQLException ex3) {
      }
    }
    if (updateShadowTablePST != null) {
      try {
        updateShadowTablePST.close();
      }
      catch (SQLException ex4) {
      }
    }
  }

  /**
   * Update records in main table when problem occure during
   * insertion and table have parent child relationship. In
   * this case update all columns values except primary.
   * @param rowElements ArrayList
   * @param primayKeyObject Object[]
   * @throws SQLException
   */

  private void updateRecordsInCaseOfParentChildTable(ArrayList rowElements) throws SQLException, RepException {
    XMLElement rowElement;
    AbstractColumnObject aco;
    ArrayList list = new ArrayList();
    ArrayList pkRowElementList = new ArrayList();
    String columnName;
    String[] exceptPKCols = null;
    int columnIndex = 1;
    ArrayList exportedColsList = mdi.getExportedTableColsList(repTable.getSchemaQualifiedName());
    list = getColsExceptPK(rowElements, exportedColsList);
    exceptPKCols = new String[list.size()];
    list.toArray(exceptPKCols);
    updateMainTableExceptPKCols = makeUpdatePreparedStmtForeignkey(exceptPKCols);

    // Set object for non primay key columns
    for (int i = 0; i < rowElements.size(); i++) {
      rowElement = (XMLElement) rowElements.get(i);
      columnName = rowElement.getAttribute();
//System.out.println("Operation Insert updateRecordsInCaseOfParentChildTable columnName : " + columnName);
      columnName = (String) allColumnsMap.get(columnName);
// System.out.println("Operation Insert updateRecordsInCaseOfParentChildTable columnName : " + columnName);

      for (int m = 0; m < primaryColumnNames.length; m++) {
        if (primaryColumnNames[m].equalsIgnoreCase(columnName)) {
          pkRowElementList.add(rowElement);
          continue;
        }
      }
      for (int k = 0; k < exceptPKCols.length; k++) {
        if (exceptPKCols[k].equalsIgnoreCase(columnName)) {
          if (!isCurrentTableCyclic) {
            aco = (AbstractColumnObject) columnObjectTreeMap.get(columnName);
            aco.setColumnObject(updateMainTableExceptPKCols, rowElement,columnIndex);
            columnIndex++;
          }
          else if (isCurrentTableCyclic &&!exportedColsList.contains(columnName)) {
            aco = (AbstractColumnObject) columnObjectTreeMap.get(columnName);
            if (!repTable.isForiegnKeyColumn(columnName))
              aco.setColumnObject(updateMainTableExceptPKCols, rowElement,columnIndex);
            else
              aco.setColumnObject(updateMainTableExceptPKCols, "null",columnIndex);
            columnIndex++;
          }
        }
      }
    }
    for (int j = 0; j < primaryColumnNames.length; j++) {
      aco = (AbstractColumnObject) columnObjectTreeMap.get(primaryColumnNames[j]);
      aco.setColumnObject(updateMainTableExceptPKCols,(XMLElement) pkRowElementList.get(j), columnIndex);
      columnIndex++;
    }
    updateMainTableExceptPKCols.execute();
  }

  /**
   * Return the list of all columns without primary key
   * @param rowElements ArrayList
   * @return ArrayList
   */
  private ArrayList getColsExceptPK(ArrayList rowElements, ArrayList exportedColsList) throws  RepException {
    XMLElement rowElement;
    ArrayList list = new ArrayList();
    String columnName;
    for (int i = 0; i < rowElements.size(); i++) {
      rowElement = (XMLElement) rowElements.get(i);
      columnName = rowElement.getAttribute();
      columnName = (String) allColumnsMap.get(columnName);
      if (repTable.isIgnoredColumn(columnName)) {
        continue;
      }
      for (int j = 0; j < primaryColumnNames.length; j++) {
        if (!primaryColumnNames[j].equalsIgnoreCase(columnName)) {
          if (isCurrentTableCyclic && !exportedColsList.contains(columnName)) {
            list.add(columnName);
          }
          else if (!isCurrentTableCyclic)
            list.add(columnName);
        }
      }

    }
    return list;
  }

  /**
   * Come in use when foreign key constraint problem occurs
   * when insert operation is performed
   */
  private PreparedStatement makeUpdatePreparedStmtForeignkey(String[] exceptPkCols) throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append("UPDATE  ");
    query.append(tableName);
    query.append(" SET ");
    for (int i = 0; i < exceptPkCols.length; i++) {
      query.append(", " + exceptPkCols[i]);
      query.append(" = ?");
    }
    int indexOfFirstComma = query.indexOf(",");
    if (indexOfFirstComma != -1) {
      query.deleteCharAt(indexOfFirstComma);
    }
    query.append(" WHERE ");
    for (int i = 0; i < primaryColumnNames.length; i++) {
      if (i != 0) {
        query.append(" AND ");
      }
      query.append(primaryColumnNames[i]);
      query.append("= ?");
    }
    return connection.prepareStatement(query.toString());
  }

  /**
   * Update the values of all the columns that are set to null in
   * first pass.
   * @param rowElements0 ArrayList
   * @param primaryKeyElements0 ArrayList
   * @param lastSyncId0 Object
   * @param conflictingPrimaryKeyValues0 Object[]
   * @throws SQLException
   * @throws RepException
   */
  private void executeUpdateInCyclic(ArrayList rowElements0,
                                     ArrayList primaryKeyElements0,
                                     Object lastSyncId0,
                                     Object[] conflictingPrimaryKeyValues0) throws SQLException, RepException {
    AbstractColumnObject aco = null;
    String updteQuery = repTable.createUpdateQueryForSynchronize();
    if (!updteQuery.trim().equals("")) {
      preparedStatement = connection.prepareStatement(updteQuery);
      int index = 0;
      for (int i = 0; i < rowElements0.size(); i++) {
        XMLElement rowElement = (XMLElement) rowElements0.get(i);
        tableColumnNames[i] = rowElement.getAttribute();
//System.out.println("Operation Insert executeUpdateInCyclic columnName : "+tableColumnNames[i]);
        tableColumnNames[i] = (String)allColumnsMap.get(tableColumnNames[i]);
//System.out.println("Operation Insert executeUpdateInCyclic columnName : "+tableColumnNames[i]);

        if (repTable.isIgnoredColumn(tableColumnNames[i])) {
          continue;
        }
        String value = rowElement.elementValue;
        record[index] = value;
        if (isCurrentTableCyclic) {
          String[] foreignKeyCols = repTable.getForeignKeyCols();
          for (int k = 0, size = foreignKeyCols.length; k < size; k++) {
            if (tableColumnNames[i].equalsIgnoreCase(foreignKeyCols[k])) {
              aco = (AbstractColumnObject) columnObjectTreeMap.get(
                  tableColumnNames[i]);
              //once setAutoCommitFlag is set to false,we shouldn't change it to true
              // by checking for other columns for that 'if' check is used
              if (setAutoCommitFlag) {
                setAutoCommitFlag = checkAutocommit(dbHandler, aco);
                connection.setAutoCommit(setAutoCommitFlag);
              }

              aco.setColumnObject(preparedStatement, rowElement, index + 1);
              index++;
            }
          }
        }

      }
      for (int i = 0; i < primaryKeyColumnsObject.length; i++) {
        aco = (AbstractColumnObject) columnObjectTreeMap.get(primaryColumnNames[i]);
        XMLElement element = (XMLElement) primaryKeyElements0.get(i);
        aco.setColumnObject(preparedStatement,(XMLElement) primaryKeyElements0.get(i), index + 1);
        index++;
      }

      try {
        preparedStatement.execute();
        updateServerName(primaryKeyElements0, lastSyncId0);
      }
      catch (SQLException ex) {
        // Ignore the exception
      }
      finally {
        if (setAutoCommitFlag == false)
          setAutocomitTrueAndCommitRecord(connection);
      }
    }
  }

  private void updateRecordsInCaseOfParentChildTableSecondPass(ArrayList rowElements) throws SQLException, RepException {
    XMLElement rowElement;
    AbstractColumnObject aco;
    ArrayList list = new ArrayList();
    ArrayList pkRowElementList = new ArrayList();
    String columnName;
    String[] exportedCols = null;
    int columnIndex = 1;
    ArrayList exportedColsList = mdi.getExportedTableColsList(repTable.getSchemaQualifiedName());
    exportedCols = new String[exportedColsList.size()];
    exportedColsList.toArray(exportedCols);
    updateMainTableExceptPKCols = makeUpdatePreparedStmtForeignkey(exportedCols);

    // Set object for non primay key columns

    for (int i = 0; i < rowElements.size(); i++) {
      rowElement = (XMLElement) rowElements.get(i);
      columnName = rowElement.getAttribute();
//System.out.println("Operation Insert updateRecordsInCaseOfParentChildTableSecondPass columnName : "+columnName);
        columnName = (String)allColumnsMap.get(columnName);
//System.out.println("Operation Insert updateRecordsInCaseOfParentChildTableSecondPass columnName : "+columnName);

      for (int m = 0; m < primaryColumnNames.length; m++) {
        if (primaryColumnNames[m].equalsIgnoreCase(columnName)) {
          pkRowElementList.add(rowElement);
          continue;
        }
      }
      if (exportedColsList.contains(columnName)) {
        aco = (AbstractColumnObject) columnObjectTreeMap.get(columnName);
        aco.setColumnObject(updateMainTableExceptPKCols, rowElement,columnIndex);
        columnIndex++;
      }
    }
    for (int j = 0; j < primaryColumnNames.length; j++) {
      aco = (AbstractColumnObject) columnObjectTreeMap.get(primaryColumnNames[j]);
      aco.setColumnObject(updateMainTableExceptPKCols,(XMLElement) pkRowElementList.get(j), columnIndex);
      columnIndex++;
    }
    updateMainTableExceptPKCols.execute();
  }

  /**
   * Set null in all column of child table that refering the
   * column of parent table for inserting new record in parent
   * table.
   * @param parentPkCols String[]
   * @param parentPkColValue0 Object[]
   * @throws RepException
   * @throws SQLException
   */
  private void setNullInColumnOfChildTableToInsertTheRecord(String[] parentPkCols, Object[] parentPkColValue0) throws RepException, SQLException {
    Statement stmt = null;
    String[] childFkColumns = null, parentReferedCols = null, parentPkColValue = null;
    Object[] object = null;
    ArrayList childTableList = mdi.getChildTables(tableName);
    String childTableName = null;
    parentPkColValue = new String[parentPkColValue0.length];
    for (int j = 0; j < parentPkColValue0.length; j++) {
      parentPkColValue[j] = parentPkColValue0[j].toString();
    }
    for (int i = 0; i < childTableList.size(); i++) {
      childTableName = (String) childTableList.get(i);
      object = mdi.getImportedColsOfChildTable(tableName, childTableName);
      childFkColumns = (String[]) object[0];
      parentReferedCols = (String[]) object[1];
      updatePreparedStatementSetNull = makeupdateQueryToSetNull(childTableName,
          childFkColumns, parentReferedCols, parentPkCols, parentPkColValue);
      Object lastSynId = getLastSyncId(dbHandler.getShadowTableName(childTableName));
      updatePreparedStatementSetNull.execute();
      String updateRemoteServerNameQuery =
          updateRemoteServerNameWhenSettingNullInChildTable(childTableName,
          childFkColumns, lastSynId, parentReferedCols, parentPkCols,parentPkColValue);
      stmt = connection.createStatement();
      stmt.executeUpdate(updateRemoteServerNameQuery);
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  private PreparedStatement makeupdateQueryToSetNull(String tableName,
      String columns[], String[] parentReferedColumns, String[] parentPkCols,
      String[] parentPkColValue) throws SQLException {
    StringBuffer sb = new StringBuffer();
    sb.append("UPDATE ").append(tableName).append(" SET ");
    for (int i = 0; i < columns.length; i++) {
      if (i != 0) {
        sb.append(" , ");
      }
      sb.append(columns[i] + " = null")
          .append(" where  " + columns[i] + " = " +
                  getParentReferedColValue(parentPkCols, parentPkColValue,parentReferedColumns[i]));

    }
    return connection.prepareStatement(sb.toString());
  }

  /**
   * Get value of a column that is refer by other table
   * for creating a update query to update the columns that
   * are set to null in first pass.
   * @param primaryColsName String[]
   * @param primaryColsValues String[]
   * @param referdColName String
   * @throws SQLException
   * @return Object
   */

  private Object getParentReferedColValue(String[] primaryColsName,
                                          String[] primaryColsValues,
                                          String referdColName) throws SQLException {
    Statement stmt = connection.createStatement();
    StringBuffer sb = new StringBuffer();
    sb.append("SELECT ").append(referdColName)
        .append(" FROM " + tableName)
        .append(" where ");
    for (int i = 0; i < primaryColsName.length; i++) {
      if (i != 0) {
        sb.append(" , ");
      }
      sb.append(primaryColsName[i] + " = " + primaryColsValues[i]);
    }
    ResultSet rs = stmt.executeQuery(sb.toString());
    rs.next();
    return rs.getObject(1);
  }

  private String updateRemoteServerNameWhenSettingNullInChildTable(String
      childTableName, String[] childColumnName, Object lastSyncID,
      String[] parentReferedColumns, String[] parentPkCols,
      String[] parentPkColValue) throws SQLException {
    StringBuffer sb = new StringBuffer();
    String tableName = dbHandler.getShadowTableName(childTableName);
    sb.append(" UPDATE " + tableName)
        .append(" SET " + RepConstants.shadow_serverName_n + " = '")
        .append(remoteServerName + "'")
        .append(" WHERE ")
        .append(RepConstants.shadow_common_id2)
        .append(" = ")
        .append("( ")
        .append("SELECT ").append(RepConstants.shadow_common_id2).append(
        " FROM " + tableName)
        .append(" WHERE ")
        .append(RepConstants.shadow_sync_id1)
        .append(" > ")
        .append("" + lastSyncID + " AND ")
        .append(RepConstants.shadow_status4)
        .append(" = 'B' AND ");
    for (int i = 0; i < childColumnName.length; i++) {
      if (i != 0) {
        sb.append(" , ");
      }
      sb.append(childColumnName[i] + " = " +
                getParentReferedColValue(parentPkCols, parentPkColValue, parentReferedColumns[i]));
    }
    sb.append(") ");
    return sb.toString();
  }

  private Object getLastSyncId(String shadowTableName) throws SQLException {
    Statement statement = null;
    ResultSet rs = null;
    boolean flag = false;
    Object lastId = null;
    try {
      StringBuffer query = new StringBuffer();
      query.append("SELECT  max(").append(RepConstants.shadow_sync_id1)
          .append(") FROM ").append(shadowTableName);
      statement = connection.createStatement();
      rs = statement.executeQuery(query.toString());
      flag = rs.next();
      lastId = rs.getObject(1);
    }
    finally {
      if (rs != null)
        rs.close();
      if (statement != null)
        statement.close();
    }
    return flag ? (lastId == null ? new Long(0) : lastId) : new Long(0);
  }
}
