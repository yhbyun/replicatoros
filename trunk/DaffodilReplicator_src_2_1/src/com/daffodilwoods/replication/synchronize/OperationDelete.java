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

import java.sql.*;
import java.util.*;
import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.DBHandler.*;
import com.daffodilwoods.replication.column.*;
import com.daffodilwoods.replication.xml.*;
import java.io.BufferedWriter;
import com.daffodilwoods.replication.MetaDataInfo;

/**
 * This is the main class which handles the synchronization operations on main table
 * when merge handler finds delete operation from the XML file.
 */

public class OperationDelete
    extends AbstractSynchronize {
  PreparedStatement preparedStatement,
      commonPreparedStatement, primaryPreparedStatement,
      updateRemoteServerNamePreparedStatement, preparedStmtDeleteFromChildTable,
      updatePreparedStatementSetNull;

  /**
   * OperationDelete handles the case related to delete operation i.e. when a
   * record with delete operation is found in XML File
   *
   * @param repTable0
   * @param connection0
   * @param columnObjectHashMap0
   * @param conisderedId
   * @param remoteServerName0
   * @throws Exception
   */
  public OperationDelete(RepTable repTable0, Connection connection0,
                         TreeMap columnObjectTreeMap0, Object conisderedId0,
                         String remoteServerName0,
                         AbstractDataBaseHandler dbHandler0, BufferedWriter bw0,
                         String replicationType0, String transactionLogType0,
                         MetaDataInfo mdi0, boolean isFirstPass0,
                         boolean isCurrentTableCyclic0) throws SQLException {
    repTable = repTable0;
    connection = connection0;
    remoteServerName = remoteServerName0;
    dbHandler = dbHandler0;
    mdi = mdi0;
    tableName = repTable.getSchemaQualifiedName().toString();
    shadowTable = dbHandler.getShadowTableName(tableName);
    preparedStatement = connection.prepareStatement(repTable.createDeleteQueryForSynchronise());
    columnObjectTreeMap = columnObjectTreeMap0;
    // preparedStatement_ShadowTable = connection.prepareStatement(repTable.createDeleteQueryForSynchronise_ShadowTable(conisderedId0, remoteServerName));
    primaryColumnNames = repTable.getPrimaryColumns();
    commonPreparedStatement = makeCommonPreparedStatement();
    primaryPreparedStatement = makePrimaryPreperedStatement(primaryColumnNames);
    conisderedId = conisderedId0;
    bw = bw0;
    replicationType = replicationType0;
    transactionLogType = transactionLogType0;
    isFirstPass = isFirstPass0;
    isCurrentTableCyclic = isCurrentTableCyclic0;
  }

  /**
   * Searches the record for deletion corresponding to the elements Pk_key
   * and deletes the record in the corresponding table.
   *
   * @param currentElement
   * @throws RepException
   * @throws SQLException
   */
  public void execute(XMLElement currentElement) throws RepException, SQLException {
    AbstractColumnObject[] columnObject = null;
    XMLElement pkColumnValueElement = null;
    ArrayList primaryKeyElements = null;
    Object[] pkValuesForSearch = null;
    if (isFirstPass) {
      ArrayList childElements = currentElement.getChildElements();
      primaryKeyElements = ( (XMLElement) childElements.get(0)).getChildElements();
      if (updateRemoteServerNamePreparedStatement == null) {
        updateRemoteServerNamePreparedStatement =makeUpdate_remoteServerName_SHADOWTABLE_Statement();
      }
      int size = primaryKeyElements.size();
      columnObject = new AbstractColumnObject[size];
      pkValuesForSearch = new Object[size];
      String[] primaryColumnValues = new String[size];
      for (int i = 0; i < size; i++) {
        String primaryColumnName = ( (XMLElement) primaryKeyElements.get(i)).getAttribute();
        pkColumnValueElement = (XMLElement) primaryKeyElements.get(i);
        columnObject[i] = (AbstractColumnObject) columnObjectTreeMap.get(primaryColumnName);
        // columnObject[i].setColumnObject(preparedStatement_ShadowTable,pkColumnValueElement, i + 1);
        columnObject[i].setColumnObject(preparedStatement, pkColumnValueElement,i + 1);
        columnObject[i].setColumnObject(updateRemoteServerNamePreparedStatement,pkColumnValueElement, i + 1);
        primaryColumnValues[i] = pkColumnValueElement.elementValue;
        pkValuesForSearch[i] = columnObject[i].getObject(pkColumnValueElement.elementValue); // Primary column value
      }
      Tracer tracer = new Tracer();
      getLastRecord(pkValuesForSearch, conisderedId, tracer);
      //kept outside as used in if and its else i.e both first pass conditions
      Object lastSyncId = getLastSyncId(shadowTable);

      try {
        //ResultSet anyRowFound = preparedStatement_ShadowTable.executeQuery();
        /* Tracer tracer = new Tracer();
         getLastRecord(pkValuesForSearch, conisderedId, tracer);*/
        // if no record is found in shadow table corresponding to the deleted one which came in XML file
        if (!tracer.recordFound) {
          if (isCurrentTableCyclic && isFirstPass) {
            setNullInColumnOfChildTableToDeleTheRecord(primaryColumnNames,primaryColumnValues);
          }
          //kept outside as used in if and its else i.e both first pass conditions
//          Object lastSyncId = getLastSyncId();
          preparedStatement.execute();
          // update shadow table with Remote Server NAme
          updateRemoteServerNamePreparedStatement.setObject(primaryKeyElements.size() + 1, lastSyncId);
          updateRemoteServerNamePreparedStatement.executeUpdate();
          loggingDeleteOperation(tableName, primaryColumnNames,pkValuesForSearch, replicationType);
          writeDeleteOperationInTransactionLogFile(bw, tableName,primaryColumnNames, pkValuesForSearch, replicationType,transactionLogType);
          deleteCount++;
        }
        // if a record is found in shadow table corresponding to the call for delete in XML file i.e. record to be deleted on local server has been updated or allready deleted
        else {
          if (! (tracer.type.equals(RepConstants.delete_operation))) { // i.e update operation was done on original record
            Object[] tablePrimaryKeys = tracer.primaryKeyValues;
            if (!repTable.isLocalServerWinner()) {
              if (isCurrentTableCyclic && isFirstPass) {
                setNullInColumnOfChildTableToDeleTheRecord(primaryColumnNames, primaryColumnValues);
              }
              deleteRecordFromTable_n_UpdateremoteServerName(primaryKeyElements,tablePrimaryKeys);
            }
            else {
              // MATCH last and original record if both are same then delet the record otherwise leave it.
              ResultSet lastRSFromShadowTable = tracer.rs;
              int count = lastRSFromShadowTable.getMetaData().getColumnCount();
              Object[] lastRecordFromShadowTbale = new Object[count - 5];
              for (int i = 5, j = 0; i <= count - 1; i++) {
                lastRecordFromShadowTbale[j++] = lastRSFromShadowTable.getObject(i);
              }
              Object[] originalRowBeforeUpdateInTable = tracer.oldRow;
              for (int i = 0; i < originalRowBeforeUpdateInTable.length; i++) {
                if (!originalRowBeforeUpdateInTable[i].equals(lastRecordFromShadowTbale[i])) {
                  return;
                }
              }
              // delete the record.
              for (int i = 0; i < tablePrimaryKeys.length; i++) {
                preparedStatement.setObject(i + 1, tablePrimaryKeys[i]);
              }
              if (isCurrentTableCyclic) {
                setNullInColumnOfChildTableToDeleTheRecord(primaryColumnNames,primaryColumnValues);
              }
              preparedStatement.execute();
              loggingDeleteOperation(tableName, primaryColumnNames,tablePrimaryKeys, replicationType);
              writeDeleteOperationInTransactionLogFile(bw, tableName, primaryColumnNames, tablePrimaryKeys, replicationType, transactionLogType);
              count++;
            }
          }
        }
      }
      catch (SQLException ex) {
        // primary key voilation
        if (dbHandler.isForiegnKeyException(ex)) {
          if (isCurrentTableCyclic) {
            setNullInColumnOfChildTableToDeleTheRecord(primaryColumnNames,primaryColumnValues);
            preparedStatement.execute();
          }
          else {
            deleteRecordFromChildTable(columnObject, pkColumnValueElement);
            preparedStatement.execute();
          }

          // update shadow table with Remote Server NAme
          updateRemoteServerNamePreparedStatement.setObject(primaryKeyElements. size() + 1, getLastSyncId(shadowTable));
          updateRemoteServerNamePreparedStatement.executeUpdate();
          loggingDeleteOperation(tableName, primaryColumnNames,pkValuesForSearch, replicationType);
          writeDeleteOperationInTransactionLogFile(bw, tableName,primaryColumnNames, pkValuesForSearch, replicationType,transactionLogType);
          deleteCount++;
        }
        else if (!dbHandler.isPrimaryKeyException(ex)) {
          RepConstants.writeERROR_FILE(ex);
          throw ex;
        }
      }
    }

  }

  /**
   * Delete record from a table and also update remoteServerName column of shadow table
   * for the record inserted due to deletion.
   * @param primaryKeyElements
   * @param tablePrimaryKeys
   * @throws SQLException
   */
  private void deleteRecordFromTable_n_UpdateremoteServerName(ArrayList
      primaryKeyElements, Object[] tablePrimaryKeys) throws RepException,
      SQLException {
    for (int i = 0; i < tablePrimaryKeys.length; i++) {
      preparedStatement.setObject(i + 1, tablePrimaryKeys[i]);
      updateRemoteServerNamePreparedStatement.setObject(i + 1,tablePrimaryKeys[i]);
    }
    Object lastSyncId = getLastSyncId(shadowTable);
    preparedStatement.execute();
    // update shadow table with Remote Server NAme
    updateRemoteServerNamePreparedStatement.setObject(primaryKeyElements.size() +1, lastSyncId);
    updateRemoteServerNamePreparedStatement.executeUpdate();
    loggingDeleteOperation(tableName, primaryColumnNames, tablePrimaryKeys,replicationType);
    writeDeleteOperationInTransactionLogFile(bw, tableName, primaryColumnNames,tablePrimaryKeys, replicationType, transactionLogType);
    deleteCount++;
  }

  /**
   * returns a ResultSet for other common record found in shadowTable corresponding to common ID in it.
   * @param rs
   * @return
   * @throws SQLException
   */
  private ResultSet getOtherCommonRecord(ResultSet rs) throws SQLException {
    Object commonId = rs.getObject(RepConstants.shadow_common_id2);
    Object UId = rs.getObject(RepConstants.shadow_sync_id1);
    commonPreparedStatement.setObject(1, UId);
    commonPreparedStatement.setObject(2, commonId);
    for (int i = 0; i < primaryColumnNames.length; i++) {
      commonPreparedStatement.setObject(i + 3,rs.getObject("REP_OLD_" +primaryColumnNames[i]));
    }
    ResultSet resultSet = commonPreparedStatement.executeQuery();
    return resultSet;
  }

  /**
   * searches the last record from the shadow table for which the primary key is passed i.e. the the record whihc actually exists the the main table which the shadow table corresponds too.
   * @param primaryColValues
   * @param UId
   * @param tracer
   * @return
   * @throws SQLException, RepException
   */
  private boolean getLastRecord(Object[] primaryColValues, Object UId,
                                Tracer tracer) throws SQLException, RepException {
    primaryPreparedStatement.setObject(1, UId);
    for (int i = 0; i < primaryColValues.length; i++) {
      primaryPreparedStatement.setObject(i + 2, primaryColValues[i]);
    }
    ResultSet rs = primaryPreparedStatement.executeQuery();
    if (!rs.next()) {
      rs.close();
      tracer.recordFound = false;
      return false;
    }
    else {
      String operation = rs.getString(RepConstants.shadow_operation3);
      if (operation.equals(RepConstants.delete_operation)) { // Delete Operation
        tracer.recordFound = true;
        tracer.type = RepConstants.delete_operation;
        return true;
      }
      else {
        tracer.setOldRow(rs);
        ResultSet resultSet = getOtherCommonRecord(rs);
        boolean updatedREcord = resultSet.next();
        if (!updatedREcord) { // other common record not found
          throw new RepException("REP051", new Object[] {rs.getObject(RepConstants. shadow_common_id2), shadowTable});
        }
        primaryColValues = new Object[primaryColumnNames.length];
        for (int i = 0; i < primaryColumnNames.length; i++) {
          primaryColValues[i] = resultSet.getObject(primaryColumnNames[i]);
        }
        Object currentUId = resultSet.getObject(RepConstants.shadow_sync_id1);
        boolean nextFound = getLastRecord(primaryColValues, currentUId, tracer);
        if (nextFound == false) {
          tracer.rs = resultSet;
          tracer.recordFound = true;
          tracer.type = RepConstants.update_operation;
          tracer.primaryKeyValues = primaryColValues;
        }
        else {
          resultSet.close();
        }
        return true;
      }
    }
  }

  private PreparedStatement makeCommonPreparedStatement() throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append(" select * from ");
    query.append(shadowTable);
    query.append(" where ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" != ");
    query.append(" ? ");
    query.append(" and ");
    query.append(RepConstants.shadow_common_id2);
    query.append(" = ");
    query.append(" ? ");
// for matching oldPrimary keys
    for (int i = 0; i < primaryColumnNames.length; i++) {
      query.append(" and ");
      query.append("REP_OLD_" + primaryColumnNames[i]);
      query.append(" = ");
      query.append(" ? ");
    }
    query.append(" and ");
    query.append(RepConstants.shadow_status4);
    query.append(" = '");
    query.append(RepConstants.afterUpdate);
    query.append("' order by ").append(RepConstants.shadow_sync_id1);
    return connection.prepareStatement(query.toString());
  }

  private PreparedStatement makePrimaryPreperedStatement(String[]
      primaryColumns) throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append(" select * from ");
    query.append(shadowTable);
    query.append(" where ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" > ?");
    for (int i = 0; i < primaryColumns.length; i++) {
      query.append(" and ");
      query.append(primaryColumns[i]);
      query.append(" = ? ");
    }
    return connection.prepareStatement(query.toString());
  }

  private Object getLastSyncId(String shadowTableName) throws SQLException {
    Statement statement = null;
    ResultSet rs = null;
    boolean flag = false;
    Object lastId = null;
    try {
//        String[] primaryColumnNames = repTable.getPrimaryColumns();
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

  private PreparedStatement makeUpdate_remoteServerName_SHADOWTABLE_Statement() throws
      SQLException {
    String[] primaryColumnNames = repTable.getPrimaryColumns();
    StringBuffer query = new StringBuffer();
    query.append(" update  ");
    query.append(shadowTable);
    query.append(" set ");
    query.append(RepConstants.shadow_serverName_n);
    query.append(" ='");
    query.append(remoteServerName);
    query.append("' where ");
    for (int i = 0; i < primaryColumnNames.length; i++) {
      if (i != 0) {
        query.append(" and ");
      }
      query.append(primaryColumnNames[i]);
      query.append(" = ?");
    }
    query.append(" and ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" >= ? ");
    return connection.prepareStatement(query.toString());
  }

  public void closeAllStatments() {
    if (preparedStatement != null) {
      try {
        preparedStatement.close();
      }
      catch (SQLException ex) {
      }
    }
    if (commonPreparedStatement != null) {
      try {
        commonPreparedStatement.close();
      }
      catch (SQLException ex1) {
      }
    }
    if (primaryPreparedStatement != null) {
      try {
        primaryPreparedStatement.close();
      }
      catch (SQLException ex2) {
      }
    }
    if (updateRemoteServerNamePreparedStatement != null) {
      try {
        updateRemoteServerNamePreparedStatement.close();
      }
      catch (SQLException ex3) {
      }
    }

    if (updatePreparedStatementSetNull != null) {
      try {
        updatePreparedStatementSetNull.close();
      }
      catch (SQLException ex4) {
      }
    }

  }

  private PreparedStatement makePSToDeleteFromChildTables(String childTableName,
      String[] primaryColumnNames) throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append(" DELETE FROM ");
    query.append(childTableName);
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

  private void deleteRecordFromChildTable(AbstractColumnObject[] columnObject,
                                          XMLElement pkColumnValueElement) throws
      RepException, SQLException {
    String[] childFkColumns = null;
    ArrayList childTableList = mdi.getChildTables(tableName);
    String childTableName = null;
    Object[] object = null;
    for (int i = 0; i < childTableList.size(); i++) {
      childTableName = (String) childTableList.get(i);
      object = mdi.getImportedColsOfChildTable(tableName, childTableName);
      childFkColumns = (String[]) object[0];
      preparedStmtDeleteFromChildTable = makePSToDeleteFromChildTables(childTableName, childFkColumns);
      for (int j = 0; j < columnObject.length; j++) {
        columnObject[j].setColumnObject(preparedStmtDeleteFromChildTable,pkColumnValueElement, j + 1);
        columnObject[j].setColumnObject(updateRemoteServerNamePreparedStatement, pkColumnValueElement, j + 1);
      }
      preparedStmtDeleteFromChildTable.execute();
    }
  }

  /**
   * Update the record of child table by NULL to Delete the
   * record from parent table
   * @param parentPkCols String[]
   * @param parentPkColValue String[]
   * @throws RepException
   * @throws SQLException
   */

  private void setNullInColumnOfChildTableToDeleTheRecord(String[] parentPkCols,
      String[] parentPkColValue) throws RepException, SQLException {
    Statement stmt = null;
    String[] childFkColumns = null, parentReferedCols = null;
    Object[] object = null;
    ArrayList childTableList = mdi.getChildTables(tableName);
    String childTableName = null;
    for (int i = 0; i < childTableList.size(); i++) {
      childTableName = (String) childTableList.get(i);
      object = mdi.getImportedColsOfChildTable(tableName, childTableName);
      childFkColumns = (String[]) object[0];
      parentReferedCols = (String[]) object[1];
      updatePreparedStatementSetNull = makeupdateQueryToSetNull(childTableName,childFkColumns, parentReferedCols, parentPkCols, parentPkColValue);
      Object lastSynId = getLastSyncId(dbHandler.getShadowTableName(childTableName));
      updatePreparedStatementSetNull.execute();
      String updateRemoteServerNameQuery =updateRemoteServerNameWhenSettingNullInChildTable(childTableName,childFkColumns, lastSynId, parentReferedCols, parentPkCols,parentPkColValue);
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
      sb.append(columns[i] + " = null").append(" where  " + columns[i] + " = " +getParentReferedColValue(parentPkCols, parentPkColValue,parentReferedColumns[i]));
    }
    return connection.prepareStatement(sb.toString());
  }

  private Object getParentReferedColValue(String[] primaryColsName,String[] primaryColsValues,String referdColName) throws
      SQLException {
    Statement stmt = connection.createStatement();
    StringBuffer sb = new StringBuffer();
    sb.append("SELECT ").append(referdColName).append(" FROM " + tableName).append(" where ");
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

  /**
   * Update dbo.rep_shadow_t10 set rep_server_name ='sube_3001' where rep_common_id = (select rep_common_id from dbo.rep_shadow_t10 where rep_Sync_id>30 and rep_status ='b' and c4 =10)
   */

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
      sb.append(childColumnName[i] + " = " +getParentReferedColValue(parentPkCols, parentPkColValue,parentReferedColumns[i]));
    }
    sb.append(") ");
    return sb.toString();
  }

  /**
   * To get the server Name for checking of delete able record
   * If Any update operation found on original record is of local server
   * then no record delete,else If any update operation found on original record
   * is of remote server,then delete operation is to be done.
   * @param rs ResultSet
   * @return String
   */
  private String getServerNameForDeleteRecord(ResultSet rs) {
    return null;
  }

}
