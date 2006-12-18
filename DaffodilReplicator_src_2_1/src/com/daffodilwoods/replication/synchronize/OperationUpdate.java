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
 * when merge handler reads Update operation from the XML file.
 */

public class OperationUpdate extends AbstractSynchronize {

 private PreparedStatement psForInitialRecordInShadowTable, psForActualUpdateInTable,
      preparedStatementForLastRecord, preparedStatementForCommonId,
      preparedStatementForDelete, updatePreparedStatement, psForOtherCommonId,
      psForInsertIntoActualTable, psForUpdateInShadowTableForRemoteServer,
      updatePreparedStatementSetNull;
  private ArrayList voilationPkKeys;
  private boolean NO_OPERATION = false;



  /**
   * OperationUpdate handles the case related to update operation i.e. when a
   * record with update operation is found in XML File.
   * @param repTable0
   * @param clientConnection0
   * @param columnObjectHashMap0
   * @param conisderedId0
   * @param remoteServerName0
   * @throws Exception
   */
  public OperationUpdate(RepTable repTable0, Connection clientConnection0,
                         TreeMap columnObjectTreeMap0, Object conisderedId0,
                         String remoteServerName0,
                         AbstractDataBaseHandler dbHandler0, BufferedWriter bw0,
                         String replicationType0, String transactionLogType0,
                         MetaDataInfo mdi0, boolean isFirstPass0,
                         boolean isCurrentTableCyclic0) throws SQLException {
    repTable = repTable0;
    allColumnsMap = repTable.getAllColumns();
    connection = clientConnection0;
    columnObjectTreeMap = columnObjectTreeMap0;
    remoteServerName = remoteServerName0;
    conisderedId = conisderedId0;
    tableName = repTable.getSchemaQualifiedName().toString();
    dbHandler = dbHandler0;
    mdi = mdi0;
    shadowTable = dbHandler.getShadowTableName(tableName);
    bw = bw0;
    replicationType = replicationType0;
    transactionLogType = transactionLogType0;

// so as to include the last record of the shadow table in the search.
// when traceOriginalRecord is called, then we dont want the last record searched to be included in the search.
// so we are increasing the Last UID by 1.
    Object lastShadowUidObj = getLastUIDFromShadowTable();
//     lastShadowUid = new Long( ( (Long) getLastUIDFromShadowTable()).longValue() + 1);
    if ((lastShadowUidObj instanceof Integer)) { //for firebird
      lastShadowUid = new Long( ( (Integer) lastShadowUidObj).longValue()+1);
    }
    else if ((lastShadowUidObj instanceof String)) { //for MySQL
     lastShadowUid = new Long((Long.parseLong((String)lastShadowUidObj)+1));
    }
    else {
      lastShadowUid = new Long(((Long)lastShadowUidObj).longValue()+1);
    }
    xmlElement_NULL = new XMLElement("NULL");
    xmlElement_NULL.elementValue = "NULL";
    isFirstPass = isFirstPass0;
    isCurrentTableCyclic = isCurrentTableCyclic0;
  }

  /**
   * searches the record for pk_key in shadow table  and updates it in the corresponding main table.
   * @param currentElement
   * @throws SQLException
   * @throws RepException
   */
  public void execute(XMLElement currentElement) throws SQLException, RepException {
    try {
      ArrayList updateElements = currentElement.getChildElements();
      ArrayList rowElements = ( (XMLElement) updateElements.get(0)).getChildElements();
      ArrayList changedColumns = ( (XMLElement) updateElements.get(1)).getChildElements();
      ArrayList primaryColumns = ( (XMLElement) updateElements.get(2)).getChildElements();
      tableColumnNames = new String[rowElements.size()];
      String[] tableColumnValues = new String[rowElements.size()];
      AbstractColumnObject[] tableColumnObj = new AbstractColumnObject[rowElements.size()];
      primaryColumnNames = repTable.getPrimaryColumns();
      int noOfPrimaryCol = primaryColumnNames.length;
      Object[] newPrimaryColumnValues = new Object[primaryColumnNames.length];

      String columnNam;
      for (int i = 0; i < rowElements.size(); i++) {
        columnNam = ( (XMLElement) rowElements.get(i)).getAttribute();
//System.out.println("OperationUpdate.execute(currentElement) : "+columnNam);
        columnNam =(String)allColumnsMap.get(columnNam);
//System.out.println("OperationUpdate.execute(currentElement) : "+columnNam);
        tableColumnNames[i] = columnNam;
//      tableColumnNames[i] = ( (XMLElement) rowElements.get(i)).getAttribute();
        tableColumnValues[i] = ( (XMLElement) rowElements.get(i)).elementValue;
        tableColumnObj[i] = ( (AbstractColumnObject) columnObjectTreeMap.get(tableColumnNames[i]));
        for (int j = 0; j < noOfPrimaryCol; j++) {
          if (tableColumnNames[i].equalsIgnoreCase(primaryColumnNames[j])) {
            newPrimaryColumnValues[j] = ( (AbstractColumnObject) tableColumnObj[i]).getObject(tableColumnValues[i]);
            break;
          }
        }
      }
      Object[] objArray = getObjectArrayForTable(tableColumnValues,tableColumnObj);
      Object[] tableColumnObjects = (Object[]) objArray[0];
      ArrayList nullTableColumnObjects = (ArrayList) objArray[1];

      // When No changes are done in updated column,i.e old row and new updated row is same ("NO_OPERATION")
      boolean changedCol = true;
      if (changedColumns.size() == 0) {
        if (repTable.isLocalServerWinner()) {
          return;
        }
        changedCol = false; // special handelling for NO_Operation ... same record with same pk willl be updated  again
      }
      int noOfChangedColumns = changedColumns.size();
      changedColumnNames = new String[noOfChangedColumns];
      changedColumnValues = new String[noOfChangedColumns];
      XMLElement[] changedColElement = new XMLElement[noOfChangedColumns];
      AbstractColumnObject[] changedColumnObject = new AbstractColumnObject[noOfChangedColumns];
      for (int i = 0; i < noOfChangedColumns; i++) {
        changedColElement[i] = (XMLElement) changedColumns.get(i);
        changedColumnNames[i] = changedColElement[i].getAttribute();
        changedColumnValues[i] = changedColElement[i].elementValue;
        changedColumnObject[i] = ( (AbstractColumnObject) columnObjectTreeMap.get(changedColumnNames[i]));
      }

      String[] primaryColumnValues = new String[noOfPrimaryCol];
      primaryKeyColumnsObject = new AbstractColumnObject[noOfPrimaryCol];

      for (int i = 0; i < noOfPrimaryCol; i++) {
        primaryColumnValues[i] = ( (XMLElement) primaryColumns.get(i)).elementValue;
        primaryKeyColumnsObject[i] = ( (AbstractColumnObject)columnObjectTreeMap.get(primaryColumnNames[i]));
      }
      Object[] primaryValueObjects = getObjectArrayForPrimaryKey(primaryColumnValues);

      // SPECIAL CASE ONLY FOR NO_OPERATION
      if (!changedCol) {
        // special handelling for NO_OPERATION ... assigning changedColumns to current primary keys
        int pkLength = primaryColumnNames.length;
        changedColElement = new XMLElement[pkLength];
        changedColumnValues = new String[pkLength];
        changedColumnNames = new String[pkLength];
        changedColumnObject = new AbstractColumnObject[pkLength];
        for (int i = 0; i < primaryColumnNames.length; i++) {
          changedColumnNames[i] = primaryColumnNames[i];
          changedColumnValues[i] = primaryColumnValues[i];
          changedColumnObject[i] = primaryKeyColumnsObject[i];
          XMLElement temp = new XMLElement(changedColumnNames[i]);
          temp.setElementValue(changedColumnValues[i]);
          changedColElement[i] = temp;
        }
        noOfChangedColumns = pkLength;
        NO_OPERATION = true;
      }

      if (psForInitialRecordInShadowTable == null) {
        psForInitialRecordInShadowTable = makePSForInitialRecordInShadowTable();

        //    if (   == null )
        //    because we can get diffrent number of changed columns for all or other rows
        //    so we have to create ps every time for this.
      }
      psForActualUpdateInTable = makePSForActualUpdateInTable();

      if (psForUpdateInShadowTableForRemoteServer == null) {
        psForUpdateInShadowTableForRemoteServer =
            makePSForUpdateInShadowTableForRemoteServer();
      }

      psForUpdateInShadowTableForRemoteServer.setString(1, remoteServerName);
      int k = 0;
      if (isFirstPass) {
        for (int i = 0; i < noOfChangedColumns; i++) {
          if (repTable.isIgnoredColumn(changedColumnNames[i]))
            continue;
          /** @todo  Changes made by sube For cyclic handling */
          AbstractColumnObject columnOject = ( (AbstractColumnObject)columnObjectTreeMap.get(changedColumnNames[i]));

          /*
           Once setAutoCommitFlag is set to true,we shouldn't change it to false
           by checking for other columns for that 'if' check is used
           */
          if (setAutoCommitFlag) {
            setAutoCommitFlag = checkAutocommit(dbHandler, columnOject);
            connection.setAutoCommit(setAutoCommitFlag);
          }

          if (isCurrentTableCyclic &&
              repTable.isForiegnKeyColumn(changedColumnNames[i])) {
            columnOject.setColumnObject(psForActualUpdateInTable, "NULL", k + 1);
            k++;
          }
          else {
            columnOject.setColumnObject(psForActualUpdateInTable,changedColElement[i], k + 1);
            k++;
          }

        }

        for (int i = 0; i < noOfPrimaryCol; i++) {
//System.out.println("$$$$$$$$$ primaryValueObjects[i]  : "+primaryValueObjects[i]);
          psForActualUpdateInTable.setObject(noOfChangedColumns + i + 1,primaryValueObjects[i]);
          psForUpdateInShadowTableForRemoteServer.setObject(3 + i,primaryValueObjects[i]);
        }

        Tracer tracer = new Tracer();
        getLastRecord(primaryValueObjects, conisderedId, tracer);
        voilationPkKeys = new ArrayList();
        if (!tracer.recordFound) {
          // get max UID/syncId from Shadow Table
          Object lastSyncId = getLastUIDFromShadowTable();
//System.out.println("246 $$$$$$$$$$$$$$  lastSyncId  : "+lastSyncId);
          // setting Object values for Update query for Shadow TABLW for REMOTE SERVER
          psForUpdateInShadowTableForRemoteServer.setObject(2, lastSyncId);

          try {
            psForActualUpdateInTable.executeUpdate();
            // STATUS - A  update remote server name for update case WHEN NO OPERATION IS PERFORMED ON CLIENT SIDE i.e. no entry is found in shadow table
            // update the old record in shadow table with remoteServer STATUIS -B
            psForUpdateInShadowTableForRemoteServer.executeUpdate(); // Status- B Update in shadow table to remoteserverName
            for (int i = 0; i < newPrimaryColumnValues.length; i++) {
              psForUpdateInShadowTableForRemoteServer.setObject(i + 3,newPrimaryColumnValues[i]);
            }
            psForUpdateInShadowTableForRemoteServer.executeUpdate(); // Status- A Update in shadow table to remoteserverName
            loggingUpdateOperation(tableName, primaryColumnNames,primaryValueObjects, changedColumnNames,changedColumnValues, replicationType);
            writeUpdateOperationInTransactionLogFile(bw, tableName,primaryColumnNames, primaryValueObjects, changedColumnNames,changedColumnValues, replicationType, transactionLogType);
            updateCount++;
          }
          catch (SQLException ex) {

//System.out.println(" Operation update   Error Code ::   "+ex.getErrorCode());

//            ex.printStackTrace();
            // PRIMARY CONSTRAINT VOILATION
            if (!dbHandler.isPrimaryKeyException(ex)) {
              RepConstants.writeERROR_FILE(ex);
              throw ex;
            }

            /*
             =============================
             COMMENTED CODE FOR ROLLBACK
             IF WE DO NOT WANT TO DELETE,
             BUT THIS MAKES A CASE TO FAIL
             INVOLVING NO_OPERATION.
             =============================
                    primaryVoilation(newPrimaryColumnValues, new Tracer()); // no use of tracer over here
                    lastSyncId = getLastUIDFromShadowTable();
                    // setting Object values for Update query for Shadow TABLW for REMOTE SERVER
             psForUpdateInShadowTableForRemoteServer.setObject(2, lastSyncId);
                    psForActualUpdateInTable.executeUpdate();
                    psForUpdateInShadowTableForRemoteServer.executeUpdate();
                    for (int i = 0; i < newPrimaryColumnValues.length; i++) {
                      psForUpdateInShadowTableForRemoteServer.setObject(i + 3,newPrimaryColumnValues[i]);
                    }
                    psForUpdateInShadowTableForRemoteServer.executeUpdate(); // Status- A Update in shadow table to remoteserverName
             */
            // deleting the record
            if (preparedStatementForDelete == null) {
              preparedStatementForDelete =makePreparedStatementForDeleteOnTable();
            }
            for (int i = 0; i < newPrimaryColumnValues.length; i++) {
// System.out.println("newPrimaryColumnValues[i] : " +newPrimaryColumnValues[i]);
              preparedStatementForDelete.setObject(i + 1,newPrimaryColumnValues[i]);
            }

            if (isCurrentTableCyclic) {
              setNullInColumnOfChildTableToUpdateRecord(primaryColumnNames,newPrimaryColumnValues);
            }
            preparedStatementForDelete.executeUpdate();
            lastSyncId = getLastUIDFromShadowTable();

            // setting Object values for Update query for Shadow TABLW for REMOTE SERVER
            psForUpdateInShadowTableForRemoteServer.setObject(2, lastSyncId);
            psForActualUpdateInTable.executeUpdate();
            loggingUpdateOperation(tableName, primaryColumnNames,primaryValueObjects, changedColumnNames,changedColumnValues, replicationType);
            writeUpdateOperationInTransactionLogFile(bw, tableName, primaryColumnNames, primaryValueObjects, changedColumnNames,changedColumnValues, replicationType, transactionLogType);
            updateCount++;
            // remoteserver name is not updated so that operaion performed on subscriber is taken on the publisher node.
          }

        }
        else {
          if (tracer.type.equals(RepConstants.delete_operation)) {
            if (NO_OPERATION) {
              return;
            }
            if (repTable.isLocalServerWinner()) {
              return;
            }
            if (psForInsertIntoActualTable == null) {
              psForInsertIntoActualTable = connection.prepareStatement(repTable.createInsertQueryForSnapShot());
            }
            int nullCnt = 0;
            int position = 0;
            for (int i = 0; i < tableColumnObjects.length; i++) {
              Object obj = tableColumnObjects[i];
              String columnName = tableColumnNames[i];
              if (repTable.isIgnoredColumn(columnName))
                continue;

              /**
               *  Following check has been added to handle the case of
               *  cyclic tables.Set value of all columns to null that are refering
               *  to other columns.
               */

              if (isCurrentTableCyclic &&
                  repTable.isForiegnKeyColumn(columnName)) {
                AbstractColumnObject aco = ( (AbstractColumnObject)columnObjectTreeMap.get(columnName));
                aco.setColumnObject(psForInsertIntoActualTable, "NULL",position + 1);
              }
              else if (obj != null) {
                psForInsertIntoActualTable.setObject(position + 1, obj);
              }
              else {
                ( (AbstractColumnObject) nullTableColumnObjects.get(nullCnt)).
                    setColumnObject(psForInsertIntoActualTable, xmlElement_NULL,position + 1);
                nullCnt++;
              }
              position++;
            }
            Object lastSyncId;
            try {
              lastSyncId = getLastUIDFromShadowTable();
              psForInsertIntoActualTable.execute();
            }
            catch (SQLException ex) { // primary constrant voilation
              if (!dbHandler.isPrimaryKeyException(ex)) {
                throw ex;
              }
              primaryVoilation(newPrimaryColumnValues, new Tracer()); // no operation found for tracer till now
              lastSyncId = getLastUIDFromShadowTable();
              psForInsertIntoActualTable.execute();
            }

            // update shadow table with remoteserverNAme FOR INSERT
            // get max UID/syncId from Shadow Table
            psForUpdateInShadowTableForRemoteServer.setObject(2, lastSyncId);
            for (int i = 0; i < newPrimaryColumnValues.length; i++) {
              psForUpdateInShadowTableForRemoteServer.setObject(i + 3,newPrimaryColumnValues[i]);
            }
            psForUpdateInShadowTableForRemoteServer.executeUpdate(); // updating new Inserted Record in shadow table with remoteServerName
          }
          else {
            // primary key values to be updated with  newPrimaryColumnValues
            Object[] primaryKeyObj = tracer.primaryKeyValues;
            if (!repTable.isLocalServerWinner()) {
              for (int i = 0; i < primaryKeyObj.length; i++) {
                psForActualUpdateInTable.setObject(noOfChangedColumns + i + 1,primaryKeyObj[i]);
                psForUpdateInShadowTableForRemoteServer.setObject(3 + i,primaryKeyObj[i]);
              }
              Object lastSyncId = getLastUIDFromShadowTable();
              try {
                psForActualUpdateInTable.executeUpdate();
                loggingUpdateOperation(tableName, primaryColumnNames,primaryValueObjects, changedColumnNames,changedColumnValues, replicationType);
                writeUpdateOperationInTransactionLogFile(bw, tableName,primaryColumnNames, primaryValueObjects, changedColumnNames, changedColumnValues, replicationType, transactionLogType);
                updateCount++;
              }
              catch (SQLException ex) { // primary key constrant voilation
                if (!dbHandler.isPrimaryKeyException(ex)) {
                  throw ex;
                }

                Tracer trForRollback = new Tracer();
                trForRollback.primaryKeyValues = primaryKeyObj;
                primaryVoilation(newPrimaryColumnValues, trForRollback);
                Object[] trPkValues = trForRollback.primaryKeyValues;
                // setting the rollbacked pk values if original pk values has also been rollbacked.
                for (int i = 0; i < trPkValues.length; i++) {
                  psForActualUpdateInTable.setObject(noOfChangedColumns + i + 1,trPkValues[i]);
                  psForUpdateInShadowTableForRemoteServer.setObject(3 + i,trPkValues[i]);
                }
                lastSyncId = getLastUIDFromShadowTable();
                psForActualUpdateInTable.executeUpdate();
              }

              // update shadow table with remoteserverNAme FOR UODATE
              psForUpdateInShadowTableForRemoteServer.setObject(2, lastSyncId);
              psForUpdateInShadowTableForRemoteServer.executeUpdate(); // Status -B Updating record with remoteSErverNAme on shadowTable

              for (int i = 0; i < newPrimaryColumnValues.length; i++) {
                psForUpdateInShadowTableForRemoteServer.setObject(i + 3,newPrimaryColumnValues[i]);
              }
              psForUpdateInShadowTableForRemoteServer.executeUpdate(); // Status- A Updating record with remoteSErverNAme on shadowTable
            }
            else {
              // case when both the records are updated on both the sides
              Object[] oldRow = tracer.oldRow;
              ResultSet trRS = tracer.rs;
//          ArrayList localChangedColumnsValues = new ArrayList();
              ArrayList localChangedColumnsName = new ArrayList();
              for (int i = 0; i < tableColumnNames.length; i++) {
                String columnName = tableColumnNames[i];
                Object newRecord = trRS.getObject(columnName);
                if (newRecord != null || oldRow[i] != null) {
                  try {
                    if (!newRecord.equals(oldRow[i])) {
                      localChangedColumnsName.add(columnName);
//              localChangedColumnsValues.add(newRecord);
                    }
                  }
                  catch (NullPointerException ex1) {
                    localChangedColumnsName.add(columnName);
                  }
                }
                else {
                  continue;
                }
              }
              ArrayList actualListOFColumns = new ArrayList();
              ArrayList actualListOFValues = new ArrayList();
              for (int i = 0; i < changedColumnNames.length; i++) {
                if (repTable.isIgnoredColumn(changedColumnNames[i]))
                  continue;
                if (!localChangedColumnsName.contains(changedColumnNames[i])) {
                  actualListOFColumns.add(changedColumnNames[i]);
                  actualListOFValues.add(changedColumnValues[i]);
                }
              }
              if (actualListOFColumns.size() > 0) {
                Object lastSyncId = getLastUIDFromShadowTable();
                String query = repTable.getUpdatePreStmt(actualListOFColumns, primaryColumnNames);
                PreparedStatement pst = connection.prepareStatement(query);
                int index = actualListOFColumns.size();
                for (int i = 0; i < index; i++) {
// System.out.println(actualListOFValues +" actualListOFColumns.get(i) : " +actualListOFColumns.get(i));
                  AbstractColumnObject columnObject = (AbstractColumnObject)columnObjectTreeMap.get(actualListOFColumns.get(i));
                  //once setAutoCommitFlag is set to false,we shouldn't change it to true
                  // by checking for other columns for that 'if' check is used
                  if (setAutoCommitFlag) {
                    setAutoCommitFlag = checkAutocommit(dbHandler, columnObject);
                    connection.setAutoCommit(setAutoCommitFlag);
                  }

                  //  Changes required to handle the case of update for clob and blob data type
                  columnObject.setColumnObject(pst,(String) actualListOFValues.get(i), i + 1);
                  //  pst.setObject(i + 1, actualListOFValues.get(i));
                }
                for (int j = 0; j < primaryKeyObj.length; j++) {
// System.out.println(" primaryKeyObj[j] : " + primaryKeyObj[j]);
                  pst.setObject(index + j + 1, primaryKeyObj[j]);
                }
                try {
                  pst.executeUpdate();
                }
                catch (SQLException ex) {
                  if (!dbHandler.isPrimaryKeyException(ex)) {
                    throw ex;
                  }
                  // primary Constrant Voilation
                  primaryVoilation(newPrimaryColumnValues, new Tracer()); // no case found till now for tracer
                  try {
                    pst.executeUpdate();
                  }
                  catch (SQLException ex10) {
                    if (!dbHandler.isPrimaryKeyException(ex)) {
                      throw ex;
                    }

                    /**
                     * In primary key voilation method we try to rollback
                     * the record upto its original record.After rollback
                     * operation we try to update the record. During update
                     * operation if primary key voilation occure then we delete
                     * the record in case of cyclic table.
                     */

                    if (isCurrentTableCyclic) {
                      deleteRecord(newPrimaryColumnValues);
                      pst.executeUpdate();
                    }
                  }
                }
                finally {
                  if (setAutoCommitFlag == false)
                    setAutocomitTrueAndCommitRecord(connection);
                }

                if (pst != null) {
                  pst.close();
                }
// update in  shadow table not done for remoteServerName because of this case will be true only in case of LOCAL SERVER WINNER AND
// THIS RECORD SHOULD BE SENT TO REMOTE SERVER FOR UPDATE...
              }
            }
          }
        }
      }
      else if (isCurrentTableCyclic) {

// System.out.println("<----------------------SECOND PASS CURRENT TABLE FOUND CYCLIC --------------------->");
        /**
         * Code given below is executed in second pass for cyclic tables.
         * Update all the columns with actual values that are set to null in first pass.
         */
        boolean isForiegnKeycolumn = false;
        int columnIndex = 0;
        String updateQueryForSecondPass = makeQueryToUpdateTheRecordInSecodPass(changedColumnNames, primaryColumnNames);
//  System.out.println("^^^^^^^^^^^^^^^^^ UPDATE QUERY SECOND PASS ::" +updateQueryForSecondPass);
        if (!updateQueryForSecondPass.equalsIgnoreCase("")) {
          psForActualUpdateInTable = connection.prepareStatement(updateQueryForSecondPass);
          for (int i = 0; i < noOfChangedColumns; i++) {
            if (repTable.isIgnoredColumn(changedColumnNames[i]))
              continue;

            // Set value of foriegn key columns
            if (isCurrentTableCyclic &&
                repTable.isForiegnKeyColumn(changedColumnNames[i])) {
// System.out.println(" changedColumnNames[i] ::  " +changedColumnNames[i]);
              AbstractColumnObject columnOject = ( (AbstractColumnObject)columnObjectTreeMap.get(changedColumnNames[i]));
              isForiegnKeycolumn = true;
//System.out.println(changedColElement[i].elementValue + "^^^^^^^^^^^^^^^ changedColElement[i]  ::::  " +changedColElement[i].elementValue);
              //once setAutoCommitFlag is set to false,we shouldn't change it to true
              // by checking for other columns for that 'if' check is used
              if (setAutoCommitFlag) {
                setAutoCommitFlag = checkAutocommit(dbHandler, columnOject);
                connection.setAutoCommit(setAutoCommitFlag);
              }
              columnOject.setColumnObject(psForActualUpdateInTable,changedColElement[i], columnIndex + 1);
              columnIndex++;
            }
          }
          // set the value of primary columns
          for (int i = 0; i < primaryValueObjects.length; i++) {
// System.out.println(" ^^^^^^^^^^^^^Primary columns vlaue :: " +primaryValueObjects[i]);
            psForActualUpdateInTable.setObject(columnIndex + 1,primaryValueObjects[i]);
          }
          // Update the records with actual values which are set to null in first pass
          if (isForiegnKeycolumn) {
            Object lastSyncId = getLastUIDFromShadowTable();
            psForActualUpdateInTable.execute();
            // update the old record in shadow table with remoteServer STATUIS -B
            // setting Object values for Update query for Shadow TABLW for REMOTE SERVER
            psForUpdateInShadowTableForRemoteServer.setObject(2, lastSyncId);
            for (int i = 0; i < newPrimaryColumnValues.length; i++) {
              psForUpdateInShadowTableForRemoteServer.setObject(i + 3,newPrimaryColumnValues[i]);
//System.out.println("newPrimaryColumnValues[i]::" +newPrimaryColumnValues[i]);
            }
            psForUpdateInShadowTableForRemoteServer.executeUpdate();
          }
        }
      }
    }
    catch (SQLException ex2) {
      if (!dbHandler.isPrimaryKeyException(ex2)) {
        throw ex2;
      }
      /**
       * This catch was added on 22 march 04 .
       * There is Primary Key Voilation occure in Scripttestcases 6 , 7 and 8
       * All these are complex cases.So the Exception has been dumped for
       * temporary solution as suggested.
       */
    }
    finally {
      if (setAutoCommitFlag == false) {
        setAutocomitTrueAndCommitRecord(connection);
      }

      if (psForActualUpdateInTable != null) {
        try {
          psForActualUpdateInTable.close();
        }
        catch (SQLException ex3) {
          //sqleception must be ignored
        }
      }
    }
  }

  /**
   * @param rst
   * @return ResultSet containing only one record corresponding to commonId
   * record may be of status 'A' or 'B'.
   * @throws SQLException
   */
  private ResultSet getOtherCommonIdResultSet(ResultSet rst) throws SQLException {
    if (psForOtherCommonId == null) {
      psForOtherCommonId = makePSForOtherCommonId();
    }

    // showCurrentRow(rst);

    Object commonId = rst.getObject(RepConstants.shadow_common_id2);
    Object syncId = rst.getObject(RepConstants.shadow_sync_id1);
    psForOtherCommonId.setObject(1, commonId);
    psForOtherCommonId.setObject(2, syncId);
    for (int i = 0; i < primaryColumnNames.length; i++) {
      psForOtherCommonId.setObject(i + 3,rst.getObject("REP_OLD_" +primaryColumnNames[i]));
    }
    return psForOtherCommonId.executeQuery();
  }

  /**
   * Handles the case when primary key voilation occurs due to synchronisation.
   * Records may be rollbacked to their original status as before synchronization
   * @param conflictingPrimaryKeyValues
   * @param trForRollback - a tracer holds the information related to the records.
   * @throws RepException
   * @throws SQLException
   */
  private void primaryVoilation(Object[] conflictingPrimaryKeyValues, Tracer trForRollback) throws RepException, SQLException {
    Object lastShadowUidObj = getLastUIDFromShadowTable();
    if ((lastShadowUidObj instanceof Integer)) { //for firebird
      lastShadowUid = new Long( ( (Integer) lastShadowUidObj).longValue() + 1);
    }
    else  if ((lastShadowUidObj instanceof String)) { //for MySQL
      lastShadowUid = new Long( ( Long.parseLong((String)lastShadowUidObj) + 1));
    }
    else {
      lastShadowUid = new Long( ( (Long) getLastUIDFromShadowTable()).longValue() +1);
    }

    Tracer tracer = new Tracer();
    // trace the orignal record inserted/updated in main table for which Primary voilation has occured
    traceOriginalRecord(conflictingPrimaryKeyValues, tracer, lastShadowUid);

    if (tracer.type == null) {
      return;
    }

    if (tracer.type.equals(RepConstants.insert_operation)) { // Insert Case

      if (preparedStatementForDelete == null) {
        preparedStatementForDelete = makePreparedStatementForDeleteOnTable();
      }
      Object[] primaryValues = tracer.primaryKeyValues;
      for (int i = 0; i < primaryValues.length; i++) {
        preparedStatementForDelete.setObject(i + 1, primaryValues[i]);
      }
      if (isCurrentTableCyclic) {
        setNullInColumnOfChildTableToUpdateRecord(primaryColumnNames,primaryValues);
      }
      // possibility of deleting the record which is to be updated afterwards
      preparedStatementForDelete.executeUpdate();
    }
    else if (tracer.type.equals(RepConstants.update_operation)) { // Update Case
      // special handlling if swapping has been done on primary keys and which due to which
      // we get stuck in recurrsion .
      boolean conflictingKeyAllreadyPresent =
          checkForPresenceOfConflictingPkKeyinArray(voilationPkKeys,
          conflictingPrimaryKeyValues);
      if (!conflictingKeyAllreadyPresent) {
        voilationPkKeys.add(conflictingPrimaryKeyValues);
      }
      else {
        if (preparedStatementForDelete == null) {
          preparedStatementForDelete = makePreparedStatementForDeleteOnTable();
        }
        Object[] primaryValuesForDelete = (Object[]) voilationPkKeys.get(
            voilationPkKeys.size());
        for (int i = 0; i < primaryValuesForDelete.length; i++) {
          preparedStatementForDelete.setObject(i + 1, primaryValuesForDelete[i]);
        }
        // possibility of deleting the record which is to be updated afterwards
        preparedStatementForDelete.executeUpdate();
        Object lastIdForInsert = getLastUIDFromShadowTable();
        return;
      }

      ResultSet newRS = tracer.rs;

      if (updatePreparedStatement == null) {
        updatePreparedStatement = makeUpdatePreparedStatement();

      }
      Object[] primaryValues = tracer.primaryKeyValues;

      // checking for record to be updated / rollbacked if it is the one to be updated originally
      // i.e after primary voilation has been done
      boolean flag = checkForConflictingRecord(trForRollback.primaryKeyValues,conflictingPrimaryKeyValues);
// System.out.println(" flag :: " + flag);
      if (flag) {
        trForRollback.primaryKeyValues = primaryValues;

        // setting old values for the current row // updating current row to its prev version.
      }
      ArrayList[] listArray = setParametersInUpdatePS(conflictingPrimaryKeyValues, newRS);
      ArrayList list = listArray[0];
      ArrayList nullColumnObj = listArray[1];

      try {
        updatePreparedStatement.executeUpdate();
      }
      catch (SQLException ex) {
//System.out.println(" Error Code :: " + ex.getErrorCode());

        /**
         * Use foriegn key error code for deleting the
         * conflicting record.
         */
        if (ex.getErrorCode() == 547) {
//System.out.println("   primary key voilation :: ");
          deleteRecord(conflictingPrimaryKeyValues);
//System.out.println(" record deleted successfully ");
          return;
        }
        if (!dbHandler.isPrimaryKeyException(ex)) {
          throw ex;
        }
        primaryVoilation(primaryValues, trForRollback);
        int nullCnt = 0;
        for (int i = 0; i < list.size(); i++) {
          Object obj = list.get(i);
          if (obj != null) {
            updatePreparedStatement.setObject(i + 1, obj);
          }
          else {
            ( (AbstractColumnObject) nullColumnObj.get(nullCnt)).setColumnObject(updatePreparedStatement, xmlElement_NULL, i + 1);
            nullCnt++;
          }
        }
        //Updating Values to prev values
        int countUpdatedR = updatePreparedStatement.executeUpdate();
      }
      //we will not  update the shadown table entries with remote server name in this case
    }
  }

  private ArrayList[] setParametersInUpdatePS(Object[] conflictingPrimaryKeyValues, ResultSet newRS) throws SQLException {
    ArrayList list = new ArrayList();
    ArrayList nullColumnObj = new ArrayList();
    int cnt = tableColumnNames.length;
    int k = 0;
    for (int i = 0; i < cnt; i++) {
      if (repTable.isIgnoredColumn(tableColumnNames[i])) {
        continue;
      }
      Object obj = newRS.getObject(tableColumnNames[i]);
//System.out.println("OperationUpdate.setParametersInUpdatePS(conflictingPrimaryKeyValues, newRS):: " +obj);
      if (obj == null) {
        AbstractColumnObject columnObj = ( (AbstractColumnObject)columnObjectTreeMap.get(tableColumnNames[i]));
        // columnObj.setColumnObject(updatePreparedStatement, xmlElement_NULL,i + 1);

        columnObj.setColumnObject(updatePreparedStatement, xmlElement_NULL,k + 1);
        list.add(obj);
        nullColumnObj.add(columnObj);
        k++;
        continue;
      }
      //    updatePreparedStatement.setObject(i + 1, obj);
      updatePreparedStatement.setObject(k + 1, obj);
      k++;
      list.add(obj);
    }

    for (int j = 0; j < conflictingPrimaryKeyValues.length; j++) {
      Object obj = conflictingPrimaryKeyValues[j];
//System.out.println("OperationUpdate.setParametersInUpdatePS() : " + obj);
      updatePreparedStatement.setObject(cnt + j + 1, obj);
      list.add(obj);
    }
//System.out.println(" list : " + list + "  nullColumnObj :: " +nullColumnObj);
    return new ArrayList[] {list, nullColumnObj};
  }

  /**
   * Get actual values from ColumnObjects for corresponding to primary key.
   * @param primaryKeyValues primary key values passed through XML file.
   * @return Object[] of primary keys
   * @throws SQLException
   */
  private Object[] getObjectArrayForPrimaryKey(String[] primaryKeyValues) throws
      SQLException {
    Object[] pkValues = new Object[primaryKeyColumnsObject.length];
    for (int i = 0; i < primaryKeyColumnsObject.length; i++) {
      pkValues[i] = primaryKeyColumnsObject[i].getObject(primaryKeyValues[i]);
    }
    return pkValues;
  }

  /**
   * Get actual values from ColumnObjects for corresponding to whole row passed in XML file.
   * @param columnValues
   * @param columnObj
   * @return
   * @throws SQLException
   */
  private Object[] getObjectArrayForTable(String[] columnValues,AbstractColumnObject[] columnObj) throws SQLException {
    Object[] values = new Object[columnValues.length];
    ArrayList nullColumnObj = new ArrayList();

    for (int i = 0; i < columnValues.length; i++) {
      if (repTable.isIgnoredColumn(tableColumnNames[i])) {
          continue;
        }
      AbstractColumnObject colObj = columnObj[i];
      values[i] = colObj.getObject(columnValues[i]);
      if (values[i] == null) {
        nullColumnObj.add(colObj);
      }
    }
    return new Object[] {values, nullColumnObj};
  }

  /**
   * tarce the record present in shadow table for the primary key passed and having syncId less than UID passed.
   * @param primaryKeyValues
   * @param tracer
   * @param Uid
   * @return
   * @throws RepException, SQLException
   */
  private boolean traceOriginalRecord(Object[] primaryKeyValues, Tracer tracer,Object Uid) throws RepException, SQLException {
    if (preparedStatementForLastRecord == null) {
      preparedStatementForLastRecord = makePreparedStatementForLastRecord();
    }
    preparedStatementForLastRecord.setObject(1, Uid);
    for (int i = 0; i < primaryKeyValues.length; i++) {
      preparedStatementForLastRecord.setObject(i + 2, primaryKeyValues[i]);
    }
    ResultSet lastRow = preparedStatementForLastRecord.executeQuery();
    lastRow = preparedStatementForLastRecord.executeQuery();
    if (!lastRow.next()) {
      /** @todo add  comment done by sube singh */
      lastRow.close();
      return false;
    }

//    showCurrentRow(lastRow);
    // finding operation performed for current record
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
      if (preparedStatementForCommonId == null) {
        preparedStatementForCommonId = makePreparedStatementForCommonId_WithBeforeUpdate_Status();
      }
      preparedStatementForCommonId.setObject(1, commonId);
      for (int i = 0; i < primaryColumnNames.length; i++) {
        preparedStatementForCommonId.setObject(i + 2,lastRow.getObject("REP_OLD_" +primaryColumnNames[i]));
      }
      ResultSet rs = preparedStatementForCommonId.executeQuery();

      if (!rs.next()) { // other reocrd for common id and status before update not found 'B'
        rs.close();
        throw new RepException("REP051", new Object[] {commonId, shadowTable});
      }

      Object[] primaryColValues = new Object[primaryColumnNames.length];
      for (int j = 0; j < primaryColumnNames.length; j++) {
        primaryColValues[j] = rs.getObject(primaryColumnNames[j]);
// System.out.println(" primaryColValues[j]  :: " + primaryColValues[j]);
      }
      Object UID = rs.getObject(RepConstants.shadow_sync_id1);
      // finding prev. record
      boolean gotRow = traceOriginalRecord(primaryColValues, tracer, UID);
      if (gotRow == false) {
        tracer.rs = rs;
        tracer.type = RepConstants.update_operation;
        tracer.primaryKeyValues = primaryColValues;
      }
      else {
        // done by sube singh
        rs.close();
      }
      return true;
    }
//        throw new Exception("ERROR ERROR < NOT POSSIBLE");
  }

  /**
   * fetches the last records from the shadpw tbale correspondinf the the
   * primary key passed i.e the original status of the record after
   * updataion/Insertion/Deletion
   * @param primaryColValues
   * @param UID
   * @param tracer
   * @return
   * @throws RepException
   * @throws SQLException
   */
  private boolean getLastRecord(Object[] primaryColValues, Object UID,Tracer tracer) throws RepException, SQLException {
    int noOfPrimaryColumns = primaryColValues.length;
    for (int i = 0; i < noOfPrimaryColumns; i++) {
      psForInitialRecordInShadowTable.setObject(i + 1, primaryColValues[i]);
    }
    psForInitialRecordInShadowTable.setObject(noOfPrimaryColumns + 1, UID);
    psForInitialRecordInShadowTable.setObject(noOfPrimaryColumns + 2,remoteServerName);
    ResultSet rs = psForInitialRecordInShadowTable.executeQuery();

    if (!rs.next()) {
      //Done by sube to control the open cursor problem in oracle
      rs.close();
      tracer.recordFound = false;
      return false;
    }
    else {
//      showCurrentRow(rs);
      String operation = rs.getString(RepConstants.shadow_operation3);
      if (operation.equals(RepConstants.delete_operation)) { // Delete Operation
        tracer.recordFound = true;
        tracer.type = RepConstants.delete_operation;
        return true;
      }
      else {
        tracer.setOldRow(rs);
        ResultSet resultSet = getOtherCommonIdResultSet(rs);
        boolean updatedREcord = resultSet.next();
        if (!updatedREcord) { // other common record not found in shadow table, may be insert case
//                  return false;
          throw new RepException("REP051",new Object[] {rs.getObject(RepConstants.shadow_common_id2), shadowTable});
        }
        primaryColValues = new Object[noOfPrimaryColumns];
        for (int i = 0; i < noOfPrimaryColumns; i++) {
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
          // Done by sube to control the open cursor problem
          resultSet.close();

        }
        return true;
      }
    }
  }

  private PreparedStatement makePSForInitialRecordInShadowTable() throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append("SELECT * FROM ");
    query.append(shadowTable);
    query.append(" WHERE ");
    for (int i = 0; i < primaryColumnNames.length; i++) {
      if (i != 0) {
        query.append(" AND ");
      }
      query.append(primaryColumnNames[i]);
      query.append("= ? ");
    }
    query.append(" AND ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" > ? "); // coincedered Id
    query.append(" AND ");
    query.append(RepConstants.shadow_serverName_n);
    query.append(" != ? ORDER BY ").append(RepConstants.shadow_sync_id1); // remote servername
    return connection.prepareStatement(query.toString());
  }

  private PreparedStatement makePSForActualUpdateInTable() throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append("UPDATE ");
    query.append(tableName);
    query.append(" SET ");
    for (int i = 0; i < changedColumnNames.length; i++) {
      if (repTable.isIgnoredColumn(changedColumnNames[i])) {
        continue;
      }
//      if (i != 0) {
//        query.append(" , ");
//      }
      query.append(" , " + changedColumnNames[i]);
      query.append(" = ? ");
    }
    int indexOfFirstComma = query.indexOf(",");
    if (indexOfFirstComma != -1) {
      query.deleteCharAt(indexOfFirstComma);
    }

    query.append("WHERE ");
    for (int i = 0; i < primaryColumnNames.length; i++) {
      if (i != 0) {
        query.append(" AND ");
      }
      query.append(primaryColumnNames[i]);
      query.append(" = ? ");
    }
//System.out.println(" $$$$$$$$$$ Make Ps for Actal update in Table :: " +query.toString());
    return connection.prepareStatement(query.toString());
  }

  private PreparedStatement makePreparedStatementForLastRecord() throws
      SQLException {
    StringBuffer query = new StringBuffer();
    query.append(" SELECT * FROM  ");
    query.append(shadowTable);
    query.append("  WHERE  ");
    query.append(RepConstants.shadow_sync_id1);
    // and not >= because this will include the last record in traceOriginal Record , which will give wrong result
    query.append(" > ");
    query.append(conisderedId);
    query.append(" AND ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" < ? ");
    for (int i = 0; i < primaryColumnNames.length; i++) {
      query.append("  AND  ");
      query.append(primaryColumnNames[i]);
      query.append(" = ? ");
    }
    query.append(" ORDER BY  ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" DESC ");
    return connection.prepareStatement(query.toString());
  }

  private PreparedStatement makePreparedStatementForCommonId_WithBeforeUpdate_Status() throws SQLException {
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

    // for matching oldPrimary keys
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

  private PreparedStatement makePreparedStatementForDeleteOnTable() throws
      SQLException {
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
// System.out.println("OperationUpdate.makePreparedStatementForDeleteOnTable() : " +query.toString());
    return connection.prepareStatement(query.toString());
  }

  private PreparedStatement makeUpdatePreparedStatement() throws SQLException {
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
      query.append(" , " + tableColumnNames[i]);
      query.append("= ?");
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
//System.out.println("PK violation makeUpdatePreparedStatement line 1063:::" +query.toString());
    return connection.prepareStatement(query.toString());
  }

  private PreparedStatement makePSForOtherCommonId() throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append("SELECT * FROM  ");
    query.append(shadowTable);
    query.append(" WHERE ");
    query.append(RepConstants.shadow_common_id2);
    query.append(" = ?  ");
    query.append(" and ");
    query.append(RepConstants.shadow_sync_id1);
    query.append(" != ? ");

// for matching oldPrimary keys
    for (int i = 0; i < primaryColumnNames.length; i++) {
      query.append(" and ");
      query.append("REP_OLD_" + primaryColumnNames[i]);
      query.append(" = ");
      query.append(" ? ");
    }
    return connection.prepareStatement(query.toString());
  }

  /**
   * returns last synchronization id from shadopw table.
   * @return
   * @throws SQLException
   */
  private Object getLastUIDFromShadowTable() throws SQLException {
    StringBuffer query = new StringBuffer();
    ResultSet rs = null;
    try {
      query.append(" SELECT max(").append(RepConstants.shadow_sync_id1).append(")").append(" FROM ").append(shadowTable);
      rs = connection.createStatement().executeQuery(query.toString());
      boolean flag = rs.next();
      Object lastId = rs.getObject(1);
//System.out.println("Operation update 1153 getLastUIDFromShadowTable  lastId  CLASS : "+lastId);
      if (lastId instanceof BigDecimal) {
        lastId = new Long( ( (BigDecimal) lastId).longValue());
      }
      else  if (lastId instanceof Double) {
        lastId = new Long( ( (Double) lastId).longValue());
      }
      else  if (lastId instanceof Long) {
        lastId = new Long( ( (Long) lastId).longValue());
      }
      else  if (lastId instanceof Integer) {
       lastId = new Long( ( (Integer) lastId).longValue());
     }
     else  if (lastId instanceof String) {
       lastId = new Long( ( Long.parseLong((String)lastId)));
     }

      return flag ? (lastId == null ? new Long(0) : lastId) : new Long(0);
    }
    finally {
      Statement st = rs.getStatement();
      rs.close();
      st.close();
    }
  }

  private PreparedStatement makePSForUpdateInShadowTableForRemoteServer() throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append(" UPDATE ").append(shadowTable).append(" SET  ").append(
        RepConstants.shadow_serverName_n)
        .append(" = ? ").append(" WHERE  ").append(RepConstants.shadow_sync_id1).
        append("> ?");
    for (int i = 0; i < primaryColumnNames.length; i++) {
      query.append(" AND ");
      query.append(primaryColumnNames[i]);
      query.append(" = ?");
    }
    return connection.prepareStatement(query.toString());
  }

  /**
   * used for debugging purpose to show a resultSet
   * @param rs
   * @throws SQLException
   */
  /*  private void showCurrentRow(ResultSet rs) throws SQLException {
      ResultSetMetaData rsmd = rs.getMetaData();
      int count = rsmd.getColumnCount();
      System.out.print("[ ");
      for (int i = 1; i <= count; i++) {
        if (i != 1)
          System.out.print(" , ");
        System.out.print(rs.getObject(i));
      }
      System.out.println(" ] ");
      System.out.println("");
    }
   */
  /**
   * searches for the conflicting Key in search key values.
   * @param searchKeyValues
   * @param conflictingPk
   * @return
   */
  private boolean checkForConflictingRecord(Object[] searchKeyValues, Object[] conflictingPk) {
    try {
      for (int i = 0; i < searchKeyValues.length; i++) {
        if (searchKeyValues[i].equals(conflictingPk[i])) {
          continue;
        }
        else {
          return false;
        }
      }
      return true;
    }
    catch (NullPointerException ex) {
      return false;
    }
  }

  private PreparedStatement makePsForUpdateChangedColumns_ExceptPk(ArrayList exceptPk_ChangedColumnNames) throws SQLException {
    StringBuffer query = new StringBuffer();
    query.append("UPDATE ");
    query.append(tableName);
    query.append(" SET ");
    for (int i = 0; i < exceptPk_ChangedColumnNames.size(); i++) {
      if (i != 0) {
        query.append(" , ");
      }
      query.append(exceptPk_ChangedColumnNames.get(i));
      query.append(" = ? ");
    }
    query.append(" WHERE ");
    for (int i = 0; i < primaryColumnNames.length; i++) {
      if (i != 0) {
        query.append(" AND ");
      }
      query.append(primaryColumnNames[i]);
      query.append(" = ? ");
    }
    return connection.prepareStatement(query.toString());
  }

  private void updateChangedColumnsAtLocalExcept_Pk(Object[]
      changedColumnObject, int noOfPrimaryCol, Object[] primaryValueObjects) throws SQLException {
    if (changedColumnNames.length > 0) {
      ArrayList exceptPk_ChangedColumnNames = new ArrayList();
      ArrayList exceptPk_ChangedColumnValues = new ArrayList();
      boolean isPk = false;
      // getting changes column names and values except for primary key
      for (int i = 0; i < changedColumnNames.length; i++) {
        for (int j = 0; j < primaryColumnNames.length; j++) {
          isPk = false;
          if (changedColumnNames[i].equalsIgnoreCase(primaryColumnNames[j])) {
            isPk = true;
            continue;
          }
        }
        if (!isPk) {
          exceptPk_ChangedColumnNames.add(changedColumnNames[i]);
          exceptPk_ChangedColumnValues.add(changedColumnObject[i]);
        }
      }
      // making prepared statement for updating values at except for primary key
      if (exceptPk_ChangedColumnNames.size() > 0) {
        PreparedStatement ps = makePsForUpdateChangedColumns_ExceptPk(
            exceptPk_ChangedColumnNames);
        for (int i = 0; i < exceptPk_ChangedColumnValues.size(); i++) {
          ps.setObject(i + 1, exceptPk_ChangedColumnValues.get(i));
        }
        for (int i = 0; i < noOfPrimaryCol; i++) {
          ps.setObject(exceptPk_ChangedColumnValues.size() + i + 1, primaryValueObjects[i]);
        }
        ps.executeUpdate();
      }
    }
  }

  private boolean checkForPresenceOfConflictingPkKeyinArray(ArrayList
      voilationPkKeys, Object[] pkValues) {
    for (int i = 0; i < voilationPkKeys.size(); i++) {
      Object[] values = (Object[]) voilationPkKeys.get(i);
      boolean flag = true;
      for (int j = 0; j < values.length; j++) {
        if (!values[j].equals(pkValues[j])) {
          flag = false;
          break;
        }
      }
      if (flag) {
        return true;
      }
    }
    return false;
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
     System.out.println(Arrays.asList(displayColumn));
     while (rs.next()) {
       Object[] columnValues = new Object[columnCount];
       for (int i = 1; i <= columnCount; i++)
         columnValues[i - 1] = rs.getObject(i);
       System.out.println(Arrays.asList(columnValues));
     }
   }*/

  public void closeAllStatments() {
    if (psForInitialRecordInShadowTable != null) {
      try {
        psForInitialRecordInShadowTable.close();
      }
      catch (SQLException ex) {
      }
    }
    if (psForActualUpdateInTable != null) {
      try {
        psForActualUpdateInTable.close();
      }
      catch (SQLException ex1) {
      }
    }
    if (preparedStatementForLastRecord != null) {
      try {
        preparedStatementForLastRecord.close();
      }
      catch (SQLException ex2) {
      }
    }
    if (preparedStatementForCommonId != null) {
      try {
        preparedStatementForCommonId.close();
      }
      catch (SQLException ex3) {
      }
    }

    if (preparedStatementForDelete != null) {
      try {
        preparedStatementForDelete.close();
      }
      catch (SQLException ex4) {
      }
    }
    if (updatePreparedStatement != null) {
      try {
        updatePreparedStatement.close();
      }
      catch (SQLException ex5) {
      }
    }
    if (psForOtherCommonId != null) {
      try {
        psForOtherCommonId.close();
      }
      catch (SQLException ex6) {
      }
    }

    if (psForInsertIntoActualTable != null) {

      try {
        psForInsertIntoActualTable.close();
      }
      catch (SQLException ex7) {
      }
    }
    if (psForUpdateInShadowTableForRemoteServer != null) {
      try {
        psForUpdateInShadowTableForRemoteServer.close();
      }
      catch (SQLException ex8) {
      }
    }
  }

  /**
   * Make a update query to update all the columns that refering
   * to other columns and and set null during first pass.If referencing
   * columns are not found then blank string return by the method.
   * @param changesColumns String[]
   * @param primaryColumns String[]
   * @return String
   */
  private String makeQueryToUpdateTheRecordInSecodPass(String[] changesColumns,String[] primaryColumns) {
    StringBuffer updateQuerySecondPass = new StringBuffer();
    updateQuerySecondPass.append("UPDATE ").append(tableName).append(" SET ");
    int size = changesColumns.length;
    boolean flag = false;
    for (int i = 0; i < size; i++) {
      if (repTable.isForiegnKeyColumn(changedColumnNames[i])) {
        if (flag)
          updateQuerySecondPass.append(",");
        flag = true;
        updateQuerySecondPass.append(changedColumnNames[i]).append("= ? ");
      }
    }
    updateQuerySecondPass.append(" WHERE  ");
    for (int i = 0; i < primaryColumns.length; i++) {
      if (i != 0) {
        updateQuerySecondPass.append(" and ");
      }
      updateQuerySecondPass.append(primaryColumns[i]).append(" = ?");
    }
    log.debug(updateQuerySecondPass.toString());
//System.out.println("Operation Update makeQueryToUpdateTheRecordInSecodPass  : " +updateQuerySecondPass.toString());
    return flag ? updateQuerySecondPass.toString() : "";
  }

  /**
   * Set null in all column of child table that refering the
   * column of parent table for updated the record of parent
   * table.
   * @param parentPkCols String[]
   * @param parentPkColValue0 Object[]
   * @throws RepException
   * @throws SQLException
   */
  private void setNullInColumnOfChildTableToUpdateRecord(String[] parentPkCols,
      Object[] parentPkColValue0) throws RepException, SQLException {
    Statement stmt = null;
    String[] childFkColumns = null, parentReferedCols = null, parentPkColValue = null;
    Object[] object = null;
    Object obj = null;
    ArrayList childTableList = mdi.getChildTables(tableName);
    String childTableName = null;
    parentPkColValue = new String[parentPkColValue0.length];
    for (int j = 0; j < parentPkColValue0.length; j++) {
      obj = parentPkColValue0[j];
//System.out.println(obj.toString() + "parentPkColValue0[j] : " +parentPkColValue0[j]);
      parentPkColValue[j] = obj.toString();
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
      int howMayServerNameupdated = stmt.executeUpdate(updateRemoteServerNameQuery);
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  private PreparedStatement makeupdateQueryToSetNull(String tableName,
      String columns[], String[] parentReferedColumns, String[] parentPkCols, String[] parentPkColValue) throws SQLException {
    StringBuffer sb = new StringBuffer();
    sb.append("UPDATE ")
        .append(tableName)
        .append(" SET ");
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
   * for creating a update query for setting null in child
   * tables.
   * @param primaryColsName String[]
   * @param primaryColsValues String[]
   * @param referdColName String
   * @throws SQLException
   * @return Object
   */
  private Object getParentReferedColValue(String[] primaryColsName, String[] primaryColsValues, String referdColName) throws SQLException {
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
                getParentReferedColValue(parentPkCols, parentPkColValue,parentReferedColumns[i]));
    }
    sb.append(") ");
    return sb.toString();
  }

  /**
   * Get last Synchronization ID from shadow table.It
   * is used to used to get the original record.
   * @param shadowTableName String
   * @throws SQLException
   * @return Object
   */
  private Object getLastSyncId(String shadowTableName) throws SQLException {
    Statement statement = null;
    ResultSet rs = null;
    boolean flag = false;
    Object lastId = null;
    try {
      StringBuffer query = new StringBuffer();
      query.append("SELECT  max(").append(RepConstants.shadow_sync_id1).append(") FROM ").append(shadowTableName);
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

  /**
   * Delete record in case of cyclic table. First we try to rollback the record upto
   * original record.After that updated statement is executed.During execution of
   * statement if primary key voilation occurs then record is deleted to remove
   * the conflict.
   * @param primaryValues Object[]
   * @throws SQLException
   */
  private void deleteRecord(Object[] primaryValues) throws SQLException, RepException {
    if (preparedStatementForDelete == null) {
      preparedStatementForDelete = makePreparedStatementForDeleteOnTable();
    }
    for (int i = 0; i < primaryValues.length; i++) {
      preparedStatementForDelete.setObject(i + 1, primaryValues[i]);
    }
    // possibility of deleting the record which is to be updated afterwards
    setNullInColumnOfChildTableToUpdateRecord(primaryColumnNames, primaryValues);
    preparedStatementForDelete.executeUpdate();
  }

}
