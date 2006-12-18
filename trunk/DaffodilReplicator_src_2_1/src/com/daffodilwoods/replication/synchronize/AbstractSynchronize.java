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

import java.io.BufferedWriter;
import com.daffodilwoods.replication.RepConstants;
import com.daffodilwoods.replication.RepException;
import java.io.*;
import java.sql.Timestamp;
import org.apache.log4j.Logger;
import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;
import com.daffodilwoods.replication.column.AbstractColumnObject;
import com.daffodilwoods.replication.DBHandler.PostgreSQLHandler;
import com.daffodilwoods.replication.column.BlobObject;
import com.daffodilwoods.replication.column.ClobObject;
import java.sql.Connection;
import java.sql.SQLException;
import com.daffodilwoods.replication.Utility;
import java.util.TreeMap;
import com.daffodilwoods.replication.MetaDataInfo;
import com.daffodilwoods.replication.RepTable;
import com.daffodilwoods.replication.xml.XMLElement;
import java.util.ArrayList;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSetMetaData;

public class AbstractSynchronize {
  protected static Logger log = Logger.getLogger(AbstractSynchronize.class.getName());

  protected boolean setAutoCommitFlag = true;
  protected boolean isFirstPass = false, isCurrentTableCyclic = false;
  protected TreeMap allColumnsMap;
  protected MetaDataInfo mdi;
  protected BufferedWriter bw;
  protected AbstractDataBaseHandler dbHandler;
  protected RepTable repTable;
  protected Connection connection;
  protected TreeMap columnObjectTreeMap;
  protected String remoteServerName, shadowTable, tableName,replicationType, transactionLogType;
  public int insertCount = 0,updateCount = 0, deleteCount=0;
  protected String[] primaryColumnNames, changedColumnNames, changedColumnValues, tableColumnNames;
  protected Object conisderedId, lastShadowUid;
  protected XMLElement xmlElement_NULL;
  protected AbstractColumnObject[] primaryKeyColumnsObject;
  protected PreparedStatement PSForLastRecordSameRecordUpdatedExceptPK,PSToGetSyncidForSameOldPKEqualsNewPks;

  public AbstractSynchronize() {
  }

  /**
   * This method has been implemented to
   * write the insert operation in transaction
   * file.
   */
  protected void writeInsertOperationInTransactionLogFile(BufferedWriter bw,
      String tableName, Object[] insertedRecrods, String replicationType,
      String transactionLogType) throws RepException {

    if (Utility.createTransactionLogFile  &&  transactionLogType.equalsIgnoreCase("true")) {
      try {
//       Timestamp dt = new Timestamp(System.currentTimeMillis());
//       bw.write("\n\n");
//       bw.write( (dt + "\n"));

        bw.write("\n");
        bw.write("[" + replicationType + "]");
        bw.write("[" + tableName + "]");
        bw.write("[" + RepConstants.insert_operation + "]");
        bw.write("[");
        for (int i = 0; i < insertedRecrods.length; i++) {
          if (i != 0) {
            bw.write("," + insertedRecrods[i]);
          }
          bw.write("" + insertedRecrods[i]);
        }
        bw.write("]");
        bw.flush();
      }
      catch (IOException ex1) {
        RepConstants.writeERROR_FILE(ex1);
        throw new RepException("REP351", new Object[] {ex1.getMessage()});
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
      }
    }
  }

  /**
   * This method has been implemented to
   * write the Delete operation in transaction
   * file.
   */

  protected void writeDeleteOperationInTransactionLogFile(BufferedWriter bw,
      String tableName, String[] pkCols, Object[] deletedRecordPky,
      String replicationType, String transactionLogType) throws RepException {
    if (Utility.createTransactionLogFile && transactionLogType.equalsIgnoreCase("true")) {
      try {
//      Timestamp dt = new Timestamp(System.currentTimeMillis());
//      bw.write("\n\n");
//      bw.write( (dt + "\n"));

        bw.write("\n");
        bw.write("[" + replicationType + "]");
        bw.write("[" + tableName + "]");
        bw.write("[" + RepConstants.delete_operation + "]");
        bw.write("[PRIMARY KEY VALUES  ");
        for (int i = 0; i < deletedRecordPky.length; i++) {
          if (i != 0) {
            bw.write("," + pkCols[i] + " = " + deletedRecordPky[i]);
          }
          bw.write(pkCols[i] + " = " + deletedRecordPky[i]);
        }
        bw.write("]");
        bw.flush();
      }
      catch (IOException ex1) {
        RepConstants.writeERROR_FILE(ex1);
        throw new RepException("REP351", new Object[] {ex1.getMessage()});
      }
      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
      }
    }
  }

  /**
   * This method has been implemented to
   * write the Update operation in transaction
   * file.
   */
  protected void writeUpdateOperationInTransactionLogFile(BufferedWriter bw,
      String tableName, String[] pkCols, Object[] oldPky,
      String updatedColsName[], String[] updatedValues, String replicationType,
      String transactionLogType) throws RepException {
    if (Utility.createTransactionLogFile && transactionLogType.equalsIgnoreCase("true")) {
      try {
//        Timestamp dt = new Timestamp(System.currentTimeMillis());
//        bw.write("\n\n");
//        bw.write( (dt + "\n"));

        bw.write("\n");
        bw.write("[" + replicationType + "]");
        bw.write("[" + tableName + "]");
        bw.write("[" + RepConstants.update_operation + "]");
        bw.write("[PRIMARY KEY VALUES  ");
        for (int i = 0; i < oldPky.length; i++) {
          if (i != 0) {
            bw.write("," + pkCols[i] + " = " + oldPky[i]);
          }
          bw.write(pkCols[i] + " = " + oldPky[i]);
        }
        bw.write("]");

        bw.write("[ CHANGED COLUMNS ");
        for (int i = 0; i < updatedColsName.length; i++) {
          if (i != 0) {
            bw.write("," + updatedColsName[i] + " = " + updatedValues[i]);
          }
          bw.write("" + updatedColsName[i] + " = " + updatedValues[i]);
        }
        bw.write("]");
        bw.flush();
      }
      catch (IOException ex1) {
        RepConstants.writeERROR_FILE(ex1);
        throw new RepException("REP351", new Object[] {ex1.getMessage()});
      }

      catch (Exception ex) {
        RepConstants.writeERROR_FILE(ex);
      }

    }
  }

  public static void writeOperationInTransactionLogFile(BufferedWriter bw,
      int insertOperation,
      int updateOperation,
      int deleteOperation,
      String replicationType) throws Exception {
//System.out.println("insertOperation :  "+insertOperation+" updateOperation : "+updateOperation+" deleteOperation : "+deleteOperation);
     bw.write("\n\n");
     bw.write(" Synchronization Type [" + replicationType + "]");
     bw.write("\n\n");
     bw.write(" Inserts [" + insertOperation + "]");
     bw.write("\n");
     bw.write(" Updates [" + updateOperation + "]");
     bw.write("\n");
     bw.write(" Deletes [" + deleteOperation + "]");
     bw.write("\n\n");
     bw.write(" [" + replicationType + " COMPLETED SUCCESSFULLY]");
     bw.write("\n");

  }

  public static void writeDateInTransactionLogFile(BufferedWriter bw) throws
      Exception {
    if(Utility.createTransactionLogFile){
      Timestamp dt = new Timestamp(System.currentTimeMillis());
      bw.write("\n\n");
      bw.write(" Operation Performed on Date: ");
      bw.write( (dt + "\n"));
    }
  }

  public static void writeUnsuccessfullOperationInTransaction(BufferedWriter bw) {
    try {
      if(Utility.createTransactionLogFile) {
        bw.write("\n\n");
        bw.write(" [ SYNCHRONIZE OPERATION NOT COMPLETED SUCCESSFULLY]");
        bw.write("\n");
      }
    }
    catch (Exception ex1) {
    }
  }

  protected void loggingInsertOperation(String tableName,
                                        Object[] insertedRecrods,
                                        String replicationType) {

    log.debug("[" + replicationType + "]" + "[" + tableName + "]" + "[" +
              RepConstants.insert_operation + "]");
    for (int i = 0; i < insertedRecrods.length; i++) {
      if (i != 0) {
        log.debug("," + insertedRecrods[i]);
      }
      log.debug("" + insertedRecrods[i]);
    }
  }

  protected void loggingDeleteOperation(String tableName, String[] pkCols,
                                        Object[] deletedRecordPky,
                                        String replicationType) {
    log.debug("[" + replicationType + "]" + "[" + tableName + "]" + "[" +
              RepConstants.delete_operation + "]" + "[PRIMARY KEY VALUES ");
    for (int i = 0; i < deletedRecordPky.length; i++) {
      if (i != 0) {
        log.debug("," + pkCols[i] + " = " + deletedRecordPky[i]);
      }
      log.debug(pkCols[i] + " = " + deletedRecordPky[i]);

    }
  }

  protected void loggingUpdateOperation(String tableName, String[] pkCols,
                                        Object[] oldPky, String updatedColsName[],
                                        String[] updatedValues,
                                        String replicationType) {
    log.debug("[" + replicationType + "]" + "[" + tableName + "]" + "[" +RepConstants.update_operation + "]" + "PRIMARY KEY VALUES ");
    for (int i = 0; i < oldPky.length; i++) {
      if (i != 0) {
        log.debug("," + pkCols[i] + " = " + oldPky[i]);
      }
      log.debug(pkCols[i] + " = " + oldPky[i]);
    }
    log.debug(" CHANGED COLUMNS ");
    for (int i = 0; i < updatedColsName.length; i++) {
      if (i != 0) {
        log.debug("," + updatedColsName[i] + " = " + updatedValues[i]);
      }
      log.debug("" + updatedColsName[i] + " = " + updatedValues[i]);
    }
  }

  /**
   * This has been implemented to handle the problem
   * related to CLOB and BLOB data type. Postgre
   * do not to insert LOB object in autocommit mode.
   * @param <any> Abs
   * @return boolean
   */
  public boolean checkAutocommit(AbstractDataBaseHandler dbHandler,
                                 AbstractColumnObject aco) {
    if (dbHandler instanceof PostgreSQLHandler &&
        (aco instanceof ClobObject || aco instanceof BlobObject)) {
      return false;
    }
    return true;
  }

  public void setAutocomitTrueAndCommitRecord(Connection conn) throws
      SQLException {
    conn.commit();
    conn.setAutoCommit(true);
  }

  //////////////////////////////////


  /**
 * It return false if record is not updated else return true
 */
protected boolean isPrimaryKeyUpdated(Object[] priamryColValues,ArrayList updatedPrimaryKey) throws SQLException, RepException {
  boolean isPrimaryKeyUpdated = false;
  if(updatedPrimaryKey.size()==0) {
     return isPrimaryKeyUpdated;
  }

  for (int i = 0; i < updatedPrimaryKey.size(); i++) {
    Object[] updatedPk = (Object[]) updatedPrimaryKey.get(i);
    for (int j = 0; j < updatedPk.length; j++) {
//System.out.println(" updatedPk :: "+updatedPk[j]+" priamryColValues[j] :: "+priamryColValues[j]);
      if (updatedPk[j].equals(priamryColValues[j])) {
        isPrimaryKeyUpdated=true;
      }
    }
    if(isPrimaryKeyUpdated) {
      updatedPrimaryKey.remove(i);
      break;
    }
  }
  return isPrimaryKeyUpdated;
}

/*
 SELECT  *  FROM  Rep_Shadow_PUSHTABLE WHERE  (Rep_sync_id = (SELECT  MAX(rep_sync_id)
 FROM  Rep_Shadow_PUSHTABLE WHERE   rollno = 11 AND (rep_old_rollNo = 11) AND rep_Status='A'))

 This method is used if same record is updated many times(except primary column i.e primary
 column is not updated) This leads to improving performance(As suggested by parveen sir) And
 to avoid traversing of the syncIds which couldnt add in the viewId we use
 addSyncidToViewIdForSameOldPKEqualsNewPks(String primaryCols[],Object[] primaryColValues,
 Object MaxUID) method.
 */
protected Object getUIDRecordUpdatedExceptPK(String
    primaryCols[], Object[] primaryColValues, Object[] oldPrimaryColValues) throws SQLException, RepException {
  ResultSet rs = null;
  try {
    //check if old primary keys and new primary keys are same or not.
    // If Yes,then return else coontinue
    for (int i = 0; i < primaryColValues.length; i++) {
      if (!primaryColValues[i].toString().equals(oldPrimaryColValues[i].toString()))
        return null;
    }
    PSForLastRecordSameRecordUpdatedExceptPK = makeQueryToGetRecordIfPrimaryKeyIsNotUpdated(primaryColumnNames);
    int indexPrimaryColumn = 0;
    for (indexPrimaryColumn = 0; indexPrimaryColumn < primaryCols.length;indexPrimaryColumn++) {
      PSForLastRecordSameRecordUpdatedExceptPK.setObject(indexPrimaryColumn +1, primaryColValues[indexPrimaryColumn]);
    }
    for (int j = 0; j < primaryCols.length; j++) {
      PSForLastRecordSameRecordUpdatedExceptPK.setObject(indexPrimaryColumn +j + 1, oldPrimaryColValues[j]);
    }
//    long startTime =System.currentTimeMillis();
    rs = PSForLastRecordSameRecordUpdatedExceptPK.executeQuery();
//System.out.println(" SSSSSSSSSS TIME TAKEN IN UID QUERY EXECUTION :: "+(System.currentTimeMillis()-startTime));
    rs.next();
    Object Uid = rs.getObject(1);
    //Add the SyncIds to viewIds Which we have considered indirectly through above Query
//    addSyncidToViewIdForSameOldPKEqualsNewPks(primaryColValues, Uid);
    return Uid;
  }
  finally {
    if (rs != null) {
      try {
        rs.close();
      }
      catch (SQLException ex2) { //ignore SQLException
      }
    }
    if (PSForLastRecordSameRecordUpdatedExceptPK != null) {
      try {
        PSForLastRecordSameRecordUpdatedExceptPK.close();
      }
      catch (SQLException ex2) { //ignore SQLException
      }

    }
  }
}

/*
    Query to get syncIds :
    " SELECT REP_SYNC_ID FROM SHADOW_TABLE WHERE
    primarycolumns=rep_old_primarycolumns  and pk=?; "
    SyncID which we get through this query if not
    already present in viewIDs and less than the MaxUId
    then add into viewID. MaxUId is the SyncId upto which
    we have considered shadow table record for that particular
    primary key
 */

protected void addSyncidToViewIdForSameOldPKEqualsNewPks(Object[]
    primaryColValues, Object MaxUID,ArrayList viewedIds) throws SQLException, RepException {
  ResultSet rs = null;
  try {
    PSToGetSyncidForSameOldPKEqualsNewPks = makeQueryToGetViewId();
    for (int i = 0; i < primaryColValues.length; i++)
      PSToGetSyncidForSameOldPKEqualsNewPks.setObject(i + 1,primaryColValues[i]);
    rs = PSToGetSyncidForSameOldPKEqualsNewPks.executeQuery();
    while (rs.next()) {
      Object Uid = rs.getObject(1);
      if (!viewedIds.contains(Uid) &&
          ( (Number) Uid).longValue() < ( (Number) MaxUID).longValue()) {
        viewedIds.add(Uid);
      }
    }
  }
  finally {
    if (rs != null) {
      try {
        rs.close();
      }
      catch (SQLException ex2) { //ignore SQLException
      }
    }
  }
}

protected PreparedStatement makeQueryToGetRecordIfPrimaryKeyIsNotUpdated(String[] primaryColsName) throws SQLException, RepException {
  StringBuffer query = new StringBuffer();
  query.append("SELECT ").append(RepConstants.shadow_sync_id1).append("  FROM  ")
      .append(shadowTable).append(" WHERE  (").append(RepConstants.shadow_sync_id1)
      .append(" = (SELECT  MAX(").append(RepConstants.shadow_sync_id1).append(") FROM ")
      .append(shadowTable).append(" WHERE ");
  for (int i = 0; i < primaryColsName.length; i++) {
    if (i != 0) {
      query.append(" AND ");
    }
    query.append(primaryColsName[i]).append("= ? ");
  }
  query.append(" AND ");
  for (int i = 0; i < primaryColsName.length; i++) {
    if (i != 0) {
      query.append(" AND ");
    }
    query.append(" REP_OLD_").append(primaryColsName[i]).append("= ? ");
  }
  query.append(" AND ").append(RepConstants.shadow_status4)
      .append(" = '").append(RepConstants.afterUpdate).append(" ')) ");
//System.out.println("Get UID  query.toString() :: "+query.toString());
  return connection.prepareStatement(query.toString());
}

protected PreparedStatement makeQueryToGetViewId() throws SQLException, RepException {
  StringBuffer query = new StringBuffer();
  query.append("SELECT ").append(RepConstants.shadow_sync_id1)
      .append("  FROM  ").append(shadowTable).append(" WHERE ");
  for (int i = 0; i < primaryColumnNames.length; i++) {
    if (i != 0)
    query.append(" AND ");
    query.append(primaryColumnNames[i]).append("= ").append(" REP_OLD_").append(primaryColumnNames[i]);
  }
  query.append(" AND ");
  for (int i = 0; i < primaryColumnNames.length; i++) {
    if (i != 0)
    query.append(" AND ");
    query.append(primaryColumnNames[i]).append("= ?");
  }
  return connection.prepareStatement(query.toString());
}


}
