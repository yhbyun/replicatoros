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

import java.util.*;
import com.daffodilwoods.replication.DBHandler.*;
import org.apache.log4j.Logger;
import java.util.TreeMap;
import java.sql.*;

/**
 * This Class stores all the relevant information for any replication table
 * involved in the publication or subscription. A publisher or subscriber saves
 * the objects of this class corresponding to all replication tables.
 * This class is used to create Reptable table for the perticular publisher or sub.
 *
 */

public class RepTable {

  private SchemaQualifiedName sname;
  private String filterClause = null;
  private String conflictResolver;
  private String[] primCols;
  private String[] foreignKeyCols;
  private TreeMap columnsTypeInfo;
  private StringBuffer questionMarksQuery;
  private String createShadowTable = RepConstants.YES;
  private String cyclicDependency = RepConstants.NO;
  private String serverType;
  private String[] columnsToBeIgnored;
  private StringBuffer columnNamesQuery;
  private TreeMap allColumns;
  protected static Logger log =Logger.getLogger(RepTable.class.getName());

  public RepTable(SchemaQualifiedName sName0, String serverType0) {
    sname = sName0;
    serverType = serverType0;
  }

  public RepTable(SchemaQualifiedName sName0, String filterClause0, String serverType0) {
    this(sName0, serverType0);
    filterClause = filterClause0;
  }

  public String getFilterClause() {
    return filterClause;
  }

  public void setFilterClause(String filterClause0) {
    filterClause = filterClause0;
  }

  public String getConflictResolver() {
    return conflictResolver;
  }

  public void setConflictResolver(String conflictResolver0) {
//    if (conflictResolver0 == null) {
//      Thread.dumpStack();
//    }
    conflictResolver = conflictResolver0;
  }

  public String getCreateShadowTable() {
    return createShadowTable;
  }

  public String[] getForeignKeyCols() {
    return foreignKeyCols;
  }

  public void setCreateShadowTable(String createShadowTable) {
    this.createShadowTable = createShadowTable;
  }

  public void setForeignKeyCols(String[] foreignKeyCols0) {
    foreignKeyCols = foreignKeyCols0;
  }

  public SchemaQualifiedName getSchemaQualifiedName() {
    return sname;
  }



  /**
   * Return a Treemap containg column Names and their respective data type.
   * @param connection
   * @param dbDatatypeHandler
   * @return
   */
  public TreeMap getColumnTreeMap(Connection connection, AbstractDataBaseHandler dbDatatypeHandler) throws
      SQLException, RepException {
    if (columnsTypeInfo != null) {
      return columnsTypeInfo;
    }

    questionMarksQuery = new StringBuffer(); // for parameterised query
    columnNamesQuery = new StringBuffer();
    // Dont use putAll method for treeMap which uses equals method of
    // comparator whihc is unImplemented
    columnsTypeInfo = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    Statement st = connection.createStatement();
    try {
      st.execute("SELECT * FROM " + sname.toString());
      ResultSetMetaData rsmd = st.getResultSet().getMetaData();
      for (int i = 0; i < rsmd.getColumnCount(); i++) {
        boolean isIgnoredColumn = false;
        String colName = rsmd.getColumnName(i + 1);
        String[] ignoredColumnNames = getColumnsToBeIgnored();
        int length = ignoredColumnNames == null ? 0 : ignoredColumnNames.length;
        for (int j = 0; j < length; j++) {
          if (colName.equalsIgnoreCase(ignoredColumnNames[j])) {
            isIgnoredColumn = true;
            break;
          }
        }
        if (!isIgnoredColumn) {
          columnNamesQuery.append(", " + colName);
          questionMarksQuery.append(" , ? "); // For parameterised Query
          int sqlType = rsmd.getColumnType(i + 1);
          String typeName = rsmd.getColumnTypeName(i + 1);
          TypeInfo typeInfo = new TypeInfo(typeName, sqlType);
          dbDatatypeHandler.setColumnPrecisionInTypeInfo(typeInfo, rsmd, i + 1);
          columnsTypeInfo.put(colName,dbDatatypeHandler.getColumnObject(typeInfo));
        }
      }
    }
    finally {
      st.close();
    }
    int indexOfFirstComma = questionMarksQuery.indexOf(",");
    if (indexOfFirstComma != -1) {
      questionMarksQuery.deleteCharAt(indexOfFirstComma);
    }
    indexOfFirstComma = columnNamesQuery.indexOf(",");
    if (indexOfFirstComma != -1) {
      columnNamesQuery.deleteCharAt(indexOfFirstComma);
    }
    return columnsTypeInfo;
  }

  public boolean isIgnoredColumn(String columnName) {
    String ignoredColumnNames[] = getColumnsToBeIgnored();
    if (ignoredColumnNames != null && ignoredColumnNames.length > 0) {
      for (int i = 0; i < ignoredColumnNames.length; i++) {
        if (ignoredColumnNames[i].equalsIgnoreCase(columnName)) {
          return true;
        }
      }
    }
    return false;
  }

  public String createInsertQueryForSnapShot() {
    StringBuffer insertQuery = new StringBuffer();
    insertQuery.append("INSERT INTO ")
        .append(sname.toString())
        .append(" ( ")
        .append(columnNamesQuery)
        .append(" )")
        .append(" VALUES ( ")
        .append(questionMarksQuery.toString())
        .append(" ) ");
        log.debug(insertQuery.toString());
       return insertQuery.toString();
  }

  public String createDeleteQueryForSynchronise() {
    StringBuffer deleteQuery = new StringBuffer();
    String[] primaryColumns = getPrimaryColumns();
    deleteQuery.append("DELETE FROM ")
        .append(sname.toString())
        .append(" WHERE ( ");
    for (int i = 0; i < primaryColumns.length; i++) {
      if (i != 0) {
        deleteQuery.append(" and ");
      }
      deleteQuery.append(primaryColumns[i]).append(" = ?");
    }
    deleteQuery.append(" ) ");
    log.debug(deleteQuery.toString());
    return deleteQuery.toString();
  }

  public String createUpdateQueryForSnapShot() {
    if (foreignKeyCols == null || foreignKeyCols.length == 0) {
      return "";
    }
    StringBuffer updateQuery = new StringBuffer();
    String primarycols[]=getPrimaryColumns();
    updateQuery.append("UPDATE ")
               .append(sname.toString())
               .append(" SET ");
    int size = foreignKeyCols.length;
    for (int i = 0; i < size; i++) {
      if(i!=0){
        updateQuery.append(",");
      }
      updateQuery.append(foreignKeyCols[i]).append("= ? ");
    }
    updateQuery.append(" WHERE  ");
   for (int i = 0; i < primarycols.length; i++) {
     if (i != 0) {
       updateQuery.append(" and ");
     }
     updateQuery.append(primarycols[i]);
     updateQuery.append("= ?");
   }
    log.debug(updateQuery.toString());
    return updateQuery.toString();
  }

  public boolean isForiegnKeyColumn(String columnName) {
    if (foreignKeyCols != null) {
      for (int i = 0, size = foreignKeyCols.length; i < size; i++) {
        if (columnName.equalsIgnoreCase(foreignKeyCols[i])) {
          return true;
        }
      }
    }
    return false;
  }

  public String createDeleteQueryForSynchronise_ShadowTable(long lastConisderedId, String remoteServerName) {
    StringBuffer selectQuery = new StringBuffer();
    String[] primaryColumns = getPrimaryColumns();
    selectQuery.append("SELECT * FROM  ")
        .append(RepConstants.shadow_Table(getSchemaQualifiedName().toString()))
        .append(" WHERE ")
        .append(RepConstants.shadow_sync_id1)
        .append(" > ")
        .append(lastConisderedId);
    for (int i = 0; i < primaryColumns.length; i++) {
      selectQuery.append(" and ")
                 .append(primaryColumns[i])
                 .append(" = ?");
    }
    selectQuery.append(" and ")
               .append(RepConstants.shadow_serverName_n)
               .append("!='")
               .append(remoteServerName)
               .append("'  ");
    return selectQuery.toString();
  }

  /**
   * Return the array of all primary columns that are declared
   * in table which is included in publication.
   * @return String[] Array of primary columns.
   */
  public String[] getPrimaryColumns() {
    return primCols;
  }

  /**
   * Retunrs weather local server is conflict resolver or not.
   * @return boolean
   */
  public boolean isLocalServerWinner() {
    if (serverType.equalsIgnoreCase(RepConstants.subscriber)) {
      return (conflictResolver.equalsIgnoreCase(RepConstants.subscriber_wins)) ? true : false;
    }
    else {
      return (conflictResolver.equalsIgnoreCase(RepConstants.publisher_wins)) ? true : false;
    }
  }

  /**
   * Retunrs a query for Update in Main table
   * This method is called for performing update
   * operation when Replicator found the update
   * operation in XML file.
   * @param loaclColumnNames
   * @param primaryColumnName
   * @return
   */
  public String getUpdatePreStmt(ArrayList loaclColumnNames,
                                     String[] primaryColumnName) {
    StringBuffer updateQuery = new StringBuffer();
    updateQuery.append(" UPDATE ")
        .append(sname.toString())
        .append(" SET ");
    for (int i = 0; i < loaclColumnNames.size(); i++) {
      if (i != 0) {
        updateQuery.append(" , ");
      }
      updateQuery.append(loaclColumnNames.get(i))
                 .append(" = ? ");
    }
    updateQuery.append(" WHERE  ");
    for (int i = 0; i < primaryColumnName.length; i++) {
      if (i != 0) {
        updateQuery.append(" and ");
      }
      updateQuery.append(primaryColumnName[i])
                 .append("= ?");
    }
    return updateQuery.toString();
  }

  public void setPrimaryColumns(String[] primCols0) {
    primCols = primCols0;
  }

  public String toString() {
    return "REPTABLE NAME [" + getSchemaQualifiedName().toString() +
        "] PRIMCOLS [" + primCols + "] ConflictResolver [" + conflictResolver +
        "] Filter [" + filterClause + "] ";
  }

  public String getRepTableQualifiedIdentifier() {
    return sname.toString();
  }

  public String getCyclicDependency() {
    return cyclicDependency;
  }


  public void setCyclicDependency(String cyclicDependency0) {
    cyclicDependency = cyclicDependency0;
  }

  public String[] getColumnsToBeIgnored() {
    return columnsToBeIgnored;
  }

  public String getServerType() {
    return serverType;
  }

  public void setColumnsToBeIgnored(String[] columnsToBeIgnored) {
    this.columnsToBeIgnored = columnsToBeIgnored;
  }

 public void setServerType(String serverType) {
    this.serverType = serverType;
  }

  public String createUpdateQueryForSynchronize() {
     if (foreignKeyCols == null || foreignKeyCols.length == 0) {
       return "";
     }
     StringBuffer updateQuery = new StringBuffer();
     String[] primaryColumns = getPrimaryColumns();
     updateQuery.append("UPDATE ").append(sname.toString()).append(" SET ");
     int size = foreignKeyCols.length;
     for (int i = 0; i < size; i++) {
       if(i!=0) {
         updateQuery.append(",");
       }
       updateQuery.append(foreignKeyCols[i]).append("= ? ");
     }
     updateQuery.append(" WHERE  ");
   for (int i = 0; i < primaryColumns.length; i++) {
     if (i != 0) {
       updateQuery.append(" and ");
     }
     updateQuery.append(primaryColumns[i]).append(" = ?");
     }

     log.debug(updateQuery.toString());
     return updateQuery.toString();
   }

   public void setAllColumns(String[] allColumns0) {
     allColumns=new TreeMap();
     for (int i = 0; i < allColumns0.length; i++) {
       allColumns.put("c"+new Integer(i + 1), allColumns0[i]);
     }
   }

    public TreeMap getAllColumns() {
      return allColumns;
    }
}
