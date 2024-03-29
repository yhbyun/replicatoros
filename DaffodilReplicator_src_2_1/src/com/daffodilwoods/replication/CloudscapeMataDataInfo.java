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

import java.sql.*;
import java.util.*;

import com.daffodilwoods.replication.DBHandler.*;
import org.apache.log4j.Logger;
import com.daffodilwoods.graph.DirectedGraph;

/**
 * This Class gets the connection object of the specified publisher or subscriber
 * and then stores it's metadata information in the DataBaseMetaData object(dbmd).
 * This inforamtion is used for performing different operations on the tables of
 * the publication or subscription and on the columns and constraint.
 */

public class CloudscapeMataDataInfo
    extends MetaDataInfo {
  protected static Logger log = Logger.getLogger(CloudscapeMataDataInfo.class.
                                                 getName());

  public CloudscapeMataDataInfo(ConnectionPool connectionPool0,
                                String pubsubName) throws RepException {
    this(connectionPool0.getConnection(pubsubName));
  }

  public CloudscapeMataDataInfo(Connection connection0) throws RepException {
    try {
      dbmd = connection0.getMetaData();
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP101", new Object[] {ex.getMessage()});
    }
  }

  public void checkTableExistance(SchemaQualifiedName sname) throws
      RepException {
    String schema = sname.getSchemaName();
    String table = sname.getTableName();
    try {
//        System.out.println(" dbmd ="+dbmd);
//        System.out.println(" schema ="+schema);
//        System.out.println(" table ="+table);

//        ResultSet rs1 = dbmd.getTables(null, null, null,new String[] {"TABLE"});
// CommonMetaDataInfo.showResultSet(rs1);
// rs1 = dbmd.getTables(null, null, table.toLowerCase(),new String[] {"TABLE"});
//CommonMetaDataInfo.showResultSet(rs1);

      ResultSet rs = dbmd.getTables(null, null, table.toUpperCase(),
                                    new String[] {"TABLE"});

      if (rs == null || !rs.next()) {
        log.debug("Resultset  found  " + rs + " OR Resultset found false");
        throw new RepException("REP017", new Object[] {sname.toString()});
      }
      sname.setSchemaName(this, rs.getString("TABLE_SCHEM"));
      if (rs.next()) {
        throw new RepException("REP018", new Object[] {table});
      }
      /** @todo rs has been closed  */
      rs.close();
    }
    catch (RepException ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP006", new Object[] {ex.getMessage()});
    }
  }

  public String[] getTablesHierarchy(String[] tableNames) throws RepException {
    ArrayList givenListOfTables = new ArrayList();
    for (int i = 0; i < tableNames.length; i++) {
      givenListOfTables.add(tableNames[i].toUpperCase());
    }
    ArrayList listToReturn = new ArrayList();
    ArrayList sublist1 = new ArrayList();
    for (int i = 0; i < tableNames.length; i++) {
      SchemaQualifiedName sname = new SchemaQualifiedName(this, tableNames[i]);
      String schemaName = sname.getSchemaName();
      String tableName = sname.getTableName();
//          checkParentTablesIncludedInList(givenListOfTables,schemaName,tableName);
      listToReturn.add(sname.toString());
      sublist1 = getTableSequenceRegardingForeignKey(schemaName, tableName,
          listToReturn);
      addTablesRecursively(givenListOfTables, listToReturn, sublist1);
      sublist1.clear();
    }
    String[] arr = new String[listToReturn.size()];
    listToReturn.toArray(arr);
    log.debug(listToReturn);
    return arr;
  }

  private void addTablesRecursively(ArrayList givenListOfTables,
                                    ArrayList listToReturn, ArrayList sublist) throws
      RepException {
    for (int j = 0; j < sublist.size(); j++) {
      String table = (String) sublist.get(j);
      if (listToReturn.contains(table)) {
        SchemaQualifiedName sname = new SchemaQualifiedName(this, table);
        String schemaName = sname.getSchemaName();
        String tableName = sname.getTableName();
        ArrayList subList1 = getTableSequenceRegardingForeignKey(schemaName,
            tableName, listToReturn);
//          if(givenListOfTables.contains(table)) {
        listToReturn.remove(listToReturn.indexOf(table));
        listToReturn.add(table);
//          }
        addTablesRecursively(givenListOfTables, listToReturn, subList1);
        subList1.clear();

      }
      else {
//            if(givenListOfTables.contains(table)) {
        listToReturn.add(table);
//            }
      }
    }
  }

  public void checkParentTablesIncludedInList(ArrayList givenListOfTables,
                                              String schemaName,
                                              String tableName) throws
      RepException {
    try {
      ResultSet rs = dbmd.getImportedKeys(null, schemaName, tableName);
      if (rs != null && rs.next()) {
        do {
          String pk_tableName = rs.getString("PKTABLE_SCHEM") + "." +
              rs.getString("PKTABLE_NAME");
          log.debug("Primary Tablename " + pk_tableName);
          if (!givenListOfTables.contains(pk_tableName.toUpperCase())) {
            int lastIndexOfDot = pk_tableName.lastIndexOf(".");
            if (lastIndexOfDot != -1) {
              String pktableName = pk_tableName.substring(lastIndexOfDot + 1);
              if (!givenListOfTables.contains(pktableName.toUpperCase())) {
                throw new RepException("REP0201",
                                       new Object[] {pk_tableName.toUpperCase(),
                                       tableName.toUpperCase()});
              }

            }
            else {
              throw new RepException("REP0201",
                                     new Object[] {pk_tableName.toUpperCase(),
                                     tableName.toUpperCase()});
            }
          }
        }
        while (rs.next());
        /** @todo rs has been closed */
        rs.close();
      }
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP0202", new Object[] {tableName, ex.getMessage()});
    }
  }

  protected ArrayList getTableSequenceRegardingForeignKey(String schemaName,
      String tableName, ArrayList tableList) throws RepException {
    ArrayList list = new ArrayList();
    String qualifiedName = null;
    try {
      log.debug(" TableName : " + schemaName + "." + tableName);
      ResultSet rs2 = dbmd.getExportedKeys(null, schemaName.toUpperCase(),
                                           tableName.toUpperCase());
      log.debug(" ExportedKeys Table Name's ResultSet " + rs2);
      if (rs2 != null && rs2.next()) {
        do {
//          String primaryTable = rs2.getString("PKTABLE_SCHEM") + "." +rs2.getString("PKTABLE_NAME");
          String foreignTable = rs2.getString("FKTABLE_SCHEM") + "." +
              rs2.getString("FKTABLE_NAME");
          if (tableList.size() > 0) {
            String table_list = (String) tableList.get(0);
            if (table_list.indexOf(".") == -1) {
              int indexInforiegnTable = foreignTable.lastIndexOf(".");
              if (indexInforiegnTable != -1) {
                foreignTable = foreignTable.substring(indexInforiegnTable + 1);
              }
            }
          }

          if (schemaName != null) {
            qualifiedName = schemaName + "." + tableName;
          }
          else {
            qualifiedName = tableName;
          }
          if (tableList.contains(foreignTable.toUpperCase()) &&
              !foreignTable.equalsIgnoreCase(qualifiedName)) {
            list.add(tableList.get(tableList.indexOf(foreignTable.toUpperCase())));
            tableList.remove(foreignTable.toUpperCase());
//            throw new RepException("REP015", new Object[] {primaryTable,foreignTable});
          }
        }
        while (rs2.next());
        /** @todo rs2 has been closed */
        rs2.close();
      }
    }
//    catch (RepException ex) {
//      throw ex;
//    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP006", new Object[] {ex.getMessage()});
    }
    log.debug(" Sublist of tables  =" + list);
    return list;
  }

  public void checkTableSequenceAccordingForeignKey(String schemaName,
      String tableName, ArrayList tableList) throws RepException {
    log.debug(" TableName : " + schemaName + "." + tableName);
    try {
      ResultSet rs2 = dbmd.getExportedKeys(null, schemaName.toUpperCase(),
                                           tableName.toUpperCase());
      log.debug(" ExportedKeys Table Name's ResultSet " + rs2);
      if (rs2 != null && rs2.next()) {
        do {
          String primaryTable = rs2.getString("PKTABLE_SCHEM") + "." +
              rs2.getString("PKTABLE_NAME");
          String foreignTable = rs2.getString("FKTABLE_SCHEM") + "." +
              rs2.getString("FKTABLE_NAME");
          if (primaryTable.equalsIgnoreCase(foreignTable))
            continue;
          if (tableList.size() > 0) {
            String table_list = (String) tableList.get(0);
            if (table_list.indexOf(".") == -1) {
              int indexInforiegnTable = foreignTable.lastIndexOf(".");
              if (indexInforiegnTable != -1) {
                foreignTable = foreignTable.substring(indexInforiegnTable + 1);
              }
            }
          }

          if (tableList.contains(foreignTable.toUpperCase())) {
            throw new RepException("REP015", new Object[] {primaryTable,
                                   foreignTable});
          }
        }
        while (rs2.next());
        /** @todo rs2 has been closed */
        rs2.close();
      }

    }
    catch (RepException ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP006", new Object[] {ex.getMessage()});
    }
  }

  public ArrayList getColumnDataTypeInfo(AbstractDataBaseHandler dbh,
                                         String schemaName, String tableName) throws
      RepException, SQLException {
    ArrayList colInfoList = new ArrayList();
    ResultSet rs = dbmd.getColumns(null, schemaName.toUpperCase(),
                                   tableName.toUpperCase(), "%");
    if (rs == null || !rs.next()) {
      throw new RepException("Internal Error", null);
    }
    do {
      TypeInfo typeInfo = new TypeInfo(
          dbh.updateDataType(rs.getString("TYPE_NAME")), rs.getInt("DATA_TYPE"));
      int columnPrecision = rs.getInt("COLUMN_SIZE");
      int columnScale = rs.getInt("DECIMAL_DIGITS");
      String typeName = rs.getString("TYPE_NAME").trim();
      columnPrecision = dbh.getAppropriatePrecision(columnPrecision, typeName);
      columnScale = dbh.getAppropriateScale(columnScale);
      typeInfo.setOptionalSizeProperty(dbh.isDataTypeOptionalSizeSupported(
          typeInfo));
      typeInfo.setColumnSize(rs.getInt("COLUMN_SIZE"));
      typeInfo.setColumnScale(columnScale);
      dbh.setTypeInfo(typeInfo, rs);
      //colInfoMap.put(rs.getString("COLUMN_NAME").trim(),typeInfo.getTypeDeclaration(rs.getInt("COLUMN_SIZE")));
      colInfoList.add(new ColumnsInfo(rs.getString("COLUMN_NAME").trim(),
                                      typeInfo.getTypeDeclaration(
          columnPrecision)));
      log.debug("Column Name " + rs.getString("COLUMN_NAME").trim());
      log.debug("Type declaration " +
                typeInfo.getTypeDeclaration(columnPrecision));
    }
    while (rs.next());
    /** @todo rs has been closed */
    rs.close();
    return colInfoList;
  }

  public HashMap getColumnsInfo(String schemaName, String tableName) throws
      RepException, SQLException {
    HashMap colInfoMap = new HashMap();
    ResultSet rs = dbmd.getColumns(null, schemaName.toUpperCase(),
                                   tableName.toUpperCase(), "%");
    if (rs == null || !rs.next()) {
      log.debug("Resultset  found" + rs + " OR Resultset found false");
      throw new RepException("Internal Error", null);
    }
    int i = 0;
    do {
      //colInfoMap.put(rs.getString("COLUMN_NAME").trim(),"");
      colInfoMap.put(new Integer(i++),
                     new ColumnsInfo(rs.getString("COLUMN_NAME").trim(), null));
    }
    while (rs.next());
    log.debug(" colInfoMap  : " + colInfoMap);

    /** @todo rs has been closed */
    rs.close();
    return colInfoMap;
  }

  /**
   * @param dbh
   * @param srcVendorType
   * @param schemaName
   * @param tableName
   * @param tgtVendorType
   * @return
   * @throws SQLException
   * @throws RepException
   */
  public String generateColumnsQueryForClientNode(AbstractDataBaseHandler dbh,
                                                  int srcVendorType,
                                                  String schemaName,
                                                  String tableName,
                                                  int tgtVendorType) throws
      RepException {
    StringBuffer sb = new StringBuffer();
    AbstractDataBaseHandler remotedbh = Utility.getDatabaseHandler(
        tgtVendorType);
    AbstractDataBaseHandler optSizedbh = (srcVendorType == tgtVendorType) ?
        dbh : remotedbh;
    try {
      ResultSet rs = dbmd.getColumns(null, schemaName.toUpperCase(),
                                     tableName.toUpperCase(), "%");
      if (rs == null || !rs.next()) {
        log.debug("Resultset  found" + rs + " OR Resultset found false");
        throw new RepException("REP033",
                               new Object[] {schemaName + "." + tableName});
      }
      do {

        String typeName = dbh.updateDataType(rs.getString("TYPE_NAME"));
        int dataType = rs.getInt("DATA_TYPE");
        TypeInfo typeInfo = new TypeInfo(typeName, dataType);
        int columnPrecision = rs.getInt("COLUMN_SIZE");
        int columnScale = rs.getInt("DECIMAL_DIGITS");
        String columnName = rs.getString("COLUMN_NAME");
        columnPrecision = optSizedbh.getAppropriatePrecision(columnPrecision,
            typeName);
        typeInfo.setColumnSize(rs.getInt("COLUMN_SIZE"));
        columnScale = optSizedbh.getAppropriateScale(columnScale);
        int columnScalePublisher = dbh.getAppropriateScale(columnScale);
        if (columnScale > columnScalePublisher) {
          typeInfo.setColumnScale(columnScalePublisher);
        }
        else {
          typeInfo.setColumnScale(columnScale);
        }
        optSizedbh.setTypeInfo(typeInfo, rs);
        typeInfo.setOptionalSizeProperty(optSizedbh.
                                         isDataTypeOptionalSizeSupported(
            typeInfo));
        String nullable = rs.getString("IS_NULLABLE").trim();
//         String columnInfo =remotedbh.getTableColumns(dbh.getvendorName(),columnName,typeInfo,columnPrecision,rs);
//         System.out.println("CLOUDSCAPECommonMetaDataInfo.GENERATECOLUMNQUERYFORCLIENT ="+columnInfo);
//         sb.append(columnInfo);
        if (nullable.equalsIgnoreCase("NO") &&
            tgtVendorType == Utility.DataBase_Cloudscape) {
          sb.append(columnName).append(" ").append(typeInfo.getTypeDeclaration(
              columnPrecision)).append(" NOT NULL ");
        }
        else if (nullable.equalsIgnoreCase("NO") &&
                 tgtVendorType == Utility.DataBase_DB2) {
          sb.append(columnName).append(" ").append(typeInfo.getTypeDeclaration(
              columnPrecision)).append(" NOT NULL ");
        }
        else {
          sb.append(columnName).append(" ").append(typeInfo.getTypeDeclaration(
              columnPrecision));
        }
        if (srcVendorType == tgtVendorType) {
          typeInfo.setTypeName(typeName);
        }
        //System.out.println(" type Name === " + typeName + " sql Type " + sqlType ) ;
        String defaultValue = rs.getString("COLUMN_DEF"); //String => default value (may be <code>null</code>)
        if (defaultValue != null && !defaultValue.equalsIgnoreCase("NULL"))
          sb.append("DEFAULT").append("  ").append(defaultValue);
//         String nullable = rs.getString("IS_NULLABLE").trim(); // String => "NO" means column definitely does not allow NULL values; "YES" means the column might allow NULL values.  An empty string means nobody knows.
        if (nullable.equalsIgnoreCase("NO")) {
          //System.out.println(" I m here");
          if (notNullColumns == null) {
            notNullColumns = new ArrayList();
          }
          notNullColumns.add(columnName);
        }
        //new ColumnsInfo(columnName,typeName,sqlType,columnSize/*,defaultValue,nullable*/);
        sb.append(" , ");
      }
      while (rs.next());
      /** @todo rs has been closed */
      rs.close();
    }
    catch (RepException ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP006", new Object[] {ex.getMessage()});
    }
    log.debug(sb.toString());
    return sb.toString();
  }

  public String getAppliedConstraints(String schemaName, String tableName,
                                      TreeMap primConsMap) throws RepException {
    StringBuffer cons = new StringBuffer();
    appendPrimaryConstraints(schemaName, tableName, cons, primConsMap);
    appendUniqueKeyConstraints(schemaName, tableName, cons);
//    appendForeignConstraints(schemaName, tableName, cons, primConsMap);
    appendCheckConstraints(cons);
    return cons.toString();
  }

  public String getAppliedConstraintsForExistingTable(String schemaName,
      String tableName, TreeMap primConsMap, AbstractDataBaseHandler dbh) throws
      RepException {
    StringBuffer cons = new StringBuffer();
    appendPrimaryConstraints(schemaName, tableName, cons, primConsMap);
    appendUniqueKeyConstraints(schemaName, tableName, cons);
//    appendForeignConstraints(schemaName, tableName, cons, primConsMap);
    addCheckConstraintForExistingTable(schemaName, tableName, primConsMap, dbh);
    appendCheckConstraints(cons);
    return cons.toString();
  }

  public void addCheckConstraintForExistingTable(String schemaName,
                                                 String tableName,
                                                 TreeMap primConsMap,
                                                 AbstractDataBaseHandler dbh) throws
      RepException {
    try {
      ResultSet rs = dbmd.getColumns(null, schemaName.toUpperCase(),
                                     tableName.toUpperCase(), "%");
      if (rs == null || !rs.next()) {
        log.debug("Resultset  found  " + rs + " OR Resultset found false");
        throw new RepException("REP033",
                               new Object[] {schemaName + "." + tableName});
      }
      do {
        String columnName = rs.getString("COLUMN_NAME");
        String nullable = rs.getString("IS_NULLABLE").trim();
        if (nullable.equalsIgnoreCase("NO")) {
          if (notNullColumns == null) {
            notNullColumns = new ArrayList();
          }
          notNullColumns.add(columnName);
        }
      }
      while (rs.next());
      /** @todo rs has been closed */
      rs.close();
    }
    catch (RepException ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP006", new Object[] {ex.getMessage()});
    }

  }

  private void appendPrimaryConstraints(String schemaName, String tableName,
                                        StringBuffer cols, TreeMap consTableMap) throws
      RepException {
    HashMap primcolmap = null;
    try {
      ResultSet rs = dbmd.getPrimaryKeys(null, schemaName.toUpperCase(),
                                         tableName.toUpperCase());
      if (rs == null || !rs.next()) {
        log.debug("Resultset  found  " + rs + " OR Resultset found false");
        throw new RepException("REP034",
                               new Object[] {schemaName + "." + tableName});
      }
      primcolmap = new HashMap();
      do {
        primcolmap.put(new Integer(rs.getInt("KEY_SEQ")),
                       rs.getString("COLUMN_NAME"));
      }
      while (rs.next());
      /** @todo rs has been closed */
      rs.close();
    }
    catch (RepException ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP006", new Object[] {ex.getMessage()});
    }
    Object[] indexes = primcolmap.keySet().toArray();
    Arrays.sort(indexes);
    cols.append(" Primary Key (");
    StringBuffer temp = new StringBuffer();
    for (int i = 0; i < indexes.length; i++) {
      if (i != 0) {
        temp.append(",");
      }
      temp.append(primcolmap.get(indexes[i]));
    }
    cols.append(temp.toString()).append(" )");
    log.debug(cols.toString());
    consTableMap.put(schemaName + "." + tableName,
                     schemaName + "." + tableName + "(" + temp.toString() + ")");
  }

  // PKTABLE_CAT</B> String => primary key table catalog being imported (may be <code>null</code>)
  // PKTABLE_SCHEM</B> String => primary key table schema being imported (may be <code>null</code>)
  // PKTABLE_NAME</B> String => primary key table name being imported
  // PKCOLUMN_NAME</B> String => primary key column name being imported
  // FKTABLE_CAT</B> String => foreign key table catalog (may be <code>null</code>)
  // FKTABLE_SCHEM</B> String => foreign key table schema (may be <code>null</code>)
  // FKTABLE_NAME</B> String => foreign key table name
  // FKCOLUMN_NAME</B> String => foreign key column name
  // KEY_SEQ</B> short => sequence number within a foreign key
  // UPDATE_RULE</B> short =>
  // DELETE_RULE</B> short =>
  // FK_NAME</B> String => foreign key name (may be <code>null</code>)
  // PK_NAME</B> String => primary key name (may be <code>null</code>)
  // DEFERRABILITY</B> short => can the evaluation of foreign key constraints be deferred until commit
  private void appendForeignConstraints(String schemaName, String tableName,
                                        StringBuffer cols, TreeMap consTableMap) throws
      RepException {
    HashMap fk_Keys = new HashMap();
    HashMap fk_pk = new HashMap();
    try {
      ResultSet rs = dbmd.getImportedKeys(null, schemaName.toUpperCase(),
                                          tableName.toUpperCase());
      if (rs == null || !rs.next()) {
        log.debug("Resultset  found  " + rs + " OR Resultset found false");
        return;
      }
      do {
        String pk_tableName = rs.getString("PKTABLE_SCHEM") + "." +
            rs.getString("PKTABLE_NAME");
        Object primObject = consTableMap.get(pk_tableName.toUpperCase());

        if (primObject == null) {
          ResultSet primaryColumns = dbmd.getPrimaryKeys(null,
              rs.getString("PKTABLE_SCHEM"), rs.getString("PKTABLE_NAME"));
          HashMap map = new HashMap();
          while (primaryColumns.next()) {
            int columnIndex = primaryColumns.getInt("KEY_SEQ");
            String columnName = primaryColumns.getString("COLUMN_NAME");
            map.put(new Integer(columnIndex), columnName);
          }
          Object[] indexes = map.keySet().toArray();
          Arrays.sort(indexes);
          StringBuffer temp = new StringBuffer();
          for (int i = 0; i < indexes.length; i++) {
            if (i != 0) {
              temp.append(",");
            }
            temp.append(map.get(indexes[i]));
          }
          consTableMap.put(pk_tableName.toUpperCase(),
                           pk_tableName.toUpperCase() + "(" + temp.toString() +
                           ") ");
          primObject = consTableMap.get(pk_tableName.toUpperCase());
        }
        if (primObject != null) {
          String fk_tableName = rs.getString("FKTABLE_SCHEM") + "." +
              rs.getString("FKTABLE_NAME");
          String mapKey = rs.getString("FK_NAME");
          Object ob = fk_Keys.get(mapKey);
          if (ob == null) {
            HashMap colsMap = new HashMap();
            colsMap.put(new Integer(rs.getInt("KEY_SEQ")),
                        rs.getString("FKCOLUMN_NAME"));
            fk_pk.put(mapKey, primObject);
            fk_Keys.put(mapKey, colsMap);
          }
          else {
            ( (HashMap) ob).put(new Integer(rs.getInt("KEY_SEQ")),
                                rs.getString("FKCOLUMN_NAME"));
          }
        }
      }
      while (rs.next());
      /** @todo rs has been closed */
      rs.close();
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP006", new Object[] {ex.getMessage()});
    }
    Object[] fkeys = fk_Keys.keySet().toArray();
    Arrays.sort(fkeys);
    for (int i = 0; i < fkeys.length; i++) {
      HashMap map = (HashMap) fk_Keys.get(fkeys[i]);
      Object[] indexes = map.keySet().toArray();
      Arrays.sort(indexes);
      cols.append(" , Foreign Key (");
      for (int j = 0; j < indexes.length; j++) {
        if (j != 0) {
          cols.append(",");
        }
        cols.append(map.get(indexes[j]));
      }
      cols.append(" ) References " + fk_pk.get(fkeys[i]));
      log.debug(cols.toString());
    }
  }

  private void appendCheckConstraints(StringBuffer sb) {
    for (int i = 0, length = notNullColumns.size(); i < length; i++) {
      sb.append(" , Check ( ").append(notNullColumns.get(i)).append(
          " is not null ) ");
    }
    notNullColumns = null;
  }

  public void setPrimaryColumns(RepTable repTable, String schemaName,
                                String tableName) throws RepException,
      SQLException {
    HashMap primcolmap = new HashMap();
    ResultSet rs = dbmd.getPrimaryKeys(null, schemaName.toUpperCase(),
                                       tableName.toUpperCase());
    if (rs == null || !rs.next()) {
      log.debug("Resultset  found " + rs + " OR Resultset found false");
      throw new RepException("REP034", new Object[] {tableName});
    }
    do {
      primcolmap.put(new Integer(rs.getInt("KEY_SEQ")),
                     rs.getString("COLUMN_NAME"));
    }
    while (rs.next());
    /** @todo rs has been closed */
    rs.close();

    String[] primColumns = new String[primcolmap.size()];
    Object[] indexes = primcolmap.keySet().toArray();
    Arrays.sort(indexes);
    for (int i = 0; i < indexes.length; i++) {
      primColumns[i] = (String) primcolmap.get(indexes[i]);
    }
    repTable.setPrimaryColumns(primColumns);
  }

  public String getExistingTableQuery(AbstractDataBaseHandler dbh,
                                      SchemaQualifiedName sname,
                                      int pubVendorType) throws RepException,
      SQLException {
    StringBuffer sb = new StringBuffer();
    String table = sname.getTableName();
    String schema = sname.getSchemaName();
    sb.append(" Create Table ").append(sname.toString()).append(" ( ");
    ResultSet rs = dbmd.getColumns(null, schema.toUpperCase(),
                                   table.toUpperCase(), "%");
    if (rs == null || !rs.next()) {
      log.debug("Resultset  found " + rs + " OR Resultset found false");
      throw new RepException("REP033", new Object[] {sname.toString()});
    }
    do {
      String typeName = dbh.updateDataType(rs.getString("TYPE_NAME"));
      int dataType = rs.getInt("DATA_TYPE");
      TypeInfo typeInfo = new TypeInfo(typeName, dataType);
      int columnPrecision = rs.getInt("COLUMN_SIZE");
      columnPrecision = dbh.getAppropriatePrecision(columnPrecision, typeName);
      String columnName = rs.getString("COLUMN_NAME");
      typeInfo.setColumnSize(rs.getInt("COLUMN_SIZE"));
      dbh.setTypeInfo(typeInfo, rs);
      typeInfo.setOptionalSizeProperty(dbh.isDataTypeOptionalSizeSupported(
          typeInfo));
      String nullable = rs.getString("IS_NULLABLE").trim();
//        String columnInfo =dbh.getTableColumns(pubVendorType,columnName,typeInfo,columnPrecision,rs);
//        System.out.println("CLOUDSCAPECommonMetaDataInfo.GetexistingTableQuery ="+columnInfo);
//         sb.append(columnInfo);
      if (nullable.equalsIgnoreCase("NO")) {
        sb.append(columnName).append(" ").append(typeInfo.getTypeDeclaration(
            columnPrecision)).append(" NOT NULL ");
      }
      else {
        sb.append(columnName).append(" ").append(typeInfo.getTypeDeclaration(
            columnPrecision));
      }
      sb.append(" , ");
    }
    while (rs.next());
    /** @todo rs has been closed */
    rs.close();
//      appendPrimaryConstraints(schema,table,sb,new HashMap());
    sb.append(" " +
              getAppliedConstraintsForExistingTable(schema, table,
        new TreeMap(String.CASE_INSENSITIVE_ORDER),
        dbh) + " ) ");
    log.debug(sb.toString());
    return sb.toString();
  }

  /**
   * checkChildTableIncludedInDropTableList
   *
   * @param pubRepTableList ArrayList
   * @param dropTableList ArrayList
   * @return ArrayList
   */
  protected void checkChildTableIncludedInDropTableList(ArrayList
      pubRepTableList, String[] dropTableList) {

  }

  public ArrayList getChildTables(String parentTable) throws RepException {
    String foreignTable = null;
    ArrayList childTableList = new ArrayList();
    try {
      SchemaQualifiedName sname = new SchemaQualifiedName(this, parentTable);
      ResultSet rs2 = dbmd.getExportedKeys(null, sname.getSchemaName(),
                                           sname.getTableName());
      log.debug(" ExportedKeys Table Name's ResultSet " + rs2);
      if (rs2 != null && rs2.next()) {
        do {
          foreignTable = rs2.getString("FKTABLE_SCHEM") + "." +
              rs2.getString("FKTABLE_NAME");
          childTableList.add(foreignTable);
        }
        while (rs2.next());
      }
      /** @todo   rs has been closed  */
      rs2.close();

    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP006", new Object[] {ex.getMessage()});
    }
    System.out.println("CommonMetaDataInfo.getChildTables() : " +
                       childTableList);
    return childTableList;
  }

  public Object[] getImportedColsOfChildTable(String parentTable,
                                              String childTable) throws
      RepException {
    ArrayList fkColsList = new ArrayList();
    ArrayList referColsList = new ArrayList();
    try {
      SchemaQualifiedName sname = new SchemaQualifiedName(this, childTable);
      ResultSet rs = dbmd.getImportedKeys(null, sname.getSchemaName(),
                                          sname.getTableName());
      if (rs == null || !rs.next()) {
        log.debug("Resultset  found  " + rs + " OR Resultset found false");
        return null;
      }
      do {
        String pk_tableName = rs.getString("PKTABLE_SCHEM") + "." +
            rs.getString("PKTABLE_NAME");
        if (pk_tableName.equalsIgnoreCase(parentTable)) {
          fkColsList.add(rs.getString("FKCOLUMN_NAME"));
          referColsList.add(rs.getString("PKCOLUMN_NAME"));
        }
      }
      while (rs.next());
      /** @todo   rs has been closed  */
      rs.close();
    }
    catch (SQLException ex) {
      log.error(ex.getMessage(), ex);
      throw new RepException("REP006", new Object[] {ex.getMessage()});
    }
    String[] fkCols = new String[fkColsList.size()];
    fkColsList.toArray(fkCols);
    String[] pkCols = new String[referColsList.size()];
    referColsList.toArray(pkCols);

    return new Object[] {
        fkCols, pkCols};

  }

  public ArrayList getImportedTables(SchemaQualifiedName schemaQualifiedName,
                                     List passedSchemaQualifiedNamesList,
                                     DirectedGraph graph,
                                     String[] removeCycleTableNames0) throws
      RepException {
    try {
      ResultSet rs = dbmd.getImportedKeys(null,
                                          schemaQualifiedName.getSchemaName(),
                                          schemaQualifiedName.getTableName());
      ArrayList listOfSuperTables = new ArrayList();
      while (rs.next()) {
        SchemaQualifiedName superTableSchemaQualifiedName = new
            SchemaQualifiedName(this, null, rs.getString(2), rs.getString(3));
        /** @todo
         * define Ex in properties file
         *  */

        if (!passedSchemaQualifiedNamesList.contains(
            superTableSchemaQualifiedName)) {
          throw new RepException("REP038", new Object[] {
                                 superTableSchemaQualifiedName,
                                 schemaQualifiedName});
        }
        //to handle tables in cycle case.we do not add edge for tables in removeCycleTableNames arraylist provided by user
        String[] removeCycleTableNames = removeCycleTableNames0;
        boolean addEdge = true;
        if (removeCycleTableNames != null) {
          for (int i = 0; i < removeCycleTableNames.length; i++) {
            StringTokenizer str = new StringTokenizer(removeCycleTableNames[i],
                "-");
            SchemaQualifiedName sname = new SchemaQualifiedName(this,
                (String) str.nextElement());
            SchemaQualifiedName sourceSname = sname;
            checkTableExistance(sname);
            if (!passedSchemaQualifiedNamesList.contains(sname))
              throw new RepException("Rep0206", new Object[] {sname});
//              System.out.println("source::::"+sourceSname.toString());
            sname = new SchemaQualifiedName(this, (String) str.nextElement());
            checkTableExistance(sname);
            if (!passedSchemaQualifiedNamesList.contains(sname))
              throw new RepException("Rep0206", new Object[] {sname});
            SchemaQualifiedName targetSname = sname;
//              System.out.println("target::::"+targetSname.toString());
//              System.out.println("schemaQualifiedName.toString():::"+schemaQualifiedName.toString());
//              System.out.println("superTableSchemaQualifiedName.toString():::"+superTableSchemaQualifiedName.toString());
            if (sourceSname.toString().equalsIgnoreCase(schemaQualifiedName.
                toString()) &&
                targetSname.toString().equalsIgnoreCase(
                superTableSchemaQualifiedName.toString()))
              addEdge = false;
          }
        }
        // to handle self referencing case
        if (schemaQualifiedName.toString().equalsIgnoreCase(
            superTableSchemaQualifiedName.toString()))
          addEdge = false;

        if (addEdge) {
//System.out.println("schemaQualifiedName.toString():::" +schemaQualifiedName.toString());
//System.out.println("superTableSchemaQualifiedName.toString():::" +superTableSchemaQualifiedName.toString());
          graph.addEdge(schemaQualifiedName, superTableSchemaQualifiedName, 1);
        }

        if (!listOfSuperTables.contains(superTableSchemaQualifiedName))
          listOfSuperTables.add(superTableSchemaQualifiedName);
      }
      rs.close();
      return listOfSuperTables;
    }
    catch (NoSuchElementException ex1) {
      throw new RepException("Rep0204", null);
    }
    catch (SQLException ex) {
      throw new RepException("REP039", new Object[] {ex.getMessage()});
    }
  }

  public ArrayList getForiegnKeyConstraints(String schemaName, String tableName) throws
      RepException {
    HashMap fk_Keys = new HashMap();
    HashMap fk_pk = new HashMap();
    HashMap consPkTableMap = new HashMap();
    HashMap referenceString = new HashMap();
    StringBuffer alterTableQuery = new StringBuffer();
    ResultSet rs;
    ArrayList foreignKeyConstraintsList = new ArrayList();
// System.out.println("in the getForiegnKeyConstraints :::::::::: ****************[" + schemaName+"] tableName ["+tableName+"]");
    try {
      rs = dbmd.getImportedKeys(null, schemaName, tableName);
      boolean next = rs.next();
      if (!next) {
        return null;
      }
      do {
        String pk_tableName = rs.getString("PKTABLE_SCHEM") + "." +
            rs.getString("PKTABLE_NAME");

        String mapPkKey = rs.getString("FK_NAME");
        //changed to handle the case for unique key

        Object primObjectTemp = consPkTableMap.get(mapPkKey);
        if (primObjectTemp == null) {
          HashMap colsPkMap = new HashMap();
          colsPkMap.put(new Integer(rs.getInt("KEY_SEQ")),
                        rs.getString("PKCOLUMN_NAME"));

          consPkTableMap.put(mapPkKey, colsPkMap);
        }
        else {
          ( (HashMap) primObjectTemp).put(new Integer(rs.getInt("KEY_SEQ")),
                                          rs.getString("PKCOLUMN_NAME"));
        }
        Object primObject = consPkTableMap.get(mapPkKey);
        if (primObject != null) {
          String fk_tableName = rs.getString("FKTABLE_SCHEM") + "." +
              rs.getString("FKTABLE_NAME");
          String mapKey = rs.getString("FK_NAME");
          Object ob = fk_Keys.get(mapKey);
          if (ob == null) {
            HashMap colsMap = new HashMap();
            colsMap.put(new Integer(rs.getInt("KEY_SEQ")),
                        rs.getString("FKCOLUMN_NAME"));
            fk_pk.put(mapKey, primObject);
            fk_Keys.put(mapKey, colsMap);
          }
          else {
            ( (HashMap) ob).put(new Integer(rs.getInt("KEY_SEQ")),
                                rs.getString("FKCOLUMN_NAME"));
          }
        }
        HashMap mapTemp = (HashMap) primObject;
        Iterator itr = mapTemp.values().iterator();
        Object colNames = null;
        do {
          if (colNames != null) {
            colNames = itr.next() + "," + colNames;
          }
          else {
            colNames = itr.next();
          }
        }
        while (itr.hasNext());
        referenceString.put(mapPkKey, pk_tableName + " ( " + colNames + " )");
      }
      while (rs.next());
      rs.close();
    }
    catch (SQLException ex) {
      throw new RepException("REP006", new Object[] {
                             ex.getMessage()});
    }

    Object[] fkeys = fk_Keys.keySet().toArray();
    Object[] pkeys = referenceString.keySet().toArray();
    for (int i = 0; i < fkeys.length; i++) {

      alterTableQuery.append("alter table ")
          .append(schemaName + "." + tableName).append(" add constraint ")
          .append(fkeys[i]);
      HashMap map = (HashMap) fk_Keys.get(fkeys[i]);
      Object[] indexes = map.keySet().toArray();
      Arrays.sort(indexes);
      alterTableQuery.append(" Foreign Key (");
      for (int j = 0; j < indexes.length; j++) {
        if (j != 0) {
          alterTableQuery.append(",");
        }
        alterTableQuery.append(map.get(indexes[j]));
      }
      alterTableQuery.append(" ) References ");

//               for (int j = 0; j < pkeys.length; j++) {
      alterTableQuery.append(referenceString.get(pkeys[i])); //fk_pk.get(fkeys[i]));
//               }
//System.out.println("Alter table query is : " + alterTableQuery.toString());
      foreignKeyConstraintsList.add(alterTableQuery.toString());
      alterTableQuery.setLength(0);

    }
    return foreignKeyConstraintsList;
  }

  public void appendUniqueKeyConstraints(String schemaName, String tableName,
                                         StringBuffer cols) throws RepException {

    HashMap consPkTableMap = new HashMap();
    HashMap uniqueCol = new HashMap();
    ArrayList primaryKeyColumns = new ArrayList();
    ResultSet rs = null, rsPk = null;

    try {
      rsPk = dbmd.getPrimaryKeys(null, schemaName, tableName);
      while (rsPk.next()) {
        primaryKeyColumns.add(rsPk.getString("PK_NAME"));
      }
      rs = dbmd.getExportedKeys(null, schemaName, tableName);
      boolean next = rs.next();
      if (!next) {
        return;
      }
      do {
        String pk_tableName = rs.getString("PKTABLE_SCHEM") + "." +
            rs.getString("PKTABLE_NAME");
//System.out.println("pk_tableName" + rs.getString("PKTABLE_NAME"));
//System.out.println("KEY_SEQ::::" + rs.getShort("KEY_SEQ"));
//System.out.println("PKCOLUMN_NAME " + pk_tableName + "(" +rs.getString("PKCOLUMN_NAME") + ")");
        String mapPkKey = rs.getString("PK_NAME");
        if (!primaryKeyColumns.contains(mapPkKey)) {
          Object primObjectTemp = consPkTableMap.get(mapPkKey);
          if (primObjectTemp == null) {
            HashMap colsPkMap = new HashMap();
            colsPkMap.put(new Integer(rs.getInt("KEY_SEQ")),
                          rs.getString("PKCOLUMN_NAME"));

            consPkTableMap.put(mapPkKey, colsPkMap);
          }
          else {
            ( (HashMap) primObjectTemp).put(new Integer(rs.getInt("KEY_SEQ")),
                                            rs.getString("PKCOLUMN_NAME"));
          }
          Object primObject = consPkTableMap.get(mapPkKey);
          HashMap mapTemp = (HashMap) primObject;
          Iterator itr = mapTemp.values().iterator();
          Object colNames = null;
          do {
            if (colNames != null) {
              colNames = itr.next() + "," + colNames;
            }
            else {
              colNames = itr.next();
            }
          }
          while (itr.hasNext());
          uniqueCol.put(mapPkKey, " Unique " + " ( " + colNames + " )");
        }
      }
      while (rs.next());

    }
    catch (SQLException ex) {
      throw new RepException("REP006", new Object[] {
                             ex.getMessage()});
    }
    finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (rsPk != null) {
          rsPk.close();
        }
      }
      catch (SQLException ex1) {
      }
    }
// System.out.println("unique cols::" + uniqueCol);
    Object[] ukeys = uniqueCol.keySet().toArray();
    for (int j = 0; j < ukeys.length; j++) {
      cols.append("," + uniqueCol.get(ukeys[j]));
// System.out.println("Ukeys::::" + cols.toString());
    }
  }

  public ArrayList getExportedTableCols(SchemaQualifiedName repTable) throws
      RepException {
    ResultSet rs = null;
    ArrayList exportedColumns = new ArrayList();
    try {
      rs = dbmd.getExportedKeys(null, repTable.getSchemaName(),
                                repTable.getTableName());
      boolean next = rs.next();
      if (!next) {
        return null;
      }
      do {
        exportedColumns.add(rs.getString("PKCOLUMN_NAME"));
        // System.out.println("Ukeys::::" + cols.toString());
      }
      while (rs.next());
    }
    catch (SQLException ex) {
      throw new RepException("REP006", new Object[] {
                             ex.getMessage()});
    }
    finally {
      try {
        if (rs != null) {
          rs.close();
        }
      }
      catch (SQLException ex1) {
      }
    }
    return exportedColumns;
  }

  public void setAllColumns(RepTable repTable, String schemaName,
                                 String tableName) throws RepException, SQLException {

         ResultSet rs = dbmd.getColumns(null, schemaName, tableName, "%");
         if (rs == null || !rs.next()) {
           log.debug("Resultset  found  " + rs + " OR Resultset found false");
           throw new RepException("REP033",
                                  new Object[] {schemaName + "." + tableName});
         }
         ArrayList allColumnsList=new ArrayList();
         try {
           do {
             allColumnsList.add(rs.getString("COLUMN_NAME"));
           }
           while (rs.next());

         }
         finally {
           if (rs != null)
             rs.close();
         }
//        for (int i = 0; i < allColumnsList.toArray().length; i++) {
// System.out.println(" allColumnsList.toArray() : "+allColumnsList.toArray()[i].getClass());
//         }
         int numberOfColumns =allColumnsList.size();
         String columns[] =new String[numberOfColumns];
         for (int i = 0; i < numberOfColumns; i++) {
           columns[i] = (String)allColumnsList.get(i);
         }
         repTable.setAllColumns(columns);
       }


}
