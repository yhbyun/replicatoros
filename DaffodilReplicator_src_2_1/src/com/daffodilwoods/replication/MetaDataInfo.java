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
import com.daffodilwoods.replication.zip.ImportedTablesInfo;
import com.daffodilwoods.graph.DirectedGraph;
import org.apache.log4j.Logger;

/**
 * This Abstract class gets the connection object of the specified publisher or subscriber
 * and then stores it's metadata information in the DataBaseMetaData object(dbmd).
 * This inforamtion is used for performing different operations on the tables of
 * the publication or subscription and on the columns and constraint.
 *
 */

public abstract class MetaDataInfo
{
    protected DatabaseMetaData dbmd;
    protected ArrayList notNullColumns;
    protected static Logger log =Logger.getLogger(MetaDataInfo.class.getName());
    public MetaDataInfo() throws RepException
    {}

    public MetaDataInfo(ConnectionPool connectionPool0, String pubsubName) throws
        RepException
    {
        this(connectionPool0.getConnection(pubsubName));
    }

    public MetaDataInfo(Connection connection0) throws RepException
    {
        try
        {
            dbmd = connection0.getMetaData();
        }
        catch (SQLException ex)
        {
          log.error(ex.getMessage(),ex);
            throw new RepException("REP101", new Object[]
                                   {ex.getMessage()});
        }
    }

    /**
     * Checks the existance of the pericular table in the database.
     * @param sname
     * @throws RepException
     */

    public void checkTableExistance(SchemaQualifiedName sname) throws
        RepException
    {
        String schema = sname.getSchemaName();
        String table = sname.getTableName();
        //ResultSet rs2 = dbmd.getTables(null,"%","%",new String[] {"TABLE"});
        //ResultSet rs3 = dbmd.getTables(null,"%",table.toUpperCase(),new String[] {"TABLE"}); // Oracle
        //showResultSet(rs3);
        //ResultSet rs = dbmd.getTables(null,schema.toUpperCase(),table.toUpperCase(),new String[] {"TABLE"}); // Oracle
    try {
      ResultSet rs = dbmd.getTables(null, schema, table, new String[] {
          "TABLE"}); // SQL Server Oracle
      try {
        if (rs == null | !rs.next()) {
          throw new RepException("REP017", new Object[] {
              sname.toString()});
        }
        sname.setSchemaName(this, rs.getString("TABLE_SCHEM"));
        if (rs.next()) {
          throw new RepException("REP018", new Object[] {
              table});
        }
      }
      finally {
        if(rs != null)
          rs.close();
            }
        }
        catch (RepException ex)
        {
           log.error(ex.getMessage(),ex);
            throw ex;
        }
        catch (SQLException ex)
        {
           log.error(ex.getMessage(),ex);
            throw new RepException("REP006", new Object[]
                                   {ex.getMessage()});
        }
    }



    /**
     * Check the sequence of table according to primary key.
     * If table are not sorted  order then throw the error
     * It checks that if there is a table with any primary key and some
     * foreign keys then all the tables of foreign key columns are also
     * included in the publisher or not.
     *
     * @param schemaName String
     * @param tableName String
     * @param tableList ArrayList
     * @throws RepException
     */
    public void checkTableSequenceAccordingForeignKey(String schemaName,String tableName, ArrayList tableList) throws RepException
    {
        try
        {
            ResultSet rs2 = dbmd.getExportedKeys(null, schemaName, tableName);
     try {
        if (rs2 != null && rs2.next()) {
          do {
            String primaryTable = rs2.getString("PKTABLE_SCHEM") + "." + rs2.getString("PKTABLE_NAME");
            String foreignTable = rs2.getString("FKTABLE_SCHEM") + "." + rs2.getString("FKTABLE_NAME");
            if (tableList.contains(foreignTable.toUpperCase())) {
              throw new RepException("REP015", new Object[] {primaryTable, foreignTable});
                    }
                }
                while (rs2.next());
            }
        }
      finally {
        rs2.close();
      }
 }
        catch (RepException ex)
        {
           log.error(ex.getMessage(),ex);
            throw ex;
        }
        catch (SQLException ex)
        {
           log.error(ex.getMessage(),ex);
            throw new RepException("REP006", new Object[]
                                   {ex.getMessage()});
        }
    }

    /**
     * For each column, its dataype is converted in respective  database datatype
     * format in typeinfo then it is set in the dbh, passed to Class ColumnsInfo
     * and it's objects are stored in colInfoList.
     *
     * @param dbh
     * @param schemaName
     * @param tableName
     * @return
     * @throws RepException
     * @throws SQLException
     */

    public ArrayList getColumnDataTypeInfo(AbstractDataBaseHandler dbh,String schemaName, String tableName) throws  RepException, SQLException
    {
        ArrayList colInfoList = new ArrayList();
        ResultSet rs = dbmd.getColumns(null, schemaName, tableName, "%");
        if (rs == null || !rs.next())
        {
            throw new RepException("Internal Error", null);
        }
    try {
       do {
            TypeInfo typeInfo = new TypeInfo(dbh.updateDataType(rs.getString("TYPE_NAME")), rs.getInt("DATA_TYPE"));
            int columnPrecision = rs.getInt("COLUMN_SIZE");
             int columnScale=rs.getInt("DECIMAL_DIGITS");
            String typeName = rs.getString("TYPE_NAME").trim();
            columnPrecision = dbh.getAppropriatePrecision(columnPrecision, typeName);
            columnScale=dbh.getAppropriateScale(columnScale);
            typeInfo.setOptionalSizeProperty(dbh.isDataTypeOptionalSizeSupported(typeInfo));
            typeInfo.setColumnSize(rs.getInt("COLUMN_SIZE"));
            typeInfo.setColumnScale(columnScale);
            dbh.setTypeInfo(typeInfo, rs);
            //colInfoMap.put(rs.getString("COLUMN_NAME").trim(),typeInfo.getTypeDeclaration(rs.getInt("COLUMN_SIZE")));
            colInfoList.add(new ColumnsInfo(rs.getString("COLUMN_NAME").trim(), typeInfo.getTypeDeclaration(columnPrecision)));
        }
        while (rs.next());
     }
     finally {
       if(rs != null)
         rs.close();
     }


        return colInfoList;
    }

    /*public HashMap getColumnDataTypeInfo(AbstractDataBaseHandler dbh,String schemaName, String tableName) throws RepException,SQLException {
       HashMap colInfoMap = new HashMap();
       ResultSet rs = dbmd.getColumns(null,schemaName,tableName,"%");
       if(rs == null || !rs.next())
         throw new RepException("Internal Error",null);
       int i = 1;
       do {
         TypeInfo typeInfo = new TypeInfo(
     dbh.updateDataType(rs.getString("TYPE_NAME")) ,rs.getInt("DATA_TYPE"));
     typeInfo.setOptionalSizeProperty(dbh.isDataTypeOptionalSizeSupported(typeInfo));
         //colInfoMap.put(rs.getString("COLUMN_NAME").trim(),typeInfo.getTypeDeclaration(rs.getInt("COLUMN_SIZE")));
         colInfoMap.put(new Integer(i++), new ColumnsInfo( rs.getString("COLUMN_NAME").trim(),typeInfo.getTypeDeclaration(rs.getInt("COLUMN_SIZE")) ));
//        System.out.println(" VALUE " + rs.getString("COLUMN_NAME"));
       }while(rs.next());
       return colInfoMap;
        }*/

    public HashMap getColumnsInfo(String schemaName, String tableName) throws RepException, SQLException
    {
        HashMap colInfoMap = new HashMap();
        ResultSet rs = dbmd.getColumns(null, schemaName, tableName, "%");
        if (rs == null || !rs.next())
        {
            throw new RepException("Internal Error", null);
        }
        int i = 0;
  try {
      do {
            //colInfoMap.put(rs.getString("COLUMN_NAME").trim(),"");
        colInfoMap.put(new Integer(i++), new ColumnsInfo(rs.getString("COLUMN_NAME").trim(), null));
        }
        while (rs.next());

    }
    finally {
      if(rs != null)
        rs.close();
    }

        return colInfoMap;
    }

    /*public String generateQueryForLocalShadowTable(AbstractDataBaseHandler dbh,String schemaName, String tableName,ArrayList ciList, int vendorType) throws SQLException {
       StringBuffer sb = new StringBuffer();
       //System.out.println(" dbh " + dbh.getClass());
       ResultSet rs = dbmd.getColumns(null,schemaName,tableName,"%");
       if(rs!=null && rs.next()) {
          do {
             TypeInfo typeInfo = new TypeInfo(rs.getString("TYPE_NAME"),
                   rs.getInt("DATA_TYPE"));
             int size = rs.getInt("COLUMN_SIZE");
             String columnName = rs.getString("COLUMN_NAME");
     typeInfo.setOptionalSizeProperty(dbh.isDataTypeOptionalSizeSupported(typeInfo));
     sb.append(columnName).append(" ").append(typeInfo.getTypeDeclaration(size));

             switch (vendorType) {
                case Utility.DataBase_SqlServer:
                   ciList.add(new ColumnsInfo(columnName,typeInfo,size));
                   break;
                case Utility.DataBase_DaffodilDB:
                case Utility.DataBase_Oracle:
                   ciList.add(columnName);
                default:
             }
             sb.append(" , ");
          }while(rs.next());
       }
       //System.out.println(" sb " + sb.toString());
       return sb.toString();
        }*/

    /**
     * Generating a query ----
     * [  columnName dataType[(size)] [default] [not null] ,
     *  [ columnName dataType[(size)] [default] [not null] ,...]]
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
        RepException
    {
        StringBuffer sb = new StringBuffer();
        AbstractDataBaseHandler remotedbh = Utility.getDatabaseHandler(
            tgtVendorType);
        AbstractDataBaseHandler optSizedbh = (srcVendorType == tgtVendorType) ?
            dbh : remotedbh;
        try
        {
            //ResultSet rs2 = dbmd.getColumns(null,schemaName, tableName,"%");
            //showResultSet(rs2);
            ResultSet rs = dbmd.getColumns(null, schemaName, tableName, "%");
            if (rs == null || !rs.next())
            {
                throw new RepException("REP033",
                                       new Object[]
                                       {schemaName + "." + tableName});
            }
            do
            {
                String typeName = dbh.updateDataType(rs.getString("TYPE_NAME"));
                TypeInfo typeInfo = new TypeInfo(typeName, rs.getInt("DATA_TYPE"));
                int columnPrecision = rs.getInt("COLUMN_SIZE");
                String columnName = rs.getString("COLUMN_NAME");
                columnPrecision = optSizedbh.getAppropriatePrecision(columnPrecision,
                    typeName);
                typeInfo.setColumnSize(rs.getInt("COLUMN_SIZE"));
                int columnScale=rs.getInt("DECIMAL_DIGITS");
                columnScale=optSizedbh.getAppropriateScale(columnScale);
                int columnScalePublisher=dbh.getAppropriateScale(columnScale);
                if(columnScale > columnScalePublisher){
                    typeInfo.setColumnScale(columnScalePublisher);
                }else{
                  typeInfo.setColumnScale(columnScale);
                }
                remotedbh.setTypeInfo(typeInfo, rs);
                typeInfo.setOptionalSizeProperty(optSizedbh.
                                                 isDataTypeOptionalSizeSupported(
                    typeInfo));
                sb.append(columnName).append(" ").append(typeInfo.getTypeDeclaration(
                    columnPrecision));
                if (srcVendorType == tgtVendorType)
                {
                    typeInfo.setTypeName(typeName);
                    //System.out.println(" type Name === " + typeName + " sql Type " + sqlType ) ;
            String defaultValue = rs.getString("COLUMN_DEF"); //String => default value (may be <code>null</code>)
            if(defaultValue != null && !defaultValue.equalsIgnoreCase("NULL"))
              sb.append("DEFAULT").append(" ").append(defaultValue);
                }
                String nullable = rs.getString("IS_NULLABLE").trim(); // String => "NO" means column definitely does not allow NULL values; "YES" means the column might allow NULL values.  An empty string means nobody knows.
                if (nullable.equalsIgnoreCase("NO"))
                {
                    if (notNullColumns == null)
                    {
                        notNullColumns = new ArrayList();
                    }
                    notNullColumns.add(columnName);
                }
                //new ColumnsInfo(columnName,typeName,sqlType,columnSize/*,defaultValue,nullable*/);
                sb.append(" , ");
            }
            while (rs.next());
        }
        catch (RepException ex)
        {
           log.error(ex.getMessage(),ex);
            throw ex;
        }
        catch (SQLException ex)
        {
           log.error(ex.getMessage(),ex);
            throw new RepException("REP006", new Object[]
                                   {ex.getMessage()});
        }
        return sb.toString();
    }

    /**
     * Gets the sequence of different constraint along with datatype in the
     * query formate.
     *
     * @param schemaName
     * @param tableName
     * @param primConsMap
     * @return
     * @throws RepException
     */

    public String getAppliedConstraints(String schemaName, String tableName,
                                        TreeMap primConsMap) throws RepException
    {
        StringBuffer cons = new StringBuffer();
        appendPrimaryConstraints(schemaName, tableName, cons, primConsMap);
        appendUniqueKeyConstraints(schemaName, tableName, cons);
//    appendForeignConstraints(schemaName, tableName, cons, primConsMap);
        appendCheckConstraints(cons);
        //System.out.println(" String Returned ===== " + cons.toString());
        return cons.toString();
    }

    public String getAppliedConstraintsForExistingTable(String schemaName,
        String tableName, TreeMap primConsMap, AbstractDataBaseHandler dbh) throws
        RepException
    {
        StringBuffer cons = new StringBuffer();
        appendPrimaryConstraints(schemaName, tableName, cons, primConsMap);
         appendUniqueKeyConstraints(schemaName, tableName, cons);
//        appendForeignConstraints(schemaName, tableName, cons, primConsMap);
        addCheckConstraintForExistingTable(schemaName, tableName, primConsMap, dbh);
        appendCheckConstraints(cons);
        return cons.toString();
    }

    public void addCheckConstraintForExistingTable(String schemaName,
        String tableName,
        TreeMap primConsMap,
        AbstractDataBaseHandler dbh) throws
        RepException
    {
        try
        {
            ResultSet rs = dbmd.getColumns(null, schemaName, tableName, "%");
            if (rs == null || !rs.next())
            {
                log.debug("Resultset  found  " + rs + " OR Resultset found false");
                throw new RepException("REP033",
                                       new Object[]
                                       {schemaName + "." + tableName});
            }
     try {
        do {
                String columnName = rs.getString("COLUMN_NAME");
                String nullable = rs.getString("IS_NULLABLE").trim();
                if (nullable.equalsIgnoreCase("NO"))
                {
                    if (notNullColumns == null)
                    {
                        notNullColumns = new ArrayList();
                    }
                    notNullColumns.add(columnName);
                }
            }
            while (rs.next());
        }
    finally {
        if(rs != null)
          rs.close();
      }

    }
        catch (RepException ex)
        {
           log.error(ex.getMessage(),ex);
            throw ex;
        }
        catch (SQLException ex)
        {
           log.error(ex.getMessage(),ex);
            throw new RepException("REP006", new Object[]
                                   {ex.getMessage()});
        }

    }

    /**
     * Gets the primary key sequence in the query format.
     *
     * @param schemaName
     * @param tableName
     * @param cols
     * @param consTableMap
     * @throws RepException
     */

    private void appendPrimaryConstraints(String schemaName, String tableName,
                                          StringBuffer cols, TreeMap consTableMap) throws
        RepException
    {
        HashMap primcolmap = null;
        try
        {
            ResultSet rs = dbmd.getPrimaryKeys(null, schemaName, tableName);
            if (rs == null || !rs.next())
            {
                log.debug("Resultset  found  " + rs + " OR Resultset found false");
                throw new RepException("REP034",
                                       new Object[]
                                       {schemaName + "." + tableName});
            }
            primcolmap = new HashMap();
     try {
        do {
          primcolmap.put(new Integer(rs.getInt("KEY_SEQ")), rs.getString("COLUMN_NAME"));
            }
            while (rs.next());
        }
     finally {
        if(rs != null)
          rs.close();
      }

    }
        catch (RepException ex)
        {
           log.error(ex.getMessage(),ex);
            throw ex;
        }
        catch (SQLException ex)
        {
           log.error(ex.getMessage(),ex);
            throw new RepException("REP006", new Object[]
                                   {ex.getMessage()});
        }
        Object[] indexes = primcolmap.keySet().toArray();
        Arrays.sort(indexes);
        cols.append(" Primary Key (");
        StringBuffer temp = new StringBuffer();
        for (int i = 0; i < indexes.length; i++)
        {
            if (i != 0)
            {
                temp.append(",");
            }
            temp.append(primcolmap.get(indexes[i]));
        }
        cols.append(temp.toString()).append(" )");
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


    /**
     * Gets foreign key sequence in query format.
     *
     * @param schemaName
     * @param tableName
     * @param cols
     * @param consTableMap
     * @throws RepException
     */

    private void appendForeignConstraints(String schemaName, String tableName,
                                          StringBuffer cols, TreeMap consTableMap) throws
        RepException
    {
        HashMap fk_Keys = new HashMap();
        HashMap fk_pk = new HashMap();
    try {
            ResultSet rs = dbmd.getImportedKeys(null, schemaName, tableName);
            if (rs == null || !rs.next())
            {
                log.debug("Resultset  found  " + rs + " OR Resultset found false");
                return;
            }
     try {
        do {
          String pk_tableName = rs.getString("PKTABLE_SCHEM") + "." + rs.getString("PKTABLE_NAME");
                Object primObject = consTableMap.get(pk_tableName);
          if (primObject != null) {
            String fk_tableName = rs.getString("FKTABLE_SCHEM") + "." + rs.getString("FKTABLE_NAME");
                    String mapKey = rs.getString("FK_NAME");
                    Object ob = fk_Keys.get(mapKey);
            if (ob == null) {
                        HashMap colsMap = new HashMap();
              colsMap.put(new Integer(rs.getInt("KEY_SEQ")), rs.getString("FKCOLUMN_NAME"));
                        fk_pk.put(mapKey, primObject);
                        fk_Keys.put(mapKey, colsMap);
                    }
            else {
               ( (HashMap) ob).put(new Integer(rs.getInt("KEY_SEQ")), rs.getString("FKCOLUMN_NAME"));
                    }
                }
            }
            while (rs.next());
        }
    finally {
        if(rs != null)
        rs.close();
      }

    }
        catch (SQLException ex)
        {
           log.error(ex.getMessage(),ex);
            throw new RepException("REP006", new Object[]
                                   {ex.getMessage()});
        }
        Object[] fkeys = fk_Keys.keySet().toArray();
        for (int i = 0; i < fkeys.length; i++)
        {
            HashMap map = (HashMap) fk_Keys.get(fkeys[i]);
            Object[] indexes = map.keySet().toArray();
            Arrays.sort(indexes);
            cols.append(" , Foreign Key (");
            for (int j = 0; j < indexes.length; j++)
            {
                if (j != 0)
                {
                    cols.append(",");
                }
                cols.append(map.get(indexes[j]));
            }
            cols.append(" ) References " + fk_pk.get(fkeys[i]));
        }
    }

    private void appendCheckConstraints(StringBuffer sb)
    {
        for (int i = 0, length = notNullColumns.size(); i < length; i++)
        {
            sb.append(" , Check ( ").append(notNullColumns.get(i)).append(
                " is not null ) ");
        }
        notNullColumns = null;
    }

  public void setPrimaryColumns(RepTable repTable, String schemaName, String tableName) throws RepException, SQLException {
        //ResultSet rs2 = dbmd.getPrimaryKeys(null,schemaName,tableName.toLowerCase());
        //Util.showResultSet(rs2);
        HashMap primcolmap = new HashMap();
        //ResultSet rs = dbmd.getPrimaryKeys(null,schemaName,tableName.toLowerCase()); // SQL Server / Daffodil DB
        ResultSet rs = dbmd.getPrimaryKeys(null, schemaName, tableName); // Oracle
        if (rs == null || !rs.next())
        {
            log.debug("Resultset  found  " + rs + " OR Resultset found false");
            throw new RepException("REP034", new Object[]
                                   {tableName});
        }

  try {
      do {
        primcolmap.put(new Integer(rs.getInt("KEY_SEQ")), rs.getString("COLUMN_NAME"));
        }
        while (rs.next());

    }
    finally {
      if(rs != null)
        rs.close();
    }

        String[] primColumns = new String[primcolmap.size()];
        Object[] indexes = primcolmap.keySet().toArray();
        Arrays.sort(indexes);
    for (int i = 0; i < indexes.length; i++) {
            primColumns[i] = (String) primcolmap.get(indexes[i]);
        }
        repTable.setPrimaryColumns(primColumns);
    }

  /** @todo On it When cyclic work is to be done */
  public void setForeignKeyColumns(RepTable repTable, String schemaName, String tableName) throws RepException, SQLException {
//System.out.println("tableName for foreignKey cols ::" + tableName);
    ArrayList foreignKeyColumnsList = new ArrayList();
     // Firebird database does not have schema. It give the null pointer exception for "schemaName.toUpperCase()"
//    ResultSet rs = dbmd.getImportedKeys(null, schemaName.toUpperCase(), tableName.toUpperCase());
      ResultSet rs = dbmd.getImportedKeys(null, schemaName, tableName);
    if (rs == null || !rs.next()) {
//System.out.println(" NO FOREIGN KEYS FOUND FOR Table ::" + tableName);
      return;
    }
   try{ do {
       String fkCol=rs.getString("FKCOLUMN_NAME");
//System.out.println(" FK_COL :: " + fkCol);
       if(!foreignKeyColumnsList.contains(fkCol))
       foreignKeyColumnsList.add(fkCol);
     }
     while (rs.next());
   }finally{
     rs.close();
   }
//System.out.println("FK Lists  " + foreignKeyColumnsList);
    if(foreignKeyColumnsList.size() > 0)
      repTable.setForeignKeyCols((String[])foreignKeyColumnsList.toArray(new String[0]));
  }

  public String getExistingTableQuery(AbstractDataBaseHandler dbh, SchemaQualifiedName sname, int pubVendorType) throws RepException, SQLException {
        StringBuffer sb = new StringBuffer();
        String table = sname.getTableName();
        String schema = sname.getSchemaName();
        sb.append(" Create Table ").append(sname.toString()).append(" ( ");
    ResultSet rs = dbmd.getColumns(null, schema.toLowerCase(), table.toLowerCase(), "%");
        //ResultSet rs = dbmd.getColumns(null,schema, table,"%");
        if (rs == null || !rs.next())
        {
            log.debug("Resultset  found  " + rs + " OR Resultset found false");
            throw new RepException("REP033", new Object[]
                                   {sname.toString()});
        }
    do {
            String typeName = dbh.updateDataType(rs.getString("TYPE_NAME"));
      TypeInfo typeInfo = new TypeInfo(typeName, rs.getInt("DATA_TYPE"));
            int size = rs.getInt("COLUMN_SIZE");
            String columnName = rs.getString("COLUMN_NAME");
            typeInfo.setColumnSize(rs.getInt("COLUMN_SIZE"));
            dbh.setTypeInfo(typeInfo, rs);
      typeInfo.setOptionalSizeProperty(dbh.isDataTypeOptionalSizeSupported(typeInfo));
            sb.append(columnName).append(" ").append(typeInfo.getTypeDeclaration(size));
            sb.append(" , ");
        }
        while (rs.next());
//      appendPrimaryConstraints(schema,table,sb,new HashMap());
    sb.append(" " + getAppliedConstraintsForExistingTable(schema, table, new TreeMap(String.CASE_INSENSITIVE_ORDER), dbh) + " ) ");
        return sb.toString();
    }

    /**
     * Return the hierarchy of tables based on parent child relationship.
     * Parent child relationship depend on Primary key and Foriegn Key
     * Relationship.It first add the parent table in array and after that
     * child tables.
     * @param tableNames String[]
     * @throws RepException
     * @return String[]
     */
  abstract protected String[] getTablesHierarchy(String[] tableNames) throws RepException;

    /**
     * Checks the given table sequence, whetehr the tables are in the sequence
     * of PK->FK or not.
     *
     * @param schemaName
     * @param tableName
     * @param tableList
     * @throws RepException
     */

  abstract protected ArrayList getTableSequenceRegardingForeignKey(String schemaName, String tableName, ArrayList tableList) throws RepException;

     /**
      * This method check that the parent table of given
      * table is included or not. If parent table not included
      * in list then it give the RepException because further
      * it create the problem in synchronization.
      * @param givenListOfTables ArrayList
      * @param schemaName String
      * @param tableName String
      * @throws RepException
      */
  abstract protected void checkParentTablesIncludedInList(ArrayList givenListOfTables, String schemaName, String tableName) throws RepException;

 public String getCatalogName(String identifier) {
      int catalog_index = identifier.indexOf('.');
    if (catalog_index != -1) {
             return  identifier.substring(0, catalog_index).toUpperCase();
          }
    else {
              return null;
          }
    }

    public String getSchemaName(String identifier) {
      int table_name_index = identifier.lastIndexOf('.');
      if (table_name_index != -1) {
        identifier = identifier.substring(0, table_name_index);
        int catalog_index = identifier.indexOf('.');
        if (catalog_index != -1) {
          return identifier.substring(catalog_index + 1).toUpperCase();
        }
        else {
        identifier = identifier.replaceAll("\"", "");
          return identifier.toUpperCase();
        }
      }
      else {
        return null;
      }

    }

     /**
      * Implemented to handle the casesensitveness of postgreSQL
      * for table and and Schema Name.Method is overrided in
      * pgMetaDataInfo.
      * @param identifier String
      * @return String
      */
    public  String getTableName(String schematable) {
    String table = null;
    int index = schematable.indexOf('.');
    if (index == -1) {
      if (schematable.lastIndexOf("\"") == -1) {
        table = schematable.substring(0, schematable.length());
        return table.toUpperCase();
      }
      else {
        table = schematable.substring(1, schematable.length() - 1);
        table = table.replaceAll("\"", "");
        return table.toUpperCase();
      }
    }
    else {
      if (schematable.lastIndexOf("\"") == -1) {
        table = schematable.substring(index + 1);
        table = table.replaceAll("\"", "");
        return table.toUpperCase();
      }
      else {

        table = schematable.substring(index + 2);
        table = table.replaceAll("\"", "");
        return table.toUpperCase();
      }
    }
  }

    /**
    * Only for debugging purpose
    * @param rs ResultSet
    * @throws SQLException
    */


    private static void showResultSet(ResultSet rs) throws SQLException {
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();
      Object[] displayColumn = new Object[columnCount];
      for (int i = 1; i <= columnCount; i++) {
        displayColumn[i - 1] = metaData.getColumnName(i);
  //     System.out.println( Arrays.asList(displayColumn) );
      }
      while (rs.next()) {
        Object[] columnValues = new Object[columnCount];
        for (int i = 1; i <= columnCount; i++) {
          columnValues[i - 1] = rs.getObject(i);
  //       System.out.println(Arrays.asList(columnValues) );
        }
      }
    }




  public Map getImportedTablesInfo(SchemaQualifiedName[] schemaQualifiedTableNames,DirectedGraph graph,String[] removeCycleTableNames) throws RepException{
    List schemaQualifiedNamesList = Arrays.asList(schemaQualifiedTableNames);
//    System.out.println("sent list :"+ schemaQualifiedNamesList);

    Map importedTablesMap = getImportedTables(schemaQualifiedNamesList,graph,removeCycleTableNames);
//    System.out.println("importedTablesMap.entryset()###"+importedTablesMap.entrySet().toString());
    Set importedTablesSet = importedTablesMap.entrySet();
//    System.out.println("importedTablesSet  :::"+importedTablesSet.toString());
    Map importedTablesInfoMap = new HashMap();
    int count = 0;
    for (Iterator iter = importedTablesSet.iterator(); iter.hasNext(); count++) {
      Map.Entry entry = (Map.Entry)iter.next();
      SchemaQualifiedName keyTable=(SchemaQualifiedName)entry.getKey();
      ArrayList superTablesList = (ArrayList)entry.getValue();
//      System.out.println("keyTable entry.getKey():::"+keyTable);
//      System.out.println("superTablesList entry.getValue():::"+superTablesList);
      boolean cyclic = false;
      ArrayList superTablesHierarcyList = new ArrayList();
      ArrayList currentTraversalList = new ArrayList();
      if(superTablesList!= null){
        superTablesHierarcyList.add(keyTable);
        currentTraversalList.add(keyTable);
//        System.out.println( count +". TABLE ::: ["+keyTable+"]");
//        System.out.println("superTablesList.size() "+superTablesList.size());
        for (int i = 0, size = superTablesList.size(); i < size; i++) {
          SchemaQualifiedName parentTable = (SchemaQualifiedName) superTablesList.get(i);
//          System.out.println("parentTable :::"+parentTable);
          superTablesHierarcyList.add(0,parentTable);
          currentTraversalList.add(0,parentTable);
          boolean cyclic1 = isCyclicRelationship(parentTable,superTablesHierarcyList,importedTablesMap,currentTraversalList);
          if (!cyclic)
            cyclic = cyclic1;
          int indexInCurrentTraversalList = currentTraversalList.indexOf(parentTable)  ;
//          System.out.println(" List before removing :: " + currentTraversalList);
          for (int k = 0; k <= indexInCurrentTraversalList; k++) {
            currentTraversalList.remove(0);
          }
//          System.out.println( indexInCurrentTraversalList+" List after removing :: " + currentTraversalList);
        }
        ImportedTablesInfo importedTablesInfo = new ImportedTablesInfo();
        superTablesHierarcyList.remove(keyTable);
        importedTablesInfo.setListOfAllAscendents(superTablesHierarcyList);
        importedTablesInfo.setListOfDirectAscendents(superTablesList);
        importedTablesInfo.setIsCyclic(cyclic);
        importedTablesInfoMap.put(keyTable,importedTablesInfo);
      }
//      System.out.println(keyTable + "ISCYCLIC ::" + cyclic);
//      System.out.println(keyTable+" LIST ::" + superTablesHierarcyList + "ISCYCLIC ::" + cyclic);
    }
    return importedTablesInfoMap;
  }
  /**
   * THIS METHOD PUT ALL THE TABLES(schemaQualifiedName) AND THEIR PARENT TABLES(innerList) IN mapForImportedTables
   * USING  mapForImportedTables.put(schemaQualifiedName,innerList);
   * @param schemaQualifiedTableNames List
   * @param graph JbDirectedGraph
   * @throws RepException
   * @return HashMap
   */
  private HashMap getImportedTables(List schemaQualifiedTableNames,DirectedGraph graph,String[] removeCycleTableNames)throws RepException{
      HashMap mapForImportedTables = new HashMap();
      for (int i = 0,size =  schemaQualifiedTableNames.size(); i < size; i++) {
         SchemaQualifiedName schemaQualifiedName = (SchemaQualifiedName)schemaQualifiedTableNames.get(i);
         ArrayList innerList=getImportedTables(schemaQualifiedName,schemaQualifiedTableNames,graph,removeCycleTableNames);
         mapForImportedTables.put(schemaQualifiedName,innerList);
      }
//      System.out.println("mapForImportedTables "+mapForImportedTables);
      return mapForImportedTables;
    }

//    private ArrayList getListForTable(String tableName)throws Exception{
//      if(mapForAllTables.size()==0){
//        mapForAllTables = getMapForAllTables();
//      }
//
//        return (ArrayList)mapForAllTables.get(tableName);
//    }


   private boolean isCyclicRelationship(SchemaQualifiedName parentTable,ArrayList superTablesHierarcyList,Map importedTablesMap,ArrayList currentTraversalList) throws RepException{
       ArrayList importedTablesList = (ArrayList)importedTablesMap.get(parentTable);
       if(importedTablesList != null){
//         System.out.println("parentTable   ::" + parentTable + " imported TAbles :: " + importedTablesList);
         boolean cyclic = false;
         for (int i =0,size = importedTablesList.size(); i < size ;i++) {
           SchemaQualifiedName nextLevelParent = (SchemaQualifiedName) importedTablesList.get(i);
//           System.out.println("calling to match again: mainTable, innerTable[" + parentTable + "], tableName[ " + nextLevelParent);
           Object initiator = currentTraversalList.get(currentTraversalList.size()-1);
//System.out.println("initiator:::::::::"+initiator);
           if (nextLevelParent.equals(initiator) ){
//System.out.println("####################################  Graph is cyclic ");
//             System.out.println("\t table in cycle "+ nextLevelParent +" CYCLIC FOR ::" + currentTraversalList);
             return true;
           }
           if(currentTraversalList.contains(parentTable)){
//             System.out.println("currentTraversalList.contains(parentTable)"+currentTraversalList.contains(parentTable));
             continue;
           }
           if(!superTablesHierarcyList.contains(nextLevelParent))
             superTablesHierarcyList.add(0,nextLevelParent);
           currentTraversalList.add(0,nextLevelParent);
           boolean cyclic1 = isCyclicRelationship(nextLevelParent, superTablesHierarcyList, importedTablesMap,currentTraversalList);
//           System.out.println("cyclic1::: "+cyclic1);
           int indexInCurrentTraversalList = currentTraversalList.indexOf(nextLevelParent);
//           System.out.println(" List before removing :: " + currentTraversalList);
           for (int k = 0; k <= indexInCurrentTraversalList; k++) {
             currentTraversalList.remove(0);
           }
//           System.out.println(indexInCurrentTraversalList +" List after removng :: "+  currentTraversalList);
//           System.out.println("nextLevelParent  :: " + nextLevelParent +" cyclic1::" + cyclic1 );
           if(!cyclic)
             cyclic = cyclic1;
         }
         return cyclic;
       }
       return false;
   }

 abstract public ArrayList getChildTables(String parentTable)throws RepException ;

 abstract public Object[] getImportedColsOfChildTable(String parentTable,String childTable)throws RepException ;


//add Edge to the graph
 abstract public ArrayList getImportedTables(SchemaQualifiedName
                                             schemaQualifiedName,
                                             List passedSchemaQualifiedNamesList,
                                             DirectedGraph graph,
                                             String[] removeCycleTableNames0) throws
     RepException;
//create alter query for table structures
 abstract public ArrayList getForiegnKeyConstraints(String schemaName,
     String tableName) throws RepException;
 abstract public void appendUniqueKeyConstraints(String schemaName, String tableName,
                                                 StringBuffer cols) throws RepException;

 abstract public ArrayList getExportedTableCols(SchemaQualifiedName repTableQualifiedtableName) throws RepException;

 public ArrayList getExportedTableColsList(SchemaQualifiedName repTableQualifiedtableName) throws RepException{
   return getExportedTableCols(repTableQualifiedtableName);
 }

abstract protected void checkChildTableIncludedInDropTableList(ArrayList pubRepTableList,String[] dropTableList) throws RepException ;

abstract public void setAllColumns(RepTable repTable, String schemaName, String tableName)throws RepException, SQLException  ;


}
