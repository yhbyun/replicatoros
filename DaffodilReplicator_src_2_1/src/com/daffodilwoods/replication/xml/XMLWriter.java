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

import java.io.*;
import java.sql.*;
import java.util.*;

import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.DBHandler.*;
import com.daffodilwoods.replication.column.*;

/**
 * This class is basically useful for writing data in to the XML file,
 * it implements _XMLWriter interface which forces it to implement the
 * method write. This method identifies the column datatype and then corresponding
 * columnObject. SO different column data are written in differnt manner. Here
 * special cases of Blob & Clob are also handled.
 */

public class XMLWriter implements _XMLWriter {

  HashMap columnObjects = new HashMap();

//  OutputStreamWriter os;
  Writer bw;
  AbstractDataBaseHandler dbDatatypeHandler;
  BlobOutPutStream bops;
  ClobOutPutStream cops;
  int noOfPrimaryCols;
  Connection pub_sub_connection = null;
  public XMLWriter(BufferedWriter bw0, AbstractDataBaseHandler dbDatatypeHandler0, Connection pub_sub_connection0) {
    bw = bw0;
    dbDatatypeHandler = dbDatatypeHandler0;
    pub_sub_connection = pub_sub_connection0;
    bops = new BlobOutPutStream(PathHandler.getBLobFilePathForClient());
    cops = new ClobOutPutStream(PathHandler.getCLobFilePathForClient());
  }

  public void write(ResultSet rows, int index, ArrayList encodedCols,
                    String col) throws SQLException, IOException, RepException {
    ResultSetMetaData rsmt = rows.getMetaData();
    int columnType = rsmt.getColumnType(index);
//  int columnPrecision=rsmt.getPrecision(index);
    TypeInfo typeInfo = new TypeInfo(rsmt.getColumnTypeName(index), columnType);
//  typeInfo.setColumnSize(columnPrecision);
    dbDatatypeHandler.setColumnPrecisionInTypeInfo(typeInfo, rsmt, index);
    dbDatatypeHandler.setTypeInfo(typeInfo, rows);
    AbstractColumnObject columnObject = getColumnObject(typeInfo);
    columnObject.setBlobHandlerObject(bops);
    columnObject.setClobHandlerObject(cops);
    columnObject.write(bw, rows, index, encodedCols, col);

  }

  private AbstractColumnObject getColumnObject(TypeInfo typeInfo) throws RepException {
    Integer key = new Integer(typeInfo.hashCode());
    AbstractColumnObject columnObject = (AbstractColumnObject) columnObjects.get(key);
    if (columnObject == null) {
      columnObject = dbDatatypeHandler.getColumnObject(typeInfo);
      columnObjects.put(key, columnObject);
    }
    return columnObject;
  }

  private void write(Writer os, Object value) throws IOException {
    os.write("<![CDATA[" + value.toString() + "]]>");
  }

  /*  public void writeRowElement(int noOfColumns, ResultSet rows_I,
                                ResultSetMetaData rsmt) throws SQLException,
        IOException, RepException {
      os.write("<row>");
      for (int c = 5; c <= noOfColumns - 1 - noOfPrimaryCols; c++) { // -1 for serverName and -noOfPrimaryCols for excluding old_Primary columns form shadow table
        String columnName = rsmt.getColumnName(c);
        os.write("<columnName name=\"" + columnName + "\">");
        write(rows_I, c);
        os.write("</columnName>\r\n");
      }
      os.write("</row>");
    } */

  public void writeRowElement(int noOfColumns, ResultSet rows_I,
                              ResultSetMetaData rsmt,
                              String[] primaryColumnNames,
                              Object[] primaryColValues, String tableName,
                              ArrayList encodedCols) throws SQLException,
      IOException, RepException {
    int columnIndex=1;
    bw.write("<row>");
    for (int c = 5; c <= noOfColumns - 2 - noOfPrimaryCols; c++) { // -1 for serverName and -noOfPrimaryCols for excluding old_Primary columns form shadow table
      String columnName = rsmt.getColumnName(c);
//System.out.println("columnName::" + columnName);
      if (!encodedCols.contains(columnName.toUpperCase())) {
        bw.write("<col name=\"" + "c"+columnIndex+ "\">");
      }
      else {
        bw.write("<col name=\"" + "c"+columnIndex+ "\"   Encode=\"Y\">");
      }
      columnIndex++;
      if (checkClobBLOB(rows_I, c)) {
        ResultSet rsClobBlob = getResultSetClobBlob(rows_I, primaryColumnNames,primaryColValues, tableName, c);
        write(rsClobBlob, 1, encodedCols, columnName);
        rsClobBlob.close();
      }
      else {
        write(rows_I, c, encodedCols, columnName);
      }
      bw.write("</col>\r\n");
    }
    bw.write("</row>");
  }

  public void writePrimaryKeyElement(String[] primaryColumnNames,
                                     Object[] primaryColValues,
                                     ArrayList encodedCols) throws IOException {
    bw.write("<primary>");
    for (int c = 0; c < primaryColumnNames.length; c++) {
      bw.write("<pk name=\"" + primaryColumnNames[c] + "\">");
      write(bw, primaryColValues[c]);
      bw.write("</pk>\r\n");
    }
    bw.write("</primary>");
  }

  public void writeRowElementForUpdate(int noOfColumns, ResultSet rows,
                                       ResultSetMetaData rsmt,
                                       ResultSet oldResultSet,
                                       String[] primaryColumnNames,
                                       Object[] primaryColValues,
                                       String tableName, ArrayList encodedCols) throws
      RepException, SQLException, IOException {
    HashMap updatedColumns = new HashMap();

    bw.write("<row>");

    AbstractColumnObject columnObject;
    int columnType, colPrecision = -1,columnIndex=1;
    String columnName;

    for (int c = 5; c <= noOfColumns - 2 - noOfPrimaryCols; c++) { // -1 for serverName and -noOfPrimaryCols for excluding old_Primary columns form shadow table
      columnName = rsmt.getColumnName(c);
      if (!encodedCols.contains(columnName.toUpperCase())) {
        bw.write("<col name=\"" +"c"+columnIndex+ "\">");
      }
      else {
        bw.write("<col name=\"" + "c"+columnIndex+ "\"   Encode=\"Y\">");
      }
      columnIndex++;
//    bw.write("<columnName name=\"" + columnName + "\">");

      columnType = rsmt.getColumnType(c);
//    colPrecision=rsmt.getPrecision(c);
      TypeInfo typeInfo = new TypeInfo(rsmt.getColumnTypeName(c), columnType);
      dbDatatypeHandler.setTypeInfo(typeInfo, rows);
      dbDatatypeHandler.setColumnPrecisionInTypeInfo(typeInfo, rsmt, c);
//    typeInfo.setColumnSize(colPrecision);
      columnObject = getColumnObject(typeInfo);
      columnObject.setBlobHandlerObject(bops);
      columnObject.setClobHandlerObject(cops);
      if (checkClobBLOB(rows, c)) {
        ResultSet rsClobBlob = getResultSetClobBlob(rows, primaryColumnNames,primaryColValues, tableName, c);
        columnObject.writeUpdate(bw, rsClobBlob, oldResultSet, 1,updatedColumns, columnName, encodedCols);
        rsClobBlob.close();
      }
      else {
        columnObject.writeUpdate(bw, rows, oldResultSet, c, updatedColumns,columnName, encodedCols);
      }
      bw.write("</col>\r\n");
    }
    bw.write("</row>");

    Iterator iterator = updatedColumns.keySet().iterator();
    bw.write("<ChangedCol>");
    if (!iterator.hasNext()) {
      bw.write("NO_OPERATION");
      bw.write("</ChangedCol>");
      return;
    }
    do {
      Object changesColumnName = iterator.next();
      bw.write("<changesCol name=\"" + changesColumnName.toString() +"\">");
      bw.write("<![CDATA[");
      Object ob = updatedColumns.get(changesColumnName);
      if (ob instanceof byte[]) {
        bw.write(new String( (byte[]) ob));
      }
      else {
        bw.write(ob.toString());
      }
      bw.write("]]></changesCol>");
    }
    while (iterator.hasNext());
    bw.write("</ChangedCol>");

  }

  public void setNoOFPrimaryColumnNumber(int number) {
    noOfPrimaryCols = number;
  }

  private ResultSet getResultSetClobBlob(ResultSet rs, String[] primaryColNames, Object[] primaryColValues,
                                         String tableName, int index) throws SQLException {

    ResultSetMetaData rsmd = rs.getMetaData();
    String columnName = rsmd.getColumnName(index);
    StringBuffer query = new StringBuffer();
    query.append("Select ");
    query.append(columnName);
    query.append("  from  ");
    query.append(tableName);
    query.append(" where ");
    for (int i = 0; i < primaryColNames.length; i++) {
      if (i != 0) {
        query.append(" and ");
      }
      query.append(primaryColNames[i]);
      query.append("= ?");
    }

    PreparedStatement ps = pub_sub_connection.prepareStatement(query.toString());

    ResultSet rss = null;
    for (int i = 0; i < primaryColValues.length; i++) {
      ps.setObject(i + 1, primaryColValues[i]);
    }
    rss = ps.executeQuery();

    rss.next();
    return rss;
  }

  private boolean checkClobBLOB(ResultSet rs, int index) throws SQLException {
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnType = rsmd.getColumnType(index);
    boolean flag = false;
    if (columnType == 2004 || columnType == 2005 || columnType == -4 ||
        columnType == -1 || columnType == -2 || columnType == -3 ||
        columnType == 1111) {
      flag = true;
    }
    return flag;

  }

  /*   public void writeEncoderDecoder(ResultSet rows, int index) throws SQLException, IOException, RepException
              {
                ResultSetMetaData rsmt = rows.getMetaData();
                int columnType = rsmt.getColumnType(index);
            //        int columnPrecision=rsmt.getPrecision(index);
   TypeInfo typeInfo = new TypeInfo(rsmt.getColumnTypeName(index), columnType);
            //       typeInfo.setColumnSize(columnPrecision);
   dbDatatypeHandler.setColumnPrecisionInTypeInfo(typeInfo, rsmt, index);
                dbDatatypeHandler.setTypeInfo(typeInfo, rows);
                AbstractColumnObject columnObject = getColumnObject(typeInfo);
                columnObject.setBlobHandlerObject(bops);
                columnObject.setClobHandlerObject(cops);
                columnObject.writeEncodedRecords(bw, rows, index);

           }*/

  public void writePrimaryKeyElement(String[] primaryColumnNames,
                                     ResultSet rs, ArrayList encodedCols) throws
      IOException, SQLException {
    bw.write("<primary>");
    for (int c = 0; c < primaryColumnNames.length; c++) {
      bw.write("<pk name=\"" + primaryColumnNames[c] + "\">");
      write(bw, rs.getObject(primaryColumnNames[c]));
      bw.write("</pk>\r\n");
    }
    bw.write("</primary>");
  }

}
