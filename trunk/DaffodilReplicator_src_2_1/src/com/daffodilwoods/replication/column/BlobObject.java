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

package com.daffodilwoods.replication.column;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.xml.*;
import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;
import com.daffodilwoods.replication.DBHandler.SQLServerHandler;
import com.daffodilwoods.replication.DBHandler.PostgreSQLHandler;

public class BlobObject extends AbstractColumnObject
{

    int sqlType;
    AbstractDataBaseHandler abstractDBHandler;
    /**
     * sets the SQL type for Blob datatype
     * @param sqlType0
     */
    public BlobObject(int sqlType0,AbstractDataBaseHandler abstractDBHandler0)
    {
        sqlType = sqlType0;
        abstractDBHandler =abstractDBHandler0;
    }

    /**
     * set the value for the corresponding datatype i.e BLob
     * @param pst
     * @param element
     * @param index
     * @throws SQLException
     */
    public void setColumnObject(PreparedStatement pst, XMLElement element,
                                int index) throws SQLException
    {
      XMLElement[] elements = (XMLElement[]) element.getChildElements().toArray(new
            XMLElement[2]);
        int start = 0;
        int length = 0;
        try
        {
            start = Integer.parseInt(elements[0].elementValue);
            length = Integer.parseInt(elements[1].elementValue);
//System.out.println(" start :: "+start+" length : "+length);
        }
        catch (NullPointerException ex)
        {
            String value = element.elementValue;
            int lenghtIndex = value.indexOf("length");
            int startIndex = value.indexOf("start");
            if (lenghtIndex == -1 && startIndex == -1)
            {
                if (value.equalsIgnoreCase("NULL"))
                {
                    pst.setNull(index, Types.BLOB);
                    return;
                }
                else
                {
                    pst.setObject(index, value);
                    return;
                }
            }
            start = Integer.parseInt(value.substring(5, lenghtIndex));
            length = Integer.parseInt(value.substring(lenghtIndex + 6));
        }

       if (length == -1)
        {
//System.out.println(" length  : "+length+" So set the null ");
            if(abstractDBHandler instanceof SQLServerHandler) {
              RBlob rblob = new RBlob(start, length);
              InputStream is = rblob.getBinaryStream();
              try {
                pst.setBinaryStream(index, is, is.available());
              }
              catch (IOException ex1) {
                RepConstants.writeERROR_FILE(ex1);
              }
            } else if( abstractDBHandler instanceof PostgreSQLHandler) {
              pst.setBlob(index, new RBlob(start, length));
            }
            else  {
              pst.setNull(index, Types.BLOB);
            }
        }
        else
        {

           RBlob rblob = new RBlob(start,length);
           InputStream is = rblob.getBinaryStream() ;
            try {
              pst.setBinaryStream(index, is, is.available());
            }
            catch (IOException ex1) {
              RepConstants.writeERROR_FILE(ex1);
            }
            // Set blob does not work in oracle database
//            pst.setBlob(index, new RBlob(start, length));
        }
    }

    public void setColumnObject(PreparedStatement pst, String value, int index) throws SQLException
    {
        int start = 0;
        int length = 0;
        int lenghtIndex = value.indexOf("length");
        int startIndex = value.indexOf("start");
        if (lenghtIndex == -1 && startIndex == -1)
        {
            if (value.equalsIgnoreCase("NULL"))
            {
                pst.setNull(index, Types.BLOB);
                return;
            }
            else
            {
                pst.setObject(index, value);
                return;
            }
        }
        start = Integer.parseInt(value.substring(5, lenghtIndex));
        length = Integer.parseInt(value.substring(lenghtIndex + 6));
//System.out.println("start :  "+start+" length : "+length);
        if (length == -1)
        {
            pst.setNull(index, Types.BLOB);
        }
        else
        {
          RBlob rblob = new RBlob(start,length);
          InputStream is = rblob.getBinaryStream() ;
           try {
             pst.setBinaryStream(index, is, is.available());
           }
           catch (IOException ex1) {
             RepConstants.writeERROR_FILE(ex1);
           }
//              pst.setBlob(index, new RBlob(start, length));
        }
    }

    /**
     * writes a blob values in XML file
     * @param os
     * @param rs
     * @param index
     * @throws IOException
     * @throws SQLException
     */

    public void write(Writer bw, ResultSet rs, int index,ArrayList encodedCols,String col) throws
        SQLException, IOException
    {
        Object objrowValue = getObject(rs, index);
        InputStream rowValue;
        if (objrowValue == null)
        {
            rowValue = null;
        }
        else
        {
            rowValue = (InputStream) objrowValue;
            /*
             System.out.println("<--------------------PRINT------------------------------->");

                  int len = rowValue.available();
                  System.out.println(" Length of data in Client ="+len);
                  byte[] bClient=new byte[len];
                  int noOfByteRead1=rowValue.read(bClient);
                  System.out.println("noOfByteRead1 "+noOfByteRead1+" BlobObject  = "+new String(bClient));
             System.out.println("<--------------------PRINT------------------------------->");
             */
//         new ByteArrayInputStream((objrowValue.toString()).getBytes());
        }
        bw.write("<start>");
        bw.write("" + blobst.getStreamStart());
        bw.write("</start>");
//RepPrinter.print(" Bfter Writing the Blob to file " + blobst);
        bw.write("<length>");
        if (rowValue != null)
        {
            bw.write("" + blobst.write(rowValue));
        }
        else
        {
            bw.write("-1");
        }
//RepPrinter.print(" After Writing the Blob to file " + blobst);
        bw.write("</length>");
    }

    /**
     * puts the value for the column aginst the column Name
     * @param os
     * @param rows
     * @param oldResultSet
     * @param index
     * @param modifiedColumns
     * @param columnName
     * @throws SQLException
     * @throws IOException
     */

    public void writeUpdate(Writer bw, ResultSet rows,
                            ResultSet oldResultSet
                            , int index, HashMap modifiedColumns,
                            String columnName,ArrayList encodedCols) throws SQLException, IOException
    {
        InputStream obj1 = null;

        try
        {
            obj1 =  rows.getBinaryStream(index);
            if (obj1 == null)
            {
                write(bw, null,encodedCols,columnName);
            }
            else
            {
                String data = getBlobNotNullData(obj1);
                write(bw, data,encodedCols,columnName);
                modifiedColumns.put(columnName, data);
            }
        }
        catch (Exception ex)
        {
//System.out.println(" ******************PROBLEM OCCURE OCCURE IN  BLOBOBJECT**************");
//      ex.printStackTrace();
//System.out.println(" ******************PROBLEM OCCURE OCCURE IN  BLOBOBJECT**************");
            RepConstants.writeERROR_FILE(ex);
        }

    }

    public void writeUpdateBKUP_11_17_04(OutputStreamWriter os, ResultSet rows,
                                         ResultSet oldResultSet
                                         , int index, HashMap modifiedColumns,
                                         String columnName) throws SQLException, IOException
    {
        Blob obj1 = null;
        Blob obj2 = null;
        try
        {
            obj1 = (Blob) rows.getObject(index);
            obj2 = (Blob) oldResultSet.getObject(index);

//       InputStream newObject = obj1.getBinaryStream();// new ByteArrayInputStream(newValue.getBytes());
//       InputStream oldObject = obj2.getBinaryStream();//new ByteArrayInputStream((obj2.toString()).getBytes());

            if (obj1 == null)
            {
//                write(os, null);
                if (obj2 != null)
                {
                    modifiedColumns.put(columnName, "NULL");
                }
            }
            else
            {
                String data = getBlobNotNullData(obj1.getBinaryStream());
//                write(os, data);
                if (obj2 != null)
                {
                    if (!checkInputStreamEquality(obj2.getBinaryStream(),
                                                  obj1.getBinaryStream()))
                    {
                        modifiedColumns.put(columnName, data);
                    }
                }
                else
                {
                    modifiedColumns.put(columnName, data);
                }

            }
        }
        catch (Exception ex)
        {
            RepConstants.writeERROR_FILE(ex);
        }

    }

    /* This method was previously written and running fine */
    public void writeUpdate1(OutputStreamWriter os, ResultSet rows,
                             ResultSet oldResultSet
                             , int index, HashMap modifiedColumns,
                             String columnName) throws SQLException, IOException
    {
        Blob newObject = rows.getBlob(index);
//RepPrinter.print(" Inside BlobObject newObject =" + newObject);
        Blob oldObject = oldResultSet.getBlob(index);
//RepPrinter.print(" Inside BlobObject oldObject =" + oldObject);
        if (newObject == null)
        {
//            write(os, "NULL");
            if (oldObject != null)
            {
                modifiedColumns.put(columnName, "NULL");
            }
        }
        else
        {
            String data = getBlobNotNullData(newObject);
//            write(os, data);
            if (oldObject != null)
            {
                if (!checkBlobEquality(oldObject, newObject))
                {
                    modifiedColumns.put(columnName, data);
                }
            }
            else
            {
                modifiedColumns.put(columnName, data);
            }

        }
    }

    /*public void writeUpdate1(OutputStreamWriter os, ResultSet rows, ResultSet oldResultSet
                            , int index, HashMap modifiedColumns, String columnName) throws SQLException, IOException{
      Blob newObject = rows.getBlob(index);
      Blob oldObject = oldResultSet.getBlob(index);

      if( newObject == null ){
        String data = getBlobNullData();
        write(os,data);
        if( oldObject != null )
          modifiedColumns.put( columnName ,data);
      }
      else {
        String data = getBlobNotNullData(newObject);
        write(os, data);
        try {
          if (oldObject != null) {
            if (!checkBlobEquality(oldObject,newObject)) {
              modifiedColumns.put(columnName, data);
            }
          }
          else {
            modifiedColumns.put(columnName, data);
          }
        }
        catch (Exception ex) {
          RepConstants.writeERROR_FILE(ex);
        }
      }
         }*/

    public Object getObject(String value)
    {
//RepPrinter.print("BlobObject.getObject(value)" + value);
        int start = 0, length = 0;
        try
        {
            int lenghtIndex = value.indexOf("length");
            start = Integer.parseInt(value.substring(5, lenghtIndex));
            length = Integer.parseInt(value.substring(lenghtIndex + 6));
        }
        catch (Exception ex)
        {
            RepConstants.writeERROR_FILE(ex);
        }
        return new RBlob(start, length);
    }

    /**
     * returns the Object corresponding to the index passed from the resultSet
     * @param row
     * @param index
     * @return
     * @throws SQLException
     */
    private Object getObject(ResultSet row, int index) throws SQLException
    {
//    Object object = row.getObject(index);
//RepPrinter.print("$$$$$$$$$$$$$$Inside BlobObject object is ="+object);
//    if (object == null) {
//      return null;
//    }
//    InputStream objInputStream = new ByteArrayInputStream( (object.toString()).
//        getBytes());
//    return objInputStream;
        return row.getBinaryStream(index);
    }

    /**
     * writes a blob values in XML file
     * @param os
     * @param rs
     * @param index
     * @throws IOException
     * @throws SQLException
     */
    private void write(Writer os, Object rowValue,ArrayList encodedCols,String col) throws
        SQLException, IOException
    {
        try
        {
            if(!encodedCols.contains(col.toUpperCase())) {
            os.write("<![CDATA[" + rowValue.toString() + "]]>");
            } else {
              os.write("<![CDATA[" +
                       EncoderDecoder.escapeUnicodeString1(rowValue.toString(), true) +
                       "]]>");
            }

        }
        catch (NullPointerException ex)
        {
            os.write("<start>");
            os.write("" + blobst.getStreamStart());
            os.write("</start><length>-1</length>");
        }
    }

    /*private void write(OutputStreamWriter os, String rowValue) throws SQLException, IOException {
      os.write(rowValue);
         }

         private String getBlobDataToWriteInXMLFile1(Object rowValue) {
      return rowValue != null ? getBlobNotNullData1(rowValue)
          : getBlobNullData1();
         }

         private String getBlobNullData1() {
      StringBuffer sb = new StringBuffer();
      sb.append("<start>").append(blobst.getStreamStart()).append("</start>")
          .append("<length>").append("-1").append("</length>");
      return sb.toString();
         }

         private String getBlobNotNullData1(Object rowValue) {
      StringBuffer sb = new StringBuffer();
      InputStream objInputStrea;
      if(rowValue==null)
        objInputStrea=null;
      else
     objInputStrea= new ByteArrayInputStream((rowValue.toString()).getBytes());
      sb.append("<start>").append(blobst.getStreamStart()).append("</start>")
          .append("<length>").append(blobst.write((InputStream)rowValue))
          .append("</length>");
      return sb.toString();
         }
     */
    private String getBlobNotNullData(Object rowValue)
    {
        StringBuffer sb = new StringBuffer();
        try
        {
//RepPrinter.print(" GET Blob Not null data " + rowValue.toString());
//InputStream inputStram =new ByteArrayInputStream((rowValue.toString()).getBytes());
            sb.append("start").append(blobst.getStreamStart())
                .append("length").append(blobst.write( (InputStream) rowValue));
//(InputStream)rowValue)
//RepPrinter.print("Inside BLOB Object  sb.toString() =" + sb.toString());
        }
        catch (Exception ex)
        {
            RepConstants.writeERROR_FILE(ex);
        }
        return sb.toString();
    }

    private boolean checkBlobEquality(Blob blob1, Blob blob2)
    {
        try
        {
            return checkInputStreamEquality(blob1.getBinaryStream(),
                                            blob2.getBinaryStream());
        }
        catch (SQLException ex)
        {
            return false;
        }
    }

    private boolean checkInputStreamEquality(InputStream stream1,
                                             InputStream stream2)
    {
        try
        {
            long len1 = stream1.available();
            long len2 = stream2.available();
            if (len1 != len2)
            {
                return false;
            }
            long read = 0;
            do
            {
                byte[] buf1 = new byte[1024];
                byte[] buf2 = new byte[1024];
                int byte1 = stream1.read(buf1);
                int byte2 = stream2.read(buf2);
                if (byte1 != byte2)
                {
                    return false;
                }
                for (int i = 0; i < buf1.length; i++)
                {
                    if (buf1[i] != buf2[i])
                    {
                        return false;
                    }
                }
                read += buf1.length;
            }
            while (read < len1);
        }
        catch (IOException ex)
        {
            return false;
        }
        return true;
    }

}
