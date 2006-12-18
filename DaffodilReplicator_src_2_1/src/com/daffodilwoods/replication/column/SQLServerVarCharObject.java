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
import org.apache.log4j.Logger;
import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;

//import test.*;

public class SQLServerVarCharObject extends AbstractColumnObject
{

    int sqlType;
    AbstractDataBaseHandler abstractDBHandler;
    protected static Logger log =Logger.getLogger(SQLServerVarCharObject.class.getName());

    /**
     * sets the SQL type fro clob datatype
     * @param sqlType0
     */

    public SQLServerVarCharObject(int sqlType0,AbstractDataBaseHandler abstractDBHandler0)
    {
        sqlType = sqlType0;
        abstractDBHandler=abstractDBHandler0;
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
        String value = element.elementValue;
        setColumnObject(pst, value, index);
    }

    public void setColumnObject(PreparedStatement pst, String value,
                                int index) throws SQLException
    {

        int start = 0;
        int length = 0;
        String valueToInsert = (String) getObject(value);
        if (valueToInsert == null)
        {
            pst.setString(index, "null");
        }
        else
        {
            pst.setString(index, valueToInsert);

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

    public void write(Writer os, ResultSet rs, int index,ArrayList encodedCols,String col) throws
        SQLException, IOException
    {
        Object rowValue = getObject(rs, index);
        if (rowValue == null)
        {
            os.write("Start0length0");
        }
        else
        {
            os.write("Start" + clobst.getStreamStart() + "length" +
                     clobst.write(new ByteArrayInputStream( ( (String) rowValue).
                getBytes())));

        }
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

    public void writeUpdate(Writer os, ResultSet rows,
                            ResultSet oldResultSet
                            , int index, HashMap modifiedColumns,
                            String columnName,ArrayList encodedCols) throws SQLException, IOException
    {
//          Clob newObject = null; Clob oldObject = null;
        try
        {
            Object obj1 = rows.getObject(index);
            Object obj2 = oldResultSet.getObject(index);
            String newObjectAsString = (String) getObject(obj1.toString());
            InputStream newObject = new ByteArrayInputStream( ( (String) getObject(
                obj1.toString())).getBytes());
//       InputStream newObjectForPrint = new ByteArrayInputStream()
            String oldObjectAsString = (String) getObject(obj2.toString());
            InputStream oldObject = new ByteArrayInputStream( ( (String) getObject(
                obj2.toString())).getBytes());
            if (newObject == null)
            {
                write(os, null,encodedCols,columnName);
                if (oldObject != null)
                {
                    modifiedColumns.put(columnName, "NULL");
                }
            }
            else
            {
//         int legthOfStream=newObject.available();
//          byte[] buf=new byte[legthOfStream];
//          int noOfBytesRead=newObject.read(buf);
//          String InputString=new String(buf);
                String data = getClobNotNullData(newObject);
                write(os, data,encodedCols,columnName);
                if (oldObject != null)
                {
                    if (!oldObjectAsString.equalsIgnoreCase(newObjectAsString))
                    {

//               if (!checkInputStreamEquality(oldObject, newObject)) {
                        modifiedColumns.put(columnName, data);
//               }
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

    public Object getObject(String value) throws SQLException
    {
        if (value == null || value == "" || value.equalsIgnoreCase("null"))
        {
            return "";
        }
        int start = 0, length = 0;
        int lengthIndex = value.indexOf("length");
        if (lengthIndex == -1 && value.length() != 0)
        {
            return value;
        }
        start = Integer.parseInt(value.substring(5, lengthIndex));
        length = Integer.parseInt(value.substring(lengthIndex + 6));
        if (length <= 0)
        {
            return "";
        }
        String path = PathHandler.getCLobFilePathForClient();
        byte[] buf = new byte[length];
        try
        {
            FileInputStream is = new FileInputStream(path);
            long skip = -1;
            if (start != 0)
            {
                skip = is.skip(start);
            }
            int lengthRead = is.read(buf, 0, length);
            String toReturn = new String(buf);
            if (toReturn.indexOf("start") != -1 && toReturn.indexOf("length") != -1)
            {
                return getObject(toReturn);
            }
            else
            {
                return toReturn;
            }
        }
        catch (IOException ex)
        {
          log.error(ex.getMessage(),ex);
            throw new SQLException(ex.getMessage());
        }

//        return new String(buf);
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
        Object object = row.getObject(index);
//RepPrinter.print("$$$$$$$$$$$$$$Inside BlobObject object is ="+object);
        if (object == null)
        {
            return null;
        }
//    InputStream objInputStream = new ByteArrayInputStream((object.toString()).getBytes());
        return object;
//      return row.getBinaryStream(index);
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
//           os.write(rowValue.toString());
           if(!encodedCols.contains(col.toUpperCase()))  {
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
            os.write("" + clobst.getStreamStart());
            os.write("</start><length>-1</length>");
        }
    }

    private String getClobNotNullDataU(Object rowValue)
    {
        StringBuffer sb = new StringBuffer();
        try
        {
//RepPrinter.print(" GET Clob Not null data " + rowValue.toString());

//InputStream inputStram =new ByteArrayInputStream((rowValue.toString()).getBytes());
            sb.append("start").append(blobst.getStreamStart())
                .append("length").append(blobst.write( (InputStream) rowValue));
//(InputStream)rowValue)
// RepPrinter.print("Inside CLOB Object  sb.toString() =" + sb.toString());
        }
        catch (Exception ex)
        {
            RepConstants.writeERROR_FILE(ex);
        }
        return sb.toString();
    }

    private String getClobNotNullData(InputStream rowValue)
    {
        StringBuffer sb = new StringBuffer();
        try
        {
            int legthOfStream = rowValue.available();
            byte[] b1 = new byte[legthOfStream];
            int noOfBytesRead = rowValue.read(b1);
            String InputString = new String(b1);
            rowValue = new ByteArrayInputStream(InputString.getBytes());
// RepPrinter.print(" GET Clob Not null data " + rowValue.toString());
//InputStream inputStram =new ByteArrayInputStream((rowValue.toString()).getBytes());
            sb.append("start").append(clobst.getStreamStart())
                .append("length").append(clobst.write( (InputStream) rowValue));
//RepPrinter.print("Inside CLOB Object  sb.toString() =" + sb.toString());
        }
        catch (Exception ex)
        {
            RepConstants.writeERROR_FILE(ex);
        }
        return sb.toString();
    }

    private boolean checkBlobEquality(Clob clob1, Clob clob2)
    {
        try
        {
            return checkInputStreamEquality(clob1.getAsciiStream(),
                                            clob2.getAsciiStream());
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
          log.error(ex.getMessage(),ex);
            return false;
        }
        return true;
    }



}
