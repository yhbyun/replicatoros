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

import com.daffodilwoods.replication.*;
import org.apache.log4j.Logger;

public class RClob implements Clob
{

    private InputStream is;
    private long start;
    private long length;
    protected static Logger log =Logger.getLogger(RClob.class.getName());

   public RClob(long start0, long length0) {
        start = start0;
        length = length0;
      try {
            String path = PathHandler.getCLobFilePathForClient();
// RepPrinter.print(" Inside RClob path for file " + path);
            is = new FileInputStream(path);
         is.skip(start);
      } catch (Exception ex) {
          log.error(ex.getMessage(),ex);
            //Ignore Exception.
        }
    }

   public long length() throws SQLException {
        return length;

//    try {
//      return (long) getAsciiStream().available();
//    }
//    catch (SQLException ex) {
//      return 0L;
//    }
//    catch (IOException ex) {
//      return 0L;
//    }
    }

   /*   public String getSubString(long pos, int length0) throws SQLException {
//       RepPrinter.print(" Inside RClob start : " + start + " length : " + length);
         if (length0 == 0) {
            return "";
        }
         if (pos != 0) {
            pos--;
        }
        long start1 = start + pos;
        length0 = (start1 + length0) < (start + length) ?
            length0 : (int) (length - pos);
//       RepPrinter.print(" Inside RClob start1 : " + start1 + " length0 : " +length0);
        byte[] buf = new byte[length0];
        int lengthRead = 0;
        long skip = -1;
         try {
            if (start1 != 0) {
                skip = is.skip(start1);
            }
            lengthRead = is.read(buf, 0, length0);
         } catch (IOException ex) {
            throw new SQLException(ex.getMessage());
         }
         return new String(buf);
      }*/

   public String getSubString(long pos, int length0) throws SQLException {
      if (length0 <= 0) {
         return "";
      }

      if (pos != 0) {pos--;
      }

      long start1 = start + pos;
      length0 = (start1 + length0) < (start + length) ?
          length0 : (int) (length - pos);
//    RepPrinter.print(" Inside RClob start1 : " + start1 + " length0 : " +length0);
      byte[] buf = new byte[length0];
      int lengthRead = 0;
      try {
        lengthRead = is.read(buf, 0, length0);
        if (lengthRead <= 0) {
          throw new SQLException("NODATA");
        }
      }
        catch (IOException ex)
        {
          log.error(ex.getMessage(),ex);
            throw new SQLException(ex.getMessage());
        }

      byte[] temp = new byte[lengthRead];
      System.arraycopy(buf, 0, temp, 0, lengthRead);
      return new String(temp);
    }

    public Reader getCharacterStream() throws SQLException
    {
        return new InputStreamReader(getAsciiStream());
    }

   public InputStream getAsciiStream() throws SQLException {
//    return new ByteArrayInputStream(getSubString(0, (int) length).getBytes());
      return new RepAsciiStream(this);
    }

    public long position(String searchstr, long start) throws SQLException
    {
        /**@todo Implement this java.sql.Clob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method position() not yet implemented.");
    }

   public long position(Clob searchstr, long start) throws SQLException {
        /**@todo Implement this java.sql.Clob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method position() not yet implemented.");
    }

    public int setString(long pos, String str) throws SQLException
    {
        /**@todo Implement this java.sql.Clob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method setString() not yet implemented.");
    }

    public int setString(long pos, String str, int offset, int len) throws
        SQLException
    {
        /**@todo Implement this java.sql.Clob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method setString() not yet implemented.");
    }

    public OutputStream setAsciiStream(long pos) throws SQLException
    {
        /**@todo Implement this java.sql.Clob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method setAsciiStream() not yet implemented.");
    }

    public Writer setCharacterStream(long pos) throws SQLException
    {
        /**@todo Implement this java.sql.Clob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method setCharacterStream() not yet implemented.");
    }

    public void truncate(long len) throws SQLException
    {
        /**@todo Implement this java.sql.Clob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method truncate() not yet implemented.");
    }

    public boolean equals(RClob obj) throws Exception
    {
        InputStream currentStream = this.getAsciiStream();
        InputStream incommingStream = obj.getAsciiStream();
        int currentByte = currentStream.read();
        int incommingByte = incommingStream.read();
      while (currentByte != -1) {
         if (incommingByte != -1) {
            if (currentByte != incommingByte) {
                    return false;
                }
                currentByte = currentStream.read();
                incommingByte = incommingStream.read();
            }
            else
            {
                return false;
            }
        }
        if (incommingByte != -1)
        {
            return false;
        }
        return true;
    }

    public boolean equals1(RClob obj) throws Exception
    {
        InputStream currentStream = this.getAsciiStream();
        InputStream incommingStream = obj.getAsciiStream();
        long len = length;
        do
        {
            byte[] buf1 = new byte[1024];
            byte[] buf2 = new byte[1024];
            int currentByte = currentStream.read(buf1);
            int incommingByte = incommingStream.read(buf2);
            if (currentByte != incommingByte)
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
            len -= currentByte;
        }
        while (len > 0);
        return true;
    }
}
