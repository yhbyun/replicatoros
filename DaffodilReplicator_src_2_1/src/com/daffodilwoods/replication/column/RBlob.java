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
import com.daffodilwoods.replication.PathHandler;

import com.daffodilwoods.replication.*;
import org.apache.log4j.Logger;

public class RBlob implements Blob
{

    private InputStream is;
    private long start;
    private int length;
    protected static Logger log =Logger.getLogger(RBlob.class.getName());

    public RBlob(long start0, int length0)
    {
        start = start0;
        length = length0;
        try
        {
            String path = PathHandler.getBLobFilePathForClient();
            is = new FileInputStream(path);
         is.skip(start);
      } catch (Exception ex) {
log.error(ex.getMessage(),ex);
            //Ignore Exception
        }
    }

    public long length() throws SQLException
    {
        return length;
    }

   /*   public byte[] getBytes(long pos, int length0) throws SQLException {
                  if (length0 == 0) {
                     return new byte[] {};
        }
//    if(pos == 0)
//      new Exception("position inside getBytes method of the RBLob class something is wrong .. ").printStackTrace() ;
//             else if (pos != 0) {
            pos--; // because we write data into blob clob file from start address 0.
        }
        long start1 = start + pos;
        length0 = (start1 + length0) < (start + length) ?
            length0 : (int) (length - pos);
//             RepPrinter.print(" Inside RBlob start1 : " + start1 + " length0 : " +length0);
        byte[] buf = new byte[ (int) start1 + length0];
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
                  byte[] temp = new byte[lengthRead];
                  System.arraycopy(buf, 0, temp, 0, lengthRead);
//             return buf;
                  return temp;
               }*/

   public byte[] getBytes(long pos, int length0) throws SQLException {
      if (length0 <= 0) {
         return new byte[0];
      }

      if (pos != 0) {pos--;
      // because we write data into blob clob file from start address 0.
      }

      long start1 = start + pos;
      length0 = (start1 + length0) < (start + length) ?
          length0 : (int) (length - pos);
//      System.out.println("Inside RBlob start1 : " + start1 + " length0 : " + length0 + " pos " + pos + " length " + length + " start " + start);

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
//    return buf;
        return temp;
    }

   public InputStream getBinaryStream() throws SQLException {
//    return new ByteArrayInputStream(getBytes(0, length));
      return new RepInputStream(this);
    }

    public long position(byte[] pattern, long start) throws SQLException
    {
        /**@todo Implement this java.sql.Blob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method position() not yet implemented.");
    }

    public long position(Blob pattern, long start) throws SQLException
    {
        /**@todo Implement this java.sql.Blob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method position() not yet implemented.");
    }

    public int setBytes(long pos, byte[] bytes) throws SQLException
    {
        /**@todo Implement this java.sql.Blob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method setBytes() not yet implemented.");
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len) throws
        SQLException
    {
        /**@todo Implement this java.sql.Blob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method setBytes() not yet implemented.");
    }

   public OutputStream setBinaryStream(long pos) throws SQLException {
        /**@todo Implement this java.sql.Blob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method setBinaryStream() not yet implemented.");
    }

   public void truncate(long len) throws SQLException {
        /**@todo Implement this java.sql.Blob method*/
        throw new java.lang.UnsupportedOperationException(
            "Method truncate() not yet implemented.");
    }

   public boolean equals(RBlob obj) throws Exception {
        InputStream currentStream = this.getBinaryStream();
        InputStream incommingStream = obj.getBinaryStream();
        int currentByte = currentStream.read();
        int incommingByte = incommingStream.read();
      while (currentByte != -1) {
         if (incommingByte != -1) {
            if (currentByte != incommingByte) {
                    return false;
                }
                currentByte = currentStream.read();
                incommingByte = incommingStream.read();
         } else {
                return false;
            }
        }
      if (incommingByte != -1) {
            return false;
        }
        return true;
    }
}
