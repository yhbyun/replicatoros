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

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

public class RepAsciiStream extends InputStream {
   private long currentPosition;
   private RClob clob;

   public RepAsciiStream(RClob clob0) {
      clob = clob0;
      currentPosition = 0;
   }

   /**
    * read
    *
    * @return int
    */
   public int read() throws IOException {
      try {
         byte b = clob.getSubString(currentPosition, 1).getBytes()[0];
         currentPosition++;
         return b;
      } catch (SQLException ex) {
         if (ex.getMessage().equalsIgnoreCase("NODATA")) {
            return -1;
         }
         throw new IOException(ex.getMessage());
      }
   }

   public int read(byte b[]) throws IOException {
      try {
         int uLength = b.length;
         byte[] temp = clob.getSubString(currentPosition, uLength).getBytes();
         currentPosition += temp.length;
         System.arraycopy(temp, 0, b, 0, temp.length);
         return temp.length;
      } catch (SQLException ex) {
         if (ex.getMessage().equalsIgnoreCase("NODATA")) {
            return -1;
         }
         throw new IOException(ex.getMessage());
      }

   }

   public int read(byte b[], int off, int len) throws IOException {
      try {
         String s = clob.getSubString(currentPosition, len);
         byte[] temp = s.getBytes();
         currentPosition += temp.length;
         System.arraycopy(temp, 0, b, off, temp.length);
         return temp.length;
      } catch (SQLException ex) {
         if (ex.getMessage().equalsIgnoreCase("NODATA")) {
            return -1;
         }
         throw new IOException(ex.getMessage());
      }
   }

   public int available() throws IOException {
      try {
         return (int) clob.length();
      } catch (SQLException ex) {
         throw new IOException(ex.getMessage());
      }
   }
}
