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

public class RepInputStream extends InputStream {
   private long currentPosition;
   private static RBlob blob;

   public RepInputStream(RBlob blob0) {
      blob = blob0;
      currentPosition = 0;
   }

   /**
    * read
    *
    * @return int
    */
   public int read() throws IOException {
      try {
         byte b = blob.getBytes(currentPosition, 1)[0];
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
         byte[] temp = blob.getBytes(currentPosition, uLength);
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
         byte[] temp = blob.getBytes(currentPosition, len);
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
         int len = (int) blob.length();
         return len;
      } catch (SQLException ex) {
         throw new IOException(ex.getMessage());
      }
   }
}
