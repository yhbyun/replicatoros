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
import java.io.File;

import com.daffodilwoods.replication.*;
import org.apache.log4j.Logger;

public class BlobOutPutStream {
  public int streamEnd;
  private String path = null; // = PathHandler.getLobFilePathForServer();
protected static Logger log =Logger.getLogger(BlobOutPutStream.class.getName());

  public BlobOutPutStream(String path0) {
    path = path0;
  }

  public int write(InputStream input) {
    int bytesToWrite = 0;
    /*      try
          {
              bytesToWrite = input.available();
          }
          catch (IOException ex2)
          {
//      ex2.printStackTrace();
          }
          if (bytesToWrite == 0 || bytesToWrite == -1)
          {
              return 0;
          } */
    int len = 0;
    int length = 0;
    byte[] buf = new byte[1024];
    try {
      FileOutputStream output = new FileOutputStream(path, true);
      while ( (len = input.read(buf)) > 0) {
        output.write(buf, 0, len);
        //lastIndex += len;
        length += len;
        bytesToWrite += len;
      }
      output.flush();
      output.close();
//      streamEnd = length;
      return bytesToWrite;
    }
    catch (IOException ex) {
      RepConstants.writeERROR_FILE(ex);
      return -1;
    }
  }

  public InputStream getInputStream() {
    try {
      File file = new File(path);
      if (!file.exists()) {
//        file = new File(path);
        try {
          file.createNewFile();
        }
        catch (IOException ex) {
          log.error(ex.getMessage(),ex);
        }
      }
      return new FileInputStream(path);
    }
    catch (FileNotFoundException ex) {
      RepConstants.writeERROR_FILE(ex);
      return null;
    }
  }

  public int getStreamStart() {
    InputStream is = null;
    int streamStart = 0;
    try {
      is = getInputStream();
      streamStart = is.available();
    }
    catch (IOException ex1) {
      log.error(ex1.getMessage(),ex1);
      // Problem with Input
    }
    finally {
      try {
        if (is != null) {
          is.close();
        }
      }
      catch (IOException ex) {
        log.error(ex.getMessage(),ex);
//        ex.printStackTrace();
      }
    }
    return streamStart;
  }

  public String toString() {
    return "[BlobOutStream end [" + streamEnd + "]]";
  }
}
