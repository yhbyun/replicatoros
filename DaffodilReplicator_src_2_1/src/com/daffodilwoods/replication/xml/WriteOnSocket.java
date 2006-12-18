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
import com.daffodilwoods.replication.*;

public class WriteOnSocket extends Thread {
  private String xmlFilePath,zipFilePath,fileName;
  private _FileUpload fileUpload;
  boolean deleteXML =false, isFileForSynchronization =false;

  public WriteOnSocket( String zipFilePath0,String xmlFilePath0,boolean deleteXML0, String fileName0,_FileUpload fileUpload0,boolean isFileForSynchronization0 ) {
    zipFilePath = zipFilePath0;
    xmlFilePath =xmlFilePath0;
    deleteXML =deleteXML0;
    fileUpload =fileUpload0;
    fileName=fileName0;
    isFileForSynchronization =isFileForSynchronization0;
  }

 // To write the zip file on client side.
  public void run() {
    try {
      FileInputStream fis = new FileInputStream(zipFilePath);
      byte[] buf = new byte[1024];
      int len = 0;
      fileUpload.fileStart(fileName,isFileForSynchronization);
      while ( (len = fis.read(buf)) > 0) {
        fileUpload.writeFileContent(buf,0,len);
      }
      fis.close();
      fileUpload.closeFile();
    }
    catch (IOException ex) {
      RepConstants.writeERROR_FILE(ex);
      new RuntimeException(ex);
    }
    catch (RepException ex) {
     RepConstants.writeERROR_FILE(ex);
     new RuntimeException(ex);
    }

    if (deleteXML) {
      // deleting zip file
      deleteFile(zipFilePath);
      // deleting xml file
      deleteFile(xmlFilePath);
    }

  }

   private void deleteFile(String fileName)
     {
         File f = new File(fileName);
         boolean deleted = f.delete();
     }

}
