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

import java.rmi.server.*;
import java.rmi.RemoteException;
import java.io.*;

public class FileUpload extends  UnicastRemoteObject implements _FileUpload{

 private File file;
 private FileOutputStream output;

  public FileUpload() throws RemoteException {
  }


  /**
   * fileStart
   *
   * @param fileName String
   */
  public void fileStart(String fileName,boolean isFileForSynchronization)  throws RepException {
  try {
    if (isFileForSynchronization) {
      file = new File(PathHandler.getDefaultZIPFilePathForClient(fileName));
    }
    else {
      file = new File(PathHandler.getDefaultZIPFilePathForCreateStructure(fileName));
    }
    output = new FileOutputStream(file);
  }
  catch (FileNotFoundException ex) {
     RepConstants.writeERROR_FILE(ex);
    throw new RepException("REP085", null);
  }
  }

  /**
   * writeFileContent
   *
   * @param bytes byte[]
   * @return int
   */
  public void writeFileContent(byte[] bytes,int offset, int length) throws RepException{
    try {
          output.write(bytes,offset,length);
          output.flush();

        }
        catch (IOException ex) {
          RepConstants.writeERROR_FILE(ex);
          throw new RepException("REP085", null);
        }
  }

  /**
   * Close the file
   *
   * @param FileName String
   */
  public void closeFile()  throws RepException {
        try {
          output.close();
        }
        catch (IOException ex) {
          RepConstants.writeERROR_FILE(ex);
         throw new RepException("REP085", null);
        }
  }

}
