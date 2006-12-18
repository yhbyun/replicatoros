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


package com.daffodilwoods.repconsole;

import java.io.File;
import java.util.Properties;
import java.io.FileInputStream;
import com.daffodilwoods.replication._Publication;
import com.daffodilwoods.replication.RepException;
import com.daffodilwoods.replication._ReplicationServer;
import com.daffodilwoods.replication.ReplicationServer;
import com.daffodilwoods.replication.RepConstants;
import javax.crypto.SecretKey;
import javax.crypto.KeyGenerator;
import java.security.*;
import java.io.ObjectInputStream;
import java.io.*;

public class PubConfiguration {

  private String pubDbDriver , pubDbURL , pubDbUser , pubDbPassword,keys ;

  private String pubName;

  private String pubSysName;

  private String pubPort;

  private _ReplicationServer repPublication;
  private _Publication pub;

  private  DesEncrypter desEncrypter;

  public PubConfiguration() {

    File f =new File("."+File.separator+"pubconfig.ini");
       Properties p =new Properties();
      try {
   p.load(new FileInputStream(f));  // Try to load props
   initialisePubInformation(p);
    repPublication =initialiseReplicationServer();
    pub = getPublication();
  }
  catch (Exception ex) {
//    ex.printStackTrace();
    RepConstants.writeERROR_FILE(ex);
  }
  }


/* private SecretKey getSecretKey(){
    File f =new File("."+File.separator+"secretkey.log");
    try {
      ObjectInputStream oos = new ObjectInputStream(new FileInputStream(f));
     SecretKey sk =  (SecretKey)oos.readObject() ;
     return sk;
    }
    catch (IOException ex) {
    }
    catch(ClassNotFoundException ex){

    }
    return null;

  }
*/



  private void  initialisePubInformation(Properties p) {
//System.out.println(" property "+ p);
       pubDbDriver = p.getProperty("DRIVER");
//System.out.println(" subDbDriver ="+pubDbDriver);
       pubDbURL =   p.getProperty("URL");
//System.out.println(" pubDbURL ="+pubDbURL);
       pubDbUser =  p.getProperty("USER");
//System.out.println(" pubDbUser ="+pubDbUser);
       pubDbPassword = p.getProperty("PASSWORD");
//       keys =p.getProperty("KEY");
// System.out.println("Pub Configuration  pubDbPassword  : "+pubDbPassword);
       pubDbPassword = decryptPassword(pubDbPassword);
// System.out.println("Pub Configuration  pubDbPassword  : "+pubDbPassword);

//System.out.println(" pubDbPassword ="+pubDbPassword);
       pubName =  p.getProperty("PUBNAME");
//System.out.println(" pubName ="+pubName);
       pubSysName = p.getProperty("SYSTEMNAME");
// System.out.println(" pubvsysname  "+ pubSysName);
       pubPort =p.getProperty("PORT");
//System.out.println(" port  "+ pubPort);


  }

  private _Publication getPublication() throws Exception {
    _Publication pub = repPublication.getPublication(pubName);
     if (pub == null) {
       throw new RepException("REP048",new Object[]{pubName});
     } else {
     return pub;
     }
  }

  private _ReplicationServer initialiseReplicationServer() throws RepException {
   _ReplicationServer   rep = ReplicationServer.getInstance(Integer.parseInt(pubPort),pubSysName);
    rep.setDataSource(pubDbDriver, pubDbURL, pubDbUser,pubDbPassword);
    return rep;
  }


    private String decryptPassword(String password){
        SecretKey sk = getSecretKey();
        desEncrypter = new DesEncrypter(sk);
      return  desEncrypter.decrypt(password);
    }


    private SecretKey getSecretKey(){
       try {
//   String path =File.separator + "com" +File.separator +"daffodilwoods" +File.separator + "repconsole" +File.separator +"EncryptDecrypt.properties";
         String path ="/com/daffodilwoods/repconsole/secretKey.obj";
         java.net.URL url1 = getClass().getResource(path);
//System.out.println(" url1 : "+url1);
          ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(url1.openStream()));
         SecretKey sk = (SecretKey) ois.readObject();
         return sk;
         }
         catch (IOException ex) {
 RepConstants.writeERROR_FILE(ex);
//           ex.printStackTrace();
         }
         catch(ClassNotFoundException ex){
//           ex.printStackTrace();
 RepConstants.writeERROR_FILE(ex);
         }
         return null;

       }


  public static void main(String[] args) {
    PubConfiguration pc  = new PubConfiguration();
  }

}
