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

import java.util.Properties;
import java.io.FileInputStream;
import java.io.File;
import com.daffodilwoods.replication._ReplicationServer;
import com.daffodilwoods.replication.ReplicationServer;
import com.daffodilwoods.replication.RepException;
import com.daffodilwoods.replication._Subscription;
import com.daffodilwoods.replication.RepConstants;
import javax.crypto.SecretKey;
import java.io.ObjectInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;

public class Subconfiguration {

  private String subDbDriver , subDbURL , subDbUser , subDbPassword ;
  private String subName;
  private String portNumber;
  private String subSysName;//   = "192.168.4.129";
  private String pubName;
  private String pubSysName;//   = "192.168.4.129";
  private String pubPortNumber;//   = 3001;
  private String   replicationType;

  _ReplicationServer repSubscription;
  _Subscription sub;
  private  DesEncrypter desEncrypter;



  public Subconfiguration()  {
    try {

     File f =new File("."+File.separator+"subconfig.ini");
     Properties p =new Properties();
      p.load(new FileInputStream(f));
//RepConstants.writeMessage_FILE("GOING TO INITIALISE THE SUB AND PUB INFORMATION");
    initialiseSubAndPubInformation(p);
//RepConstants.writeMessage_FILE("SUB AND PUB INFORMATION INITIALISED SUCCESSFULLY");
//RepConstants.writeMessage_FILE("GOING TO START THE REPLICATION SERVER");
       repSubscription = initialiseReplicationServer();
//RepConstants.writeMessage_FILE("REPLICATION SERVER STARTED SUCCESSFULLY");
       sub =getSubscription(subName,pubName);
//RepConstants.writeMessage_FILE("sUBSCRIPTION GETTED ");
       performReplication(sub,replicationType);

    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
 System.out.println(" Problem occured in pull replication. For details see the error.lg");
//      System.exit(1);
    }
  }



 private void  performReplication(_Subscription sub,String replicationType) throws RepException{
try{
//RepConstants.writeMessage_FILE("PRFORME PULL ");
  sub.pull();
//RepConstants.writeMessage_FILE(" PULLED SUCCESS FULYY ");
System.out.println("Pulled  successfully ");
}finally{
  System.exit(1);
}
  /*
   if(replicationType.equalsIgnoreCase("Sanpshot")) {
     sub.getSnapShot();
System.out.println(" SanpShot Taken Successfully");
   }else if(replicationType.equalsIgnoreCase("Synchronize")) {
     sub.synchronize();
System.out.println("Synchronize successfully ");
   } else if(replicationType.equalsIgnoreCase("Pull")) {
     sub.pull();

   }
   else if(replicationType.equalsIgnoreCase("Push")) {
     sub.push();
System.out.println("Pushed  successfully ");

   } */
 }


  private void initialiseSubAndPubInformation( Properties p) {
        //set subscriber data
     subDbDriver = p.getProperty("DRIVER");
//System.out.println(" subDbDriver ="+subDbDriver);
     subDbURL =   p.getProperty("URL");
//System.out.println(" subDbURL ="+subDbURL);
     subDbUser =  p.getProperty("USER");
//System.out.println(" subDbUser ="+subDbUser);
     subDbPassword = p.getProperty("PASSWORD");
//System.out.println(" subDbPassword ="+subDbPassword);
     subDbPassword = decryptPassword(subDbPassword);
//System.out.println(" subDbPassword ="+subDbPassword);
     subName =  p.getProperty("SUBNAME");
//System.out.println(" subName ="+subName);
     portNumber = p.getProperty("PORT");
//System.out.println(" portNumber ="+portNumber);
     subSysName = p.getProperty("SYSTEMNAME");
//System.out.println(" subSysName ="+subSysName);
     pubName =p.getProperty("PUBNAME");
//System.out.println(" pubName ="+pubName);
     pubSysName =p.getProperty("PUB_SYSTEMNAME");
//System.out.println(" pubSysName ="+pubSysName);
     pubPortNumber =p.getProperty("PUB_PORT");
//System.out.println(" pubPortNumber ="+pubPortNumber);
     replicationType =p.getProperty("REPLICATIONTYPE");

  }


  private _ReplicationServer  initialiseReplicationServer() throws RepException {
  _ReplicationServer  repSubscription = ReplicationServer.getInstance(Integer.parseInt(portNumber),subSysName);
    repSubscription.setDataSource(subDbDriver,subDbURL,subDbUser,subDbPassword);
   return  repSubscription;
  }


   private _Subscription getSubscription(String subName, String pubName) throws RepException {
   _Subscription  sub = repSubscription.getSubscription(subName);
    if (sub == null) {
      sub = repSubscription.createSubscription(subName, pubName);
      sub.setRemoteServerPortNo(Integer.parseInt(pubPortNumber));
      sub.setRemoteServerUrl(pubSysName);
      try {
        sub.subscribe();
      }
      catch (RepException ex) {
       throw ex;
//        ex.printStackTrace();
      }
    }
    else {
      sub.setRemoteServerPortNo(Integer.parseInt(pubPortNumber));
      sub.setRemoteServerUrl(pubSysName);
    }
//    System.out.println(" SUBSCRIBED ");
    return sub;
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
//             ex.printStackTrace();
           }
           catch(ClassNotFoundException ex){
             RepConstants.writeERROR_FILE(ex);
//             ex.printStackTrace();

           }
           return null;

         }



public static void main(String[] args) {
      Subconfiguration sc = new Subconfiguration();
  }

}
