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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import javax.crypto.SecretKey;
import javax.crypto.KeyGenerator;
import java.security.*;
import com.daffodilwoods.replication.PathHandler;
import java.io.File;
import java.io.FileOutputStream;
import java.io.*;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

public class EncryptDecryptPassword {
   private DesEncrypter encrypter;
   private String publisherpassword="",pubYes="";
   private String subscriberpassword="",subyes="";
   private SecretKey key;


   public EncryptDecryptPassword() {
   getPasswordFromEndUser();
   initialiseDesEncryptor();
   encryptPassword();
  }


  private void initialiseDesEncryptor() {
    try {
     key = getSecretKey();
     String key1 = new String(key.getEncoded() );
//     writePublisherPassword("key",key1);
//     writeSubscriberPassword("key",key1);
     encrypter = new DesEncrypter(key);
   }
   catch (Exception ex) {
   }

  }

  private void  getPasswordFromEndUser() {
    BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
    try {
    System.out.print("Press Y to encrypt password for publication datasource else N :");
    pubYes=(input.readLine());
    pubYes =pubYes.trim();
    if(pubYes.equalsIgnoreCase("y")){

      System.out.print("Enter Publisher DataSource Password  : ");
      publisherpassword = (input.readLine());
      publisherpassword =publisherpassword.trim();
      if (publisherpassword.equalsIgnoreCase("\n") ||
      publisherpassword.equalsIgnoreCase("") ) {
      System.out.println("         Invalid Password     ");
      getPasswordFromEndUser();
      }

    }else  if(pubYes.equalsIgnoreCase("n")){

      System.out.print("Enter Subscriber DataSource Password : ");
      subscriberpassword = (input.readLine());
      subscriberpassword =subscriberpassword.trim();
      if(subscriberpassword.equalsIgnoreCase("\n") ||
      subscriberpassword.equalsIgnoreCase("")) {
      System.out.println("        Invalid Password     ");
      getPasswordFromEndUser();
      }
    }else {
      System.out.println("       Invalid Option     ");
      System.out.print("       Continue(Y/N)  :");
      String  tes=(input.readLine());
      if(tes.equalsIgnoreCase("y"))
      getPasswordFromEndUser();
      else
      System.exit(00);
    }
    }
    catch (Exception e) {
//      System.err.println(e.getMessage());
//      e.printStackTrace();
    }
  }





  private void encryptPassword() {
   if(pubYes.equalsIgnoreCase("y")){
    publisherpassword = encrypter.encrypt(publisherpassword);
    writePublisherPassword("PASSWORD",publisherpassword);
  }else if(pubYes.equalsIgnoreCase("n")){
    subscriberpassword = encrypter.encrypt(subscriberpassword);
    writeSubscriberPassword("PASSWORD",subscriberpassword);
    }
  }


  private void writePublisherPassword(String  key,String valueTowrite){
    try {
      File f = new File("." + File.separator + "pubconfig.ini");
      FileOutputStream fos = new FileOutputStream(f,true);
      OutputStreamWriter os = new OutputStreamWriter(fos);
      os.write("\r\n");
      os.write(key+"="+valueTowrite);
      os.write("\r\n");
      os.flush();
    }
    catch (Exception ex) {

//      ex.printStackTrace() ;
    }
  }


  private void writeSubscriberPassword(String  key,String valueTowrite){
     try {
       File f = new File("." + File.separator + "Subconfig.ini");
       FileOutputStream fos = new FileOutputStream(f,true);
       OutputStreamWriter os = new OutputStreamWriter(fos);
       os.write("\r\n");
       os.write(key+"="+valueTowrite);
       os.write("\r\n");
       os.flush();
     }
     catch (Exception ex) {
//       ex.printStackTrace() ;
     }
   }

  private static void EcodePassword(String password){
    byte[] b =password.getBytes();
    byte five = (byte)5;
//System.out.println("five  byte : "+five);
    for (int i = 0; i < b.length; i++) {
//System.out.println(" b[i] ="+b[i]);
      b[i] = (byte)(b[i]);
    }

//System.out.println(" Encrypt password : "+new String(b));

   for (int i = 0; i < b.length; i++) {
     b[i] = (byte)(b[i]/five*five);
   }
//System.out.println(" Decrypt password : "+new String(b));
  }


  private SecretKey getSecretKey(){
    try {
      String path ="/com/daffodilwoods/repconsole/secretKey.obj";
      java.net.URL url1 = getClass().getResource(path);
//       System.out.println(" url1 : "+url1);
       ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(url1.openStream()));
      SecretKey sk = (SecretKey) ois.readObject();
      return sk;
      }
      catch (IOException ex) {
//        ex.printStackTrace();
      }
      catch(ClassNotFoundException ex){
//        ex.printStackTrace();

      }
      return null;

    }



  public static void main(String[] args) {
//    EcodePassword("hrhk12");
    EncryptDecryptPassword en = new EncryptDecryptPassword();
  }


}
