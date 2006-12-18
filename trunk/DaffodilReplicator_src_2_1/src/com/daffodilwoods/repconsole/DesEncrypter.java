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

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.IllegalBlockSizeException;
import java.io.UnsupportedEncodingException;
import javax.crypto.KeyGenerator;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class DesEncrypter {
  Cipher ecipher;
  Cipher dcipher;

       public DesEncrypter(SecretKey key) {
         try {
           ecipher = Cipher.getInstance("DES");
           dcipher = Cipher.getInstance("DES");
           ecipher.init(Cipher.ENCRYPT_MODE, key);
           dcipher.init(Cipher.DECRYPT_MODE, key);
         }
         catch (javax.crypto.NoSuchPaddingException e) {
         }
         catch (java.security.NoSuchAlgorithmException e) {
         }
         catch (java.security.InvalidKeyException e) {
         }
       }

       public String encrypt(String str) {
         try {
           // Encode the string into bytes using utf-8
           byte[] utf8 = str.getBytes("UTF8");
           // Encrypt
           byte[] enc = ecipher.doFinal(utf8);
           // Encode bytes to base64 to get a string
           return new sun.misc.BASE64Encoder().encode(enc);
         }
         catch (javax.crypto.BadPaddingException e) {
         }
         catch (IllegalBlockSizeException e) {
         }
         catch (UnsupportedEncodingException e) {
         }
         catch (java.io.IOException e) {
         }
         return null;
       }

       public String decrypt(String str) {
         try {
           // Decode base64 to get bytes
           byte[] dec = new sun.misc.BASE64Decoder().decodeBuffer(str);

           // Decrypt
           byte[] utf8 = dcipher.doFinal(dec);

           // Decode using utf-8
           return new String(utf8, "UTF8");
         }
         catch (javax.crypto.BadPaddingException e) {
         }
         catch (IllegalBlockSizeException e) {
         }
         catch (UnsupportedEncodingException e) {
         }
         catch (java.io.IOException e) {
         }
         return null;
       }





       /*  public static void main(String[] args) {

    String password = "";

    BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Enter Password");
    try {
      password = (input.readLine());
      System.out.println(" passsword :" + password);
    }
    catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    }

    //Here's an example that uses the class:
    try {
// Generate a temporary key. In practice, you would save this key.
// See also e464 Encrypting with DES Using a Pass Phrase.
      SecretKey key = KeyGenerator.getInstance("DES").generateKey();

// Create encrypter/decrypter class
      DesEncrypter encrypter = new DesEncrypter(key);

// Encrypt

      System.out.println("password : " + password);
      String encrypted = encrypter.encrypt(password);

      System.out.println(" encrypted : " + encrypted);

// Decrypt
      String decrypted = encrypter.decrypt(encrypted);

      System.out.println("decrypted : " + decrypted);

    }
    catch (Exception e) {
    }

  }
*/

}
