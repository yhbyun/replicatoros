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

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.StringTokenizer;

/**
 * This class is used for getting the paths of the files needed at the time
 * of synchronization. Replicator uses perticular paths for the specified files
 * like XMLs or Zips or Lobs  etc.
 *
 */

public class PathHandler {
  private static String repHome = null;
  private static String errorFilePath = null;
  private static String repHome_passedByUser = null;

  static {
    initialiseReplicatioHome();
  }

  private static String createReplicationHomeDirectories(String
      replicationHome0) throws RepException {
    File directory0 = new File(replicationHome0);
    if (!directory0.exists()) {
      throw new RepException("REP300", new Object[] {replicationHome0});
    }
    else {
      replicationHome0 = replicationHome0 + File.separator + "Replication";
      File directory = new File(replicationHome0);
      if (!directory.exists() ||
          (directory.exists() && !directory.isDirectory())) {
        directory.mkdir();
      }
    }
    return replicationHome0;
  }

  private static void initialiseReplicatioHome() {
    try {
      File f = new File("." + File.separator + "config.ini");
      if (!f.exists())
        f.createNewFile();
      Properties p = new Properties();
      p.load(new FileInputStream(f)); // Try to load props
      repHome_passedByUser = p.getProperty("REPLICATIONHOME", "Default");
      if (repHome_passedByUser.equalsIgnoreCase("Default") ||
          repHome_passedByUser.trim().equalsIgnoreCase("")) {
        repHome_passedByUser = System.getProperty("user.home") + File.separator +
            "Replication";
        File directory = new File(repHome_passedByUser);
        if (!directory.exists() ||
            (directory.exists() && !directory.isDirectory())) {
          directory.mkdir();
        }
        repHome = repHome_passedByUser;
      }
      else {
        repHome = createReplicationHomeDirectories(repHome_passedByUser);
      }
    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
//      ex.printStackTrace();
    }
  }

  public static String fullOrPartialTransactionLogFile() {
    String fullOrPartial = "";
    try {
      File f = new File("." + File.separator + "config.ini");
      if (!f.exists())
        f.createNewFile();
      Properties p = new Properties();
      p.load(new FileInputStream(f)); // Try to load props
      fullOrPartial = p.getProperty("TRANSACTIONDETAIL", "false");
      if (fullOrPartial.trim().equalsIgnoreCase("")) {
        fullOrPartial = "false";
      }
    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
//      ex.printStackTrace();
    }
    return fullOrPartial;
  }

  private static String getCreateStructureFilePath(String name) {
    String strucPath = repHome + File.separator + "struct";
    File directory = new File(strucPath);
    if (!directory.exists() ||
        (directory.exists() && !directory.isDirectory())) {
      directory.mkdir();
    }
    return strucPath; //+File.separator+name+"."+"xml";
  }

//  public static String getDefaultFilePathForServer(String name) {
//    String strucPath = getStrucPathSynchronize();
//    return strucPath + File.separator + name + ".xml";
//  }

  private static String getStrucPathSynchronize() {
    String strucPath = repHome + File.separator + "synchronize"; //+ File.separator + "server" ;
    File directory = new File(strucPath);
    if (!directory.exists() ||
        (directory.exists() && !directory.isDirectory())) {
      directory.mkdir();
    }

    strucPath = strucPath + File.separator + "server";
    directory = new File(strucPath);
    if (!directory.exists() ||
        (directory.exists() && !directory.isDirectory())) {
      directory.mkdir();
    }
    return strucPath;
  }

  public static String getStrucPathSynchronizeClient() {
    String strucPath = repHome + File.separator + "synchronize"; //+ File.separator + "server" ;
    File directory = new File(strucPath);
    if (!directory.exists() ||
        (directory.exists() && !directory.isDirectory())) {
      directory.mkdir();
    }

    strucPath = strucPath + File.separator + "client";
    directory = new File(strucPath);
    if (!directory.exists() ||
        (directory.exists() && !directory.isDirectory())) {
      directory.mkdir();
    }
    return strucPath;
  }

  /**
   * create a drictory for transaction log file of publisher.
   * @return String
   */
  private static String getTransactionlogPathPublisher() {
    String trasactionLogPath = repHome + File.separator + "Transactionlog"; //+ File.separator + "server" ;
    File directory = new File(trasactionLogPath);
    if (!directory.exists() ||
        (directory.exists() && !directory.isDirectory())) {
      directory.mkdir();
    }

    trasactionLogPath = trasactionLogPath + File.separator + "publisher";
    directory = new File(trasactionLogPath);
    if (!directory.exists() ||
        (directory.exists() && !directory.isDirectory())) {
      directory.mkdir();
    }
    return trasactionLogPath;
  }

  /**
   * create a drictory for transaction log file of subscriber.
   * @return String
   */
  private static String getTransactionlogPathSubscriber() {
    String trasactionLogPath = repHome + File.separator + "Transactionlog"; //+ File.separator + "server" ;
    File directory = new File(trasactionLogPath);
    if (!directory.exists() ||
        (directory.exists() && !directory.isDirectory())) {
      directory.mkdir();
    }

    trasactionLogPath = trasactionLogPath + File.separator + "subscriber";
    directory = new File(trasactionLogPath);
    if (!directory.exists() ||
        (directory.exists() && !directory.isDirectory())) {
      directory.mkdir();
    }
    return trasactionLogPath;
  }

  // return the default path for transaction log file of publisher
  public static String getDefaultTransactionLogFilePathForPublisher(String name) {
    String strucPath = getTransactionlogPathPublisher();
    Timestamp t = new Timestamp(System.currentTimeMillis());
    String dt = t.toString().replaceAll(":", "_");
    dt = dt.replaceAll(" ", "_");
    dt = dt.replaceAll("-", "_");
    dt = dt.substring(0, dt.indexOf("."));

    return strucPath + File.separator + name + "_" + dt + "." + "lg";
  }

  // return the default path for transaction log file of subscriber
  public static String getDefaultTransactionLogFilePathForSubscriber(String
      name) {

    String strucPath = getTransactionlogPathSubscriber();
    Timestamp t = new Timestamp(System.currentTimeMillis());
    String dt = t.toString().replaceAll(":", "_");
    dt = dt.replaceAll(" ", "_");
    dt = dt.replaceAll("-", "_");
    dt = dt.substring(0, dt.indexOf("."));

    return strucPath + File.separator + name + "_" + dt + "." + "lg";
  }

//  public static String getDefaultZIPFilePathForServer(String name) {
//    String strucPath = getStrucPathSynchronize();
//    return strucPath + File.separator + name + "." + "zip";
//  }

  public static String getDefaultZIPFilePathForClient(String name) {
    String strucPath = getStrucPathSynchronizeClient();
    return strucPath + File.separator + name + "." + "zip";
  }

  public static String getDefaultFilePathForClient(String name) {
    String strucPath = getStrucPathSynchronizeClient();
    return strucPath + File.separator + name + "." + "xml";
  }

  public static String getDefaultFilePathForCreateStructure(String name) {
    String strucPath = getCreateStructureFilePath(name);
    return strucPath + File.separator + name + "." + "xml";
  }

  public static String getDefaultZIPFilePathForCreateStructure(String name) {
    String strucPath = getCreateStructureFilePath(name);
    return strucPath + File.separator + name + "." + "zip";
  }

  public static String getRepHome() {
    return repHome;
  }

  public static String getRepHomeByUser() {
    return repHome_passedByUser;
  }

  public static String getErrorFilePath() throws IOException {
    if (errorFilePath != null) {
      return errorFilePath;
    }
    errorFilePath = repHome + File.separator + "log";
    File directory = new File(errorFilePath);
    if (!directory.exists() || (directory.exists() && !directory.isDirectory())) {
      directory.mkdir();
    }
    errorFilePath = errorFilePath + File.separator + "ERROR.lg";
    return errorFilePath;
  }

//  public static String getCLobFilePathForServer() {
//    String strucPath = getStrucPathSynchronize();
//    return strucPath + File.separator + "clob.lob";
//  }

  public static String getCLobFilePathForClient() {
    String strucPath = getStrucPathSynchronizeClient();
    return strucPath + File.separator + "clob.lob";
  }

//  public static String getBLobFilePathForServer() {
//    String strucPath = getStrucPathSynchronize();
//    return strucPath + File.separator + "blob.lob";
//  }

  public static String getBLobFilePathForClient() {
    String strucPath = getStrucPathSynchronizeClient();
    return strucPath + File.separator + "blob.lob";
  }

  public  static ArrayList getEncodedColumns(String tableName) {

    ArrayList list =new ArrayList();
    try {
      File f = new File("." + File.separator + "Encodeconfig.ini");
      if (!f.exists())
        f.createNewFile();

      Properties p = new Properties();
      p.load(new FileInputStream(f)); // Try to load props
      String colslist = p.getProperty(tableName.toUpperCase(), "");
      if(colslist.indexOf(",")==-1) {
        list.add(colslist);
      return list;
      }
      else {
        StringTokenizer st =new StringTokenizer(colslist,",");
        String str =null;
        while( st.hasMoreTokens()) {
          str =st.nextToken();
//          System.out.println("Columns  str  : "+str);
          list.add(str);
        }
      }

    }
    catch (Exception ex) {
      RepConstants.writeERROR_FILE(ex);
    }
//System.out.println("PathHandler.getEncodedColumns(tableName) list : "+list);
    return list;
  }

}
