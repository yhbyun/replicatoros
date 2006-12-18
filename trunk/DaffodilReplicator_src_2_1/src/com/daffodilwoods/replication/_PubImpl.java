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

import java.rmi.*;
import java.sql.*;
import java.util.ArrayList;
import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;

/**
 * This is a remote interface which is implemented by the remote server class
 * publication. This interface contains the declaration of all the methods
 * which are needed by the subscriber at the time of 1. subscribing 2. taking
 * snapshot 3. synchronization.
 *
 */

public interface _PubImpl extends Remote {

    void createStructure(int vendorName, String subName, String url, boolean isSchemaSupported,_FileUpload fileUpload,String remoteMachineAddress) throws RemoteException, SQLException, RepException;

    public void createSnapShot(String subName,boolean isSchemaSupported,_FileUpload fileUpload,String remoteMachineAddress) throws RemoteException, SQLException, RepException;

    public void synchronize(String subName, String remoteServerName,boolean isCreateTransactionLogFile,String remoteMachineAddress) throws
        RemoteException, RepException;

    public void push(String subName, String remoteServerName,boolean isCreateTransactionLogFile,String remoteMachineAddress) throws
        RemoteException, RepException;

    public Object[] createXMLForClient(String subName,String clientServerName,boolean isSchemaSupported,_FileUpload fileUpload,String remoteMachineAddress) throws
        RemoteException, RepException;

    public Object[] getPublisherAddressAndPort() throws
      RemoteException, RepException;

    String getConflictResolver() throws RemoteException;

    String getFilterClause(SchemaQualifiedName tableName) throws RemoteException;

    public String getServerName() throws RemoteException;

    public void dropSubscription(String subName) throws RemoteException,
        SQLException, RepException;

    public void releaseLOCK() throws RemoteException;


  public void checkForLock(String pubSubName) throws RepException,RemoteException;

//    public void checkForLock() throws RepException,RemoteException;
//
    public void saveSubscriptionData(String subName) throws RemoteException;

    public int getPubVendorName() throws RemoteException, RepException;

  public void updateBookMarkLastSyncId(String remote_Pub_Sub_Name,
                                       Object[] lastId) throws RemoteException,
      SQLException, RepException;
  public void saveSubscriptionNewData(String subName) throws RemoteException ;

  public void createSnapShotAfterUpdateSub(String address, int portNo, String subName,ArrayList tablesForSnapShot,boolean isSchemaSupported,_FileUpload fileUpload,String remoteMachineAddress) throws
      SQLException, RemoteException, RepException;

  public ArrayList dropTableListForSub(String subName) throws RepException,
      SQLException,RemoteException ;

  public void updatePublisherShadowAndBookmarkTableAfterPullOnSubscriber(String remote_Pub_Sub_Name,
     Object[] lastId) throws RemoteException,SQLException, RepException ;

 public _FileUpload getFileUploader() throws RepException,
      SQLException,RemoteException ;

}
