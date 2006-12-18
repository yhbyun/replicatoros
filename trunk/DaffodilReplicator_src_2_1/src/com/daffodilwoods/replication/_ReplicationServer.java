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

/**
 * _ReplicationServer is an interface implemented by ReplicationServer class.
 * It holds abstraction of all the relevant methods, needed for creating
 * publication, creating subscription, setting data source for the replication
 * server, and getting connection from it. Besides it , it holds the abstraction
 * of the methods for getting the publication and subscription objects later on.
 *
 */

public interface _ReplicationServer
{
     _Publication getPublication(String pubName) throws RemoteException,
        RepException;

    _Subscription getSubscription(String subName) throws RepException;

    _Subscription createSubscription(String subName, String pubName) throws
        RepException;

    _Publication createPublication(String pubName, String[] tableNames) throws
        RepException;

    _Publication createPublication(String pubName, String[] tableNames,String[] removeCycleTableNames) throws
        RepException;

    void setDataSource(String driver0, String url0, String user0,
                       String password0) throws RepException;
    void setDataSource(String  dataBaseName,String user,String password0, String dBPortNo0,String databaseServerName, String vendorName0) throws RepException;

    Connection getConnection(String pub_sub_Name) throws RepException;

    Connection getDefaultConnection();
    ArrayList getTablesInCycle();
}
