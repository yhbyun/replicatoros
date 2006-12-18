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

import java.util.*;
import java.sql.Timestamp;

/**
 * _Subscription is an interface implemented by Subscription class.
 * It holds abstraction of all the relevant methods, needed for the completion of
 * creating subscription. It holds the abstraction of the methods which set the
 * subscription parameters as well as which create Replication system tables
 * (and triggers) for storing subscription information and needed for replication.
 * This interface holds the declaration of the methods needed for getting snapshot
 * and for synchronization.
 * This Interface actually is the bound over the user's access on the methods of
 * the subscription class , as the user can access only those methods which are
 * declared here.
 *
 */

public interface _Subscription {

  void setRemoteServerUrl(String remoteUrl0) throws RepException;

  void setRemoteServerPortNo(int remotePort0);

  void getSnapShot() throws RepException;

  void subscribe() throws RepException;

  void synchronize() throws RepException;

  void unsubscribe() throws RepException;

  void pull() throws RepException;

  void push() throws RepException;

  void addSchedule(String scheduleName, String subscriptionName,String scheduleType,
                   String publicationServerName, String publicationPortNo,
                   String recurrenceType,
                   String replicationType, Timestamp startDateTime,
                   int scheduleCounter) throws RepException;

  void editSchedule(String scheduleName, String subName, String newPubServerName,
                    String newPubPortNo) throws RepException;

  void removeSchedule(String scheduleName, String subscriptionName) throws RepException;

  static boolean xmlAndShadow_entries = true;

  public String getRemoteServerUrl();

  public int getRemoteServerPortNo();

  void updateSubscription() throws RepException ;

  public  void getSnapShotAfterUpdatingSubscriber() throws RepException ;

  ArrayList getRepTables();


}
