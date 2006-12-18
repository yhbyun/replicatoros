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

package com.daffodilwoods.replication.schedule;

import java.sql.*;
import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;
import org.apache.log4j.Logger;
import java.util.HashMap;

/**
 * <p>Description:This class implements Runnable and hence has a Run method overridden
 * which starts a thread and performs the required replication operation</p>
 */

public class ScheduledSynchronizer implements Runnable {
  private  _ReplicationServer repServer;
  private Connection connection;
  private Statement stmt = null;
  private String subName = null,
         scheduleName = null,
         scheduleType = null,
         publicationServerName = null,
         publicationPortNo = null,
         recurrenceType = null,
         replicationType = null;
  private long scheduleTime = 0;
  private int counter = 0;
  private _Subscription sub =null;
  private AbstractDataBaseHandler dbHandler =null;
  protected boolean stopSchedule=false;
  private HashMap threadMap =null;
  protected static Logger log =Logger.getLogger(ScheduledSynchronizer.class.getName());

  /**
   *This is main class responible for performing the required replication operation at right scheduled time.
   * and updating the scheduled time for next schedule in Rep_ScheduleTable.
   * @param repServer0 _ReplicationServer
   * @param subName0 String
   * @param scheduleName0 String
   * @param scheduleType0 String
   * @param remoteServerName String
   * @param remotePortNo String
   * @param schType String
   * @param repType String
   * @param schTime long
   * @param counter0 int
   * @throws RepException
   */

  public ScheduledSynchronizer(_ReplicationServer repServer0,AbstractDataBaseHandler dbh, String subName0,
                               String scheduleName0, String scheduleType0,
                               String remoteServerName,
                               String remotePortNo, String recurrenceType0,
                               String repType, long schTime, int counter0, HashMap threadMap0) throws
      RepException {
      subName = subName0;
      scheduleName = scheduleName0;
      scheduleType = scheduleType0;
      publicationServerName = remoteServerName;
      publicationPortNo = remotePortNo;
      recurrenceType = recurrenceType0;
      replicationType = repType;
      scheduleTime = schTime;
      counter = counter0;
      repServer = repServer0;
      connection = repServer.getDefaultConnection();
      try {
        stmt = connection.createStatement();
      }
      catch (SQLException ex) {
      }
      sub = repServer.getSubscription(subName);
      sub.setRemoteServerUrl(publicationServerName);
      sub.setRemoteServerPortNo(Integer.parseInt(publicationPortNo));
      dbHandler = dbh; //Utility.getDatabaseHandler(sub.getConnectionPool(), con);
      threadMap=threadMap0;

  }

  /**
   * This method performs the required replication operation between publisher and subscriber when the schedule time is reached
   * It updates schedule time in the Rep_ScheduleTable and then enters into wait till the
   * time for the next operation.
   */

  public void run() {
    try {
      //if Schedule Type is realtime
      if (scheduleType.equalsIgnoreCase(RepConstants.scheduleType_realTime)) {
        Utility.createTransactionLogFile=false;
        realTimeSchedule();
      }
      //if Schedule is non realtime
      else if (scheduleType.equalsIgnoreCase(RepConstants.scheduleType_nonRealTime)) {
        long startTime = scheduleTime;
        int updatedQuery = 0;
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        long threadSleepTime = startTime - (System.currentTimeMillis());
        boolean flag = true;
        while (flag) {
          //It means the start time is more then current time,sleep
          //otherwise depending on counter thread will be terminated or run
          if (startTime > currentTime.getTime()) {
            Thread.sleep(threadSleepTime);
            startTime = System.currentTimeMillis();
          }
          //if schedule time has reached or past away
          if (startTime <= System.currentTimeMillis()) {
            //if schedule is to repeated more than once
            if (counter > 0) {
              //depending on schedule type and counter update the next schedule time untill it get more or equal than current time.
              do {
               Timestamp newTime = new Timestamp(startTime);
                if (recurrenceType.equalsIgnoreCase(RepConstants.recurrence_yearType)) {
                  newTime = new Timestamp( (newTime.getYear() + counter),
                                          newTime.getMonth(), newTime.getDate(),
                                          newTime.getHours(),
                                          newTime.getMinutes(),
                                          newTime.getSeconds(),
                                          newTime.getNanos());
                }
                else if (recurrenceType.equalsIgnoreCase(RepConstants.recurrence_monthType)) {
                  newTime = new Timestamp(newTime.getYear(),
                                          (newTime.getMonth() + counter),
                                          newTime.getDate(), newTime.getHours(),
                                          newTime.getMinutes(),
                                          newTime.getSeconds(),
                                          newTime.getNanos());
                }
                else if (recurrenceType.equalsIgnoreCase(RepConstants.recurrence_dayType)) {
                  newTime = new Timestamp(newTime.getYear(), newTime.getMonth(),
                                          (newTime.getDate() + counter),
                                          newTime.getHours(),
                                          newTime.getMinutes(),
                                          newTime.getSeconds(),
                                          newTime.getNanos());
                }
                else if (recurrenceType.equalsIgnoreCase(RepConstants.recurrence_hourType)) {
                  newTime = new Timestamp(newTime.getYear(), newTime.getMonth(),
                                          newTime.getDate(),
                                          (newTime.getHours() + counter),
                                          newTime.getMinutes(),
                                          newTime.getSeconds(),
                                          newTime.getNanos());
                }
                else if (recurrenceType.equalsIgnoreCase(RepConstants.recurrence_minuteType)) {
                 newTime = new Timestamp(newTime.getYear(), newTime.getMonth(),
                                         newTime.getDate(),
                                         newTime.getHours(),
                                         (newTime.getMinutes()+ counter),
                                         newTime.getSeconds(),
                                         newTime.getNanos());
               }

                startTime = newTime.getTime();
              }
              while ( (startTime <= System.currentTimeMillis()));
              threadSleepTime = startTime - System.currentTimeMillis();
              StringBuffer query = new StringBuffer();
              query.append("UPDATE ").append(dbHandler.getScheduleTableName()).
                  append(" SET ")
                  .append(RepConstants.schedule_time)
                  .append(" = ")
                  .append(startTime).append(" WHERE ")
                  .append(RepConstants.schedule_Name)
                  .append(" = '")
                  .append(scheduleName).append("' AND ")
                  .append(RepConstants.subscription_subName1).append(" = '")
                  .append(subName).append("'");
              updatedQuery = stmt.executeUpdate(query.toString());
              log.debug(" UPDATE SCHEDULE TABLE QUERY ::  "+query.toString());
              //if schedule is already dropped,no updation would have been done,so stopping thread in this case.
              if (updatedQuery == 0) {
                flag = false;
                break;
              }
            }
            //if schedule is for once only and its time has past away,thread will be stopped.
            else if (counter == 0 && startTime < System.currentTimeMillis()) {
              break;
            }
            //performs the replication operation now
         if (replicationType.equalsIgnoreCase(RepConstants.replication_snapshotType)) {
           log.debug(" CALLING SNAPSHOT AT "+new Timestamp(System.currentTimeMillis()));
//           System.out.println(" GOING TO TAKE SNAPSHOT IN  SCHEDULING ");
           sub.getSnapShot();
//           System.out.println(" SNAPSHOT TAKEN SUCCESSFULLY ");
           log.debug(" SNAPSHOT DONE SUCCESSFULLY AT "+new Timestamp(System.currentTimeMillis()));
         }
         else if (replicationType.equalsIgnoreCase(RepConstants.replication_synchronizeType)) {
           log.debug(" CALLING SYNCHRONIZATION AT "+new Timestamp(System.currentTimeMillis()));
           sub.synchronize();
           log.debug("SYNCHRONIZATION DONE SUCCESSFULLY AT  "+new Timestamp(System.currentTimeMillis()));
         }
         else if (replicationType.equalsIgnoreCase(RepConstants.replication_pullType)) {
           log.debug(" CALLING PULL AT  "+new Timestamp(System.currentTimeMillis()));
           sub.pull();
           log.debug(" PULL DONE SUCCESSFULLY AT  "+new Timestamp(System.currentTimeMillis()));
         }
         else if (replicationType.equalsIgnoreCase(RepConstants.replication_pushType)) {
           log.debug(" CALLING PUSH AT  "+new Timestamp(System.currentTimeMillis()));
           sub.push();
           log.debug(" PUSH DONE SUCCESSFULLY AT  "+new Timestamp(System.currentTimeMillis()));
         }
            if (threadSleepTime == 0 || counter == 0||stopSchedule==true){
              stopScheduledThread();
//              flag = false;
//              break;
            }
          }
        }
//        if(stopSchedule){
//      stopScheduledThread();
//    }
        stmt.close();
      }
    }
    catch (SQLException ex2) {
      log.error(ex2,ex2);
      RepConstants.writeERROR_FILE(ex2);
//       throw new RuntimeException(ex2);
    }
    catch (InterruptedException ex3) {
      log.error(ex3,ex3);
      RepConstants.writeERROR_FILE(ex3);
//      throw new RuntimeException(ex3);
    }
    catch (Exception ex4) {
      log.error(ex4,ex4);
      RepConstants.writeERROR_FILE(ex4);
//      throw new RuntimeException(ex4);
    }
  }

  private void realTimeSchedule() {
    while (true) {
      try {
        //performs the replication operation now
        if (replicationType.equalsIgnoreCase(RepConstants.replication_snapshotType)) {
          log.debug(" CALLING SNAPSHOT AT  "+new Timestamp(System.currentTimeMillis()));
          sub.getSnapShot();
          log.debug(" SNAPSHOT DONE SUCCESSFULYY AT  "+new Timestamp(System.currentTimeMillis()));
        }
        else if (replicationType.equalsIgnoreCase(RepConstants.replication_synchronizeType)) {
         log.debug("CALLING SYNCHRONIZATION AT  "+new Timestamp(System.currentTimeMillis()));
          sub.synchronize();
        log.debug(" SYNCHRONIZATION DONE SUCCESSFULLY AT "+new Timestamp(System.currentTimeMillis()));
        }
        else if (replicationType.equalsIgnoreCase(RepConstants.replication_pullType)) {
          log.debug("CALLING PULL AT  "+new Timestamp(System.currentTimeMillis()));
          sub.pull();
          log.debug(" PULL DONE SUCCESSFULLY AT  "+new Timestamp(System.currentTimeMillis()));
        }
        else if (replicationType.equalsIgnoreCase(RepConstants.replication_pushType)) {
          log.debug(" CALLING PUSH AT  "+new Timestamp(System.currentTimeMillis()));
          sub.push();
          log.debug(" PUSH DONE SUCCESSFULLY AT  "+new Timestamp(System.currentTimeMillis()));
        }
      }
      catch (Exception ex1) {
        log.error(ex1,ex1);
        RepConstants.writeERROR_FILE(ex1);
//      throw new RuntimeException(ex1);
      }
      if(!stopSchedule){
      try {
        Thread.sleep(10000);
      }
      catch (InterruptedException ex) {
        log.error(ex,ex);
      RepConstants.writeERROR_FILE(ex);
      }
    }else{
      stopScheduledThread();
    }
    }
  }
  private void stopScheduledThread(){
    Thread scheduleThread = (Thread) threadMap.get(scheduleName);
      if(scheduleThread!=null){
        Thread thread = (Thread) threadMap.remove(scheduleName);
        thread.stop();
      }
    }
}
