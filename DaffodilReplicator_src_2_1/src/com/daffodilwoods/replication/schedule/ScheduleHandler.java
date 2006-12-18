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
import java.util.*;

import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;
import org.apache.log4j.Logger;
/**
 *
 * <p>Title: </p>
 * <p>Description:This class Starts the thread for Scheduling.Whenever the replication Server starts
* it starts scheduling in seperate thread.This classs is used for:start schedule,edit schedule and remove schedule.
*  </p>
 * <p>Copyright:
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */
public class ScheduleHandler {
  _Subscription sub;
  _ReplicationServer repServer;
  Connection con;
  AbstractDataBaseHandler dbHandler;
  ArrayList schedulerList = new ArrayList();
  HashMap map = new HashMap();
  String subName = null;
  String scheduleName = null;
  String scheduleType=null;
  String publicationServerName = null;
  String publicationPortNo = null;
  String recurrenceType = null;
  String replicationType = null;
  long scheduleTime = 0;
  int counter = 0;
  ScheduledSynchronizer scheduledSynchronizer;
  Thread t;
  protected static Logger log =Logger.getLogger(ScheduleHandler.class.getName());

  public ScheduleHandler(_ReplicationServer repServer0,
                         AbstractDataBaseHandler dbHandler0) throws RepException {
    log.debug("intializing ScheduleHandler");
    repServer = repServer0;
    dbHandler = dbHandler0;
    con = repServer.getDefaultConnection();
    Statement st = null;
    ResultSet rs = null;
    try {

       st = con.createStatement();
       rs = st.executeQuery("select * from " +
                                     dbHandler.getScheduleTableName()); //select query
      while (rs.next()) {
        scheduleName = rs.getString(1);
        subName = rs.getString(2);
        scheduleType=rs.getString(3);
        publicationServerName = rs.getString(4);
        publicationPortNo = rs.getString(5);
        recurrenceType = rs.getString(6);
        replicationType = rs.getString(7);
        scheduleTime = rs.getLong(8);
        counter = rs.getInt(9);

        scheduledSynchronizer = new
            ScheduledSynchronizer(repServer,dbHandler,subName, scheduleName, scheduleType, publicationServerName,
                                  publicationPortNo, recurrenceType,
                                  replicationType, scheduleTime, counter,map);
         t = new Thread(scheduledSynchronizer,
                              (scheduleName));
        map.put(scheduleName, t);
        log.info("Schedule "+scheduleName+" going to start");
        t.start();
      }
    }catch (SQLException ex) {
      log.debug(ex);
    }
    catch(RepException ex1){
      log.debug(ex1,ex1);
      throw ex1;
    }
    catch (Exception ex) {
      log.debug(ex);
    } finally {
      try {
        if (st != null) {
          st.close();
        }
        if (rs != null) {
          rs.close();
        }
      }
      catch (SQLException ex2) {
      }
    }
  }
  /**
   * startSchedule is called after saving Schedule information in Rep_ScheduleTable and when user edits a schedule
   * @param subName0 String
   * @param scheduleName0 String
   * @param remoteServerName String
   * @param remotePortNo String
   * @param recurrenceType0 String
   * @param repType String
   * @param schTime long
   * @param counter0 int
   * @throws RepException
   */
  public void startSchedule(String subName0, String scheduleName0,String  scheduleType0,
                            String remoteServerName, String remotePortNo,
                            String recurrenceType0, String repType, long schTime,
                            int counter0) throws RepException {
    subName = subName0;
    scheduleName = scheduleName0;
    scheduleType= scheduleType0;
    subName = subName0;
    scheduleName = scheduleName0;
    publicationServerName = remoteServerName;
    publicationPortNo = remotePortNo;
    recurrenceType = recurrenceType0;
    replicationType = repType;
    scheduleTime = schTime;
    counter = counter0;
     scheduledSynchronizer = new
        ScheduledSynchronizer(repServer,dbHandler,subName, scheduleName,scheduleType, publicationServerName,
                              publicationPortNo, recurrenceType, repType,
                              scheduleTime, counter,map);

    t = new Thread(scheduledSynchronizer,
                          (scheduleName));
    map.put(scheduleName, t);
    log.info("Schedule "+scheduleName+" going to start");
    t.start();
  }
  /**
   * This method drop the schedule by deleting the schedule information from schedule table and
   * stopping the Schedule thread
   * @param subName0 String
   * @throws RepException
   */
  public void dropSchedule(String subName0) throws
      RepException {
    Statement st =null;
    ResultSet rs =null;
    try {
      subName = subName0;
      st = con.createStatement();
      StringBuffer select =new StringBuffer();
      select.append("select " + RepConstants.schedule_Name + " from  ")
          .append(dbHandler.getScheduleTableName())
          .append(" where ").append(RepConstants.subscription_subName1)
          .append(" = '").append(subName).append("'");
       rs =  st.executeQuery(select.toString());
      if(!(rs.next())){
        throw new RepException("REP203", new Object[] {scheduleName});
      }
      scheduleName = rs.getString(1);
      Thread scheduleThread = (Thread) map.get(scheduleName);
      if(scheduleThread!=null){
        scheduledSynchronizer.stopSchedule=true;
//         Thread thread =(Thread) map.remove(scheduleName);
//         System.out.println("thread before stopping::" + thread);
//        thread.stop();
//        System.out.println("thread after stopping::" + thread);
      }
       StringBuffer query=new StringBuffer();
      query=query.append("delete from " ).append(dbHandler.getScheduleTableName())
                         .append(" where ").append(RepConstants.subscription_subName1)
                         .append(" = '").append(subName).append( "'");
      st.executeUpdate(query.toString());
log.info("remove schedule query "+query.toString());
      Utility.createTransactionLogFile =true;
    }
    catch (SQLException ex) {
//      ex.printStackTrace();
//      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP204", new Object[] {scheduleName,
                             ex.getMessage()});
    }catch(RepException ex1){
      throw ex1;
    }
    catch (Exception ex2) {
      RepConstants.writeERROR_FILE(ex2);
    } finally {
      try {
        if (st != null)
          st.close();
        if(rs!=null)
          rs.close();
      }
      catch (SQLException ex3) {
      }
    }
  }
  /**
   * Whenever user edits the schedule information,this methods first stops the running thread then update the schedule information in
   * Rep_ScheduleTable then start the schedule again by giving call to startSchedule() method.
   * (only publisher server name and publisher port no. is allowed to be edited)
   *
   * @param scheduleName0 String
   * @param subName0 String
   * @param newPubServerName String
   * @param newPubPortNo String
   * @throws RepException
   */
  public void editSchedule(String scheduleName0, String subName0,
                           String newPubServerName, String newPubPortNo) throws
      RepException {
   Statement st =null;
   ResultSet rs =null;
    try {
      subName = subName0;
      scheduleName = scheduleName0;
      st = con.createStatement();
      Thread scheduleThread = (Thread) map.get(scheduleName);
      if(scheduleThread!=null){
            scheduledSynchronizer.stopSchedule=true;
//         Thread thread =(Thread) map.remove(scheduleName);
//        thread.stop();
      }
      StringBuffer query=new StringBuffer();
      query=query.append("Update ").append(dbHandler.getScheduleTableName())
                       .append(" set ").append(RepConstants.publication_portNo)
                       .append(" = '" ).append(newPubPortNo).append("',").append(RepConstants.publication_serverName3)
                       .append(" = '").append(newPubServerName)
                       .append("' where " )
                       .append(RepConstants.subscription_subName1)
                       .append(" = '").append(subName).append("' and ").append( RepConstants.schedule_Name)
                       .append(" = '").append(scheduleName).append("'");
          int updateQuery=st.executeUpdate(query.toString());
          log.info("edit schedule query "+query.toString());
          if(updateQuery==0){
            throw new RepException("REP221", new Object[] {scheduleName});
          }

        StringBuffer query1=new StringBuffer();
        query1=query1.append("select * from ").append(dbHandler.getScheduleTableName())
                                         .append(" where ").append(RepConstants.schedule_Name )
                                         .append(" = '").append(scheduleName).append( "' and " )
                                         .append(RepConstants.subscription_subName1 )
                                         .append(" = '").append(subName).append( "'");

        rs = st.executeQuery(query1.toString()); //select query
        while (rs.next()) {
          scheduleName = rs.getString(1);
          subName= rs.getString(2);
          scheduleType=rs.getString(3);
          publicationServerName = rs.getString(4);
          publicationPortNo = rs.getString(5);
          recurrenceType = rs.getString(6);
          replicationType = rs.getString(7);
          scheduleTime = rs.getLong(8);
          counter = rs.getInt(9);
        }
      startSchedule(subName, scheduleName, scheduleType,publicationServerName,
                    publicationPortNo, recurrenceType, replicationType,
                    scheduleTime, counter);
    }
    catch (SQLException ex) {
//      RepConstants.writeERROR_FILE(ex);
      throw new RepException("REP219",new Object[] {scheduleName,subName});
    }catch(RepException ex1){
      throw ex1;
    }catch(Exception ex){
      RepConstants.writeERROR_FILE(ex);
    } finally {
      try {
        if (st != null)
          st.close();
          if(rs!=null)
          rs.close();
      }
      catch (SQLException ex2) {
      }
    }
  }

}
