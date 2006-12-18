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

/**
 * _Publication is an interface implemented by Publication class.
 * It holds abstraction of all the relevant methods, needed for the completion of
 * creating publication. It holds the abstraction of the methods which set the
 * publication parameters as well as which create Replication system tables
 * (and triggers) for storing publication information and needed for replication.
 * This Interface actually is the bound over the user's access on the methods of
 * the publication class , as the user can access only those methods which are
 * declared here.
 *
 */

public interface _Publication {

    final static String subscriber_wins = "subscriber_wins";
    final static String publisher_wins = "publisher_wins";

    //Delete all entries from shadow table and delete the xml file
    static boolean xmlAndShadow_entries = true;
    void setConflictResolver(String conflictReolver0) throws RepException;

    void setFilter(String tableName0, String filterClause0) throws RepException;

   void setFilter(String tableName0, String filterClause0,int paramCount) throws RepException;

   void setCreateShadowTable(String tableName0,boolean createShadowTable)throws RepException;

   void publish() throws RepException;
  /**
   * This method sets the columns whose values are to be ignored while taking snapshot,synchronizing etc.
   * @param tableName String
   */
  void setIgnoredColumns(String tableName,String columnNamesToBeIgnored[]) throws RepException;

  void unpublish() throws RepException;

  void addTableToPublication(String[] newTableList,String[] filterClauses) throws RepException ;

  void dropTableFromPublication(String[] dropTableList) throws RepException;
 }
