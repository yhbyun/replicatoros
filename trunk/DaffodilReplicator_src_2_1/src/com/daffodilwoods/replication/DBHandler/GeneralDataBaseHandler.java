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

package com.daffodilwoods.replication.DBHandler;

import java.sql.*;
import java.util.*;

import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.column.*;
import org.apache.log4j.Logger;
public class GeneralDataBaseHandler extends AbstractDataBaseHandler
{
  protected static Logger log=Logger.getLogger(GeneralDataBaseHandler.class.getName());

    public GeneralDataBaseHandler()
    {}

    public GeneralDataBaseHandler(ConnectionPool connectionPool0) {
      connectionPool = connectionPool0;
      vendorType = Utility.DataBase_General;
    }


    protected void createSuperLogTable(String pubName) throws SQLException,
        RepException
    {}

    protected void createRepTable(String pubName) throws SQLException,
        RepException
    {}

    public void createShadowTable(String pubsubName, String tableName,
                                  String allColSequence,String[] primaryColumns) throws RepException
    {}

    public void createShadowTableTriggers(String pubName, String tableName,
                                          ArrayList colNameDataType,
                                          String[] primCols) throws RepException
    {}

    public boolean isDataTypeOptionalSizeSupported(TypeInfo typeInfo)
    {
        new Exception(" NOT IMPLENMENTED. ").printStackTrace();
        return false;
    }

    public void setTypeInfo(TypeInfo typeInfo, ResultSet rs) throws RepException,
        SQLException
    {}

    public AbstractColumnObject getColumnObject(TypeInfo typeInfo) throws
        RepException
    {
        throw new RepException("", null);
    }

    public boolean isPrimaryKeyException(SQLException ex) throws SQLException
    {
        return false;
    }

    protected void createIndex(String pubsubName,String tableName) throws RepException {

    }


    public int getAppropriateScale(int columnScale) {
          return columnScale;
        }
        public  PreparedStatement makePrimaryPreperedStatement(String[] primaryColumns,String shadowTable,String local_pub_sub_name) throws SQLException, RepException {
           return null;
         }




    public boolean isForiegnKeyException(SQLException ex) throws SQLException {
      return false;
    }

  /**
   * makeCommonPreparedStatement
   *
   * @param shadowTable String
   * @param local_pub_sub_name String
   * @return PreparedStatement
   */
  public PreparedStatement makeCommonPreparedStatement(String shadowTable,
      String local_pub_sub_name) {
    return null;
  }
  protected void createIgnoredColumnsTable(String pubName) throws SQLException,
             RepException { }

  /**
   * createTrackReplicationTablesUpdationTable
   *
   * @param pubSubName String
   */
  protected void createTrackReplicationTablesUpdationTable(String pubSubName) {
  }

  /**
   * createTriggerForTrackReplicationTablesUpdationTable
   *
   * @param pubSubName String
   */
  protected void createTriggerForTrackReplicationTablesUpdationTable(String
      pubSubName) {
  }

  public PreparedStatement makePrimaryPreperedStatementBackwardTraversing(
      String[] primaryColumns, long lastId, String local_pub_sub_name,
      String shadowTable) throws SQLException, RepException {
    return null;
  }
protected void createTrackPrimaryTable(String pubsubName,String[] primCols) throws RepException {

}

  /**
   * isSchemaSupported
   *
   * @return boolean
   */
  public boolean isSchemaSupported() {
    return true;
  }
}
