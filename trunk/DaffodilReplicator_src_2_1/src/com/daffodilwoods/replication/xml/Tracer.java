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

package com.daffodilwoods.replication.xml;

import java.sql.*;

/**
 * This class is used in handling the case of tracing the records at the time of
 * shadow table search for the records with same pk  or with same common_id.
 * Tracer keeps track of this trace.
 */

public class Tracer
{

    public String type;
    public boolean recordFound;
    public ResultSet rs;
    public Object[] oldRow;
    public Object[] primaryKeyValues;
    public Tracer()
    {
    }

    /**
     * set original record from shadow table only once during call for getLastReocrd in operationUpdate or OperationDelete
     * @param rs
     * @throws SQLException
     */
    public void setOldRow(ResultSet rs) throws SQLException
    {
        if (oldRow != null)
        {
            return;
        }
        int count = rs.getMetaData().getColumnCount();
        oldRow = new Object[count - 5];
        for (int i = 5, j = 0; i <= count - 1; i++)
        {
            oldRow[j++] = rs.getObject(i);
        }

    }

}
