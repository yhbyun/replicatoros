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

package com.daffodilwoods.replication.column;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.daffodilwoods.replication.xml.*;

/**
 * This class is the abstract class which is extended by all the different
 * type of ColumnObject classses. This is implemented so that different types
 * of data can be written differently by different type of databases.
 * This is most  useful in case of Blob and Clob type of datatype, in which
 * data is written in to saperate files.
 *
 */

public abstract class AbstractColumnObject
{

    protected BlobOutPutStream blobst = null;
    protected ClobOutPutStream clobst = null;

    public void setBlobHandlerObject(BlobOutPutStream bo0)
    {
//    RepPrinter.print(bo0.hashCode() + " Inside AbstractColumnObject setBlobHandlerObject method ");
        blobst = bo0;
    }

    public void setClobHandlerObject(ClobOutPutStream co0)
    {
//    RepPrinter.print(co0.hashCode() +" Inside AbstractColumnObject setClobHandlerObject method ");
        clobst = co0;
    }

    abstract public void setColumnObject(PreparedStatement pst, XMLElement value,
                                         int index) throws SQLException;

    abstract public void setColumnObject(PreparedStatement pst, String value,
                                         int index) throws SQLException;

    /**
     * This method is overridden by different ColumnObject classes.
     * So that data can be written differently for different datatypes.
     * It is best suitable for Blob & Clob datatypes where data is
     * written in the saperate files.
     *
     * @param os
     * @param rs
     * @param index
     * @throws SQLException
     * @throws IOException
     */

    abstract public void write(Writer bw, ResultSet rs, int index,ArrayList encodedCols,String col) throws
        SQLException, IOException;

    abstract public void writeUpdate(Writer bw, ResultSet rows,
                                     ResultSet oldResultSet, int index,
                                     HashMap modifiedColumnsMap,
                                     String columnName,ArrayList encodedCols) throws SQLException,
        IOException;

    abstract public Object getObject(String value) throws SQLException;

}
