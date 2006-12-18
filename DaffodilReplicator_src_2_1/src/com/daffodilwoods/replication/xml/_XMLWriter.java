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

import java.io.*;
import java.sql.*;

import com.daffodilwoods.replication.*;
import java.util.ArrayList;

/**
 * This interface is implemented by the class XMLWriter.
 * This interface gives declaration of the method write , which is implemented
 * in XML writer , this method is responsible for writing data in to  XML file
 * as per there datatypes and special cases of Blob and Clob are also needed to
 * be handled.
 */

public interface _XMLWriter
{

    public void write(ResultSet rs, int index,ArrayList encodedCols,String col) throws SQLException, IOException,
        RepException;
}
