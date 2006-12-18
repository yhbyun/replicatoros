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

import com.daffodilwoods.replication.DBHandler.*;

/**
 * _Replicator is an interface implemented by both publisher and subscriber.
 * It makes these classes to implement all the methods which are called
 * at the time of synchronization to get the subscriber's or publisher's information.
 *
 */

public interface _Replicator
{
    public RepTable getRepTable(String tableName) throws RepException;

    public AbstractDataBaseHandler getDBDataTypeHandler();

    public String getPub_SubName();
}
