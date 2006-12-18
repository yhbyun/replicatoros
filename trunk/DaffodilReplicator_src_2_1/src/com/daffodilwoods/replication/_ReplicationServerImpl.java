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

/**
 * It is called at the time of:
 * subscribe.
 * snapshot.
 * synchronize... For getting the publication's instance.
 *
 */

public interface _ReplicationServerImpl extends Remote
{
    _PubImpl getRemotePublication(String pubName) throws RemoteException, RepException;

    public String getServerName() throws RemoteException;

    public String getRemoteAddress() throws RemoteException, RepException ;

}
