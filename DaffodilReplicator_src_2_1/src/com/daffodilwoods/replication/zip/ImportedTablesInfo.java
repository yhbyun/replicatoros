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


package com.daffodilwoods.replication.zip;

import java.util.ArrayList;

public class ImportedTablesInfo {
  private ArrayList listOfDirectAscendents;
  private ArrayList listOfAllAscendents;
  private boolean cyclic;

  public ImportedTablesInfo() {
  }

  public boolean isCyclic() {
    return cyclic;
  }
  public void setIsCyclic(boolean cyclic0) {
    cyclic = cyclic0;
  }

  public ArrayList getListOfDirectAscendents() {
    return listOfDirectAscendents;
  }

  public void setListOfDirectAscendents(ArrayList listOfDirectAscendents0) {
    listOfDirectAscendents = listOfDirectAscendents0;
  }
  public ArrayList getListOfAllAscendents() {
    return listOfAllAscendents;
  }
  public void setListOfAllAscendents(ArrayList listOfAllAscendents0) {
    listOfAllAscendents = listOfAllAscendents0;
  }


}
