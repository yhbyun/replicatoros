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

import org.apache.log4j.Logger;

/**
 * This class is helpful for the multiple datatype handling in the multidatabse
 * environment. For each column this typeinfo object stores it's sqlType which
 * can be obtained from the JVM. Now by using the database handlers this typeInfo
 * is set to it's corresponding database datatype , this inforamtion is saved in
 * typeName. It also maintains information for the optional size supported datatypes.
 *
 */
public class TypeInfo
{

  protected static Logger log = Logger.getLogger(TypeInfo.class.
                                                 getName());

    private String typeName;
    private int sqlType;
    private boolean optsize;
    private int columnSize0 = -1;
    private int columnScale=-1;


    public TypeInfo(String typeName0, int sqlType0)
    {
        typeName = typeName0;
        sqlType = sqlType0;
    }

    public void setColumnSize(int columnSize)
    {
        columnSize0 = columnSize;
    }

    public int getcolumnSize()
    {
        return columnSize0;
    }

    public int getSqlType()
    {
        return sqlType;
    }

    public String getTypeName()
    {
        return typeName;
    }

  public void setSqlType(int sqlType0) {
        sqlType = sqlType0;
    }

  public void setTypeName(String typeName0) {
        typeName = typeName0;
    }

  public void setOptionalSizeProperty(boolean optsize0) {
        optsize = optsize0;
    }

  public String getTypeDeclaration(int size) {
    if (size == -1 || size == 0 || !optsize) {
            return typeName;
        }
        StringBuffer temp = new StringBuffer();
        if (columnScale != 0) {
          temp.append(typeName).append("( ").append(size).append(" , ").append(
              columnScale).append(" )");
        }
        else {
          temp.append(typeName).append("( ").append(size).append(" )");
        }
        return temp.toString();
    }

  public int hashCode() {
        return 17 * typeName.hashCode() + 47 * sqlType;
    }

    public String toString()
    {
      log.debug("[TYPEINFO[typename=" + typeName + "][sqlType=" + sqlType + "]]");
        return "[TYPEINFO[typename=" + typeName + "][sqlType=" + sqlType + "]]";
    }

    public void setColumnScale(int columnScale0)
   {
       columnScale = columnScale0;
   }

   public int getColumnScale()
   {
       return columnScale;
   }

}
