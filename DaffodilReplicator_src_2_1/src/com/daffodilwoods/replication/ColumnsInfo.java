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

import java.util.*;

/**
 * This is a supporting class of different database handlers. It  helps in storing
 * and getting the column's information.
 *
 */

public class ColumnsInfo
{

    private String columnName;
    private String typeDeclaration;
    private static HashMap colTypeMap;

    public ColumnsInfo(String columnName0, String typeDeclaration0)
    {

        columnName = columnName0;
        typeDeclaration = typeDeclaration0;
        if (colTypeMap == null)
        {
            colTypeMap = new HashMap();
        }
        colTypeMap.put(columnName, typeDeclaration);
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getDataTypeDeclaration()
    {
        return typeDeclaration;
    }

    public static String getDataTypeDeclaration(String colName)
    {
        return (String) colTypeMap.get(colName);
    }

    /*  private TypeInfo typeInfo;
      private String typeName;
      //private int sqlType;
      private int columnSize;
      //private String defaultValue;
      //private String nullable;
      //private boolean optsize;

     public ColumnsInfo(String columnName0, TypeInfo typeInfo0,  int columnSize0) {
        columnName=columnName0;
        typeInfo = typeInfo0;
        typeName=typeInfo0.getTypeName();
        //sqlType=typeInfo0.getSqlType();
        columnSize=columnSize0;
      }

      public ColumnsInfo(TypeInfo typeInfo0,  int columnSize0) {
        typeInfo = typeInfo0;
        typeName=typeInfo0.getTypeName();
        //sqlType=typeInfo0.getSqlType();
        columnSize=columnSize0;
      }

      public ColumnsInfo(String columnName0, String typeName0, int sqlType0,  int columnSize0) {
        columnName=columnName0;
        typeName=typeName0;
        sqlType=sqlType0;
        columnSize=columnSize0;
      }

      public ColumnsInfo(String columnName0, String typeName0, int sqlType0,  int columnSize0, String defaultValue0, String nullable0) {
        columnName=columnName0;
        typeName=typeName0;
        sqlType=sqlType0;
        columnSize=columnSize0;
        defaultValue = defaultValue0;
        nullable = nullable0;
      }

      public String getDataTypeName() {
        return typeName;
      }

//  public String getDataTypeNameWithOptionalSize() {
//    if(columnSize==-1 || columnSize==0)
     public String getDataTypeNameWithOptionalSize(AbstractDataBaseHandler dbh) {
//    if(!dbh.isDataTypeOptionalSizeSupported(typeinfo))
//      return typeName;
//    return columnSize!=-1 ? typeName+"("+columnSize+")" : typeName;
//    typeIno
        return typeInfo.getTypeDeclaration(columnSize);
      }*/
}
