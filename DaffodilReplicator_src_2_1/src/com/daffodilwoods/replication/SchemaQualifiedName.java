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

public class SchemaQualifiedName
{

    private String catalog;
    private String schema;
    private String tableName;

    /**
     * This class is responsible for keeping catalog, schema and tablename sapearte.
     * It is used to find out the schema name or catalog name or table name from the
     * specified formate tablename.
     * It performs all saperation and conjunction operations over above specified
     * fields.
     */

  public SchemaQualifiedName() {
    }

    public SchemaQualifiedName(MetaDataInfo mdi, String catalog0, String schema0,String tableName0) {
      catalog = catalog0;
      if (schema0 != null) {
        if (mdi instanceof pgMetaDataInfo) {
          schema = schema0;
        }
        else {
          schema = schema0.toUpperCase();
        }
      }
      tableName = tableName0;
    }

  public SchemaQualifiedName(MetaDataInfo mdi, String identifier) {
    catalog = mdi.getCatalogName(identifier);
    schema = mdi.getSchemaName(identifier);
    tableName = mdi.getTableName(identifier);
    }

  public String getCatalogName() {
        return catalog;
    }

  public String getSchemaName() {
        //return schema != null ? schema.toUpperCase() : null;
        return schema;
    }

  public String getTableName() {
        //return tableName != null ? tableName.toUpperCase() : null;
        return tableName;
    }


    public void setSchemaName(MetaDataInfo mds,String schemaName0)
    {
    	if(schemaName0!=null){
        if(mds instanceof pgMetaDataInfo) {
          schema = schemaName0;
    }
    else {
          schema = schemaName0.toUpperCase();
        }
       }
    }

  public boolean equals(Object obj) {
        SchemaQualifiedName sname = (SchemaQualifiedName) obj;
    if(schema!=null)
    return schema.equalsIgnoreCase(sname.getSchemaName()) && tableName.equalsIgnoreCase(sname.getTableName());
    else
    return  tableName.equalsIgnoreCase(sname.getTableName());
    }

  public int hashCode() {
    if(schema!=null)
    return  13 * schema.toUpperCase().hashCode() + 17 * tableName.toUpperCase().hashCode();
   else
     return 17 * tableName.toUpperCase().hashCode();
    }

  public String toString() {
    return getSchemaName() != null ? getSchemaName() + "." + getTableName() : getTableName();
    }

//  public String toString1() {
//    return getSchemaName()!=null ?
//                            getSchemaName()+".\""+getTableName()+"\""
//                            : "\""+getTableName()+"\"";
//  }
//  public static void main(String[] args) {
//    SchemaQualifiedName sname1 = new SchemaQualifiedName("sachin.agarwal.kumar");
//    SchemaQualifiedName sname2 = new SchemaQualifiedName("sachin.agarwal");
//    SchemaQualifiedName sname3 = new SchemaQualifiedName("sachin");
//    SchemaQualifiedName sname1 = new SchemaQualifiedName("a.b.c");
//    SchemaQualifiedName sname2 = new SchemaQualifiedName("a.b");
//    SchemaQualifiedName sname3 = new SchemaQualifiedName("a");
//  }
}
