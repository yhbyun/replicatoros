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

import java.util.*;

import org.xml.sax.*;
import org.xml.sax.helpers.*;
import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.MetaDataInfo;

public class DDLHandler extends DefaultHandler
{

    XMLElement currentElement = new XMLElement(""); ;
    private Subscription subscription;
    private ArrayList tables;
    private String tableName;
    private String filterClause;
    private ArrayList repTables;
    private HashMap primColMap;
    private ArrayList primCols;
    private ArrayList foreignKeyCols;
    private ArrayList schemas;
    private String conflictResolver;
    private MetaDataInfo mdi;
    private String createShadowTable;
    private String cyclicDependency;
    private ArrayList columnsToBeIgnored;

    private ArrayList alterTableAddForeignKeyQueries;
    /**
     * This class is the implementation class for ContentHandler for parsing the
     * structure XML file. This class implements differnt event methods which automatically
     * are called by the parser. It implementsthese methods to get and use the different
     * values stored on the XML file. The methods are implemented for getting the shemas,
     * queries, other stored information needed for subscribing.
     */

    public DDLHandler(Subscription subscription0, ArrayList schemas0,
                      ArrayList tables0, String conflictResolver0,
                      HashMap primColMap0,MetaDataInfo mdi0)
    {
        subscription = subscription0;
        schemas = schemas0;
        tables = tables0;
        conflictResolver = conflictResolver0;
        primColMap = primColMap0;
        mdi =mdi0;
        repTables = new ArrayList();
    alterTableAddForeignKeyQueries = new ArrayList();
    }

    public void startElement(String namespace, String localname,
                             String qname, Attributes atts) throws SAXException
    {
        XMLElement childElement = new XMLElement(qname);
        currentElement.addChild(childElement);
        childElement.setParentElement(currentElement);
        currentElement = childElement;
        if (qname.equalsIgnoreCase("PrimaryColumns"))
        {
            primCols = new ArrayList();
        }
    else if (qname.equalsIgnoreCase("ForeignKeyColumns")) {
      foreignKeyCols = new ArrayList();
    }
    else if (qname.equalsIgnoreCase("IgnoredColumns")) {
      columnsToBeIgnored = new ArrayList();
    }

    }

    public void endElement(String namespace, String localname, String qname) throws
        SAXException
    {
        if (qname.equalsIgnoreCase("Database"))
        {
            subscription.setSubscriptionTables(repTables);
      subscription.setAlterTableAddFKStatements(alterTableAddForeignKeyQueries);
        }
        if (qname.equalsIgnoreCase("SchemaName"))
        {
            schemas.add(currentElement.elementValue);
        }
        if (qname.equalsIgnoreCase("Table"))
        {
            createRepTable();
            primColMap.put(tableName.toLowerCase(), primCols);
            primCols = null;
        }
        if (qname.equalsIgnoreCase("ColumnName"))
        {
            primCols.add(currentElement.elementValue);
        }
    if (qname.equalsIgnoreCase("FKColumnName")) {
      foreignKeyCols.add(currentElement.elementValue);
    }
    if (qname.equalsIgnoreCase("Query")) {
            tables.add(currentElement.elementValue);
        }
    if (currentElement.elementName.equalsIgnoreCase(
        "AlterTableForeignKeyStatement")) {
      alterTableAddForeignKeyQueries.add(currentElement.elementValue);
    }
    if (qname.equalsIgnoreCase("IgnoredColumnName")) {
      columnsToBeIgnored.add(currentElement.elementValue);
    }

        XMLElement parentElement = currentElement.getParentElement();
        currentElement = parentElement;
    }

    public void characters(char[] ch, int start, int len)
    {
        String elementValue = new String(ch, start, len);
        if (elementValue.equalsIgnoreCase("") || elementValue.equalsIgnoreCase("\n"))
        {
            return;
        }
        currentElement.setElementValue(elementValue);
        if (currentElement.elementName.equalsIgnoreCase("TableName"))
        {
            tableName = currentElement.elementValue;
        }
    if (currentElement.elementName.equalsIgnoreCase("CreateShadowTable")) {
      createShadowTable = currentElement.elementValue;
    }
    if (currentElement.elementName.equalsIgnoreCase("CyclicDependency")) {
      cyclicDependency = currentElement.elementValue;
    }
    if (currentElement.elementName.equalsIgnoreCase("FilterClause")) {
      if (currentElement.elementValue.equalsIgnoreCase("NO_DATA")) {
                filterClause = null;
            }
            else
            {
                filterClause = currentElement.elementValue;
            }
        }
    }

    private void createRepTable() throws SAXException
    {
        SchemaQualifiedName sname = new SchemaQualifiedName(mdi,tableName);
        RepTable repTable = new RepTable(sname, RepConstants.subscriber);
        repTable.setFilterClause(filterClause);
        repTable.setCreateShadowTable(createShadowTable);
        repTable.setCyclicDependency(cyclicDependency);
        repTable.setConflictResolver(conflictResolver);
       //MetaDataInfo mdi = new MetaDataInfo(con);
        //mdi.setPrimaryColumns(repTable,sname.getSchemaName(),sname.getTableName());
        repTable.setPrimaryColumns( (String[]) primCols.toArray(new String[0]));
    if (foreignKeyCols.size() > 0)
//System.out.println("DDHandler foreignKeyCols : "+foreignKeyCols);
      repTable.setForeignKeyCols( (String[]) foreignKeyCols.toArray(new String[0]));
    if (columnsToBeIgnored.size() > 0) {
      repTable.setColumnsToBeIgnored( (String[]) columnsToBeIgnored.toArray(new
          String[0]));
    }
        repTables.add(repTable);
    }
}
