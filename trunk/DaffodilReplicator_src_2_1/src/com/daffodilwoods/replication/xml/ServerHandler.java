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

import org.xml.sax.*;
import org.xml.sax.helpers.*;
import com.daffodilwoods.replication.*;

public class ServerHandler extends DefaultHandler
{
    XMLElement currentElement;
    Connection subConnection;
    Statement statement;

    public ServerHandler(Connection connection)
    {
        try
        {
            currentElement = new XMLElement("root");
            subConnection = connection;
            statement = subConnection.createStatement();
        }
        catch (SQLException ex)
        {
            RepConstants.writeERROR_FILE(ex);
        }

    }

    public void startElement(String namespace, String localname, String qname,
                             Attributes atts) throws SAXException
    {
        XMLElement childElement = new XMLElement(qname);
        currentElement.addChild(childElement);
        childElement.setParentElement(currentElement);
        currentElement = childElement;
    }

    public void endElement(String namespace, String localname, String qname) throws
        SAXException
    {
        if (qname.equalsIgnoreCase("row"))
        {
            createQuery();
        }
        if (qname.equalsIgnoreCase("tablename"))
        {
            currentElement.elementList.clear();
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
    }

    /** @todo
     *  Implement it */
    public void createQuery()
    {
        // ALL code to be written
//        try {
//            String tableName  = currentElement.getParentElement().elementValue;
//            System.out.println("Query for Table > "+ tableName);
//
//            statement.execute("delete from "+tableName);
//            StringBuffer query = new StringBuffer("Insert into "
//                    +tableName);
//            query.append(" values ");
//            ArrayList elements = currentElement.getChildElements();
//            StringBuffer columns = new StringBuffer(" ( ");
//            StringBuffer values = new StringBuffer(" ( ");
//            for (int i = 0; i < elements.size(); i++) {
//                columns.append(((XMLElement)elements.get(i)).elementName);
//                values.append(((XMLElement)elements.get(i)).elementValue);
//            }
//            query.append(columns.toString()+" )");
//            query.append(values.toString()+" )");
//            System.out.println("SNAPSHOT HANDELER  Insert Query   "+ query.toString());
//
//            statement.execute(query.toString());
//
//        }
//        catch (Exception ex) {
//            System.out.println(" EXCPTION IN CREATE QUERY ");
//            throw ex;
//        }

    }

}
