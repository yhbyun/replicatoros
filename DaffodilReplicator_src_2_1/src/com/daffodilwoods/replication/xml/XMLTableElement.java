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

/**
 * This class is the representation of the XML elements , written in the XML files.
 * It stores different parameters of the XML element as it's value, attribute, parent
 * element, child element etc. This all helps at the time of parsing.
 *
 */

public class XMLTableElement extends XMLElement
{

  private XMLElement child;

    public XMLTableElement(String elementName0)
    {
      super(elementName0);
    }

    public void addChild(XMLElement child0)
    {
        child = child0;
    }

    public void setParentElement(XMLElement parentElement0)
    {
        parentElement = parentElement0;
    }

    public XMLElement getParentElement()
    {
        return parentElement;
    }

    public ArrayList getChildElements()
    {
        return elementList;
    }

    public void addAtt(String value)
    {
        attValue = value;
    }

    public String getAttribute()
    {
        return attValue;
    }

    public void setElementValue(String value0)
    {
        elementValue = elementValue + value0;
    }

//    private void createColumnTypeObject(int type, String typeName, int index)  {
//        columnTypes = new HashMap();
//        int[] varType = new int[]{-4,-3,-1,1,12,1111,2000}; // should be sorted
//
//        int[] dateType = new int[]{-4,-3,-1,1,12,1111,2000}; // should be sorted
//
//        int[] timeType = new int[]{-4,-3,-1,1,12,1111,2000}; // should be sorted
//
//        int isVar = java.util.Arrays.binarySearch(varType,type);
//    }

    public String toString()
    {
        StringBuffer s = new StringBuffer(" [ " + elementName + " - " +
                                          elementValue);
        for (int i = 0; i < elementList.size(); i++)
        {
            s.append( (XMLElement) elementList.get(i)).toString();
        }
        s.append(" ] ");
        return s.toString();
    }

}
