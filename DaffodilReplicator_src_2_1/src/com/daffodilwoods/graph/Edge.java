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


package com.daffodilwoods.graph;

import org.apache.log4j.Logger;
import java.util.*;

/**
  Instances of this class act as edges in the graph. Instances of this
  class always "hang" from a <CODE>JbListElement</CODE> instance. They
  have a <CODE>int weight</CODE> and a link to the vertex object to
  which they lead. Null target vertices are not supported.
 */

public class Edge {

  protected static Logger log = Logger.getLogger(Edge.class.getName());
  // The weight of this edge.
  private int weight;

  //  The vertex where the edge leads to.
  public Vertex targetVertex;


  private ArrayList attributesList;

  /**
     An edge object leading from one vertex to another.
     Edges are unique, there can only be one edge
     from vertex A to vertex B. The weight cannot be changed
     after the object is constructed.
   */

  public Edge(Vertex target, int weightValue, ArrayList listOfAttributes) {
    this.weight = weightValue;
    targetVertex = target;
    this.attributesList = listOfAttributes;
  }

  public Edge(Vertex target, int weightValue) {
    this.weight = weightValue;
    targetVertex = target;
    attributesList = new ArrayList();
  }



  /**
   Return the weight of the edge.
  */

  public int whatIsWeight() {
    return this.weight;
  }

  public void addAttribute(String[] attributes) {
    if(attributesList ==null)
      attributesList= new ArrayList();
    attributesList.add(attributes);
  }

  public ArrayList getAttributesList() {
    return this.attributesList;
  }
}

