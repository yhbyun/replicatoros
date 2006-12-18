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
   /**
        Instances of this class act as elements of a linked list. It is used for keeping
        <CODE>Vertex</CODE>-instances and <CODE>Edge</CODE>-instances in order.
	<UL><LI>The collision management strategy of the hash table,</LI>
	<LI>the general vertex list of the graph,</LI>
	<LI>and the edges of a single vertex</LI>
	</UL>are all kept in a linked list of of <CODE>ListElement</CODE> instances.
    */

    public class ListElement{

        protected static Logger log = Logger.getLogger(Edge.class.getName());

       // A link to the next element in the linked list.
        public ListElement next;

        /**
		A link to the actual object represented by this "hanger" instance.
		Usually an instance of <CODE>Vertex</CODE> or <CODE>Edge</CODE>.
        */
        public Object hangingVertexOrEdge;


	/**
		The null constructor gets called when we
		initialize the hash table. We attach null-
		containing-list elements to each hash index, for
		ease of coding. (If you want to understand why
		this is important, try to visualize the return
		value of the method <CODE>findVertex</CODE>
		without guarantee that each index in the hash
		table will have at least one
		<CODE>ListElement</CODE>...)
	*/

	public ListElement(){
	  next=null;
	  hangingVertexOrEdge=null;
	}

	/**
		The list element is a general linked list
		element. It contains a link to the next element,
		and a hanger for the vertex or the edge that
		it represents.
		@param objToHang The object that we wish to "hang"
		in this list element, usually an instance of
		<CODE>Vertex</CODE> or <CODE>Edge</CODE>
	*/

        public ListElement(Object objToHang){
	  next=null;
	  hangingVertexOrEdge=objToHang;
        }



        public String toString(){
          String str=hangingVertexOrEdge.toString();
          String s=str;
          ListElement le= this;
          while(s!=null){
            le= le.next;
            if(le!=null)
            str+=le.hangingVertexOrEdge;
          else
              s=null;
          }
          return str;
        }
    }

