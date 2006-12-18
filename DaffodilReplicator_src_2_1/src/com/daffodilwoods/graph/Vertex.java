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
        Instances of this class act as wrappers for the real vertex object, which
        can be for example Integer, or any object. Instances of this
        class always "hang" from a <CODE>ListElement</CODE> instance.
    */

    public class Vertex {


       protected static Logger log = Logger.getLogger(Vertex.class.getName());

	/**
		The number of edges this vertex has coming in
		from another vertex.
	*/
        private int inGraphEdges;

        /**
                The number of edges this vertex has going out
                to another vertex.
        */
        private int outGraphEdges;

        /**
                A link to the beginning of a linked list structure of
		the edges that leave this vertex.
        */
        public ListElement firstEdge;

        /**
                A link to the actual abject, which can be any Object,
		which this vertex instance "wraps".
        */
        public Object vertexObject;

        /**
                A Graph marker variable used for cycle detection, topological sorting
		and similar.
        */
	public int graphMarker;



        /**
                A wrapper class for the actual java.lang.Object to be
		situated in the graph.
		@param objToEmbed The java.lang.Object that we wish to have wrapped.
        */
        public Vertex(Object objToEmbed){

	  // Initialize all vars..
          inGraphEdges=0;
	  outGraphEdges=0;
	  graphMarker=0;
	  firstEdge=null;
	  vertexObject=objToEmbed;
	}

        public String toString() {
          return vertexObject.toString();
        }


        /**
		Returns the number of edges coming into this vertex.
		@return The in-degree of this vertex.
        */
	public int whatIsInDegree(){
		return this.inGraphEdges;
	}

        /**
		Returns the number of edges leaving this vertex.
		@return The out-degree of this vertex.
        */
        public int whatIsOutDegree(){
                return this.outGraphEdges;
        }

        /**
		Accessor method to increment in-degree by one.
        */
	public void addInDegree(){
		this.inGraphEdges++;
	}

        /**
		Accessor method to increment out-degree by one.
        */
	public void addOutDegree(){
		this.outGraphEdges++;
	}
    }
