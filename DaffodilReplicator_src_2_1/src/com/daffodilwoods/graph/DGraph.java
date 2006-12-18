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

import java.util.ArrayList;
import com.daffodilwoods.replication.RepException;
/**
   A directed graph with weighted edges. The weights are non-negative. The
   directed graph can contain any object as a vertex. Null references are
   not allowed. A vertex can have a self loop, i. e. an edge that leaves from
   and enters the same vertex. There cannot be more than
   Integer.MAX_VALUE vertices in the directed graph. Removing vertices
   from the directed graph is not supported. The directed graph is not
   thread-safe: it must not be used concurrently by multiple threads.
   <p>
   The behavior of the directed graph is not specified if the value of an object
   is changed in a manner that affects comparisons by the equals(object) method
   while the object is a vertex in the graph. It is prohibited for a directed
   graph to contain itself as a vertex.
 */

public interface DGraph {


    //   Returns true if the specified object is also a directed graph
    //   and the two directed graphs have the same vertices and edges.


    public boolean equals(Object obj) ;

    /**
       Returns the hash code value for this directed graph.  The hash code of a
       directed graph is defined to be the sum of the hash codes of the vertices
       in the directed graph. The sum is allowed to overflow if needed. This
       definition ensures that if <code>s1.equals(s2)</code> then
       <code>s1.hashCode() == s2.hashCode()</code> for any two directed graphs
       <code>s1</code> and <code>s2</code>, as required by the general contract
       of the <tt>java.lang.Object.hashCode</tt> method.
     */

    public int hashCode();

    /**
       Adds the specified vertex to this directed graph if the vertex is not
       null and the vertex is not already present in this directed graph. More
       formally, adds the specified vertex <code>obj</code> to this directed
       graph if <code>obj != null</code> and this directed graph contains no
       vertex <code>e</code> such that <code>obj.equals(e)</code>. The number of
       vertices in this directed graph may not exceed <tt>Integer.MAX_VALUE</tt>.
     */

    public boolean addVertex(Object object) throws RepException;

    /**
       Returns <tt>true</tt> if this directed graph contains the specified
       vertex.  More formally, returns <tt>true</tt> if and only if
       <code>obj != null</code> and this directed graph contains an vertex
       <code>e</code> such that <code>obj.equals(e)</code>.
     */

  public boolean hasVertex(Object ob);


    //   Returns true if there are vertices in this directed graph.

    public boolean hasVertices();

    /**
       Returns the number of vertices in this directed graph.
     */

    public int vertexCount();

    /**
       Returns an array containing all vertices in this directed graph. The
       vertices need not be in any special order.
       <p>
       The returned array will be "safe" in that no references to it are
       maintained by this directed graph. In other words, this method must
       allocate a new array even if this collection is backed by an array. The
       caller is thus free to modify the returned array.
       @return an array containing all vertices in this directed graph.
     */

    public Object[] verticesToArray();

    /**
       Adds an edge of a specified weight from the vertex <tt>first</tt> to the
       vertex <tt>second</tt>.
     */

    public boolean addEdge(Object first, Object second, int weight) throws RepException;

    /**
       Returns <tt>true</tt> if this directed graph contains an edge that leaves
       from the vertex <tt>first</tt> and enters the vertex <tt>second</tt>.
     */

  public boolean hasEdge(Object first, Object second) ;

    /**
       Returns the weight of the edge that leaves from the vertex <tt>first</tt>
       and enters the vertex <tt>second</tt>.
     */

  public int edgeWeight(java.lang.Object first, java.lang.Object second) ;

    /**
       Returns the number of edges that enter the specified vertex.
     */

  public int inDegree(Object object) throws RepException;

    /**
       Returns the number of edges that leave from the specified vertex.
     */

  public int outDegree(Object vertex) throws RepException;

    /**
       Returns an array containing all vertices that are adjacent to the
       specified vertex. A vertex v is adjacent to a vertex u if there is an
       edge that leaves from u and enters v.
       <p>
       The returned array will be "safe" in that no references to it are
       maintained by this directed graph. In other words, this method must
       allocate a new array even if this collection is backed by an array. The
       caller is thus free to modify the returned array.
     */

  public Object[] adjacentsOf(Object ob) throws RepException;

    /**
       <p>Returns <tt>true</tt> if the specified vertex is isolated. A vertex is
       isolated if there are no edges leaving from or entering the vertex, or
       the only edge leaving from the vertex enters the vertex itself and the
       only edge entering the vertex leaves from the vertex itself.
     */

  public boolean isIsolated(Object ob) throws RepException;

    /**
       Returns <tt>true</tt> if there are edges in this directed graph.
       @return <tt>true</tt> if there are edges in this directed graph.
     */

    public boolean hasEdges();

    /**
       Returns the number of edges in this directed graph. If this directed
       graph contains more than <tt>Integer.MAX_VALUE</tt> edges, returns
       <tt>Integer.MAX_VALUE</tt>.
     */

    public int edgeCount();

    /**
       Returns the sum of the weights of all edges in this directed graph. If
       the sum is more than <tt>Integer.MAX_VALUE</tt>, returns
       <tt>Integer.MAX_VALUE</tt>.
     */

  public int edgeWeight() ;

    /**
       Returns an array of all of the vertices in this directed graph in some
       topological sort order.
       <p>
       The returned array will be "safe" in that no references to it are
       maintained by this directed graph. In other words, this method must
       allocate a new array even if this collection is backed by an array. The
       caller is thus free to modify the returned array.
       @return an array of all of the vertices in this directed graph in some
       topological sort order. Returns null if no topological sort is possible.
     */

    public Object[] topologicalSort();

    /**
       Returns <tt>true</tt> if there is a cycle in the directed graph.
       @return <tt>true</tt> if there is a cycle in the directed graph.
     */

     public boolean hasCycle();

    /**
       Returns <tt>true</tt> if the directed graph forms a linear list. A
       directed graph with no vertices or one vertex without a self loop is
       also considered to form a linear list.
     */

     public boolean isLinearList();
}

