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
import java.math.*;
import java.util.*;
import com.daffodilwoods.replication.RepException;



public class DirectedGraph
    implements DGraph {

  protected static Logger log = Logger.getLogger(DirectedGraph.class.getName());

 //  The total amount of vertices in the graph.
  private int totalVerticesGraph;

  /**
   The total amount of edges in the graph. Needs to
   be <CODE>long</CODE>, because more than
   <CODE>Integer.MAX_VALUE</CODE> edges are allowed.
   (Special measures are taken in this case, though.)
   */
  private long EdgesInGraph;

  // The current cylcle status when searching the entire graph.
  private boolean cycleStatus;


  // A linked list of all vertex objects in the graph,
  // for iterative access. (Link to beginning of list)

  private ListElement mainVertexList;

 // A linked list of all edge objects in the graph,
 // for iterative access. (Link to beginning of list)

  private ListElement mainEdgeList;


   //The main hash table, which is an array of list elements,
   //with a length that is always prime.
  private ListElement[] mainHashTable;


  ArrayList tablesInCycle=new ArrayList();


  /**
   A directed graph with weighted edges. Constructed with an
   integer, which is the first size of the hash table.
   */

  public DirectedGraph(int hashTableInit) {

    totalVerticesGraph = 0;
    EdgesInGraph = 0;
    mainVertexList = null;
    mainEdgeList = null;
    if (hashTableInit < 11) hashTableInit = 11;
    mainHashTable = new ListElement[hashTableInit];
    for (int i = 0; i < mainHashTable.length; i++) {
      mainHashTable[i] = new ListElement();
    }
  }


  /**
    Does the hashing. Finds the
    place where the object goes or is.
    It wants an embedded vertex object
    as its parameter, and it will return the
    list element<UL>
    <LI>where the object is or</LI>
    <LI>the element BEFORE the element where
    the object should go.</LI></UL>
    In other words, it returns the list element
    where the search terminates in the hash table.
    @return ListElement instance where the object
        is, or after which the object should come, the
    testing of which case is the reality should be
    done at the caller
    @param objInVertex the object inside the vertex
    that we want to find
   */

        public ListElement searchVertex(Object objInVertex) {
          ListElement endOfSearchChain;
          Vertex vertex = null;
          int index = Math.abs(objInVertex.hashCode()) %mainHashTable.length;
          endOfSearchChain = mainHashTable[index];
          if (endOfSearchChain.next == null) {
            return endOfSearchChain;
          }
          while (endOfSearchChain.next != null) {
            endOfSearchChain = endOfSearchChain.next;
            vertex = (Vertex) endOfSearchChain.hangingVertexOrEdge;
            if (vertex.vertexObject.equals(objInVertex)) {
              return endOfSearchChain;
            }
          }
    return endOfSearchChain;
  }

  /**
    This method is used when we know that the vertex
    is NOT in the hash table, and we want to keep all
    edge associations. (during a full reHash() operation)
    @param the JbVertex we wish to reinsert into table.
    @return <tt>true</tt> if the hashing proceeds as normal.
   */

  private boolean hashVertex(Vertex vertexToHash) {

    ListElement hanger = new ListElement(vertexToHash);
    ListElement previous = searchVertex(vertexToHash.vertexObject);
    previous.next = hanger;
    return true;
  }

  /**
    Finds the edge-object between 2 vertices.
    @param first The origin of the edge
    @param second The destination of the edge
    @throws java.lang.IllegalArgumentException if either of the
    param vertices are not found in this graph
    @return the edge from first to second, null if no edge from
    first to second
   */

  private Edge searchEdge(Object first, Object second)  {
    //finding vertex1
    ListElement targetLE = searchVertex(first);
    Vertex firstVertex = (Vertex) targetLE.hangingVertexOrEdge;
    if (firstVertex == null) {
      return null;
//      throw new IllegalArgumentException("First param-vertex not found.");
    }
    if (!firstVertex.vertexObject.equals(first)){
      return null;
//      throw new IllegalArgumentException("First param-vertex not found.");
    }
    //finding vertex2
    targetLE = searchVertex(second);
    Vertex secondVertex = (Vertex) targetLE.hangingVertexOrEdge;
    if (secondVertex == null ){
       return null;
//      throw new IllegalArgumentException("Second param-vertex not found.");
    }
    if (!secondVertex.vertexObject.equals(second)){
       return null;
//      throw new IllegalArgumentException("Second param-vertex not found.");
    }
    //searching edge chain for Edge:(1 --> 2)
    Edge edge;
    for (ListElement i = firstVertex.firstEdge; i != null; i = i.next) {
      edge = (Edge) i.hangingVertexOrEdge;
      if (edge.targetVertex == secondVertex)
        return edge;
    }
    return null;
  }

  /**
   Called when the addVertex-method sees that the load
   of the hash table is > 1. Allocates a new hash table
   with a length of at least 2x+1, where x is the old
   length. The new length is (also) prime. Then iterates
   through all vertex objects in graph and relocates them
   into the new table accordingly.
   @return <tt>true</tt> if all vertices successfully rehashed
   into a new hash table
   */

  private boolean reHash() {

    BigInteger ourPrime =
        new BigInteger(String.valueOf(mainHashTable.length * 2 + 1));
    BigInteger two = new BigInteger("2");
    //probability that we end up with a non-prime
    //is 1/32 (2^n where n=5, prob=rather small)
    while (!ourPrime.isProbablePrime(5)) {
      ourPrime = ourPrime.add(two);
    }
    //test if the new number is bigger than Integer.MAX_VALUE
    //use "longValue" because "intValue" would not work, obviously
    if (ourPrime.longValue() > Integer.MAX_VALUE) {
      //then make the table size Integer.MAX_VALUE
      mainHashTable = new ListElement[Integer.MAX_VALUE];
    }
    else {
      //make the table size
      mainHashTable = new ListElement[ourPrime.intValue()];
    }
    for (int k = 0; k < mainHashTable.length; k++) {
      mainHashTable[k] = new ListElement();
    }
    //go throught the full vertex list and
    //hash each into the new table
    for (ListElement j = mainVertexList; j != null; j = j.next) {
      if (!hashVertex( (Vertex) j.hangingVertexOrEdge))
        return false;
    }
    return true;
  }

  /**
    Recursively seeks a cycle starting from the
    vertex given as a parameter. Marks its way
    through the vertices by setting the marker-
    attribute in each vertex-object.
    @param Vertex where the search begins.
   */

  public void findCycle(Vertex startVertex) {
     if (cycleStatus == true)return;
     if (startVertex.graphMarker > 0) {
       cycleStatus = true;
       return;
     }
     startVertex.graphMarker++;
     Edge current;
     for (ListElement i = startVertex.firstEdge; i != null; i = i.next) {
       current = (Edge) i.hangingVertexOrEdge;
       tablesInCycle.add(current.targetVertex);
       findCycle(current.targetVertex);
       current.targetVertex.graphMarker--;
     }
  }

  /**
        Clears all vertex markers
   */

  private void clearAllMarkers() {
    Vertex vertex;
    for (ListElement i = mainVertexList; i != null; i = i.next) {
      vertex = (Vertex) i.hangingVertexOrEdge;
      vertex.graphMarker = 0;
    }
  }

  /**
    This method draws a "picture" of the main hash table.
    It works nicely if the max chain length is ~< 4.
    Of course it sorta scrolls the screen for big
    graphs, so.. directing to a file isn't a bad
    option sometimes. It does NOT attempt to draw any
    kind of representation of the graph or edges or
    anything complex like that. It can help in debugging
    matters pertaining to the hash table structure.
   */

  public String toString() {
    String result = "";
    Double load = new Double( (double) totalVerticesGraph /
                             (double) mainHashTable.length);
    Vertex vertex;
    for (int i = 0; i < mainHashTable.length; i++) {
      for (ListElement j = mainHashTable[i]; j != null; j = j.next) {
        vertex = (Vertex) j.hangingVertexOrEdge;
        if (vertex == null) {
          result += " nil";
        }
        else {
          if (vertex.vertexObject != null)
            result += "LEobj"; // vertex.vertexObject);
        }
        if (j.next != null) result += "-> ";
        if (j.next == null) result += "-|\n";
      }
    }
    result += "Vertex Total:    " + this.totalVerticesGraph + "\n";
    result += "Hash Table size: " + this.mainHashTable.length + "\n";
    result += "Load factor :    " + load + "\n";
    return result;
  }

//---------------------------------------------------------------------
//  Predefined methods required in implementation. (interface)
//---------------------------------------------------------------------
  /**
     Compares the specified object with this directed graph for equality.
     Returns <tt>true</tt> if the specified object is also a directed graph
     and the two directed graphs have the same vertices and edges.
     @param obj the object to be compared for equality with this directed
     graph.
     @return <tt>true</tt> if the specified Object is equal to this directed
     graph.
   */

  public boolean equals(Object obj) {
    DGraph other;
    try {
      other = (DGraph) obj;
    }
    catch (java.lang.ClassCastException e) {
      return false;
    }

    if ( (other.vertexCount() != this.totalVerticesGraph) ||
        (other.edgeCount() != this.EdgesInGraph)) {
      return false;
    }
    //since equal number of vertices is now tested, we
    //can jump out if total==0 (both==0)
    if (totalVerticesGraph == 0)return true;
    Vertex vertex;
    Edge edge;
    ListElement edgeLE;
    for (ListElement i = mainVertexList; i != null; i = i.next) {
      vertex = (Vertex) i.hangingVertexOrEdge;
      Object thisObj = vertex.vertexObject; //shorthand assignment
      if (!other.hasVertex(thisObj))return false;
      //testing each edge from this vertex
      edgeLE = vertex.firstEdge; //"edge-list-element"
      for (edgeLE = vertex.firstEdge; edgeLE != null; edgeLE = edgeLE.next) {
        edge = (Edge) edgeLE.hangingVertexOrEdge;
        if (!other.hasEdge(thisObj, edge.targetVertex.vertexObject))return false;
        if (other.edgeWeight(thisObj, edge.targetVertex.vertexObject) !=
            edge.whatIsWeight()) {
          return false;
        }
      }
    }
    //All vertices and edges tested to be equal.
    //And we're still here. So it must be equal.

    return true;
  }

  /**
     Returns the hash code value for this directed graph.  The hash code of a
     directed graph is defined to be the sum of the hash codes of the vertices
     in the directed graph. The sum is allowed to overflow if needed. This
     definition ensures that if <code>s1.equals(s2)</code> then
     <code>s1.hashCode() == s2.hashCode()</code> for any two directed graphs
     <code>s1</code> and <code>s2</code>, as required by the general contract
     of the <tt>java.lang.Object.hashCode</tt> method.
     @return the hash code value for this directed graph.
   */

  public int hashCode() {
    int code = 0;
    Vertex vertex;
    for (ListElement i = mainVertexList; i != null; i = i.next) {
      vertex = (Vertex) i.hangingVertexOrEdge;
      code += vertex.vertexObject.hashCode();
    }
    return code;
  }

  /**
     Adds the specified vertex to this directed graph if the vertex is not
     null and the vertex is not already present in this directed graph. More
     formally, adds the specified vertex <code>obj</code> to this directed
     graph if <code>obj != null</code> and this directed graph contains no
     vertex <code>e</code> such that <code>obj.equals(e)</code>. The number of
     vertices in this directed graph may not exceed <tt>Integer.MAX_VALUE</tt>.
     @param obj the vertex to be added to this directed graph.
     @return <tt>true</tt> if the specified vertex was added to this directed
     graph.
     @throws java.lang.IllegalStateException if this directed graph contains
     the maximum number of vertices.
     @throws java.lang.IllegalArgumentException if a specified vertex is null.
   */

  public boolean addVertex(Object obj) throws RepException {
    if (obj == null){
       throw new RepException("REP020",new Object[]{"","Param-vertex was null."});
    }
    if (obj == this){
      throw new RepException("REP020",new Object[]{"","Param-vertex cannot be this graph."});
    }
    if (totalVerticesGraph + 1 > Integer.MAX_VALUE){
       throw new RepException("REP020",new Object[]{"","Vertex count exceeds Integer.MAX_VALUE!"});
    }
    if (totalVerticesGraph + 1 > mainHashTable.length) reHash();
    Vertex theNewVertex = new Vertex(obj);
    ListElement place = searchVertex(obj);
    if (place.hangingVertexOrEdge != null) {
      Vertex lastFound = (Vertex) place.hangingVertexOrEdge;
      if (lastFound.vertexObject.equals(obj)) {
        return false; //already exists in table!!
      }
    }
    ListElement newHashLE = new ListElement(theNewVertex);
    place.next = newHashLE;
    ListElement newListLE = new ListElement(theNewVertex);
    newListLE.next = mainVertexList;
    mainVertexList = newListLE;
    totalVerticesGraph++;
    return true;
  }

  /**
     Returns <tt>true</tt> if this directed graph contains the specified
     vertex.  More formally, returns <tt>true</tt> if and only if
     <code>obj != null</code> and this directed graph contains a vertex
     <code>e</code> such that <code>obj.equals(e)</code>.
     @param obj the vertex whose presence in this directed graph is to be
     tested.
     @return <tt>true</tt> if this directed graph contains the specified
     vertex.
     @throws java.lang.IllegalArgumentException if the specified vertex is
     null.
   */

  public boolean hasVertex(Object obj) {
    if (obj == null) {
      return false;
//      throw new IllegalArgumentException("Param-vertex is null.");
    }
    Vertex vertex;
    ListElement requested = searchVertex(obj);
    vertex = (Vertex) requested.hangingVertexOrEdge;
    if (vertex == null)
      return false; //no chain even exists at table[hash(obj)]
    if (vertex.vertexObject.equals(obj)) {
      return true;
    }
    return false; //object was not in collision chain
  }

  /**
     Returns <tt>true</tt> if there are vertices in this directed graph.
     @return <tt>true</tt> if there are vertices in this directed graph.
   */

  public boolean hasVertices() {
    return (totalVerticesGraph > 0);
  }

  /**
     Returns the number of vertices in this directed graph.
     @return the number of vertices in this directed graph.
   */

  public int vertexCount() {
    return (int) totalVerticesGraph;
  }

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

  public Object[] verticesToArray() {
    Object[] arrayOfAllVertices = new Object[totalVerticesGraph];
    int j = 0; // alternate counter for array
    Vertex vertex;
    for (ListElement i = mainVertexList; i != null; i = i.next) {
      vertex = (Vertex) i.hangingVertexOrEdge;
      arrayOfAllVertices[j++] = vertex.vertexObject;
    }
    return arrayOfAllVertices;
  }

  /**
     Adds an edge of a specified weight from the vertex <tt>first</tt> to the
     vertex <tt>second</tt>.
     @param first the vertex that the edge leaves from.
     @param second the vertex that the edge enters.
     @param weight the non-negative weight of the edge.
     @return <tt>true</tt> if the specified vertex was added to this directed
     graph, <tt>false</tt> if there already is an edge from the vertex
     <tt>first</tt> to the vertex <tt>second</tt>.
     @throws java.lang.IllegalArgumentException if a specified vertex was null
     or not found in this directed graph, or if the specified weight was
     negative.
   */

  public boolean addEdge(java.lang.Object first, java.lang.Object second,
                         int weight) throws RepException {
    if (weight < 0){
      throw new RepException("REP020",new Object[]{"","Weight was negative."});
    }
    if (hasEdge(first, second))return false;
    //finding target vertex
    ListElement requestedLE = searchVertex(second);
    Vertex targetVertex = (Vertex) requestedLE.hangingVertexOrEdge;
    if (targetVertex == null){
      throw new RepException("REP020",new Object[]{"","Second param-vertex (target) not found."});
    }
    //finding source vertex
    requestedLE = searchVertex(first);
    Vertex sourceVertex = (Vertex) requestedLE.hangingVertexOrEdge;
    if (sourceVertex == null){
      throw new RepException("REP020",new Object[]{"","First param-vertex (source) not found."});
    }
    //creating new edge, then creating 2 LEobjects with it.
    Edge theNewEdge = new Edge(targetVertex, weight);
    ListElement hangerInVertex = new ListElement(theNewEdge);
    ListElement hangerInList = new ListElement(theNewEdge);
    //adding hangerInVertex to beginning of edge chain inside vertex
    hangerInVertex.next = sourceVertex.firstEdge;
    sourceVertex.firstEdge = hangerInVertex;
    //incrementing  out degree of child
    sourceVertex.addOutDegree();
    //incrementing  in degree of parent
    targetVertex.addInDegree();
    //adding listLEobject to beginning of
    //main linked edge list
    hangerInList.next = mainEdgeList;
    mainEdgeList = hangerInList;
    EdgesInGraph++;
    return true;
  }

  /**
     Returns <tt>true</tt> if this directed graph contains an edge that leaves
     from the vertex <tt>first</tt> and enters the vertex <tt>second</tt>.
     @param first the vertex that the edge should leave from.
     @param second the vertex that the edge should enter.
     @return <tt>true</tt> if this directed graph contains an edge that leaves
     from the vertex <tt>first</tt> and enters the vertex <tt>second</tt>.
     @throws java.lang.IllegalArgumentException if a specified vertex was null
     or not found in this directed graph.
   */

  public boolean hasEdge(java.lang.Object first, java.lang.Object second) {
    if (searchEdge(first, second) != null)return true;
    //no exceptions actually need to be thrown here, as
    //the real work is doen by the helper method findEdge()
    return false;
  }

  /**
     Returns the weight of the edge that leaves from the vertex <tt>first</tt>
     and enters the vertex <tt>second</tt>.
     @param first the vertex that the edge should leave from.
     @param second the vertex that the edge should enter.
     @return the weight of the edge that leaves from the vertex <tt>first</tt>
     and enters the vertex <tt>second</tt>. Returns -1 if there is no such
     edge.
     @throws java.lang.IllegalArgumentException if a specified vertex was null
     or not found in this directed graph.
   */

  public int edgeWeight(java.lang.Object first, java.lang.Object second){
    Edge targetEdge = searchEdge(first, second);
    if (targetEdge == null)return -1;
    return targetEdge.whatIsWeight();
  }

  /**
     Returns the number of edges that enter the specified vertex.
     @param vertex the vertex that the edges should enter.
     @return the number of edges that enter the specified vertex.
     @throws java.lang.IllegalArgumentException if a specified vertex was null
     or not found in this directed graph.
   */

  public int inDegree(java.lang.Object obj) throws RepException {
    //check if the obj is null
    if (obj == null){
       throw new RepException("REP020",new Object[]{"","Param vertex object was null."});
    }
    ListElement requested;
    requested = searchVertex(obj);
    Vertex vertex = (Vertex) requested.hangingVertexOrEdge;
    if (vertex == null) {
       throw new RepException("REP020",new Object[]{"","Param vertex not found in graph. (hash index empty)"});
    }
    if (vertex.vertexObject.equals(obj)) {
      return vertex.whatIsInDegree();
    }
    else {
      //was not found in hash table
        throw new RepException("REP020",new Object[]{"","Param vertex not found in graph. (not in collision chain)"});
    }
  }

  /**
     Returns the number of edges that leave from the specified vertex.
     @param obj the vertex that the edges should leave from.
     @return the number of edges that leave from the specified vertex.
     @throws java.lang.IllegalArgumentException if a specified vertex was null
     or not found in this directed graph.
   */

  public int outDegree(java.lang.Object obj) throws RepException {
    //check if the obj is null
    if (obj == null){
        throw new RepException("REP020",new Object[]{"","Param vertex object was null."});
    }
    ListElement requested;
    requested = searchVertex(obj);
    Vertex vertex = (Vertex) requested.hangingVertexOrEdge;
    if (vertex == null) {
      throw new RepException("REP020",new Object[]{"","Param vertex not found in graph. (hash index empty)"});
    }
    if (vertex.vertexObject.equals(obj)) {
      return vertex.whatIsOutDegree();
    }
    else {
      //was not found in hash table
      throw new RepException("REP020",new Object[]{"","Param vertex not found in graph. (not in collision chain)"});
    }
  }

  /**
     Returns an array containing all vertices that are adjacent to the
     specified vertex. A vertex v is adjacent to a vertex u if there is an
     edge that leaves from u and enters v.
     <p>
     The returned array will be "safe" in that no references to it are
     maintained by this directed graph. In other words, this method must
     allocate a new array even if this collection is backed by an array. The
     caller is thus free to modify the returned array.
     @param obj the specified vertex.
     @return an array containing all vertices that are adjacent to the
     specified vertex.
     @throws java.lang.IllegalArgumentException if a specified vertex was null
     or not found in this directed graph.
   */

  public Object[] adjacentsOf(java.lang.Object obj) throws  RepException {
    if (obj == null){
      throw new RepException("REP020",new Object[]{"","Null parameter specified."});
    }
    //finding source vertex
    ListElement requested = searchVertex(obj);
    Vertex vertex = (Vertex) requested.hangingVertexOrEdge;
    if (vertex == null) {
      throw new RepException("REP020",new Object[]{"","Param vertex not found in graph. (hash index empty)"});
    }
    if (vertex.vertexObject.equals(obj)) {
      Object[] arrayOfAdjacents = new Object[vertex.whatIsOutDegree()];
      ListElement theEdgeHanger = vertex.firstEdge;
      Edge theEdge;
      for (int i = 0; i < arrayOfAdjacents.length; i++) {
        theEdge = (Edge) theEdgeHanger.hangingVertexOrEdge;
        arrayOfAdjacents[i] = theEdge.targetVertex.vertexObject;
        theEdgeHanger = theEdgeHanger.next;
      }
      return arrayOfAdjacents;
    }
    else {
      //was not found in hash table
      throw new RepException("REP020",new Object[]{"","Param vertex not found in graph. (not in collision chain)"});
    }
  }

  /**
     <p>Returns <tt>true</tt> if the specified vertex is isolated. A vertex is
     isolated if there are no edges leaving from or entering the vertex, or
     the only edge leaving from the vertex enters the vertex itself and the
     only edge entering the vertex leaves from the vertex itself.
     @param obj the vertex that is to be tested for isolation.
     @return <tt>true</tt> if the specified vertex is isolated.
     @throws java.lang.IllegalArgumentException if a specified vertex was null
     or not found in this directed graph.
   */

  public boolean isIsolated(java.lang.Object obj) throws RepException {
    if (obj == null){
       throw new RepException("REP020",new Object[]{"","The parameter given was null."});
    }
    ListElement targetLE = searchVertex(obj);
    Vertex thisVertex = (Vertex) targetLE.hangingVertexOrEdge;
    if (thisVertex == null){
       throw new RepException("REP020",new Object[]{"","Specified vertex not found in graph. (hash index empty)"});
    }
    if (thisVertex.vertexObject != obj){
       throw new RepException("REP020",new Object[]{"","Specified vertex not found in graph. (not in collision chain)"});
    }
    if ( (thisVertex.whatIsInDegree() == 0) &&
        (thisVertex.whatIsOutDegree() == 0))return true;
// if it is a self loop i.e  A<-->A
    if ( (thisVertex.whatIsInDegree() == 1) &&
        (thisVertex.whatIsOutDegree() == 1)) {
      targetLE = thisVertex.firstEdge;
      Edge onlyEdge = (Edge) targetLE.hangingVertexOrEdge;
      if (onlyEdge.targetVertex == thisVertex)return true;
    }
    return false;
  }

  /**
     Returns <tt>true</tt> if there are edges in this directed graph.
     @return <tt>true</tt> if there are edges in this directed graph.
   */

  public boolean hasEdges() {
    return (EdgesInGraph > 0);
  }

  /**
     Returns the number of edges in this directed graph. If this directed
     graph contains more than <tt>Integer.MAX_VALUE</tt> edges, returns
     <tt>Integer.MAX_VALUE</tt>.
     @return the number of edges in this directed graph.
   */

  public int edgeCount() {
    if (EdgesInGraph > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    else {
      return (int) EdgesInGraph;
    }
  }

  /**
     Returns the sum of the weights of all edges in this directed graph. If
     the sum is more than <tt>Integer.MAX_VALUE</tt>, returns
     <tt>Integer.MAX_VALUE</tt>.
     @return the sum of the weights of all edges in this directed graph.
   */

  public int edgeWeight() {
    //check for edges then
    //create some vars for intermediate storage
    if (this.EdgesInGraph == 0)return 0;
    long sum = 0;
    Edge edge;
    //go through the mainEdgeList
    //increment sum
    for (ListElement i = mainEdgeList; i != null; i = i.next) {
      edge = (Edge) i.hangingVertexOrEdge;
      if (sum + edge.whatIsWeight() > Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }
      sum += edge.whatIsWeight();
    }
    return (int) sum;
  }

  /**
     Returns an array of all of the vertices in this directed graph in some
     topological sort order. <p>
     The returned array will be "safe" in that no references to it are
     maintained by this directed graph. In other words, this method must
     allocate a new array even if this collection is backed by an array. The
     caller is thus free to modify the returned array.
     @return an array of all of the vertices in this directed graph in some
     topological sort order. Returns null if no topological sort is possible.
   */

  public Object[] topologicalSort() {
    if (hasCycle() == true) {
      //cycle found, no topological sort possible
      return null;
    }
    //we don't creat the array until we have checked
    //no cycles exist.
    Object[] topologicalArray = new Object[totalVerticesGraph];
    topoIndex = 0;
    Vertex vertex;
    //Start going through vertices, setting all markers to indegree,
    //when =0, put into array and sub-seek adjacents, decrementing
    //simultaneously and recursively putting all who == 0 after decr.
    for (ListElement n = mainVertexList; n != null; n = n.next) {
      vertex = (Vertex) n.hangingVertexOrEdge;
      if (vertex.graphMarker == 0) vertex.graphMarker = vertex.whatIsInDegree();
      if (vertex.graphMarker == 0) {
        //after the inDegree is assigned, if this vertex
        //STILL has 0 (no "dependents") we can call put()
        putAndDecrement(topologicalArray, vertex);
      }
    }
    clearAllMarkers();
    return topologicalArray;
  }

  /**
    Helper variable to keep track of topoligical sort
    array indexing. A global var, passing it around in
    recursive calls would be very messy.
   */

  private int topoIndex;

  /**
    A recursive helper method to put found vertex into array,
    and decrement adjacents. It is necessary to pass the array
    we are constructing around from method call to method call
    in order to maximize effectiveness, this way it's not a
    global variable, in case we don't ever need to create it.
    @param theArray the array of vertices that we are constructing
    @param start the vertex where we are starting this sub-call
   */

  private void putAndDecrement(Object[] theArray, Vertex start) {
    //putting the vertex object into the next free spot in array
    theArray[topoIndex++] = start.vertexObject;
    //when the vertex has been put into array we mark it with -1
    start.graphMarker = -1;
    Edge edge;
    //now we are going to go through the edge-list and decrement
    //all the vertex-inDegrees (markers) by one,
    //and if we decrement any of them to 0, we'll call this
    //method recursively again to put that vertex into the array
    //next.
    for (ListElement i = start.firstEdge; i != null; i = i.next) {
      edge = (Edge) i.hangingVertexOrEdge;
      if (edge.targetVertex.graphMarker == 0) {
        //this is a case where the topologicalSort()
        //method has not got this far yet, so we need
        //to do som of its work for it, and mark the inDegree.
        edge.targetVertex.graphMarker = edge.targetVertex.whatIsInDegree();
      }
      //decrementing the adjacent vertex
      edge.targetVertex.graphMarker--;
      if (edge.targetVertex.graphMarker == 0) {
        //if 0, we can now move on through this edge to
        //put its vertex recursively as well
        putAndDecrement(theArray, edge.targetVertex);
      }
    }
  }

  /**
     Returns <tt>true</tt> if there is a cycle in the directed graph.
     @return <tt>true</tt> if there is a cycle in the directed graph.
   */

 public boolean hasCycle() {
    cycleStatus = false;
    Vertex eachVertexInTurn;
    for (ListElement i = mainVertexList; (i != null) && (cycleStatus != true);i = i.next) {
      eachVertexInTurn = (Vertex) i.hangingVertexOrEdge;
      findCycle(eachVertexInTurn);
      if (cycleStatus) {
         //newList to add first vertex from which traversing of graph was started
         ArrayList newList = new ArrayList();
         newList.add(eachVertexInTurn);
         newList.addAll(tablesInCycle);
         tablesInCycle.clear();
         tablesInCycle.addAll(newList);
       }
      clearAllMarkers();
    }
    if (cycleStatus == false)return false;
    return true;
  }

  /**
     Returns <tt>true</tt> if the directed graph forms a linear list. A
     directed graph with no vertices or one vertex without a self loop is
     also considered to form a linear list.
     @return <tt>true</tt> if the directed graph forms a linear list.
   */

  public boolean isLinearList() {
    //we start by checking some obvious
    //conditions, the simplest ones
    if (totalVerticesGraph == 0)return true;
    if (totalVerticesGraph == 1) {
      //if only one vertex, check for cycle
      // then return the result and we're done
      if (EdgesInGraph == 1) {
        if (hasCycle() == true)return false;
        return true;
      }
    }
    //after having established the above conditions,
    //the next obvious sign that it's not a linear list
    //is if there isn't EXACTLY one less edge than vertices.
    if (totalVerticesGraph != (EdgesInGraph + 1))return false;
    //now we start looking for a place to start the checking (in=0)
    //if at any time we find a vertex with inDegree or outDegree
    //of more than 1, we can quit and return false
    Vertex vertex;
    Vertex startVertex = null;
    for (ListElement n = mainVertexList; n != null; n = n.next) {
      vertex = (Vertex) n.hangingVertexOrEdge;
      if ( (vertex.whatIsInDegree() > 1) || (vertex.whatIsOutDegree() > 1)) {
        return false;
      }
      if (vertex.whatIsInDegree() == 0) {
        //if this is not the only vertex with in=0, return false
        if (startVertex != null)return false;
        startVertex = vertex;
      }
    }
    //now we have a either a link to the "first"
    //vertex in the supposed linear list,
    //or then we have a null
    if (startVertex == null)return false;
    if (hasCycle() == true)return false;
    //if all of the above conditions are done,
    //and we are still here, then our graph is a
    //linear list
    return true;
  }

//return ArrayList of tables in cycle
public ArrayList TablesInCycle(){
  return tablesInCycle;
}
}
