/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.dag

import scala.collection.mutable

/**
  * BFS
  *
  * [[Iterator]] that performs breadth-first-search given nodes and edges.
  * This is not meant to be used with large graphs.
  *
  * @param nodes
  * @param edges
  * @param attemptLimit max number of attempt to enqueue node in traversal - so as to prevent infinite looping.
  * @tparam T
  *
  * @author belbis
  * @since 1.0.0
  */
class BFS[T](nodes: List[Node[T]], edges: List[Edge], attemptLimit: Int = 10)
  extends Iterator[Node[T]] {

  lazy val roots: List[Node[T]] = nodes.filterNot(n => edges.map(_.to).contains(n.id))
  private lazy val toVisit: mutable.Queue[Node[T]] = {
    val q = mutable.Queue.empty[Node[T]]
    q.enqueue(roots: _*)
    q
  }
  private val _visited = mutable.ListBuffer.empty[Node[T]]
  private val attempts = mutable.Map.empty[String, Int]

  /**
    * visited
    *
    * Accessor for private _visited.
    *
    * @return
    */
  def visited: List[Node[T]] = _visited.toList

  /**
    * hasNext
    *
    * Check whether there are any more elements to visit.
    *
    * @return
    */
  override def hasNext: Boolean = toVisit.nonEmpty

  /**
    * next
    *
    * visits and returns the next node
    *
    * @return the next element
    * @throws NoSuchElementException no more elements found
    */
  override def next(): Node[T] = {
    if (!hasNext) throw new NoSuchElementException
    val cur = toVisit.dequeue()
    if (parents(cur).forall(visited.contains)) {
      children(cur).filterNot {
        c => visited.contains(c) || toVisit.contains(c)
      }.foreach(toVisit.enqueue(_))
      visit(cur)
    } else if (attempts.getOrElseUpdate(cur.id, 0) < attemptLimit) {
      attempts(cur.id) += 1
      toVisit.enqueue(cur)
      next()
    }
    else {
      throw new RuntimeException(s"Node ${cur.id} enqueued too many times.")
    }
  }

  /**
    * visit
    *
    * default is noop
    *
    * @param n
    */
  def visit(n: Node[T]): Node[T] = {
    _visited += n
    n
  }

  /**
    * children
    *
    * Find the children of the given node.
    *
    * @param n
    * @return
    */
  def children(n: Node[T]): List[Node[T]] = {
    val ids = edges.filter(_.from == n.id).map(_.to).toSet
    nodes.filter(i => ids.contains(i.id))
  }

  /**
    * parents
    *
    * Find the parents of the given node.
    *
    * @param n
    * @return
    */
  def parents(n: Node[T]): List[Node[T]] = {
    val ids = edges.filter(_.to == n.id).map(_.from).toSet
    nodes.filter(i => ids.contains(i.id))
  }

}
