/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.dag

import org.apache.spark.ml.{Estimator, PipelineStage, Transformer}
import org.apache.spark.sql.Dataset

import scala.collection.mutable

/**
  * DatasetBFS
  *
  * @param nodes
  * @param edges
  * @author belbis
  * @since 1.0.0
  */
private[ml] class DatasetBFS(val nodes: List[Node[PipelineStage]], val edges: List[Edge])
  extends BFS[PipelineStage](nodes, edges) {

  private val _datasets = mutable.Map.empty[String, Dataset[_]]

  /**
    * visit
    *
    * Executes a [[Transformer]] transform or [[Estimator]] fit function
    *
    * @param n
    * @return
    */
  override def visit(n: Node[PipelineStage]): Node[PipelineStage] = {
    super.visit(n)
    n match {
      case i: IdentityNode[_] => // aka root node
        i.identity match {
          case d: Dataset[_] => _datasets(i.id) = d
          case _ =>
            val err = s"Invalid IdentityNode ${i.identity.getClass} found, Dataset required."
            throw new RuntimeException(err)
        }
        i
      case j: JoinerNode =>
        _datasets(j.id) = j.value.setRight(datasets(j.right)).transform(datasets(j.left))
        j
      case p: Node[PipelineStage] =>
        val pParents = parents(p)

        // validate parents
        require(pParents.nonEmpty && pParents.size == 1)

        val parentDataset = datasets(pParents.head.id)
        val transformer = p match {
          case e: EstimatorNode[_] =>
            TransformerNode(e.value.fit(parentDataset))
          case t: TransformerNode => t
        }
        _datasets(p.value.uid) = transformer.value.transform(parentDataset)
        transformer
    }
  }

  def datasets: Map[String, Dataset[_]] = _datasets.toMap
}
