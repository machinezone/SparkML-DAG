/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.dag

import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
  * SchemaBFS
  *
  * [[BFS]] for schemas
  *
  * @param nodes
  * @param edges
  *
  * @author belbis
  * @since 1.0.0
  */
class SchemaBFS(val nodes: List[Node[PipelineStage]], val edges: List[Edge])
  extends BFS[PipelineStage](nodes, edges) {

  private val _schemas = mutable.Map.empty[String, StructType]

  /**
    * visit
    *
    * Store the transformed schema and return the node.
    *
    * @param n
    * @return
    */
  override def visit(n: Node[PipelineStage]): Node[PipelineStage] = {
    super.visit(n)
    n match {
      case i: IdentityNode[_] =>
        i.identity match {
          case st: StructType => _schemas(i.id) = st
          case ds: Dataset[_] => _schemas(i.id) = ds.schema
        }
      case j: JoinerNode =>
        _schemas(j.id) = StructType(schemas(j.left).union(schemas(j.right)))
      case p: Node[PipelineStage] =>
        val pParents = parents(p)

        // validate parents
        require(pParents.nonEmpty && pParents.size == 1)

        val parentSchema = _schemas(pParents.head.id)
        _schemas(p.value.uid) = p.value.transformSchema(parentSchema)
    }
    n
  }

  def schemas: Map[String, StructType] = _schemas.toMap

}

