/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature

import org.apache.spark.ml.{Estimator, Model, PipelineStage, Transformer}

/**
  * dag
  *
  * Contains all [[org.apache.spark.ml.feature.dag.Node]] and
  * [[org.apache.spark.ml.feature.dag.Edge]] classes for
  * [[org.apache.spark.ml.feature.dag.DAGPipeline]].
  *
  * @author belbis
  * @since 1.0.0
  */
package object dag {

  /**
    * Node
    *
    * Simple node class.
    *
    * @param id
    * @param value
    * @tparam T
    * @author belbis
    * @since 1.0.0
    */
  abstract class Node[T](val id: String, val value: T)

  /**
    * TransformingNode
    *
    * @param value
    */
  abstract class TransformingNode(value: Transformer) extends Node[PipelineStage](value.uid, value)

  /**
    * Edge
    *
    * Simple edge class. Defines an edge between two [[Node]] objects.
    *
    * @param from
    * @param to
    * @author belbis
    * @since 1.0.0
    */
  case class Edge(from: String, to: String)

  /**
    * JoinerNode
    *
    * @param value
    * @param left
    * @param right
    */
  case class JoinerNode(override val value: Joiner, left: String, right: String)
    extends TransformingNode(value)

  /**
    * EstimatorNode
    *
    * @param value
    * @tparam M
    */
  case class EstimatorNode[M <: Model[M]](override val value: Estimator[M])
    extends Node[PipelineStage](value.uid, value)

  /**
    * TransformerNode
    *
    * @param value
    */
  case class TransformerNode(override val value: Transformer)
    extends TransformingNode(value)

  /**
    * IdentityNode
    *
    * @param identity
    * @param value
    * @tparam T
    */
  case class IdentityNode[T](identity: T, override val value: Transformer)
    extends TransformingNode(value)


}
