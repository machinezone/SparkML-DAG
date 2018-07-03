/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.dag

import org.apache.spark.ml._
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable

/**
  * DAGPipeline
  *
  * A [[org.apache.spark.ml.Pipeline]] that based on input [[Node]]s and [[Edge]]s
  *
  * @author belbis
  * @since 1.0.0
  */
class DAGPipeline(override val uid: String) extends Estimator[DAGPipelineModel] with Params {

  val nodes = new Param[Array[Node[PipelineStage]]](this, "stages", "List of " +
    "PipelineStageNode objects containing PipelineStages to traverse in building the model")
  val edges = new Param[Array[Edge]](this, "", "")

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("DAGPipeline"))

  def getNodes: Array[Node[PipelineStage]] = $(nodes)

  def setNodes(value: Array[Node[PipelineStage]]): this.type = set(nodes, value)

  def getEdges: Array[Edge] = $(edges)

  def setEdges(value: Array[Edge]): this.type = set(edges, value)

  override def fit(dataset: Dataset[_]): DAGPipelineModel = {

    // we should use identity nodes
    require(dataset == dataset.sparkSession.emptyDataFrame)

    transformSchema(dataset.schema, logging = true)
    val bfs = new DatasetBFS($(nodes).toList, $(edges).toList)
    new DAGPipelineModel(uid, bfs.toArray.filterNot(_.isInstanceOf[IdentityNode[_]]), $(edges))
  }

  override def transformSchema(schema: StructType): StructType = {
    require(schema == StructType(Nil))
    val bfs = new SchemaBFS($(nodes).toList, $(edges).toList)
    val traversal = bfs.toList
    bfs.schemas(traversal.last.id)
  }

  override def copy(extra: ParamMap): Estimator[DAGPipelineModel] = defaultCopy(extra)
}

/**
  * DAGPipelineModel
  *
  * [[PipelineModel]] that is a DAG.
  *
  * @param uid
  * @param nodes
  * @param edges
  * @author belbis
  * @since 1.0.0
  */
class DAGPipelineModel(override val uid: String,
                       nodes: Array[Node[PipelineStage]],
                       edges: Array[Edge])
  extends Model[DAGPipelineModel] {

  private val identities = mutable.Map.empty[String, Dataset[_]]

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this(nodes: Array[Node[PipelineStage]], edges: Array[Edge]) =
    this(Identifiable.randomUID(getClass.getSimpleName), nodes, edges)

  /**
    * setIdentity
    *
    * populate an identity node with the supplied value.
    *
    * @param name
    * @param value
    */
  def setIdentity(name: String, value: Dataset[_]): Unit = {
    identities.put(name, value)
  }

  /**
    * transform
    *
    * Because this interface uses single in/out [[Dataset]],
    * return the last processed [[Dataset]]. Because this operates
    * on potentially multiple input [[Dataset]]s, the input
    * [[Dataset]] should not be processed. Rather, there should
    * be populated [[IdentityNode]] objects that contain the
    * input [[Dataset]]s.
    *
    * @param dataset
    * @return
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    require(dataset == dataset.sparkSession.emptyDataFrame)

    val bfs = new DatasetBFS(identityNodes.union(nodes.toList), edges.toList)
    val traversal = bfs.toList
    bfs.datasets(traversal.last.id).toDF
  }

  /**
    * identityNodes
    *
    * private accessor to identity nodes.
    *
    * @return
    */
  private def identityNodes: List[IdentityNode[_]] = identities.map {
    case (key, value) =>
      IdentityNode(value, new IdentityTransformer(key))
  }.toList

  /**
    * transformSchema
    *
    * Because this interface uses single in/out [[StructType]],
    * return the last processed [[StructType]].
    *
    * @param schema
    * @return
    */
  override def transformSchema(schema: StructType): StructType = {
    require(schema == StructType(Nil))
    val bfs = new SchemaBFS(identityNodes.union(nodes.toList), edges.toList)
    val traversal = bfs.toList
    bfs.schemas(traversal.last.id)
  }

  override def copy(extra: ParamMap): DAGPipelineModel = defaultCopy(extra)

}
