/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.dag

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Binarizer, CountVectorizer, IDF, SQLTransformer}

/**
  * DAGPipelineTest
  *
  * Unit tests for [[DAGPipeline]].
  *
  * @author belbis
  * @since 1.0.0
  */
class DAGPipelineTest extends TestBase {

  "#transform()" should "work on a fitted model" in {
    val df = spark.createDataFrame(Seq(
      ("one", 1, 0.0d, 100L, "to"),
      ("one", 2, 0.1d, 50L, "be"),
      ("one", 3, 0.2d, 80L, "or"),
      ("two", 4, 0.3d, 75L, "not"),
      ("two", 5, 0.4d, 92L, "to"),
      ("two", 6, 0.5d, 108L, "be")
    )).toDF("_1", "_2", "_3", "_4", "_5")
    val p1 = new Binarizer()
      .setInputCol("_3")
      .setOutputCol("_3_bin")
    val p2 = new Pipeline().setStages(Array(
      new SQLTransformer()
        .setStatement("select _1, collect_list(_5) as _5_list from __THIS__ group by _1"),
      new CountVectorizer()
        .setInputCol("_5_list")
        .setOutputCol("_5_tf"),
      new IDF()
        .setInputCol("_5_tf")
        .setOutputCol("_5_idf")
    ))
    val p3 = new Pipeline().setStages(Array(
      new SQLTransformer()
        .setStatement("select _1, _2, min(_3) as _3_min from __THIS__ group by _1, _2")
    ))
    val j = new Joiner("joiner").setJoinCols(Array("_1"))
    val r1 = IdentityNode(df, new IdentityTransformer("_root"))
    val n1 = TransformerNode(p1)
    val n2 = EstimatorNode(p2)
    val n3 = EstimatorNode(p3)
    val n4 = JoinerNode(j, n1.id, n2.id)
    val p = new DAGPipeline()
      .setNodes(Array(r1, n1, n2, n3, n4))
      .setEdges(Array(Edge(r1.id, n1.id),
        Edge(n1.id, n2.id), Edge(n1.id, n3.id),
        Edge(n3.id, n4.id), Edge(n2.id, n4.id)
      ))
    val model = p.fit(df.sparkSession.emptyDataFrame)

    model.setIdentity("_root", df)
    val out = model.transform(df.sparkSession.emptyDataFrame)

    out.show
  }

}
