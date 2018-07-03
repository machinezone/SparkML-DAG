/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.dag

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * IdentityTransformer
  *
  * [[Transformer]] that returns input. This is most often going to
  * be used to generate [[IdentityNode]]s for [[DAGPipeline]]s.
  *
  * @param uid
  * @author belbis
  * @since 1.0.0
  */
class IdentityTransformer(override val uid: String) extends Transformer {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("IdentityTransformer"))

  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF

  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
