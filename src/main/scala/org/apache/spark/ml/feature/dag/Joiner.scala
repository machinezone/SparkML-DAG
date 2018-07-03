/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.dag

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Joiner
  *
  * [[Transformer]] that configures a join between two [[Dataset]]s.
  *
  * @param uid
  *
  * @author belbis
  * @since 1.0.0
  */
class Joiner(override val uid: String) extends Transformer with Params {

  val right = new Param[Dataset[_]](this, "right", "Dataset to be used as the right table in " +
    "join.")
  val joinCols = new StringArrayParam(this, "joinCols", "Column names on which to join the two " +
    "datasets.")
  val joinType = new Param[String](this, "joinType", "Type of join to use. Default is `left`.")

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("Joiner"))

  def getRight: Dataset[_] = $(right)

  def setRight(value: Dataset[_]): this.type = set(right, value)

  def getJoinCols: Array[String] = $(joinCols)

  def setJoinCols(value: Array[String]): this.type = set(joinCols, value)

  def getJoinType: String = $(joinType)

  def setJoinType(value: String): this.type = set(joinType, value)

  setDefault(joinType, "left")

  /**
    * transform
    *
    * Joins the dataset to configured right dataset.
    *
    * @param dataset
    * @return
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    dataset.join($(right), $(joinCols), $(joinType))
  }

  /**
    * transformSchema
    *
    * Adds right columns to left schema.
    *
    * @param schema
    * @return
    */
  override def transformSchema(schema: StructType): StructType = {
    val newCols = $(right).schema.fields.filterNot(c => $(joinCols).contains(c.name))
    val badCols = schema.fieldNames.intersect(newCols)
    if (badCols.nonEmpty) {
      val err = s"Cannot duplicate columns. These exist in source table: ${badCols.mkString(",")}"
      throw new RuntimeException(err)
    }

    newCols.foldLeft(schema) {
      case (sch, field) => sch.add(field)
    }

  }

  override def copy(extra: ParamMap): Joiner = defaultCopy(extra)

}
