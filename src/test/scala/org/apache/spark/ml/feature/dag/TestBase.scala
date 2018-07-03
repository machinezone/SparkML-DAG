/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.dag

import org.apache.spark.sql.SparkSession
import org.scalatest._

/**
  * TestBase
  *
  * @author belbis
  * @since 1.0.0
  */
trait TestBase extends FlatSpec with Matchers {

  lazy val spark: SparkSession = {
    SparkSession.builder
      .master(master)
      .appName(appName)
      .getOrCreate
  }
  val master = "local[*]"
  val appName = "test-app"

}
