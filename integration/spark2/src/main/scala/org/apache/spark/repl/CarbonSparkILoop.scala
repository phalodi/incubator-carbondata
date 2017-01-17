/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.repl

class CarbonSpark2ILoop extends SparkILoop {

  override def initializeSpark() {
    intp.beQuietDuring {
      processLine("""
        @transient val spark = if (org.apache.spark.repl.Main.sparkSession != null) {
            org.apache.spark.repl.Main.sparkSession
          } else {
            org.apache.spark.repl.Main.createSparkSession()
          }
        @transient val sc = {
          val _sc = spark.sparkContext
          if (_sc.getConf.getBoolean("spark.ui.reverseProxy", false)) {
            val proxyUrl = _sc.getConf.get("spark.ui.reverseProxyUrl", null)
            if (proxyUrl != null) {
              println(s"Spark Context Web UI is available at ${proxyUrl}/proxy/${_sc.applicationId}")
            } else {
              println(s"Spark Context Web UI is available at Spark Master Public URL")
            }
          } else {
            _sc.uiWebUrl.foreach {
              webUrl => println(s"Spark context Web UI available at ${webUrl}")
            }
          }
          println("Spark context available as 'sc' " +
            s"(master = ${_sc.master}, app id = ${_sc.applicationId}).")
          println("Spark session available as 'spark'.")
          _sc
        }
                  """)
      processLine("import java.io.File")
      processLine("import org.apache.commons.io.FileUtils")
      processLine("import org.apache.spark.SparkContext._")
      processLine("import spark.implicits._")
      processLine("import spark.sql")
      processLine("import org.apache.spark.sql.functions._")
      processLine("import org.apache.spark.sql.CarbonSession")
      processLine("import org.apache.spark.sql.SparkSession.Builder")
      processLine(
        """val metastoredb = this.getClass.getResource("../").getPath+"target/metastore/" """.stripMargin)
      processLine("""println(":::::::::::::"+this.getClass.getResource("../").getPath+"target/metastore/")""")
      processLine(
        """val warehouse = this.getClass.getResource("../").getPath+"target/warehouse"""".stripMargin)
      processLine("val _builder = new CarbonSession.CarbonBuilder(new Builder().config(\"spark" +
                  ".sql.warehouse.dir\", s\"$warehouse\").config(\"javax.jdo.option" +
                  ".ConnectionURL\",s\"jdbc:derby:;databaseName=$metastoredb;create=true\"));")
      processLine("@transient val carbon = _builder.getOrCreateCarbonSession();")
      processLine("println(\"Carbon session available as 'carbon'.\")")

    }
  }
}
