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

class CarbonSparkILoop extends SparkILoop {

  override def initializeSpark() {
    intp.beQuietDuring {
      processLine("""
         if(org.apache.spark.repl.carbon.Main.interp == null) {
           org.apache.spark.repl.carbon.Main.main(Array[String]())
         }
              """)
      processLine("val i1 = org.apache.spark.repl.carbon.Main.interp")
      processLine("import i1._")
      processLine("""
         @transient val sc = {
           val _sc = i1.createSparkContext()
           println("Spark context available as sc.")
           _sc
         }
              """)
      processLine("import org.apache.spark.SparkContext._")
      processLine("import org.apache.spark.sql.CarbonContext")
      processLine("""
         @transient val cc = {
           val _cc = {
             import java.io.File
             val path = System.getenv("CARBON_HOME") + "/bin/carbonshellstore"
             val store = new File(path)
             store.mkdirs()
             val storePath = sc.getConf.getOption("spark.carbon.storepath")
                  .getOrElse(store.getCanonicalPath)
             new CarbonContext(sc, storePath, store.getCanonicalPath)
           }
           println("Carbon context available as cc.")
           _cc
         }
              """)

      processLine("import org.apache.spark.sql.SQLContext")
      processLine("""
         @transient val sqlContext = {
           val _sqlContext = new SQLContext(sc)
           println("SQL context available as sqlContext.")
           _sqlContext
         }
              """)
      processLine("import sqlContext.implicits._")
      processLine("import sqlContext.sql")

      processLine("import cc.implicits._")
      processLine("import cc.sql")
      processLine("import org.apache.spark.sql.functions._")
    }
  }
}
