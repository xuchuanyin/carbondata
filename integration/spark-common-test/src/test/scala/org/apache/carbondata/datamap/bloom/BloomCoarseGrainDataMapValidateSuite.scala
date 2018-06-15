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

package org.apache.carbondata.datamap.bloom

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class BloomCoarseGrainDataMapValidateSuite
  extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val bloomTable = "table_bloom_date"
  val bloomName = "dm_bloom_date"
  val csvPath = s"$resourcesPath/datasamplefordate"

  override def afterEach(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  test("test bloom filter datamap on date column") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $bloomTable(empno string, doj date, salary float)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='string')
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $bloomName ON TABLE $bloomTable USING 'bloomfilter' DMPROPERTIES (
         | 'INDEX_COLUMNS'='doj')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$csvPath' INTO TABLE $bloomTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomTable WHERE dob='2016-03-14'").show(false)

  }
}
