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
package org.apache.carbondata.spark.testsuite.longstring

import java.io.{File, PrintWriter}

import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class LongStringDataTypesLoadTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  private val longStringTable = "long_string_table"
  private val inputDir = s"$resourcesPath${File.separator}longstring${File.separator}"
  private val fileName = s"longStringData.csv"
  private val inputFile = s"$inputDir$fileName"
  private val lineNum = 1000
  private val head = 0
  private var line_head: String = _
  private val mid = lineNum / 2
  private var line_half: String = _
  private var tail = lineNum - 1
  private var line_tail: String = _

  override def beforeAll(): Unit = {
    deleteFile(inputFile)
    if (!new File(inputDir).exists()) {
      new File(inputDir).mkdir()
    }
    createFile(inputFile, line = lineNum)
  }

  override def afterAll(): Unit = {
    deleteFile(inputFile)
    if (new File(inputDir).exists()) {
      new File(inputDir).delete()
    }
  }

  override def beforeEach(): Unit = {
    sql(s"drop table if exists $longStringTable")
  }

  override def afterEach(): Unit = {
    sql(s"drop table if exists $longStringTable")
  }

  test("Load file to table with long string datatype: safe") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    sql(
      s"""
         | CREATE TABLE if not exists $longStringTable(
         | id INT, name STRING, description STRING, address STRING
         | ) STORED BY 'carbondata'
         | TBLPROPERTIES('LONG_STRING_COLUMNS'='description', 'SORT_COLUMNS'='name')
         |""".stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputFile' INTO TABLE $longStringTable
         | OPTIONS('header'='false')
       """.stripMargin)
    checkAnswer(sql(s"SELECT id, name, description, address FROM $longStringTable where id = $head"),
      Row(head, s"name_$head", line_head, s"address_$head"))
    checkAnswer(sql(s"SELECT id, name, description, address FROM $longStringTable where id = $mid"),
      Row(mid, s"name_$mid", line_half, s"address_$mid"))
    checkAnswer(sql(s"SELECT id, name, description, address FROM $longStringTable where id = $tail"),
      Row(tail, s"name_$tail", line_tail, s"address_$tail"))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
      CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
  }

  test("Load file to table with long string datatype: unsafe") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    sql(
      s"""
         | CREATE TABLE if not exists $longStringTable(
         | id INT, name STRING, description STRING, address STRING
         | ) STORED BY 'carbondata'
         | TBLPROPERTIES('LONG_STRING_COLUMNS'='description', 'SORT_COLUMNS'='name')
         |""".stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputFile' INTO TABLE $longStringTable
         | OPTIONS('header'='false')
       """.stripMargin)
    checkAnswer(sql(s"SELECT id, name, description, address FROM $longStringTable where id = $head"),
      Row(head, s"name_$head", line_head, s"address_$head"))
    checkAnswer(sql(s"SELECT id, name, description, address FROM $longStringTable where id = $mid"),
      Row(mid, s"name_$mid", line_half, s"address_$mid"))
    checkAnswer(sql(s"SELECT id, name, description, address FROM $longStringTable where id = $tail"),
      Row(tail, s"name_$tail", line_tail, s"address_$tail"))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
      CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
  }

  private def createFile(filePath: String, line: Int = 10000, start: Int = 0): Unit = {
    if (!new File(filePath).exists()) {
      val write = new PrintWriter(new File(filePath))
      for (i <- start until (start + line)) {
        val description = RandomStringUtils.randomAlphabetic(Short.MaxValue + 1000)
        val line = s"$i,name_$i,$description,address_$i"
        if (head == i) {
          line_head = description
        } else if (mid == i) {
          line_half = description
        } else if (tail == i) {
          line_tail = description
        }
        write.println(line)
      }
      write.close()
    }
  }

  private def deleteFile(filePath: String): Unit = {
    val file = new File(filePath)
    if (file.exists()) {
      file.delete()
    }
  }
}
