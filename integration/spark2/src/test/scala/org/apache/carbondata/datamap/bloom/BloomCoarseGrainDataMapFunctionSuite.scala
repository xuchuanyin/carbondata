package org.apache.carbondata.datamap.bloom

import java.io.File

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.bloom.BloomCoarseGrainDataMapTestUtil.deleteFile
import org.apache.carbondata.datamap.bloom.BloomCoarseGrainDataMapTestUtil.createFile
import org.apache.carbondata.datamap.bloom.BloomCoarseGrainDataMapTestUtil.checkBasicQuery

class BloomCoarseGrainDataMapFunctionSuite  extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val bigFile = s"$resourcesPath/bloom_datamap_input_big_function.csv"
  val smallFile = s"$resourcesPath/bloom_datamap_input_small_function.csv"
  val normalTable = "carbon_normal"
  val bloomDMSampleTable = "carbon_bloom"
  val dataMapName = "bloom_dm"

  override protected def beforeAll(): Unit = {
    deleteFile(bigFile)
    deleteFile(smallFile)
    new File(CarbonProperties.getInstance().getSystemFolderLocation).delete()
    createFile(bigFile, line = 50000)
    createFile(smallFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }


  override def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test bloom datamap: index column is integer, dictionary column, sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128', 'dictionary_include'='id', 'sort_columns'='id')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    var map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    // load two segments
    (1 to 2).foreach { i =>
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
           | OPTIONS('header'='false')
         """.stripMargin)
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomDMSampleTable
           | OPTIONS('header'='false')
         """.stripMargin)
    }

    map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    sql(s"select * from $bloomDMSampleTable where city = 'city_1'").show(false)
    checkBasicQuery(dataMapName, bloomDMSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test bloom datamap: index column is integer, dictionary column, not sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128', 'dictionary_include'='id', 'sort_columns'='name')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    var map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    // load two segments
    (1 to 2).foreach { i =>
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
           | OPTIONS('header'='false')
         """.stripMargin)
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomDMSampleTable
           | OPTIONS('header'='false')
         """.stripMargin)
    }

    map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    sql(s"select * from $bloomDMSampleTable where city = 'city_1'").show(false)
    sql(s"select * from $bloomDMSampleTable where id = 1").show(false)
    checkBasicQuery(dataMapName, bloomDMSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }


  override def afterAll(): Unit = {
    deleteFile(bigFile)
    deleteFile(smallFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }
}
