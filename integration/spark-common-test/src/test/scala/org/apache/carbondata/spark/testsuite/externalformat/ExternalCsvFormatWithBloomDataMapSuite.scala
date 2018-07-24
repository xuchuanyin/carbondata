package org.apache.carbondata.spark.testsuite.externalformat

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class ExternalCsvFormatWithBloomDataMapSuite extends QueryTest
  with BeforeAndAfterEach with BeforeAndAfterAll {
  val carbonTable = "fact_carbon_table"
  val csvCarbonTable = "fact_carbon_csv_table"
  val dataMapOnCarbonTable = s"bloom_on_$carbonTable"
  val dataMapOnCsvTable = s"bloom_on_$csvCarbonTable"
  val csvFile = s"$resourcesPath/datawithoutheader.csv"

  // prepare normal carbon table for comparison
  override protected def beforeAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $carbonTable")
    sql(
      s"""
         | CREATE TABLE $carbonTable(empno smallint, empname String, designation string,
         | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
         | deptname String, projectcode int, projectjoindate String,projectenddate String,
         | attendance String, utilization String,salary String)
         | STORED BY 'carbondata'
       """.stripMargin
    )
    sql(
      s"""
         | CREATE DATAMAP $dataMapOnCarbonTable ON TABLE $carbonTable
         | USING 'bloomfilter'
         | DMPROPERTIES('index_columns'='empno')
       """.stripMargin)

    // it seems that carbondata now cannot handle load the same file for multiple times in one load,
    // so we load 5 times with comparision with external format.
    (0 until 5).foreach { i =>
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$csvFile' INTO TABLE $carbonTable
           | OPTIONS('DELIMITER'=',',
           | 'QUOTECHAR'='\"',
           | 'FILEHEADER'='EMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY')
        """.stripMargin)
    }
    sql(s"show segments for table $carbonTable").show(false)
  }

  override protected def afterAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $carbonTable")
  }

  override protected def beforeEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $csvCarbonTable")
  }

  override protected def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $csvCarbonTable")
  }

  private def checkAnswerProxy(df: DataFrame,
      expectedAnswer: DataFrame): Unit = {
    df.show(false)
    expectedAnswer.show(false)
    super.checkAnswer(df, expectedAnswer)
  }

  private def explainQuery() {
    // query all the columns
    sql(s"EXPLAIN SELECT eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY FROM $csvCarbonTable WHERE empno = 15").show(false)
    sql(s"EXPLAIN SELECT eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY FROM $carbonTable WHERE empno = 15").show(false)
    // query part of the columns
    sql(s"EXPLAIN SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 15").show(false)
    sql(s"EXPLAIN SELECT empno,empname, deptname, doj FROM $carbonTable WHERE empno = 15").show(false)
    // sequence of projection column are not same with that in DDL
    sql(s"EXPLAIN SELECT empname, empno, deptname, doj FROM $csvCarbonTable WHERE empno = 15").show(false)
    sql(s"EXPLAIN SELECT empname, empno, deptname, doj FROM $carbonTable WHERE empno = 15").show(false)
    // query with greater
    sql(s"EXPLAIN SELECT empname, empno, deptname, doj FROM $csvCarbonTable WHERE empno > 15").show(false)
    sql(s"EXPLAIN SELECT empname, empno, deptname, doj FROM $carbonTable WHERE empno > 15").show(false)
    // query with filter on dimension
    sql(s"EXPLAIN SELECT empname, empno, deptname, doj FROM $csvCarbonTable WHERE empname = 'ayushi'").show(false)
    sql(s"EXPLAIN SELECT empname, empno, deptname, doj FROM $carbonTable WHERE empname = 'ayushi'").show(false)
    // aggreate query
    sql(s"EXPLAIN SELECT designation, sum(empno), avg(empno) FROM $csvCarbonTable GROUP BY designation").show(false)
    sql(s"EXPLAIN SELECT designation, sum(empno), avg(empno) FROM $carbonTable GROUP BY designation").show(false)
  }

  private def checkQuery() {
    // query all the columns
    checkAnswerProxy(sql(s"SELECT eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY FROM $csvCarbonTable WHERE empno = 15"),
      sql(s"SELECT eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY FROM $carbonTable WHERE empno = 15"))
    // query part of the columns
    checkAnswerProxy(sql(s"SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 15"),
      sql(s"SELECT empno,empname, deptname, doj FROM $carbonTable WHERE empno = 15"))
    // sequence of projection column are not same with that in DDL
    checkAnswerProxy(sql(s"SELECT empname, empno, deptname, doj FROM $csvCarbonTable WHERE empno = 15"),
      sql(s"SELECT empname, empno, deptname, doj FROM $carbonTable WHERE empno = 15"))
    // query with greater
    checkAnswerProxy(sql(s"SELECT empname, empno, deptname, doj FROM $csvCarbonTable WHERE empno > 15"),
      sql(s"SELECT empname, empno, deptname, doj FROM $carbonTable WHERE empno > 15"))
    // query with filter on dimension
    checkAnswerProxy(sql(s"SELECT empname, empno, deptname, doj FROM $csvCarbonTable WHERE empname = 'ayushi'"),
      sql(s"SELECT empname, empno, deptname, doj FROM $carbonTable WHERE empname = 'ayushi'"))
    // aggreate query
    checkAnswerProxy(sql(s"SELECT designation, sum(empno), avg(empno) FROM $csvCarbonTable GROUP BY designation"),
      sql(s"SELECT designation, sum(empno), avg(empno) FROM $carbonTable GROUP BY designation"))
  }

  test("test external csv format carbon table with index datamap: basic test for loading and querying with index datamap") {
    // create external csv format
    sql(
      s"""
         | CREATE TABLE $csvCarbonTable(empno smallint, empname String, designation string,
         | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
         | deptname String, projectcode int, projectjoindate String,projectenddate String,
         | attendance String, utilization String,salary String)
         | STORED BY 'carbondata'
         | TBLPROPERTIES(
         | 'format'='csv',
         | 'csv.header'='eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY'
         | )
       """.stripMargin
    )

    // create bloom index datamap
    sql(
      s"""
         | CREATE DATAMAP $dataMapOnCsvTable ON TABLE $csvCarbonTable
         | USING 'bloomfilter'
         | DMPROPERTIES('index_columns'='empno')
       """.stripMargin)

    sql(s"SHOW DATAMAP ON TABLE $csvCarbonTable").show(false)

    // load data through add segment statement
    (0 until 2).foreach { i =>
      sql(s"ALTER TABLE $csvCarbonTable ADD SEGMENT LOCATION '$csvFile'")
    }
    sql(s"ALTER TABLE $csvCarbonTable ADD SEGMENT LOCATION '$csvFile,$csvFile,$csvFile'")
    sql(s"show segments for table $csvCarbonTable").show(false)

    // check the bloom index has been generated correctly

    // check query: check correctness and datamap hitness
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
    explainQuery()
    checkQuery()
//    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
//    explainQuery()
//    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  }

  // test incremental build

  // test rebuild datamap

  // test datamaps

}
