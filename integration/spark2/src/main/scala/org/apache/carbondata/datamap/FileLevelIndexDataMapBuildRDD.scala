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
package org.apache.carbondata.datamap

import java.text.SimpleDateFormat
import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{Job, RecordReader, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.vectorized.ColumnarBatch

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.block.TableBlockInfo
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, TableInfo}
import org.apache.carbondata.core.statusmanager.{FileFormat, SegmentStatusManager}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit, CarbonProjection, CsvRecordReader}
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.format.{CsvReadSupport, VectorCsvReadSupport}
import org.apache.carbondata.spark.{FileLevelDataMapBuildResult, FileLevelDataMapBuildResultImpl}
import org.apache.carbondata.spark.rdd.{CarbonRDDWithTableInfo, CarbonSparkPartition}
import org.apache.carbondata.spark.util.SparkDataTypeConverterImpl

object FileLevelIndexDataMapBuildRDD {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def buildDataMap(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      loadModel: CarbonLoadModel,
      dataMapSchemas: List[DataMapSchema]): Unit = {
    LOGGER.error(s"XU enter build datamap for ${carbonTable.getTableName}")
    val status = new FileLevelIndexDataMapBuildRDD[String, (String, Boolean)](
      sparkSession,
      new FileLevelDataMapBuildResultImpl,
      carbonTable.getTableInfo,
      loadModel,
      null,
      dataMapSchemas).collect()

    val failedTasks = status
      .filter { case (segment, (factFile, buildStatus)) =>
        !buildStatus
      }
      .map { task =>
        val segmentId = task._1
        // todo: clear index data for this segment
      }

    if (failedTasks.nonEmpty) {
      throw new Exception(s"Failed to build datamap for table" +
        s" ${carbonTable.getDatabaseName}.${carbonTable.getTableName}")
    }
  }
}

/**
 * generate all the index in one run. Task will be launched per segment per fact file.
 * for data loading procedure, carbonLoadModel is not null and segmentIds is null;
 * for index rebuilding procedure, carbonLoadModel is null and segmentIds is not null.
 */
class FileLevelIndexDataMapBuildRDD[K, V](
    session: SparkSession,
    result: FileLevelDataMapBuildResult[K, V],
    @transient tableInfo: TableInfo,
    carbonLoadModel: CarbonLoadModel,
    segmentIds: List[String],
    dataMapSchemas: List[DataMapSchema]
) extends CarbonRDDWithTableInfo[(K, V)](session.sparkContext, Nil, tableInfo.serialize()) {

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new util.Date())
  }
  private val queryId = sparkContext.getConf.get("queryId", System.nanoTime().toString)
  private val carbonTable = CarbonTable.buildFromTableInfo(getTableInfo)
  private val storageFormat = getTableInfo.getFormat
  private val allIndexColumns = dataMapSchemas.flatMap(p => p.getIndexColumns).distinct.toArray
  private var enableVectorReader = false

  override def internalCompute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    var status = false

    val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value.getAllSplits.get(0)
    val segmentId = inputSplit.getSegmentId
    val factFile = inputSplit.getPath
    LOGGER.error(s"XU FileLevelBuildRDD,internalCompute segment->$segmentId, file->$factFile")

    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(new Configuration(), attemptId)

    try {
      // read the file and generate indexes
      val recordReader = createRecordReader(inputSplit, attemptContext)
      recordReader.initialize(inputSplit, attemptContext)

      if (enableVectorReader) {
        while (recordReader.nextKeyValue()) {
          val rowBatch = recordReader.getCurrentValue.asInstanceOf[ColumnarBatch]
          throw new UnsupportedOperationException(
            "unsupport vector reader for FileLevel IndexDataMap rebuild")
        }
      } else {
        while (recordReader.nextKeyValue()) {
          val row = recordReader.getCurrentValue.asInstanceOf[GenericInternalRow]
          LOGGER.error(s"XU buildDataMap GenericInterRow->$row")
          buildDataMapForNonVector(row)
        }
      }




      status = true
    } finally {

    }

    new Iterator[(K, V)] {
      var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey(split.index.toString, (segmentId, status))
      }
    }
  }

  private def prepareInputFormat(attemptContext: TaskAttemptContextImpl,
      segmentId: String): CarbonInputFormat[Object] = {
    val format = new CarbonTableInputFormat[Object]
    val conf = attemptContext.getConfiguration
    val tableInfo = getTableInfo
    CarbonInputFormat.setTableInfo(conf, tableInfo)
    CarbonInputFormat.setDatabaseName(conf, tableInfo.getDatabaseName)
    CarbonInputFormat.setTableName(conf, tableInfo.getFactTable.getTableName)
    CarbonInputFormat.setDataTypeConverter(conf, classOf[SparkDataTypeConverterImpl])
    val identifier = tableInfo.getOrCreateAbsoluteTableIdentifier()
    CarbonInputFormat.setTablePath(conf,
      identifier.appendWithLocalPrefix(identifier.getTablePath))
    CarbonInputFormat.setSegmentsToAccess(conf, Segment.toSegmentList(Array(segmentId), null))
    CarbonInputFormat.setColumnProjection(conf, new CarbonProjection(allIndexColumns))

    format
  }

  private def createRecordReader(inputSplit: CarbonInputSplit,
      attemptContext: TaskAttemptContextImpl): RecordReader[Void, Object] = {
    require(storageFormat.equals("csv"), "Currently we only support csv as external file format")

    val format = prepareInputFormat(attemptContext, inputSplit.getSegmentId)
    attemptContext.getConfiguration.set(
      CarbonCommonConstants.CARBON_EXTERNAL_FORMAT_CONF_KEY, storageFormat)
    val queryModel = format.createQueryModel(inputSplit, attemptContext)
    queryModel.setQueryId(queryId)
    val recordReader = format.createRecordReader(inputSplit, attemptContext)
      .asInstanceOf[CsvRecordReader[Object]]
    // todo: set by condition
    recordReader.setVectorReader(enableVectorReader)
    recordReader.setQueryModel(queryModel)
    if (enableVectorReader) {
      recordReader.setReadSupport(new VectorCsvReadSupport[Object]())
    } else {
      recordReader.setReadSupport(new CsvReadSupport[Object]())
    }
    recordReader
  }

  private def buildDataMapForNonVector(row: GenericInternalRow): Unit = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

    val indexCol2Ordinal = allIndexColumns.zipWithIndex
    dataMapSchemas.foreach { dmSchema =>
      val dmName = dmSchema.getDataMapName
      val cols = dmSchema.getIndexColumns
      // todo: add listener?
    }

  }

  override protected def getPartitions: Array[Partition] = {
    val rtn = if (carbonLoadModel != null) {
      getPartitionsForLoad(carbonLoadModel)
    } else if (segmentIds != null) {
      getPartitionForSegments(segmentIds)
    } else {
      Array[Partition]()
    }
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    LOGGER.error("XU FileLevelIndexBuildRDD.getPartitions size->" + rtn.size)
    rtn
  }

  private def getPartitionsForLoad(loadModel: CarbonLoadModel): Array[Partition] = {
    val segmentId = loadModel.getSegmentId
    val segment2FactFiles = loadModel.getFactFilePath.split(",").map(p => segmentId -> p)
    getPartitionForFiles(segment2FactFiles)
  }

  private def getPartitionForSegments(segmentList: List[String]): Array[Partition] = {
    val segment2FactFiles = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
      .filter(loadDetails => segmentList.contains(loadDetails.getLoadName))
      .flatMap { l =>
        l.getFactFilePath.split(",").map(p => l.getLoadName -> p)
      }
    getPartitionForFiles(segment2FactFiles)
  }

  // per segment per file is a partition to make the task more distributable
  private def getPartitionForFiles(segment2Files : Seq[(String, String)]): Array[Partition] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    LOGGER.error("XU FileLevelIndexBuildRDD.getPartitionForFiles segment2Files->" +
                 segment2Files.mkString(", "))
    segment2Files
      .map { case (segmentId, file) =>
        val factFile = FileFactory.getCarbonFile(file)
        val factPath = new Path(factFile.getPath)
        val fs = FileFactory.getFileSystem(factPath)
        val fileStatus = fs.getFileStatus(factPath)
        val hdfsBlkLocations = fs.getFileBlockLocations(factPath, 0, fileStatus.getLen).apply(0)
        val split = new CarbonInputSplit(segmentId,
          factPath,
          0,
          fileStatus.getLen,
          hdfsBlkLocations.getHosts,
          hdfsBlkLocations.getCachedHosts,
          FileFormat.EXTERNAL)
        new CarbonMultiBlockSplit(Seq(split).asJava, split.getLocations)
      }
      .zipWithIndex
      .map(p => new CarbonSparkPartition(id, p._2, p._1))
      .toArray
  }
}
