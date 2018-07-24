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

package org.apache.spark.sql.execution.command.management

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.FileUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.datamap.FileLevelIndexDataMapBuildRDD
import org.apache.carbondata.events.{BuildDataMapPostExecutionEvent, BuildDataMapPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadMetadataEvent, LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

/**
 * support `alter table tableName add segment location 'path'` command.
 * It will create a segment and map the path of datafile to segment's storage
 */
case class CarbonAddSegmentCommand(
    dbNameOp: Option[String],
    tableName: String,
    filePathFromUser: String,
    var operationContext: OperationContext = new OperationContext) extends AtomicRunnableCommand {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  var carbonTable: CarbonTable = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val dbName = CarbonEnv.getDatabaseName(dbNameOp)(sparkSession)
    carbonTable = {
      val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
      if (relation == null) {
        LOGGER.error(s"Add segment failed due to table $dbName.$tableName not found")
        throw new NoSuchTableException(dbName, tableName)
      }
      relation.carbonTable
    }

    if (carbonTable.isHivePartitionTable) {
      throw new UnsupportedOperationException(
        "Carbondata currently does not support hive partition table for AddSegment command")
    }

    if (carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException(
        "Carbondata currently does not support child table for AddSegment command")
    }

    if (CarbonUtil.hasAggregationDataMap(carbonTable)) {
      val loadMetadataEvent = new LoadMetadataEvent(carbonTable, false)
      OperationListenerBus.getInstance().fireEvent(loadMetadataEvent, operationContext)
    }
    Seq.empty
  }

  // will just mapping external files to segment metadata
  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val currentLoadMetadataDetails = SegmentStatusManager.readLoadMetadata(
      CarbonTablePath.getMetadataPath(carbonTable.getTablePath))
    val newSegmentId = SegmentStatusManager.createNewSegmentId(currentLoadMetadataDetails).toString
    // create new segment folder in carbon store
    CarbonLoaderUtil.checkAndCreateCarbonDataLocation(newSegmentId, carbonTable)

    // associate segment meta with file path, files are separated with comma
    val loadModel: CarbonLoadModel = new CarbonLoadModel
    loadModel.setSegmentId(newSegmentId)
    loadModel.setDatabaseName(carbonTable.getDatabaseName)
    loadModel.setTableName(carbonTable.getTableName)
    loadModel.setTablePath(carbonTable.getTablePath)
    loadModel.setCarbonTransactionalTable(carbonTable.isTransactionalTable)
    loadModel.readAndSetLoadMetadataDetails()
    loadModel.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime())
    val factFilePath = FileUtils.getPaths(filePathFromUser)
    loadModel.setFactFilePath(factFilePath)
    val loadSchema: CarbonDataLoadSchema = new CarbonDataLoadSchema(carbonTable)
    loadModel.setCarbonDataLoadSchema(loadSchema)

    val uuid = if (carbonTable.isChildDataMap) {
      Option(operationContext.getProperty("uuid")).getOrElse("").toString
    } else if (carbonTable.hasAggregationDataMap) {
      UUID.randomUUID().toString
    } else {
      ""
    }
    operationContext.setProperty("uuid", uuid)
    val loadTablePreExecutionEvent: LoadTablePreExecutionEvent = new LoadTablePreExecutionEvent(
      carbonTable.getCarbonTableIdentifier, loadModel)
    operationContext.setProperty("isOverwrite", false)
    OperationListenerBus.getInstance().fireEvent(loadTablePreExecutionEvent, operationContext)
    // add pre event listener for index datamap
    val dmOperationCxt = new OperationContext
    val indexDMs = DataMapStoreManager.getInstance().getAllDataMap(carbonTable)
    import scala.collection.JavaConverters._
    var dmSchemas: mutable.Seq[DataMapSchema]= null
    if (null != indexDMs) {
      dmSchemas = indexDMs.asScala
        .filter(_.getDataMapSchema.isIndexDataMap)
        .map(_.getDataMapSchema)
      val dmNames = dmSchemas.map(_.getDataMapName)
      val buildDataMapPreExecutionEvent: BuildDataMapPreExecutionEvent =
        BuildDataMapPreExecutionEvent(sparkSession, carbonTable.getAbsoluteTableIdentifier, dmNames)
      OperationListenerBus.getInstance().fireEvent(buildDataMapPreExecutionEvent, dmOperationCxt)
    }

    // clean up invalid segment before creating a new entry
    SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable, false, null)

    val newLoadMetadataDetail: LoadMetadataDetails = new LoadMetadataDetails
    newLoadMetadataDetail.setSegmentFile(null)
    newLoadMetadataDetail.setSegmentStatus(SegmentStatus.SUCCESS)
    newLoadMetadataDetail.setLoadStartTime(loadModel.getFactTimeStamp)
    newLoadMetadataDetail.setLoadEndTime(CarbonUpdateUtil.readCurrentTime())
    newLoadMetadataDetail.setIndexSize("1")
    newLoadMetadataDetail.setDataSize("1")
    newLoadMetadataDetail.setFileFormat(FileFormat.EXTERNAL)
    newLoadMetadataDetail.setFactFilePath(loadModel.getFactFilePath)

    // generate index datamap at file level
    if (null != dmSchemas && dmSchemas.nonEmpty) {
      FileLevelIndexDataMapBuildRDD.buildDataMap(sparkSession, carbonTable, loadModel,
        dmSchemas.toList)
    }
    // for external datasource table, there are no index files, so no need to write segment file

    // update table status file
    val done = CarbonLoaderUtil.recordNewLoadMetadata(newLoadMetadataDetail, loadModel, true,
      false, uuid)
    if (!done) {
      val errorMsg =
        s"""
           | Data load is failed due to table status update failure for
           | ${loadModel.getDatabaseName}.${loadModel.getTableName}
         """.stripMargin
      throw new Exception(errorMsg)
    } else {
      DataMapStatusManager.disableAllLazyDataMaps(carbonTable)
    }

    val loadTablePostExecutionEvent: LoadTablePostExecutionEvent =
      new LoadTablePostExecutionEvent(carbonTable.getCarbonTableIdentifier, loadModel)
    OperationListenerBus.getInstance().fireEvent(loadTablePostExecutionEvent, operationContext)
    if (null != indexDMs) {
      val buildDataMapPostExecutionEvent: BuildDataMapPostExecutionEvent =
        BuildDataMapPostExecutionEvent(sparkSession, carbonTable.getAbsoluteTableIdentifier)
      OperationListenerBus.getInstance().fireEvent(buildDataMapPostExecutionEvent, dmOperationCxt)
    }
    Seq.empty
  }
}
