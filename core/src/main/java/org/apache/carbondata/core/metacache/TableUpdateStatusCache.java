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

package org.apache.carbondata.core.metacache;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;

public class TableUpdateStatusCache extends AbstractBaseCache implements Serializable {
  private static final long serialVersionUID = 20160822L;
  private static final LogService LOGGER = LogServiceFactory.getLogService(
      TableUpdateStatusCache.class.getName());

  private LoadingCache<SimpleTableModel, SegmentUpdateDetails[]> tableUpdateStatusCache = null;

  /**
   * private constructor
   */
  private TableUpdateStatusCache() {
    CacheLoader<SimpleTableModel, SegmentUpdateDetails[]>
        tableUpdateStatusCacheLoader =
        new CacheLoader<SimpleTableModel, SegmentUpdateDetails[]>() {
          @Override public SegmentUpdateDetails[] load(SimpleTableModel simpleTableModel)
              throws IOException {
            return readTableUpdateStatus(simpleTableModel);
          }
        };
    this.tableUpdateStatusCache = CacheBuilder.newBuilder()
        .expireAfterAccess(3, TimeUnit.DAYS)
        .build(tableUpdateStatusCacheLoader);
  }

  /**
   * private static holder
   */
  private static class SingletonHolder {
    public static final TableUpdateStatusCache INSTANCE = new TableUpdateStatusCache();
  }

  /**
   * get instance
   *
   * @return instance
   */
  public static TableUpdateStatusCache getInstance() {
    return TableUpdateStatusCache.SingletonHolder.INSTANCE;
  }

  /**
   * read resolve
   *
   * @return instance
   */
  protected Object readResolve() {
    return getInstance();
  }

  private SegmentUpdateDetails[] readTableUpdateStatus(
      SimpleTableModel simpleTableModel) throws IOException {
    LoadMetadataDetails[] loadMetadataDetails =
        TableStatusCache.getInstance().getValueFromCache(simpleTableModel);

    if (loadMetadataDetails.length == 0) {
      return new SegmentUpdateDetails[0];
    }

    String updateStatusIdentifier = loadMetadataDetails[0].getUpdateStatusFileName();
    // table has not been updated
    if (updateStatusIdentifier.length() == 0) {
      return new SegmentUpdateDetails[0];
    }

    CarbonTable carbonTable = CarbonTableCache.getInstance().getValueFromCache(simpleTableModel);
    String tableUpdateStatusFile = carbonTable.getMetaDataFilepath()
        + CarbonCommonConstants.FILE_SEPARATOR + updateStatusIdentifier;
    if (!FileFactory.isFileExist(tableUpdateStatusFile,
        FileFactory.getFileType(tableUpdateStatusFile))) {
      return new SegmentUpdateDetails[0];
    }

    DataInputStream dataInputStream = null;
    BufferedReader bufferedReader = null;
    InputStreamReader inputStreamReader = null;

    try {
      Gson gsonObjectToRead = new Gson();
      AtomicFileOperations fileOperations = new AtomicFileOperationsImpl(tableUpdateStatusFile,
          FileFactory.getFileType(tableUpdateStatusFile));
      dataInputStream = fileOperations.openForRead();
      inputStreamReader = new InputStreamReader(dataInputStream,
          CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT);
      bufferedReader = new BufferedReader(inputStreamReader);
      return gsonObjectToRead.fromJson(bufferedReader, SegmentUpdateDetails[].class);
    } catch (IOException e) {
      LOGGER.error(e, "Exception occurs when reading segmentUpdate details");
      throw e;
    } finally {
      CarbonUtil.closeStream(bufferedReader);
      CarbonUtil.closeStream(inputStreamReader);
      CarbonUtil.closeStream(dataInputStream);
    }
  }

  @Override
  public SegmentUpdateDetails[] getValueFromCache(final SimpleTableModel simpleTableModel)
      throws IOException {
    try {
      // load cache asynchronous
      return this.tableUpdateStatusCache
          .get(simpleTableModel, new Callable<SegmentUpdateDetails[]>() {
            @Override public SegmentUpdateDetails[] call() throws IOException {
              return readTableUpdateStatus(simpleTableModel);
            }
          });
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override public void refreshCache(SimpleTableModel simpleTableModel) {
    this.tableUpdateStatusCache.invalidate(simpleTableModel);
  }
}
