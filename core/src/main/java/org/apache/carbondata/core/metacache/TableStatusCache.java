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
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;

public class TableStatusCache extends AbstractBaseCache implements Serializable {
  private static final long serialVersionUID = 20160822L;
  private static final LogService LOGGER = LogServiceFactory.getLogService(
      TableStatusCache.class.getName());

  private LoadingCache<SimpleTableModel, LoadMetadataDetails[]> tableStatusCache = null;

  /**
   * private constructor
   */
  private TableStatusCache() {
    CacheLoader<SimpleTableModel, LoadMetadataDetails[]> tableStatusCacheLoader =
        new CacheLoader<SimpleTableModel, LoadMetadataDetails[]>() {
          @Override public LoadMetadataDetails[] load(SimpleTableModel simpleTableModel)
              throws IOException {
            return readTableStatus(simpleTableModel);
          }
        };
    this.tableStatusCache = CacheBuilder.newBuilder()
        .expireAfterAccess(3, TimeUnit.DAYS)
        .build(tableStatusCacheLoader);
  }

  /**
   * private static holder
   */
  private static class SingletonHolder {
    public static final TableStatusCache INSTANCE = new TableStatusCache();
  }

  /**
   * get instance
   *
   * @return instance
   */
  public static TableStatusCache getInstance() {
    return TableStatusCache.SingletonHolder.INSTANCE;
  }

  /**
   * read resolve
   *
   * @return instance
   */
  protected Object readResolve() {
    return getInstance();
  }

  /**
   * read loadMetadataDetails for table
   *
   * @param simpleTableModel table
   * @return array of LoadMetadataDetails
   * @throws IOException error occurs when reading tablestatus file
   */
  private LoadMetadataDetails[] readTableStatus(SimpleTableModel simpleTableModel)
      throws IOException {
    CarbonTable carbonTable = CarbonTableCache.getInstance().getValueFromCache(simpleTableModel);
    AbsoluteTableIdentifier absTableIdentifier = carbonTable.getAbsoluteTableIdentifier();

    CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(absTableIdentifier);
    String tableStatusFile = carbonTablePath.getTableStatusFilePath();

    if (!FileFactory.isFileExist(tableStatusFile, FileFactory.getFileType(tableStatusFile))) {
      return new LoadMetadataDetails[0];
    }

    DataInputStream dataInputStream = null;
    BufferedReader bufferedReader = null;
    InputStreamReader inputStreamReader = null;

    try {
      Gson gsonObjectToRead = new Gson();
      AtomicFileOperations fileOperations =
          new AtomicFileOperationsImpl(tableStatusFile, FileFactory.getFileType(tableStatusFile));
      dataInputStream = fileOperations.openForRead();
      inputStreamReader = new InputStreamReader(dataInputStream, "UTF-8");
      bufferedReader = new BufferedReader(inputStreamReader);
      return gsonObjectToRead.fromJson(bufferedReader, LoadMetadataDetails[].class);
    } catch (IOException e) {
      LOGGER.error(e, "Exception occurs when reading loadMetadata details");
      throw e;
    } finally {
      CarbonUtil.closeStream(bufferedReader);
      CarbonUtil.closeStream(inputStreamReader);
      CarbonUtil.closeStream(dataInputStream);
    }
  }

  @Override
  public LoadMetadataDetails[] getValueFromCache(final SimpleTableModel simpleTableModel)
      throws IOException {
    try {
      // load cache asynchronous
      return this.tableStatusCache
          .get(simpleTableModel, new Callable<LoadMetadataDetails[]>() {
            @Override public LoadMetadataDetails[] call() throws IOException {
              return readTableStatus(simpleTableModel);
            }
          });
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void refreshCache(SimpleTableModel simpleTableModel) {
    this.tableStatusCache.invalidate(simpleTableModel);
  }
}
