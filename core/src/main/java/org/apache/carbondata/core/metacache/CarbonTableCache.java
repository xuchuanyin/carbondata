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

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.thrift.TBase;

public class CarbonTableCache extends AbstractBaseCache implements Serializable {
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 20160822L;
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonTableCache.class.getName());
  private LoadingCache<SimpleTableModel, CarbonTable> table2CarbonTableCache = null;

  /**
   * private constructor
   */
  private CarbonTableCache() {
    CacheLoader<SimpleTableModel, CarbonTable> table2IdentifierCacheLoader =
        new CacheLoader<SimpleTableModel, CarbonTable>() {
          @Override public CarbonTable load(SimpleTableModel simpleTableModel)
              throws Exception {
            return readCarbonTable(simpleTableModel);
          }
        };
    this.table2CarbonTableCache = CacheBuilder.newBuilder()
        .expireAfterAccess(3, TimeUnit.DAYS)
        .build(table2IdentifierCacheLoader);
  }

  /**
   * SingletonHolder
   */
  private static class SingletonHolder {
    public static final CarbonTableCache INSTANCE = new CarbonTableCache();
  }

  /**
   * get instance
   *
   * @return 单例对象
   */
  public static CarbonTableCache getInstance() {
    return SingletonHolder.INSTANCE;
  }

  /**
   * for resolve from serialized
   *
   * @return 反序列化得到的对象
   */
  protected Object readResolve() {
    return getInstance();
  }

  private CarbonTable readCarbonTable(SimpleTableModel simpleTableModel)
      throws IOException {
    String dbName = simpleTableModel.getDbName();
    String tableName = simpleTableModel.getTableName();
    String storePath = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.STORE_LOCATION);

    CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, dbName,
        tableName);
    String schemaFilePath = carbonTablePath.getSchemaFilePath();
    if (FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.LOCAL) ||
        FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.HDFS) ||
        FileFactory.isFileExist(schemaFilePath, FileFactory.FileType.VIEWFS)) {

      ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator() {
        public TBase create() {
          return new org.apache.carbondata.format.TableInfo();
        }
      };
      ThriftReader thriftReader =
          new ThriftReader(carbonTablePath.getSchemaFilePath(), createTBase);
      thriftReader.open();
      org.apache.carbondata.format.TableInfo tableInfo =
          (org.apache.carbondata.format.TableInfo) thriftReader.read();
      thriftReader.close();

      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      TableInfo wrapperTableInfo = schemaConverter
          .fromExternalToWrapperTableInfo(tableInfo,
              dbName, tableName, storePath);
      wrapperTableInfo.setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath));
      CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);
      return CarbonMetadata.getInstance().getCarbonTable(wrapperTableInfo.getTableUniqueName());
    } else {
      throw new IOException("File does not exist: " + schemaFilePath);
    }
  }

  @Override
  public CarbonTable getValueFromCache(final SimpleTableModel simpleTableModel)
      throws IOException {
    try {
      return this.table2CarbonTableCache.get(simpleTableModel, new Callable<CarbonTable>() {
        @Override public CarbonTable call() throws Exception {
          return readCarbonTable(simpleTableModel);
        }
      });
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void refreshCache(final SimpleTableModel simpleTableModel) {
    this.table2CarbonTableCache.invalidate(simpleTableModel);
  }
}
