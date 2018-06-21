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

package org.apache.carbondata.datamap.bloom;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.converter.impl.FieldEncoderFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.Key;

/**
 * BloomDataCoarseGrainMap is constructed in blocklet level. For each indexed column,
 * a bloom filter is constructed to indicate whether a value belongs to this blocklet.
 * More information of the index file can be found in the corresponding datamap writer.
 */
@InterfaceAudience.Internal
public class BloomCoarseGrainDataMap extends CoarseGrainDataMap {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BloomCoarseGrainDataMap.class.getName());
  public static final String BLOOM_INDEX_SUFFIX = ".bloomindex";
  private List<CarbonColumn> indexedColumn;
  private Map<String, CarbonColumn> name2Col;
  private List<BloomDMModel> bloomIndexList;
  private String shardName;
  private BloomDataMapCache bloomDataMapCache;
  private Path indexPath;
  // this is used to convert literal filter value to internal carbon value
  private Map<String, FieldConverter> name2Converters;
  private BadRecordLogHolder badRecordLogHolder;
  private KeyGenerator keyGenerator;

  @Override
  public void init(DataMapModel dataMapModel) throws IOException {
    this.indexPath = FileFactory.getPath(dataMapModel.getFilePath());
    this.shardName = indexPath.getName();
    FileSystem fs = FileFactory.getFileSystem(indexPath);
    if (!fs.exists(indexPath)) {
      throw new IOException(
          String.format("Path %s for Bloom index dataMap does not exist", indexPath));
    }
    if (!fs.isDirectory(indexPath)) {
      throw new IOException(
          String.format("Path %s for Bloom index dataMap must be a directory", indexPath));
    }
    this.bloomDataMapCache = BloomDataMapCache.getInstance();
  }

  /**
   * init converts that are used to convert literal value in filter
   * to carbon internal value that stored in carbonfile
   */
  public void initConverters(CarbonTable carbonTable, List<CarbonColumn> indexedColumn)
      throws IOException {
    this.indexedColumn = indexedColumn;
    this.name2Col = new HashMap<>(indexedColumn.size());
    for (CarbonColumn col : indexedColumn) {
      this.name2Col.put(col.getColName(), col);
    }

    this.name2Converters = new HashMap<>(indexedColumn.size());

    AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier.from(
        carbonTable.getTablePath(), carbonTable.getCarbonTableIdentifier());
    String nullFormat = "";
    // will not use dictionary client if not use one pass
    DictionaryClient client = null;
    boolean onePass = false;
    Map<Object, Integer>[] localCaches = new Map[indexedColumn.size()];
    boolean isEmptyBadRecord = false;

    for (int i = 0; i < indexedColumn.size(); i++) {
      localCaches[i] = new ConcurrentHashMap<>();
      DataField dataField = new DataField(indexedColumn.get(i));
      dataField.setDateFormat(CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
      dataField.setTimestampFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
      FieldConverter fieldConverter = FieldEncoderFactory.getInstance().createFieldEncoder(
          dataField, absoluteTableIdentifier, i, nullFormat,
          client, onePass, localCaches[i], isEmptyBadRecord);
      this.name2Converters .put(indexedColumn.get(i).getColName(), fieldConverter);
    }

    this.badRecordLogHolder = new BadRecordLogHolder();
    this.badRecordLogHolder.setLogged(false);
  }

  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      List<PartitionSpec> partitions) {
    this.keyGenerator = segmentProperties.getDimensionKeyGenerator();
    List<Blocklet> hitBlocklets = new ArrayList<Blocklet>();
    if (filterExp == null) {
      // null is different from empty here. Empty means after pruning, no blocklet need to scan.
      return null;
    }

    List<BloomQueryModel> bloomQueryModels = getQueryValue(filterExp.getFilterExpression());
    for (BloomQueryModel bloomQueryModel : bloomQueryModels) {
      LOGGER.debug("prune blocklet for query: " + bloomQueryModel);
      BloomDataMapCache.CacheKey cacheKey = new BloomDataMapCache.CacheKey(
          this.indexPath.toString(), bloomQueryModel.columnName);
      List<BloomDMModel> bloomDMModels = this.bloomDataMapCache.getBloomDMModelByKey(cacheKey);
      for (BloomDMModel bloomDMModel : bloomDMModels) {
        boolean scanRequired = bloomDMModel.getBloomFilter().membershipTest(
            new Key(bloomQueryModel.filterValue));
        if (scanRequired) {
          LOGGER.debug(String.format("BloomCoarseGrainDataMap: Need to scan -> blocklet#%s",
              String.valueOf(bloomDMModel.getBlockletNo())));
          Blocklet blocklet = new Blocklet(shardName, String.valueOf(bloomDMModel.getBlockletNo()));
          hitBlocklets.add(blocklet);
        } else {
          LOGGER.debug(String.format("BloomCoarseGrainDataMap: Skip scan -> blocklet#%s",
              String.valueOf(bloomDMModel.getBlockletNo())));
        }
      }
    }
    return hitBlocklets;
  }

  private List<BloomQueryModel> getQueryValue(Expression expression) {
    List<BloomQueryModel> queryModels = new ArrayList<BloomQueryModel>();
    if (expression instanceof EqualToExpression) {
      Expression left = ((EqualToExpression) expression).getLeft();
      Expression right = ((EqualToExpression) expression).getRight();
      String columnName;
      if (left instanceof ColumnExpression && right instanceof LiteralExpression) {
        columnName = ((ColumnExpression) left).getColumnName();
        if (this.name2Col.containsKey(columnName)) {
          BloomQueryModel bloomQueryModel =
              createQueryModelFromExpression((ColumnExpression) left, (LiteralExpression) right);
          queryModels.add(bloomQueryModel);
        }
      } else if (left instanceof LiteralExpression && right instanceof ColumnExpression) {
        columnName = ((ColumnExpression) right).getColumnName();
        if (this.name2Col.containsKey(columnName)) {
          BloomQueryModel bloomQueryModel =
              createQueryModelFromExpression((ColumnExpression) right, (LiteralExpression) left);
          queryModels.add(bloomQueryModel);
        }
      }
      return queryModels;
    }

    for (Expression child : expression.getChildren()) {
      queryModels.addAll(getQueryValue(child));
    }
    return queryModels;
  }

  private BloomQueryModel createQueryModelFromExpression(ColumnExpression ce,
      LiteralExpression le) {
    String columnName = ce.getColumnName();
    DataType dataType = ce.getDataType();
    Object value = le.getLiteralExpValue();
    return createQueryModel(this.name2Col.get(columnName), value, dataType);
  }

  private BloomQueryModel createQueryModel(CarbonColumn carbonColumn, Object filterValue,
      DataType filterValueDataType) throws RuntimeException {
    // the origin filterValue is not string,
    // need to convert it the string and it will be used during converting
    String stringFilterValue = null;
    if (null != filterValue) {
      stringFilterValue = String.valueOf(filterValue);
    }
    Object convertedValue =  this.name2Converters.get(carbonColumn.getColName()).convert(
            stringFilterValue, badRecordLogHolder);

    byte[] internalFilterValue = null;
    try {
      if (carbonColumn.getEncoder().contains(Encoding.DICTIONARY)) {
        internalFilterValue = this.keyGenerator.generateKey(new int[] { (int) convertedValue });
      } else {
        internalFilterValue = convertValueAsBytes(convertedValue);
      }
    } catch (KeyGenException e) {
      throw new RuntimeException(e);
    }
    LOGGER.error("XU createQueryModel: col->" + carbonColumn.getColName()
        + ", colDatatype->" + carbonColumn.getDataType()
        + ", filterValue->" + filterValue
        + ", filterValueDataType->" + filterValueDataType
        + ", convertedValue->" + convertedValue
        + ", internalValue->" + Arrays.toString(internalFilterValue));
    return new BloomQueryModel(carbonColumn.getColName(), internalFilterValue);
  }

  private byte[] convertValueAsBytes(Object value) {
    if (value instanceof Integer) {
      return ByteUtil.toBytes((int) value);
    } else if (value instanceof Long) {
      return ByteUtil.toBytes((long) value);
    } else if (value instanceof Short) {
      return ByteUtil.toBytes((short) value);
    } else if (value instanceof Double) {
      return ByteUtil.toBytes((double) value);
    } else if (value instanceof String) {
      return ByteUtil.toBytes((String) value);
    } else if (value instanceof Boolean) {
      return ByteUtil.toBytes((boolean) value);
    } else if (value instanceof byte[]) {
      return (byte[]) value;
    } else {
      throw new RuntimeException("Unknown data type for value: " + value);
    }
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
    return true;
  }

  @Override
  public void clear() {
    bloomIndexList.clear();
    bloomIndexList = null;
  }

  /**
   * get bloom index file
   * @param shardPath path for the shard
   * @param colName index column name
   */
  public static String getBloomIndexFile(String shardPath, String colName) {
    return shardPath.concat(File.separator).concat(colName).concat(BLOOM_INDEX_SUFFIX);
  }
  static class BloomQueryModel {
    private String columnName;
    private byte[] filterValue;

    private BloomQueryModel(String columnName, byte[] filterValue) {
      this.columnName = columnName;
      this.filterValue = filterValue;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("BloomQueryModel{");
      sb.append("columnName='").append(columnName).append('\'');
      sb.append(", filterValue=").append(filterValue);
      sb.append('}');
      return sb.toString();
    }
  }

  @Override
  public void finish() {

  }
}
