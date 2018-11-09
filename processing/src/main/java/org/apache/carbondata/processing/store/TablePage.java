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

package org.apache.carbondata.processing.store;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.EncodedTablePage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodingFactory;
import org.apache.carbondata.core.datastore.page.key.TablePageKey;
import org.apache.carbondata.core.datastore.page.statistics.KeyPageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.LVLongStringStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.LVShortStringStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.ComplexColumnInfo;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.datatypes.GenericDataType;

import org.apache.log4j.Logger;

/**
 * Represent a page data for all columns, we store its data in columnar layout, so that
 * all processing apply to TablePage can be done in vectorized fashion.
 */
public class TablePage {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(TablePage.class.getName());

  // For all dimension and measure columns, we store the column data directly in the page,
  // the length of the page is the number of rows.

  // TODO: we should have separate class for key columns so that keys are stored together in
  // one vector to make it efficient for sorting
  private ColumnPage[] dictDimensionPages;
  private ColumnPage[] noDictDimensionPages;
  private ColumnPage[] measurePages;
  private ComplexColumnPage[] complexDimensionPages;

  // the num of rows in this page, it must be less than short value (65536)
  private int pageSize;

  private CarbonFactDataHandlerModel model;

  private TablePageKey key;

  private EncodedTablePage encodedTablePage;

  private EncodingFactory encodingFactory = DefaultEncodingFactory.getInstance();

  // true if it is last page of all input rows
  private boolean isLastPage;

  // used for complex column to deserilize the byte array in input CarbonRow
  private Map<Integer, GenericDataType> complexIndexMap = null;
  // name of compressor that used to compress column data,
  // currently all the columns share the same compressor.
  private String columnCompressor;
  private boolean enableParallelizeEncoding;
  private ExecutorService encoderExecutorService;
  private Future<List<EncodedColumnPage>>[] dimEncoderFutures;
  private Future<EncodedColumnPage>[] msrEncoderFutures;

  TablePage(CarbonFactDataHandlerModel model, int pageSize) throws MemoryException {
    this.model = model;
    this.pageSize = pageSize;
    int numDictDimension = model.getMDKeyGenerator().getDimCount();
    TableSpec tableSpec = model.getTableSpec();
    this.columnCompressor = model.getColumnCompressor();
    this.enableParallelizeEncoding = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_LOAD_PARALLELIZE_ENCODING_ENABLE,
        CarbonCommonConstants.CARBON_LOAD_PARALLELIZE_ENCODING_ENABLE_DEFAULT)
        .equalsIgnoreCase("true");
    if (enableParallelizeEncoding) {
      encoderExecutorService =
          Executors.newFixedThreadPool(tableSpec.getNumDimensions() + tableSpec.getNumMeasures());
      dimEncoderFutures = new Future[tableSpec.getNumDimensions()];
      msrEncoderFutures = new Future[tableSpec.getNumMeasures()];
    }

    dictDimensionPages = new ColumnPage[numDictDimension];
    noDictDimensionPages = new ColumnPage[model.getNoDictionaryCount()];
    int tmpNumDictDimIdx = 0;
    int tmpNumNoDictDimIdx = 0;
    for (int i = 0; i < dictDimensionPages.length + noDictDimensionPages.length; i++) {
      TableSpec.DimensionSpec spec = tableSpec.getDimensionSpec(i);
      ColumnType columnType = tableSpec.getDimensionSpec(i).getColumnType();
      ColumnPage page;
      if (ColumnType.GLOBAL_DICTIONARY == columnType
          || ColumnType.DIRECT_DICTIONARY == columnType) {
        page = ColumnPage.newPage(
            new ColumnPageEncoderMeta(spec, DataTypes.BYTE_ARRAY, columnCompressor), pageSize);
        page.setStatsCollector(KeyPageStatsCollector.newInstance(DataTypes.BYTE_ARRAY));
        dictDimensionPages[tmpNumDictDimIdx++] = page;
      } else {
        // will be encoded using string page
        LocalDictionaryGenerator localDictionaryGenerator =
            model.getColumnLocalDictGenMap().get(spec.getFieldName());
        DataType dataType = DataTypes.STRING;
        if (DataTypes.VARCHAR == spec.getSchemaDataType()) {
          dataType = DataTypes.VARCHAR;
        }
        ColumnPageEncoderMeta columnPageEncoderMeta =
            new ColumnPageEncoderMeta(spec, dataType, columnCompressor);
        if (null != localDictionaryGenerator) {
          page = ColumnPage.newLocalDictPage(
              columnPageEncoderMeta, pageSize, localDictionaryGenerator, false);
        } else {
          if (DataTypeUtil.isPrimitiveColumn(spec.getSchemaDataType())) {
            if (spec.getSchemaDataType() == DataTypes.TIMESTAMP) {
              columnPageEncoderMeta =
                  new ColumnPageEncoderMeta(spec, DataTypes.LONG, columnCompressor);
            } else {
              columnPageEncoderMeta =
                  new ColumnPageEncoderMeta(spec, spec.getSchemaDataType(), columnCompressor);
            }
            // create the column page according to the data type for no dictionary numeric columns
            if (DataTypes.isDecimal(spec.getSchemaDataType())) {
              page = ColumnPage.newDecimalPage(columnPageEncoderMeta, pageSize);
            } else {
              page = ColumnPage.newPage(columnPageEncoderMeta, pageSize);
            }
          } else {
            page = ColumnPage.newPage(columnPageEncoderMeta, pageSize);
          }
        }
        // set the stats collector according to the data type of the columns
        if (DataTypes.VARCHAR == dataType) {
          page.setStatsCollector(LVLongStringStatsCollector.newInstance());
        } else if (DataTypeUtil.isPrimitiveColumn(spec.getSchemaDataType())) {
          if (spec.getSchemaDataType() == DataTypes.TIMESTAMP) {
            page.setStatsCollector(PrimitivePageStatsCollector.newInstance(DataTypes.LONG));
          } else {
            page.setStatsCollector(
                PrimitivePageStatsCollector.newInstance(spec.getSchemaDataType()));
          }
        } else {
          page.setStatsCollector(LVShortStringStatsCollector.newInstance());
        }
        noDictDimensionPages[tmpNumNoDictDimIdx++] = page;
      }
    }
    complexDimensionPages = new ComplexColumnPage[model.getComplexColumnCount()];
    for (int i = 0; i < complexDimensionPages.length; i++) {
      // here we still do not the depth of the complex column, it will be initialized when
      // we get the first row.
      complexDimensionPages[i] = null;
    }
    measurePages = new ColumnPage[model.getMeasureCount()];
    DataType[] dataTypes = model.getMeasureDataType();
    for (int i = 0; i < measurePages.length; i++) {
      ColumnPageEncoderMeta columnPageEncoderMeta = new ColumnPageEncoderMeta(
          model.getTableSpec().getMeasureSpec(i), dataTypes[i], columnCompressor);
      ColumnPage page;
      if (DataTypes.isDecimal(columnPageEncoderMeta.getSchemaDataType())) {
        page = ColumnPage.newDecimalPage(columnPageEncoderMeta, pageSize);
      } else {
        page = ColumnPage.newPage(columnPageEncoderMeta, pageSize);
      }
      page.setStatsCollector(PrimitivePageStatsCollector.newInstance(dataTypes[i]));
      measurePages[i] = page;
    }

    boolean hasNoDictionary = noDictDimensionPages.length > 0;
    this.key = new TablePageKey(pageSize, model.getSegmentProperties(), hasNoDictionary);

    // for complex type, `complexIndexMap` is used in multithread (in multiple Producer),
    // we need to clone the index map to make it thread safe
    this.complexIndexMap = new HashMap<>();
    for (Map.Entry<Integer, GenericDataType> entry: model.getComplexIndexMap().entrySet()) {
      this.complexIndexMap.put(entry.getKey(), entry.getValue().deepCopy());
    }
  }

  /**
   * Add one row to the internal store
   *
   * @param rowId Id of the input row
   * @param row   row object
   */
  public void addRow(int rowId, CarbonRow row) throws KeyGenException {
    // convert each column category, update key and stats
    byte[] mdk = WriteStepRowUtil.getMdk(row, model.getMDKeyGenerator());
    convertToColumnarAndAddToPages(rowId, row, mdk);
    key.update(rowId, row, mdk);
  }

  // convert the input row object to columnar data and add to column pages
  private void convertToColumnarAndAddToPages(int rowId, CarbonRow row, byte[] mdk)
      throws KeyGenException {
    // 1. convert dictionary columns
    byte[][] keys = model.getSegmentProperties().getFixedLengthKeySplitter().splitKey(mdk);
    for (int i = 0; i < dictDimensionPages.length; i++) {
      dictDimensionPages[i].putData(rowId, keys[i]);
    }

    // 2. convert noDictionary columns and complex columns and varchar columns.
    int noDictionaryCount = noDictDimensionPages.length;
    int complexColumnCount = complexDimensionPages.length;
    if (noDictionaryCount > 0 || complexColumnCount > 0) {
      TableSpec tableSpec = model.getTableSpec();
      List<TableSpec.DimensionSpec> noDictionaryDimensionSpec =
          tableSpec.getNoDictionaryDimensionSpec();
      Object[] noDictAndComplex = WriteStepRowUtil.getNoDictAndComplexDimension(row);
      for (int i = 0; i < noDictAndComplex.length; i++) {
        if (noDictionaryDimensionSpec.get(i).getSchemaDataType()
            == DataTypes.VARCHAR) {
          byte[] valueWithLength = addIntLengthToByteArray((byte[]) noDictAndComplex[i]);
          noDictDimensionPages[i].putData(rowId, valueWithLength);
        } else if (i < noDictionaryCount) {
          if (DataTypeUtil
              .isPrimitiveColumn(noDictDimensionPages[i].getColumnSpec().getSchemaDataType())) {
            // put the actual data to the row
            Object value = noDictAndComplex[i];
            // in compaction flow the measure with decimal type will come as Spark decimal.
            // need to convert it to byte array.
            if (DataTypes.isDecimal(noDictDimensionPages[i].getDataType()) && model
                .isCompactionFlow() && value != null) {
              value = DataTypeUtil.getDataTypeConverter().convertFromDecimalToBigDecimal(value);
            }
            noDictDimensionPages[i].putData(rowId, value);
          } else {
            // noDictionary columns, since it is variable length, we need to prepare each
            // element as LV result byte array (first two bytes are the length of the array)
            byte[] valueWithLength = addShortLengthToByteArray((byte[]) noDictAndComplex[i]);
            noDictDimensionPages[i].putData(rowId, valueWithLength);
          }
        } else {
          // complex columns
          addComplexColumn(i - noDictionaryCount, rowId, (byte[]) noDictAndComplex[i]);
        }
      }
    }

    // 3. convert measure columns
    Object[] measureColumns = WriteStepRowUtil.getMeasure(row);
    for (int i = 0; i < measurePages.length; i++) {
      Object value = measureColumns[i];

      // in compaction flow the measure with decimal type will come as Spark decimal.
      // need to convert it to byte array.
      if (DataTypes.isDecimal(measurePages[i].getDataType()) &&
          model.isCompactionFlow() &&
          value != null) {
        value = DataTypeUtil.getDataTypeConverter().convertFromDecimalToBigDecimal(value);
      }
      measurePages[i].putData(rowId, value);
    }
  }

  /**
   * add a complex column into internal member complexDimensionPage
   *
   * @param index          index of the complexDimensionPage
   * @param rowId          Id of the input row
   * @param complexColumns byte array the complex columm to be added, extracted of input row
   */
  // TODO: this function should be refactoried, ColumnPage should support complex type encoding
  // directly instead of doing it here
  private void addComplexColumn(int index, int rowId, byte[] complexColumns) {
    GenericDataType complexDataType = complexIndexMap.get(
        index + model.getPrimitiveDimLens().length);

    // initialize the page if first row
    if (rowId == 0) {
      List<ComplexColumnInfo> complexColumnInfoList = new ArrayList<>();
      complexDataType.getComplexColumnInfo(complexColumnInfoList);
      complexDimensionPages[index] = new ComplexColumnPage(complexColumnInfoList);
      try {
        complexDimensionPages[index].initialize(
            model.getColumnLocalDictGenMap(), pageSize, columnCompressor);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    }

    int depthInComplexColumn = complexDimensionPages[index].getComplexColumnIndex();
    // this is the result columnar data which will be added to page,
    // size of this list is the depth of complex column, we will fill it by input data
    List<ArrayList<byte[]>> encodedComplexColumnar = new ArrayList<>(depthInComplexColumn);
    for (int k = 0; k < depthInComplexColumn; k++) {
      encodedComplexColumnar.add(new ArrayList<byte[]>());
    }

    // apply the complex type data and fill columnsArray
    try {
      ByteBuffer byteArrayInput = ByteBuffer.wrap(complexColumns);
      ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutput);
      complexDataType.parseComplexValue(byteArrayInput, dataOutputStream,
          model.getComplexDimensionKeyGenerator());
      complexDataType.getColumnarDataForComplexType(encodedComplexColumnar,
          ByteBuffer.wrap(byteArrayOutput.toByteArray()));
      byteArrayOutput.close();
    } catch (IOException | KeyGenException e) {
      throw new CarbonDataWriterException("Problem while bit packing and writing complex datatype",
          e);
    }

    for (int depth = 0; depth < depthInComplexColumn; depth++) {
      complexDimensionPages[index].putComplexData(depth, encodedComplexColumnar.get(depth));
    }
  }

  void freeMemory() {
    for (ColumnPage page : dictDimensionPages) {
      page.freeMemory();
    }
    for (ColumnPage page : noDictDimensionPages) {
      page.freeMemory();
    }
    for (ColumnPage page : measurePages) {
      page.freeMemory();
    }
    for (ComplexColumnPage page : complexDimensionPages) {
      if (null != page) {
        page.freeMemory();
      }
    }
    if (encoderExecutorService != null) {
      encoderExecutorService.shutdown();
    }
  }

  // Adds length as a short element (first 2 bytes) to the head of the input byte array
  private byte[] addShortLengthToByteArray(byte[] input) {
    if (input.length > Short.MAX_VALUE) {
      throw new RuntimeException("input data length " + input.length +
          " bytes too long, maximum length supported is " + Short.MAX_VALUE + " bytes");
    }
    byte[] output = new byte[input.length + 2];
    ByteBuffer buffer = ByteBuffer.wrap(output);
    buffer.putShort((short)input.length);
    buffer.put(input, 0, input.length);
    return output;
  }

  // Adds length as a integer element (first 4 bytes) to the head of the input byte array
  private byte[] addIntLengthToByteArray(byte[] input) {
    byte[] output = new byte[input.length + 4];
    ByteBuffer buffer = ByteBuffer.wrap(output);
    buffer.putInt(input.length);
    buffer.put(input, 0, input.length);
    return output;
  }

  void encode() throws KeyGenException, MemoryException, IOException {
    // encode dimensions and measure
    EncodedColumnPage[] dimensions;
    EncodedColumnPage[] measures;
    if (!enableParallelizeEncoding) {
      dimensions = encodeAndCompressDimensions();
      measures = encodeAndCompressMeasures();
    } else {
      try {
        encodeAndCompressDimensionsAsyn();
        encodeAndCompressMeasuresAsyn();
        List<EncodedColumnPage> encodedDims = new ArrayList<>();
        for (Future<List<EncodedColumnPage>> dimFurture : dimEncoderFutures) {
          encodedDims.addAll(dimFurture.get());
        }
        dimensions = encodedDims.toArray(new EncodedColumnPage[0]);

        List<EncodedColumnPage> encodedMsrs = new ArrayList<>();
        for (Future<EncodedColumnPage> msrFurture : msrEncoderFutures) {
          encodedMsrs.add(msrFurture.get());
        }
        measures = encodedMsrs.toArray(new EncodedColumnPage[0]);
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error(e);
        throw new IOException(e);
      }
    }
    this.encodedTablePage = EncodedTablePage.newInstance(pageSize, dimensions, measures, key);
  }

  public EncodedTablePage getEncodedTablePage() {
    return encodedTablePage;
  }

  // apply measure and set encodedData in `encodedData`
  private EncodedColumnPage[] encodeAndCompressMeasures()
      throws MemoryException, IOException {
    EncodedColumnPage[] encodedMeasures = new EncodedColumnPage[measurePages.length];
    for (int i = 0; i < measurePages.length; i++) {
      ColumnPageEncoder encoder = encodingFactory.createEncoder(
          model.getTableSpec().getMeasureSpec(i), measurePages[i]);
      encodedMeasures[i] = encoder.encode(measurePages[i]);
    }
    return encodedMeasures;
  }

  private void encodeAndCompressMeasuresAsyn()
      throws MemoryException, IOException {
    for (int i = 0; i < measurePages.length; i++) {
      msrEncoderFutures[i] = encoderExecutorService.submit(new MeasureEncodeThread(i));
    }
  }

  private final class MeasureEncodeThread implements Callable<EncodedColumnPage> {
    private int idxOffset;

    public MeasureEncodeThread(int idxOffset) {
      this.idxOffset = idxOffset;
    }

    @Override
    public EncodedColumnPage call() throws Exception {
      ColumnPageEncoder encoder = encodingFactory.createEncoder(
          model.getTableSpec().getMeasureSpec(idxOffset), measurePages[idxOffset]);
      return encoder.encode(measurePages[idxOffset]);
    }
  }

  // apply and compress each dimension, set encoded data in `encodedData`
  private EncodedColumnPage[] encodeAndCompressDimensions()
      throws KeyGenException, IOException, MemoryException {
    List<EncodedColumnPage> encodedDimensions = new ArrayList<>();
    List<EncodedColumnPage> encodedComplexDimenions = new ArrayList<>();
    TableSpec tableSpec = model.getTableSpec();
    int dictIndex = 0;
    int noDictIndex = 0;
    int complexDimIndex = 0;
    int numDimensions = tableSpec.getNumDimensions();
    for (int i = 0; i < numDimensions; i++) {
      ColumnPageEncoder columnPageEncoder;
      EncodedColumnPage encodedPage;
      TableSpec.DimensionSpec spec = tableSpec.getDimensionSpec(i);
      switch (spec.getColumnType()) {
        case GLOBAL_DICTIONARY:
        case DIRECT_DICTIONARY:
          columnPageEncoder = encodingFactory.createEncoder(
              spec,
              dictDimensionPages[dictIndex]);
          encodedPage = columnPageEncoder.encode(dictDimensionPages[dictIndex++]);
          encodedDimensions.add(encodedPage);
          break;
        case PLAIN_VALUE:
          columnPageEncoder = encodingFactory.createEncoder(
              spec,
              noDictDimensionPages[noDictIndex]);
          encodedPage = columnPageEncoder.encode(noDictDimensionPages[noDictIndex]);
          if (LOGGER.isDebugEnabled()) {
            DataType targetDataType =
                columnPageEncoder.getTargetDataType(noDictDimensionPages[noDictIndex]);
            if (null != targetDataType) {
              LOGGER.debug(
                  "Encoder result ---> Source data type: " + noDictDimensionPages[noDictIndex]
                      .getDataType().getName() + " Destination data type: " + targetDataType
                      .getName() + " for the column: " + noDictDimensionPages[noDictIndex]
                      .getColumnSpec().getFieldName() + " having encoding type: "
                      + columnPageEncoder.getEncodingType());
            }
          }
          noDictIndex++;
          encodedDimensions.add(encodedPage);
          break;
        case COMPLEX:
          EncodedColumnPage[] encodedPages = ColumnPageEncoder.encodeComplexColumn(
              complexDimensionPages[complexDimIndex++]);
          encodedComplexDimenions.addAll(Arrays.asList(encodedPages));
          break;
        default:
          throw new IllegalArgumentException("unsupported dimension type:" + spec
              .getColumnType());
      }
    }

    encodedDimensions.addAll(encodedComplexDimenions);
    return encodedDimensions.toArray(new EncodedColumnPage[encodedDimensions.size()]);
  }

  private final class DimensionEncodeThread implements Callable<List<EncodedColumnPage>> {
    private TableSpec.DimensionSpec spec;
    private int idxOffset;

    public DimensionEncodeThread(TableSpec.DimensionSpec spec, int idxOffset) {
      this.spec = spec;
      this.idxOffset = idxOffset;
    }

    @Override
    public List<EncodedColumnPage> call() throws Exception {
      ColumnPageEncoder columnPageEncoder;
      switch (spec.getColumnType()) {
        case GLOBAL_DICTIONARY:
        case DIRECT_DICTIONARY:
          columnPageEncoder = encodingFactory.createEncoder(
              spec,
              dictDimensionPages[idxOffset]);
          return Arrays.asList(columnPageEncoder.encode(dictDimensionPages[idxOffset]));
        case PLAIN_VALUE:
          columnPageEncoder = encodingFactory.createEncoder(
              spec,
              noDictDimensionPages[idxOffset]);
          if (LOGGER.isDebugEnabled()) {
            DataType targetDataType =
                columnPageEncoder.getTargetDataType(noDictDimensionPages[idxOffset]);
            if (null != targetDataType) {
              LOGGER.debug(
                  "Encoder result ---> Source data type: " + noDictDimensionPages[idxOffset]
                      .getDataType().getName() + " Destination data type: " + targetDataType
                      .getName() + " for the column: " + noDictDimensionPages[idxOffset]
                      .getColumnSpec().getFieldName() + " having encoding type: "
                      + columnPageEncoder.getEncodingType());
            }
          }
          return Arrays.asList(columnPageEncoder.encode(noDictDimensionPages[idxOffset]));
        case COMPLEX:
          return Arrays.asList(
              ColumnPageEncoder.encodeComplexColumn(complexDimensionPages[idxOffset]));
        default:
          throw new IllegalArgumentException("unsupported dimension type:" + spec.getColumnType());
      }
    }
  }

  private void encodeAndCompressDimensionsAsyn()
      throws KeyGenException, IOException, MemoryException {
    TableSpec tableSpec = model.getTableSpec();
    int dictIndex = 0;
    int noDictIndex = 0;
    int complexDimIndex = 0;
    int numDimensions = tableSpec.getNumDimensions();
    Future<List<EncodedColumnPage>>[] tmpFutures = new Future[numDimensions];

    List<Integer> complexPagesIdx = new ArrayList<>();
    for (int i = 0; i < numDimensions; i++) {
      TableSpec.DimensionSpec spec = tableSpec.getDimensionSpec(i);
      switch (spec.getColumnType()) {
        case GLOBAL_DICTIONARY:
        case DIRECT_DICTIONARY:
          tmpFutures[i] = encoderExecutorService.submit(
              new DimensionEncodeThread(spec, dictIndex));
          dictIndex++;
          break;
        case PLAIN_VALUE:
          tmpFutures[i] = encoderExecutorService.submit(
              new DimensionEncodeThread(spec, noDictIndex));
          noDictIndex++;
          break;
        case COMPLEX:
          tmpFutures[i] = encoderExecutorService.submit(
              new DimensionEncodeThread(spec, complexDimIndex));
          complexPagesIdx.add(i);
          complexDimIndex++;
          break;
        default:
          throw new IllegalArgumentException("unsupported dimension type:" + spec
              .getColumnType());
      }
    }

    // rearrange the position of each fields so that complex will at the end
    int j = 0;
    int k = 0;
    for (int i = 0; i < tmpFutures.length; i++) {
      if (complexPagesIdx.contains(i)) {
        dimEncoderFutures[tmpFutures.length - complexPagesIdx.size() + k] = tmpFutures[i];
        k++;
      } else {
        dimEncoderFutures[j] = tmpFutures[i];
        j++;
      }
    }
  }

  /**
   * return column page of specified column name
   */
  public ColumnPage getColumnPage(String columnName) {
    int dictDimensionIndex = -1;
    int noDictDimensionIndex = -1;
    ColumnPage page = null;
    TableSpec spec = model.getTableSpec();
    int numDimensions = spec.getNumDimensions();
    for (int i = 0; i < numDimensions; i++) {
      ColumnType type = spec.getDimensionSpec(i).getColumnType();
      if ((type == ColumnType.GLOBAL_DICTIONARY) || (type == ColumnType.DIRECT_DICTIONARY)) {
        page = dictDimensionPages[++dictDimensionIndex];
      } else if (type == ColumnType.PLAIN_VALUE) {
        page = noDictDimensionPages[++noDictDimensionIndex];
      } else {
        // do not support datamap on complex column
        continue;
      }
      String fieldName = spec.getDimensionSpec(i).getFieldName();
      if (fieldName.equalsIgnoreCase(columnName)) {
        return page;
      }
    }
    int numMeasures = spec.getNumMeasures();
    for (int i = 0; i < numMeasures; i++) {
      String fieldName = spec.getMeasureSpec(i).getFieldName();
      if (fieldName.equalsIgnoreCase(columnName)) {
        return measurePages[i];
      }
    }
    throw new IllegalArgumentException("DataMap: must have '" + columnName + "' column in schema");
  }

  public boolean isLastPage() {
    return isLastPage;
  }

  public void setIsLastPage(boolean isWriteAll) {
    this.isLastPage = isWriteAll;
  }

  public int getPageSize() {
    return pageSize;
  }
}


