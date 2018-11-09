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
package org.apache.carbondata.core.scan.result.iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.scan.result.RowBatch;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.log4j.Logger;

/**
 * This is a wrapper iterator over the detail raw query iterator.
 * This iterator will handle the processing of the raw rows.
 * This will handle the batch results and will iterate on the batches and give single row.
 */
public class RawResultIterator extends CarbonIterator<Object[]> {

  private final SegmentProperties sourceSegProperties;

  private final SegmentProperties destinationSegProperties;
  /**
   * Iterator of the Batch raw result.
   */
  private CarbonIterator<RowBatch> detailRawQueryResultIterator;

  private ExecutorService executorService;
  private Object[] currentRawRow = null;

  private int prefetchParallelism;
  private Future[] backupFutures;
  private List<Object[]>[] backupBuffers;
  private int currentWorkingBufferIdx;
  private int currentRowIdxInBuffer;
  private int currentFillingBufferIdx;
  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(RawResultIterator.class.getName());

  public RawResultIterator(CarbonIterator<RowBatch> detailRawQueryResultIterator,
      SegmentProperties sourceSegProperties, SegmentProperties destinationSegProperties,
      boolean isStreamingHandoff) {
    this.detailRawQueryResultIterator = detailRawQueryResultIterator;
    this.sourceSegProperties = sourceSegProperties;
    this.destinationSegProperties = destinationSegProperties;
    String parallelismStr = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_PARALLELISM,
        CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_PARALLELISM_DEFAULT);
    try {
      prefetchParallelism = Integer.parseInt(parallelismStr);
      if (prefetchParallelism < 1) {
        throw new NumberFormatException("Value should be greater than 0");
      }
    } catch (NumberFormatException e) {
      LOGGER.warn(String.format(
          "The configured value %s for %s is invalid. Using default value %s.",
          parallelismStr,
          CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_PARALLELISM,
          CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_PARALLELISM_DEFAULT));
      prefetchParallelism = Integer.parseInt(
          CarbonCommonConstants.CARBON_COMPACTION_PREFETCH_PARALLELISM_DEFAULT);
    }
    this.executorService = Executors.newFixedThreadPool(prefetchParallelism);
    this.backupFutures = new Future[prefetchParallelism];
    this.backupBuffers = new List[prefetchParallelism];

    if (!isStreamingHandoff) {
      init();
    }
  }

  private void init() {
    try {
      backupBuffers[0] = new RowsFetcher().call();
      currentFillingBufferIdx = 1;
      while (currentFillingBufferIdx < prefetchParallelism) {
        backupFutures[currentFillingBufferIdx] = executorService.submit(new RowsFetcher());
        currentFillingBufferIdx++;
      }
      currentFillingBufferIdx = (currentFillingBufferIdx - 1) % prefetchParallelism;

    } catch (Exception e) {
      LOGGER.error("Error occurs while fetching records", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * fetch rows
   */
  private final class RowsFetcher implements Callable<List<Object[]>> {
    private RowsFetcher() {
    }

    @Override
    public List<Object[]> call() throws Exception {
      List<Object[]> converted = new ArrayList<>();
      if (detailRawQueryResultIterator.hasNext()) {
        for (Object[] r : detailRawQueryResultIterator.next().getRows()) {
          converted.add(convertRow(r));
        }
      }
      return converted;
    }
  }

  private void fillDataFromPrefetch() {
    try {
      if (currentRowIdxInBuffer >= backupBuffers[currentWorkingBufferIdx].size()
          && 0 != currentRowIdxInBuffer) {
        if (prefetchParallelism == 1) {
          backupBuffers[currentWorkingBufferIdx] = new RowsFetcher().call();
          currentRowIdxInBuffer = 0;
          return;
        }

        // clear previous buffer and switch to next buffer as working buffer
        backupBuffers[currentWorkingBufferIdx] = null;
        currentWorkingBufferIdx = (currentWorkingBufferIdx + 1) % prefetchParallelism;
        backupBuffers[currentWorkingBufferIdx] =
            (List<Object[]>) backupFutures[currentWorkingBufferIdx].get();
        currentRowIdxInBuffer = 0;
        // filling backup buffer asynchronously
        currentFillingBufferIdx = (currentFillingBufferIdx + 1) % prefetchParallelism;
        backupFutures[currentFillingBufferIdx] = executorService.submit(new RowsFetcher());
      }
    } catch (Exception e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * populate a row with index counter increased
   */
  private void popRow() {
    fillDataFromPrefetch();
    currentRawRow = backupBuffers[currentWorkingBufferIdx].get(currentRowIdxInBuffer);
    currentRowIdxInBuffer++;
  }

  /**
   * populate a row with index counter unchanged
   */
  private void pickRow() {
    fillDataFromPrefetch();
    currentRawRow = backupBuffers[currentWorkingBufferIdx].get(currentRowIdxInBuffer);
  }

  @Override
  public boolean hasNext() {
    fillDataFromPrefetch();
    if (currentRowIdxInBuffer < backupBuffers[currentWorkingBufferIdx].size()) {
      return true;
    }

    return false;
  }

  @Override
  public Object[] next() {
    try {
      popRow();
      return this.currentRawRow;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * for fetching the row with out incrementing counter.
   * @return
   */
  public Object[] fetchConverted() throws KeyGenException {
    pickRow();
    return this.currentRawRow;
  }

  private Object[] convertRow(Object[] rawRow) throws KeyGenException {
    byte[] dims = ((ByteArrayWrapper) rawRow[0]).getDictionaryKey();
    long[] keyArray = sourceSegProperties.getDimensionKeyGenerator().getKeyArray(dims);
    byte[] covertedBytes =
        destinationSegProperties.getDimensionKeyGenerator().generateKey(keyArray);
    ((ByteArrayWrapper) rawRow[0]).setDictionaryKey(covertedBytes);
    return rawRow;
  }

  public void close() {
    if (null != executorService) {
      executorService.shutdownNow();
    }
  }
}