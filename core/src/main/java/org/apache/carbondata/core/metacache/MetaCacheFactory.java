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
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

public class MetaCacheFactory {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(MetaCacheFactory.class.getName());
  private static final Map<MetaCacheType, AbstractBaseCache> allCache =
      new HashMap<MetaCacheType, AbstractBaseCache>(3);
  static {
    allCache.put(MetaCacheType.CARBON_TABLE_CACHE, CarbonTableCache.getInstance());
    allCache.put(MetaCacheType.TABLE_STATUS_CACHE, TableStatusCache.getInstance());
    allCache.put(MetaCacheType.TABLE_UPDATE_STATUS_CACHE, TableUpdateStatusCache.getInstance());
  }

  private static AbstractBaseCache getCacheByType(MetaCacheType metaCacheType) {
    AbstractBaseCache baseCache = allCache.get(metaCacheType);
    if (null == baseCache) {
      throw new IllegalArgumentException("Unsupported cache type: " + metaCacheType);
    }
    return baseCache;
  }

  public static Object getValueFromCache(MetaCacheType metaCacheType,
      SimpleTableModel simpleTableModel) throws IOException {
    AbstractBaseCache baseCache = getCacheByType(metaCacheType);
    return baseCache.getValueFromCache(simpleTableModel);
  }

  public static Object getValueFromCacheOrElse(MetaCacheType metaCacheType,
      SimpleTableModel simpleTableModel, Object defaultValue) {
    AbstractBaseCache baseCache = getCacheByType(metaCacheType);
    return baseCache.getValueFromCacheOrElse(simpleTableModel, defaultValue);
  }

  public static void refreshCache(MetaCacheType metaCacheType,
      SimpleTableModel simpleTableModel) {
    AbstractBaseCache baseCache = getCacheByType(metaCacheType);
    baseCache.refreshCache(simpleTableModel);
  }

  public static void refreshAllCache(SimpleTableModel simpleTableModel) {
    LOGGER.info("XU-refresh all caches for " + simpleTableModel);
    refreshCache(MetaCacheType.CARBON_TABLE_CACHE, simpleTableModel);
    refreshCache(MetaCacheType.TABLE_STATUS_CACHE, simpleTableModel);
    refreshCache(MetaCacheType.TABLE_UPDATE_STATUS_CACHE, simpleTableModel);
  }
}
