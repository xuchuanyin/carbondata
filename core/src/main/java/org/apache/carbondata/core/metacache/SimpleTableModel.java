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

import java.util.Objects;

public class SimpleTableModel {
  private String dbName;
  private String tableName;

  public SimpleTableModel(String dbName, String tableName) {
    this.dbName = dbName;
    this.tableName = tableName;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SimpleTableModel)) return false;
    SimpleTableModel that = (SimpleTableModel) o;
    return Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName);
  }

  @Override public int hashCode() {
    return Objects.hash(dbName, tableName);
  }

  @Override public String toString() {
    final StringBuffer sb = new StringBuffer("SimpleTableModel{");
    sb.append("dbName='").append(dbName).append('\'');
    sb.append(", tableName='").append(tableName).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
