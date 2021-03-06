/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.client;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * A Pinot query result set for aggregation results without group by clauses, of which there is one
 * of per aggregation
 * function in the query.
 */
class AggregationResultSet extends AbstractResultSet {
  private final JSONObject _jsonObject;

  public AggregationResultSet(JSONObject jsonObject) {
    _jsonObject = jsonObject;
  }

  @Override
  public int getRowCount() {
    return 1;
  }

  @Override
  public int getColumnCount() {
    return 1;
  }

  @Override
  public String getColumnName(int columnIndex) {
    try {
      return _jsonObject.getString("function");
    } catch (JSONException e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public String getString(int rowIndex, int columnIndex) {
    if (columnIndex != 0) {
      throw new IllegalArgumentException(
          "Column index must always be 0 for aggregation result sets");
    }

    if (rowIndex != 0) {
      throw new IllegalArgumentException("Row index must always be 0 for aggregation result sets");
    }

    try {
      return _jsonObject.getString("value");
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public int getGroupKeyLength() {
    return 0;
  }

  @Override
  public String getGroupKeyColumnName(int groupKeyColumnIndex) {
    throw new AssertionError("No group key column name for aggregation results");
  }

  @Override
  public String getGroupKeyString(int rowIndex, int groupKeyColumnIndex) {
    throw new AssertionError("No grouping key for queries without a group by clause");
  }

  @Override
  public String toString() {
    int numColumns = getColumnCount();
    TextTable table = new TextTable();
    String[] columnNames = new String[numColumns];

    for (int c = 0; c < getColumnCount(); c++) {
      columnNames[c] = getColumnName(c);
    }
    table.addHeader(columnNames);

    int numRows = getRowCount();
    for (int r = 0; r < numRows; r++) {
      String[] columnValues = new String[numColumns];
      for (int c = 0; c < getColumnCount(); c++) {
        columnValues[c] = getString(r, c);
      }
      table.addRow(columnValues);
    }
    return table.toString();
  }
}
