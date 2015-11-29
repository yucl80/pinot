/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools.scan.query;

import java.util.Collections;


public class AvgFunction extends AggregationFunc {
  private static final String _name = "avg";

  AvgFunction(ResultTable rows, String column) {
    super(rows, column);
  }

  @Override
  public ResultTable run() {
    Double sum = 0.0;
    int numEntries = 0;

    for (ResultTable.Row row : _rows) {
      Object value = row.get(_column, _name);
      if (value instanceof double []) {
        double [] valArray = (double []) value;
        sum += valArray[0];
        numEntries += valArray[1];
      } else {
        sum += new Double(row.get(_column, _name).toString());
        ++numEntries;
      }
    }

    double[] average = new double[2];
    average[0] = sum;
    average[1] = numEntries;

    ResultTable resultTable = new ResultTable(Collections.emptyList(), 1);
    resultTable.add(0, average);
    return resultTable;
  }
}