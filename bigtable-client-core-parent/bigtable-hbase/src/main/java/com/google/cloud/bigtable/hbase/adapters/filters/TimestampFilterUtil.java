/*
 * Copyright 2017 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.hbase.adapters.filters;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.hbase.BigtableConstants;

/**
 * Common utilities for Timestamp filters.
 */
public class TimestampFilterUtil {

  /**
   * Converts an HBase timestamp in milliseconds to a Cloud Bigtable timestamp in Microseconds.
   * @param timestamp a long value.
   * @return a long value.
   */
  public static long hbaseToBigtableTimeUnit(long timestamp) {
    return BigtableConstants.BIGTABLE_TIMEUNIT.convert(
        timestamp, BigtableConstants.HBASE_TIMEUNIT);
  }

  /**
   * Converts a [startMs, endMs) timestamps to a Cloud Bigtable [startMicros, endMicros) filter.
   *
   * @param hbaseStartTimestamp a long value.
   * @param hbaseEndTimestamp a long value.
   * @return a {@link Filter} object.
   */
  public static Filter hbaseToTimestampRangeFilter(long hbaseStartTimestamp,
      long hbaseEndTimestamp) {
    return toTimestampRangeFilter(hbaseToBigtableTimeUnit(hbaseStartTimestamp),
      hbaseToBigtableTimeUnit(hbaseEndTimestamp));
  }

  /**
   * Converts a [startMicros, endNons) timestamps to a Cloud Bigtable [startMicros, endMicros)
   * filter.
   *
   * @param bigtableStartTimestamp a long value.
   * @param bigtableEndTimestamp
   * @return a {@link Filter} object.
   */
  public static Filter toTimestampRangeFilter(long bigtableStartTimestamp,
      long bigtableEndTimestamp) {
    return FILTERS.timestamp().range().of(bigtableStartTimestamp, bigtableEndTimestamp);
  }
}
