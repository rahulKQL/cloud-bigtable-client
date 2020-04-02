/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.Batcher;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.Result;

/**
 * Adapter for sending batch reads.
 *
 * <p>This class works with {@link com.google.cloud.bigtable.hbase.BatchExecutor} to enable bulk
 * reads from the hbase api.
 *
 * <p>This class is not thread safe. It must be used on a single thread
 */
public class BulkReadVeneerApi implements BulkReadWrapper {

  private final BigtableDataClient client;
  private final String tableId;
  private final Map<RowFilter, Batcher<ByteString, Row>> batchers;

  // TODO: remove this once gax-java's Batcher supports asyncClose(). This will eliminate the need
  //  to track individual entries
  private final AtomicLong cleanupBarrier;

  BulkReadVeneerApi(BigtableDataClient client, String tableId) {
    this.client = client;
    this.tableId = tableId;

    this.batchers = new HashMap<>();
    this.cleanupBarrier = new AtomicLong();
    this.cleanupBarrier
        .incrementAndGet(); // wait for sendAsync to signal before cleaning up the batcher map
  }

  @Override
  public ApiFuture<Result> add(ByteString rowKey, @Nullable Filters.Filter filter) {
    cleanupBarrier.incrementAndGet();

    ApiFuture<Row> rowFuture = getOrCreateBatcher(filter).add(rowKey);
    rowFuture.addListener(
        new Runnable() {
          @Override
          public void run() {
            notifyArrival();
          }
        },
        MoreExecutors.directExecutor());

    return ApiFutures.transform(
        rowFuture,
        new ApiFunction<Row, Result>() {
          @Override
          public Result apply(Row row) {
            return Adapters.ROW_ADAPTER.adaptResponse(row);
          }
        },
        MoreExecutors.directExecutor());
  }

  private void notifyArrival() {
    if (cleanupBarrier.decrementAndGet() == 0) {
      cleanUp();
    }
  }

  /** close all outstanding Batchers */
  private void cleanUp() {
    for (Batcher<ByteString, Row> batcher : batchers.values()) {
      try {
        batcher.close();
      } catch (Throwable ignored) {
        // Ignored
      }
    }
    batchers.clear();
  }

  @Override
  public void sendAsync() {
    for (Batcher<ByteString, Row> batcher : batchers.values()) {
      batcher.sendOutstanding();
    }
    notifyArrival();
  }

  private Batcher<ByteString, Row> getOrCreateBatcher(@Nullable Filters.Filter filter) {
    RowFilter proto = filter == null ? null : filter.toProto();

    Batcher<ByteString, Row> batcher = batchers.get(proto);
    if (batcher == null) {
      batcher = client.newBulkReadRowsBatcher(tableId, filter);
      batchers.put(proto, batcher);
    }
    return batcher;
  }
}