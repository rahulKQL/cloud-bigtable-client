/*
 * Copyright 2020 Google LLC.
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
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.client.Result;

public class BulkReadVeneerApi implements BulkReadWrapper {
  private final String tableId;
  private final BigtableDataClient dataClient;
  private final RequestContext requestContext;
  private final Map<RowFilter, Batcher<ByteString, Row>> batches;

  public BulkReadVeneerApi(String tableId, BigtableDataClient dataClient) {
    this.tableId = tableId;
    this.dataClient = dataClient;
    this.batches = new HashMap<>();
    // required to retrieve the row key from Query instance
    this.requestContext = RequestContext.create("", "", "");
  }

  @Override
  public ApiFuture<Result> add(Query query) {
    Preconditions.checkNotNull(query);
    ReadRowsRequest request = query.toProto(requestContext);

    Preconditions.checkArgument(request.getRows().getRowKeysCount() == 1);
    ByteString rowKey = request.getRows().getRowKeysList().get(0);
    Preconditions.checkArgument(!rowKey.equals(ByteString.EMPTY));

    RowFilter filter = request.getFilter();
    Batcher<ByteString, Row> batcher = batches.get(filter);
    if (batcher == null) {
      batcher = dataClient.newBulkReadRowsBatcher(tableId, Filters.FILTERS.fromProto(filter));
      batches.put(filter, batcher);
    }
    return ApiFutures.transform(
        batcher.add(rowKey),
        new ApiFunction<Row, Result>() {
          @Override
          public Result apply(Row row) {
            return Adapters.ROW_ADAPTER.adaptResponse(row);
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public void flush() {
    // BulkRead#flush() doesn't wait till entries are resolved.
    for (Batcher<ByteString, Row> batch : batches.values()) {
      batch.sendOutstanding();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      for (Batcher<ByteString, Row> batch : batches.values()) {
        batch.close();
      }
    } catch (InterruptedException exception) {
      throw new IOException("Could not close the bulk read Batcher", exception);
    }
  }
}
