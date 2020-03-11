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
package com.google.cloud.bigtable.hbase.wrappers;

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;

/**
 * Common API surface for data operation.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public interface DataClientWrapper extends AutoCloseable {

  /** Creates instance of bulkMutation with specified table ID. */
  BulkMutationWrapper createBulkMutation(String tableId);

  /** Creates {@link BulkReadWrapper} with specified table ID. */
  BulkReadWrapper createBulkRead(String tableId);

  /** Mutate a row atomically. */
  ApiFuture<Void> mutateRowsAsync(RowMutation rowMutation);

  /** Perform an atomic read-modify-write operation on a row. */
  ApiFuture<Result> readModifyRowsAsync(ReadModifyWriteRow readModifyWriteRow);

  /** Mutate a row atomically dependent on a precondition. */
  ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation);

  /**
   * Sample row keys from a table, returning a Future that will complete when the sampling has
   * completed.
   */
  ApiFuture<List<KeyOffset>> sampleRowKeysAsync(String tableId);

  /** Perform a scan over {@link Result}s, in key order. */
  ResultScanner<Result> readRows(Query request);

  /** Read multiple {@link Result}s into an in-memory list, in key order. */
  ApiFuture<List<Result>> readRowsAsync(Query request);

  /** Read {@link Result} asynchronously, and pass them to a stream observer to be processed. */
  void readRowsAsync(Query request, StreamObserver<Result> observer);
}
