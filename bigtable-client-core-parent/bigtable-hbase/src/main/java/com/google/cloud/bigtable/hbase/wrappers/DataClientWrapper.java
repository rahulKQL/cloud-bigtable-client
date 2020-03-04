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
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;

public interface DataClientWrapper extends AutoCloseable {

  BulkMutationWrapper createBulkMutationWrapper(String tableId);

  BulkReadWrapper createBulkReadWrapper(String tableId);

  /**
   * Mutate a row atomically.
   *
   * @param rowMutation a {@link RowMutation} model object.
   * @return a {@link ApiFuture} of type {@link Void} will be set when request is successful
   *     otherwise exception will be thrown.
   */
  ApiFuture<Void> mutateRowsAsync(RowMutation rowMutation);

  /**
   * Perform an atomic read-modify-write operation on a row.
   *
   * @param readModifyWriteRow a {@link ReadModifyWriteRow} model object.
   * @return a {@link ApiFuture} of type {@link Result} will be set when request is successful
   *     otherwise exception will be thrown.
   */
  ApiFuture<Result> readModifyRowsAsync(ReadModifyWriteRow readModifyWriteRow);

  /**
   * Mutate a row atomically dependent on a precondition.
   *
   * @param conditionalRowMutation a {@link ConditionalRowMutation} model object.
   * @return a {@link ApiFuture} of type {@link Boolean} will be set when request is successful
   *     otherwise exception will be thrown.
   */
  ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation);

  /**
   * Sample row keys from a table, returning a Future that will complete when the sampling has
   * completed.
   *
   * @param tableId a String object.
   * @return a {@link ApiFuture} object.
   */
  ApiFuture<List<KeyOffset>> sampleRowKeysAsync(String tableId);

  /**
   * Perform a scan over {@link Result}s, in key order.
   *
   * @param request a {@link Query} object.
   * @return a {@link Result} object.
   */
  ResultScanner<Result> readRows(Query request);

  /**
   * Read multiple {@link Result}s into an in-memory list, in key order.
   *
   * @return a {@link ApiFuture} that will finish when all reads have completed.
   * @param request a {@link Query} object.
   */
  ApiFuture<List<Result>> readRowsAsync(Query request);

  /**
   * Read {@link Result} asynchronously, and pass them to a stream observer to be processed.
   *
   * @param request a {@link Query} object.
   * @param observer a {@link StreamObserver} object.
   */
  void readRowsAsync(Query request, StreamObserver<Result> observer);
}
