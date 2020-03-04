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
import com.google.api.gax.rpc.StateCheckingResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.read.ResultRowAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.ResultScannerExtended;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;

public class DataClientVeneerApi implements DataClientWrapper {

  private final BigtableDataClient delegate;

  DataClientVeneerApi(BigtableDataClient delegate) {
    this.delegate = delegate;
  }

  @Override
  public BulkMutationWrapper createBulkMutationWrapper(String tableId) {
    return new BulkMutationVeneerApi(delegate.newBulkMutationBatcher(tableId));
  }

  @Override
  public BulkReadWrapper createBulkReadWrapper(String tableId) {
    return new BulkReadVeneerApi(tableId, delegate);
  }

  @Override
  public ApiFuture<Void> mutateRowsAsync(RowMutation rowMutation) {
    return delegate.mutateRowAsync(rowMutation);
  }

  @Override
  public ApiFuture<Result> readModifyRowsAsync(ReadModifyWriteRow readModifyWriteRow) {
    return ApiFutures.transform(
        delegate.readModifyWriteRowAsync(readModifyWriteRow),
        new ApiFunction<Row, Result>() {
          @Override
          public Result apply(Row row) {
            return Adapters.ROW_ADAPTER.adaptResponse(row);
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation) {
    return delegate.checkAndMutateRowAsync(conditionalRowMutation);
  }

  @Override
  public ApiFuture<List<KeyOffset>> sampleRowKeysAsync(String tableId) {
    return delegate.sampleRowKeysAsync(tableId);
  }

  @Override
  public ResultScanner<Result> readRows(Query request) {
    return new ResultScannerExtended<>(
        delegate.readRowsCallable(new ResultRowAdapter()).call(request), new Result[0]);
  }

  @Override
  public ApiFuture<List<Result>> readRowsAsync(Query request) {
    return delegate.readRowsCallable(new ResultRowAdapter()).all().futureCall(request);
  }

  @Override
  public void readRowsAsync(Query request, StreamObserver<Result> observer) {
    delegate
        .readRowsCallable(new ResultRowAdapter())
        .call(request, new StreamObserverAdapter<>(observer));
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  /**
   * To wrap stream of native CBT client's {@link StreamObserver} onto GCJ {@link
   * com.google.api.gax.rpc.ResponseObserver}.
   */
  private static class StreamObserverAdapter<T> extends StateCheckingResponseObserver<T> {
    private final StreamObserver<T> delegate;

    StreamObserverAdapter(StreamObserver<T> delegate) {
      this.delegate = delegate;
    }

    protected void onStartImpl(StreamController controller) {}

    protected void onResponseImpl(T response) {
      this.delegate.onNext(response);
    }

    protected void onErrorImpl(Throwable t) {
      this.delegate.onError(t);
    }

    protected void onCompleteImpl() {
      this.delegate.onCompleted();
    }
  }
}
