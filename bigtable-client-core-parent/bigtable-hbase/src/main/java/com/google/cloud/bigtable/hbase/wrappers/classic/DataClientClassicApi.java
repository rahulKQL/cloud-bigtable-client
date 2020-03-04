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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BulkReadWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;

public class DataClientClassicApi implements DataClientWrapper {

  private final IBigtableDataClient delegate;
  private final BigtableSession session;
  private final BigtableInstanceName instanceName;

  DataClientClassicApi(BigtableSession session) {
    this.session = session;
    this.delegate = session.getDataClientWrapper();
    this.instanceName = session.getOptions().getInstanceName();
  }

  @Override
  public BulkMutationWrapper createBulkMutationWrapper(String tableId) {
    return new BulkMutationClassicApi(
        session.createBulkMutation(instanceName.toTableName(tableId)));
  }

  @Override
  public BulkReadWrapper createBulkReadWrapper(String tableId) {
    return new BulkReadClassicApi(session.createBulkRead(instanceName.toTableName(tableId)));
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
    return null;
  }

  @Override
  public ApiFuture<List<Result>> readRowsAsync(Query request) {
    return ApiFutures.transform(
        delegate.readFlatRowsAsync(request),
        new ApiFunction<List<FlatRow>, List<Result>>() {
          @Override
          public List<Result> apply(List<FlatRow> rows) {
            ImmutableList.Builder<Result> resultList = ImmutableList.builder();
            for (FlatRow flatRow : rows) {
              resultList.add(Adapters.FLAT_ROW_ADAPTER.adaptResponse(flatRow));
            }
            return resultList.build();
          }
        },
        MoreExecutors.directExecutor());
  }

  @Override
  public void readRowsAsync(Query request, final StreamObserver<Result> observer) {
    delegate.readFlatRowsAsync(
        request,
        new StreamObserver<FlatRow>() {
          @Override
          public void onNext(FlatRow flatRow) {
            observer.onNext(Adapters.FLAT_ROW_ADAPTER.adaptResponse(flatRow));
          }

          @Override
          public void onError(Throwable throwable) {
            observer.onError(throwable);
          }

          @Override
          public void onCompleted() {
            observer.onCompleted();
          }
        });
  }

  @Override
  public void close() throws Exception {
    session.close();
  }
}
