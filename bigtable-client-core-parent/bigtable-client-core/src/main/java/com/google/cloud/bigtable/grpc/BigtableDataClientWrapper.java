/*
 * Copyright 2018 Google LLC.  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import javax.annotation.Nullable;

/**
 * This class implements the {@link IBigtableDataClient} interface which provides access to google cloud
 * java.
 */
public class BigtableDataClientWrapper implements IBigtableDataClient {

  private final BigtableDataClient delegate;
  private final RequestContext requestContext;

  public BigtableDataClientWrapper(BigtableDataClient bigtableDataClient,
      BigtableOptions options) {
    this.delegate = bigtableDataClient;
    this.requestContext = RequestContext
        .create(InstanceName.of(options.getProjectId(),
            options.getInstanceId()),
            options.getAppProfileId()
        );
  }

  /** {@inheritDoc} */
  @Override
  public void mutateRow(RowMutation rowMutation) {
    MutateRowRequest mutateRowRequest = rowMutation.toProto(requestContext);
    delegate.mutateRow(mutateRowRequest);
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Void> mutateRowAsync(RowMutation rowMutation) {
    ListenableFuture<MutateRowResponse> response =
        delegate.mutateRowAsync(rowMutation.toProto(requestContext));
    return  Futures.transform(response, new Function<MutateRowResponse, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable MutateRowResponse mutateRowResponse) {
        return null;
      }
    }, MoreExecutors.directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public Row readModifyWriteRow(ReadModifyWriteRow readModifyWriteRow) {
    ReadModifyWriteRowResponse response =
        delegate.readModifyWriteRow(readModifyWriteRow.toProto(requestContext));
    return transformResponse(response);
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    ListenableFuture<ReadModifyWriteRowResponse> response =
        delegate.readModifyWriteRowAsync(readModifyWriteRow.toProto(requestContext));
    return Futures.transform(response, new Function<ReadModifyWriteRowResponse, Row>() {
      @Override
      public Row apply(ReadModifyWriteRowResponse readModifyRowResponse) {
        return transformResponse(readModifyRowResponse);
      }
    }, MoreExecutors.directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public IBulkMutation createBulkMutationBatcher() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /** {@inheritDoc} */
  @Override
  public ListenableFuture<Boolean> checkAndMutateRowAsync(
      ConditionalRowMutation conditionalRowMutation) {
    final CheckAndMutateRowRequest request = conditionalRowMutation.toProto(requestContext);
    final ListenableFuture<CheckAndMutateRowResponse> response =
        delegate.checkAndMutateRowAsync(request);
    return Futures.transform(response, new Function<CheckAndMutateRowResponse, Boolean>() {

      @Override
      public Boolean apply(CheckAndMutateRowResponse checkAndMutateRowResponse) {
        return checkAndMutateRowResponse.getPredicateMatched();
      }
    }, MoreExecutors.directExecutor());
  }

  /** {@inheritDoc} */
  @Override
  public Boolean checkAndMutateRow(ConditionalRowMutation conditionalRowMutation) {
    CheckAndMutateRowResponse response =
        delegate.checkAndMutateRow(conditionalRowMutation.toProto(requestContext));
    return response.getPredicateMatched();
  }

  /**
   * This method converts instances of {@link ReadModifyWriteRowResponse} to {@link Row}.
   *
   * @param response an instance of {@link ReadModifyWriteRowResponse} type.
   * @return an instance of {@link Row}.
   */
  private Row transformResponse(ReadModifyWriteRowResponse response) {
    ImmutableList.Builder<RowCell> rowCells  = ImmutableList.builder();

    for (Family family : response.getRow().getFamiliesList()) {
      for (Column column : family.getColumnsList()) {
        for (Cell cell : column.getCellsList()) {
          rowCells.add(
              RowCell.create(
                  family.getName(),
                  column.getQualifier(),
                  cell.getTimestampMicros(),
                  cell.getLabelsList(),
                  cell.getValue()));
        }
      }
    }
    return Row.create(response.getRow().getKey(), rowCells.build());
  }
}
