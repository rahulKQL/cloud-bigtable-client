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

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.ListenableFutureToApiFuture;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.MutateRowRequest;
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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.xml.crypto.dsig.keyinfo.KeyValue;

/**
 * This class implements the {@link IBigtableDataClient} interface which provides access to google cloud
 * java
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

  @Override
  public void mutateRow(RowMutation rowMutation) {
    MutateRowRequest mutateRowRequest = rowMutation.toProto(requestContext);
    delegate.mutateRow(mutateRowRequest);
  }

  @Override
  public ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Row readModifyWriteRow(ReadModifyWriteRow readModifyWriteRow) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ApiFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    ListenableFuture<ReadModifyWriteRowResponse> response = delegate
        .readModifyWriteRowAsync(readModifyWriteRow.toProto(requestContext));
    ApiFuture<ReadModifyWriteRowResponse> apiRes = new ListenableFutureToApiFuture<>(response);
    return ApiFutures.transform(apiRes, new ApiFunction<ReadModifyWriteRowResponse, Row>() {

      @Override
      public Row apply(ReadModifyWriteRowResponse input) {
        com.google.bigtable.v2.Row bigtableRow = input.getRow();
        List<RowCell> modelRowCells = adpatToRowCell(bigtableRow);

       return Row.create(bigtableRow.getKey(), modelRowCells);
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  public IBulkMutation createBulkMutationBatcher() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Boolean checkAndMutateRow(ConditionalRowMutation conditionalRowMutation) {
    throw new UnsupportedOperationException("Not implemented yet");
  }


  private List<RowCell> adpatToRowCell(com.google.bigtable.v2.Row response){
    if(response == null){
      return Collections.EMPTY_LIST;
    }
    //TODO(rahulkql): need to introduce Comparator.
    SortedSet<RowCell> rowCells = new TreeSet<>();
    for (Family family : response.getFamiliesList()) {

      for (Column column : family.getColumnsList()) {

        for (Cell cell : column.getCellsList()) {
          //TODO(rahulkql): RowCell requires Labels, in that case what to do with this check?
          // Cells with labels are for internal use, do not return them.
          // TODO(kevinsi4508): Filter out targeted {@link WhileMatchFilter} labels.
          if (cell.getLabelsCount() > 0) {
            continue;
          }
          List<String> labels = cell.getLabelsList().subList(0, cell.getLabelsList().size());

          // Bigtable timestamp has more granularity than HBase one. It is possible that Bigtable
          // cells are deduped unintentionally here. On the other hand, if we don't dedup them,
          // HBase will treat them as duplicates.
          //long hbaseTimestamp = TimestampConverter.bigtable2hbase(cell.getTimestampMicros());

          //TODO(rahulkql): move TimestampConverter from bigtable-hbase to bigtable-hbase-core.
          long hbaseTimestamp = cell.getTimestampMicros();
          RowCell rowCell =  RowCell.create(family.getName(), column.getQualifier(), hbaseTimestamp,  labels, cell.getValue());

          rowCells.add(rowCell);
        }
      }
    }

    return new ArrayList<>(rowCells);
  }


}
