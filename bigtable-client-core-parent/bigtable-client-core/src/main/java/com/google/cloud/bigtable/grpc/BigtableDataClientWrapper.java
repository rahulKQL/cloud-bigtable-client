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

import com.google.api.core.ApiFuture;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

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
    throw new UnsupportedOperationException("Not implemented yet");
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
}
