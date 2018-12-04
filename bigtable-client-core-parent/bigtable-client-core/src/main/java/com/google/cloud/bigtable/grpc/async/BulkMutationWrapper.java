/*
 * Copyright 2018 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.async;

import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.util.concurrent.ListenableFuture;

/**
 *  a wrapper class to adapt to {@link RowMutation}.
 */
public class BulkMutationWrapper implements IBulkMutation {

  private BulkMutation delegate;
  private final RequestContext requestContext;

  public BulkMutationWrapper(BulkMutation bulkMutation, BigtableOptions options){
    this.delegate = bulkMutation;
    this.requestContext = RequestContext
        .create(InstanceName.of(options.getProjectId(),
            options.getInstanceId()),
            options.getAppProfileId()
        );
  }

  @Override
  public void flush() throws InterruptedException {
    delegate.flush();
  }

  @Override
  public void sendUnsent() {
    delegate.sendUnsent();
  }

  @Override
  public boolean isFlushed() {
    return delegate.isFlushed();
  }

  @Override
  public ListenableFuture<MutateRowResponse> add(RowMutation rowMutation) {
    return delegate.add(rowMutation.toProto(requestContext));
  }
}
