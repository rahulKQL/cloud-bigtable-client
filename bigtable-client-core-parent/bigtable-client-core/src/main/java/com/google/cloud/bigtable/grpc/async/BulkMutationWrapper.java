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

import com.google.api.core.AbstractApiFuture;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
  public void flush() throws InterruptedException, TimeoutException {
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

  @Override public ApiFuture<Void> add(RowMutation rowMutation) {
    final ListenableFuture<MutateRowResponse> response =
        delegate.add(rowMutation.toProto(requestContext));

    final ApiFuture<Void> future = new AbstractApiFuture<Void>() {
      @Override public boolean cancel(boolean mayInterruptIfRunning) {
        response.cancel(mayInterruptIfRunning);
        return false;
      }

      @Override public Void get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        response.get(timeout, unit);
        return null;
      }

      @Override public Void get() throws InterruptedException, ExecutionException {
        response.get();
        return null;
      }
    };

    ApiFutureCallback<Void> callback = new ApiFutureCallback<Void>() {
      @Override public void onFailure(Throwable t) {
        future.isCancelled();
      }

      @Override public void onSuccess(Void result) {
        future.isDone();
      }
    };
    ApiFutures.addCallback(future, callback, MoreExecutors.directExecutor());

    return future;
  }
}
