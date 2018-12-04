/*
 * Copyright 2018 Google LLC
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
package com.google.cloud.bigtable.core;

import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface to support batching multiple {@link RowMutation} request in to singe grpc request.
 */
public interface IBulkMutation {

  /**
   * Send any outstanding {@link RowMutation} and wait until all requests are complete.
   */
  void flush() throws InterruptedException;

  void sendUnsent();

  /**
   * @return false if there are any outstanding {@link RowMutation} that still need to be sent.
   */
  boolean isFlushed();

  /**
   * Adds a {@link RowMutation} to the underlying IBulkMutation mechanism.
   *
   * @param rowMutation The {@link RowMutation} to add.
   * @return a {@link ListenableFuture} of type {@link MutateRowResponse} will be set when request is
   *     successful otherwise exception will be thrown.
   */
  ListenableFuture<MutateRowResponse> add(RowMutation rowMutation);
}
