/*
 * Copyright 2020 Google LLC.
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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBigtableSession;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.grpc.async.BulkMutationWrapper;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import java.io.IOException;

/**
 * Wraps and delegate calls to existing {@link BigtableSession}.
 *
 * <p>For internal use only - public for technical reasons.
 *
 * <p>See {@link BigtableSession} as a public alternative.
 */
@InternalApi("For internal usage only - please use BigtableSession")
public class BigtableSessionClassicClient implements IBigtableSession {

  private final BigtableSession session;

  public BigtableSessionClassicClient(BigtableOptions options) throws IOException {
    this.session = new BigtableSession(options);
  }

  @Override
  public IBigtableDataClient getDataClient() {
    return new BigtableDataClientWrapper(
        session.getDataClient(),
        RequestContext.create(
            getOptions().getProjectId(),
            getOptions().getInstanceId(),
            getOptions().getAppProfileId()));
  }

  @Override
  public IBigtableTableAdminClient getTableAdminClient() throws IOException {
    return new BigtableTableAdminClientWrapper(session.getTableAdminClient(), getOptions());
  }

  @Override
  public BigtableInstanceClient getInstanceAdminClient() throws IOException {
    return session.getInstanceAdminClient();
  }

  @Override
  public IBulkMutation createBulkMutation(BigtableTableName tableName) {
    return new BulkMutationWrapper(session.createBulkMutation(tableName));
  }

  @Override
  public BulkRead createBulkRead(BigtableTableName tableName) {
    return session.createBulkRead(tableName);
  }

  @Override
  public BigtableOptions getOptions() {
    return session.getOptions();
  }

  @Override
  public void close() throws IOException {
    session.close();
  }
}
