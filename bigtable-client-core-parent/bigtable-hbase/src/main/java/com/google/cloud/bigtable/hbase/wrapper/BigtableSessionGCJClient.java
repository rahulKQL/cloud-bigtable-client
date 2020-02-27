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
package com.google.cloud.bigtable.hbase.wrapper;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBigtableSession;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

// TODO: JavaDoc
/**
 * For internal use only - public for technical reasons.
 *
 * <p>See {@link BigtableSession} as a public alternative.
 */
@InternalApi("For internal usage only")
public class BigtableSessionGCJClient implements IBigtableSession {

  private final BigtableSession delegate;

  // Argument is kept Configuration to support veneer settings in future
  public BigtableSessionGCJClient(Configuration configuration) throws IOException {
    this.delegate = new BigtableSession(BigtableOptionsFactory.fromConfiguration(configuration));
  }

  @Override
  public IBigtableDataClient getDataClient() {
    return delegate.getDataClientWrapper();
  }

  @Override
  public IBigtableTableAdminClient getTableAdminClient() throws IOException {
    return delegate.getTableAdminClientWrapper();
  }

  @Override
  public BigtableInstanceClient getInstanceAdminClient() throws IOException {
    return delegate.getInstanceAdminClient();
  }

  @Override
  public IBulkMutation createBulkMutation(BigtableTableName tableName) {
    return delegate.createBulkMutationWrapper(tableName);
  }

  @Override
  public BulkRead createBulkRead(BigtableTableName tableName) {
    return delegate.createBulkRead(tableName);
  }

  @Override
  public BigtableOptions getOptions() {
    return delegate.getOptions();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
