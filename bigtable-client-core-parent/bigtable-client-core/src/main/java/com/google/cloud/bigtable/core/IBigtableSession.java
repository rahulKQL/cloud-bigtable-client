package com.google.cloud.bigtable.core;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.async.BulkRead;

@InternalApi
public interface IBigtableSession {

  IBigtableDataClient getDataClient();

  IBigtableTableAdminClient getTableAdminClient();

  IBulkMutation createBulkMutation(String tableId);

  BulkRead createBulkRead(String tableId);

  BigtableInstanceClient getInstanceAdminClient();
}
