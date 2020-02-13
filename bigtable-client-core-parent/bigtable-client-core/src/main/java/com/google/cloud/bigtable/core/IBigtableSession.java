package com.google.cloud.bigtable.core;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import java.io.IOException;

@InternalApi
public interface IBigtableSession {

  IBigtableDataClient getDataClient();

  IBigtableTableAdminClient getTableAdminClient() throws IOException;

  IBulkMutation createBulkMutation(String tableId);

  BulkRead createBulkRead(String tableId);

  BigtableInstanceClient getInstanceAdminClient();
}
