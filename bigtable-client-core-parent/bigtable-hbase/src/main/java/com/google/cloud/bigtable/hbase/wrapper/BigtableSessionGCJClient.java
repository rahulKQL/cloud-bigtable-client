package com.google.cloud.bigtable.hbase.wrapper;

import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBigtableSession;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import javax.security.auth.login.Configuration;

// TODO:
public class BigtableSessionGCJClient implements IBigtableSession {

  BigtableSessionGCJClient(Configuration configuration) {}

  @Override
  public IBigtableDataClient getDataClient() {
    return null;
  }

  @Override
  public IBigtableTableAdminClient getTableAdminClient() {
    return null;
  }

  @Override
  public IBulkMutation createBulkMutation(String tableId) {
    return null;
  }

  @Override
  public BulkRead createBulkRead(String tableId) {
    return null;
  }

  @Override
  public BigtableInstanceClient getInstanceAdminClient() {
    return null;
  }
}
