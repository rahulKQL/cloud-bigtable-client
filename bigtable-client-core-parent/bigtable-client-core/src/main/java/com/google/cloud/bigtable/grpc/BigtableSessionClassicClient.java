package com.google.cloud.bigtable.grpc;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBigtableSession;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
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
  private final String projectId;
  private final String instanceId;

  BigtableSessionClassicClient(BigtableOptions options) throws IOException {
    this.session = new BigtableSession(options);
    this.projectId = session.getOptions().getProjectId();
    this.instanceId = session.getOptions().getInstanceId();
  }

  @Override
  public IBigtableDataClient getDataClient() {
    return session.getDataClientWrapper();
  }

  @Override
  public IBigtableTableAdminClient getTableAdminClient() throws IOException {
    return session.getTableAdminClientWrapper();
  }

  @Override
  public IBulkMutation createBulkMutation(String tableId) {
    return session.createBulkMutationWrapper(
        new BigtableTableName(NameUtil.formatTableName(projectId, instanceId, tableId)));
  }

  @Override
  public BulkRead createBulkRead(String tableId) {
    return session.createBulkRead(
        new BigtableTableName(NameUtil.formatTableName(projectId, instanceId, tableId)));
  }

  @Override
  public void close() throws Exception {
    session.close();
  }
}
