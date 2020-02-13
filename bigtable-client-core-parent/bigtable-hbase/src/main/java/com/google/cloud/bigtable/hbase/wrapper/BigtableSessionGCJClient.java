package com.google.cloud.bigtable.hbase.wrapper;

import com.google.api.core.InternalApi;
import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.rpc.ClientContext;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBigtableSession;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.grpc.BigtableDataGCJClient;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools;
import com.google.cloud.bigtable.grpc.BigtableTableAdminGCJClient;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.util.ReferenceCountedHashMap;
import com.google.common.base.MoreObjects;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;

// TODO:
@InternalApi
public class BigtableSessionGCJClient implements IBigtableSession {

  private static Map<String, ManagedChannel> cachedDataChannelPools = new HashMap<>();

  // Map containing ref-counted, cached connections to specific destination hosts for GCJ client
  private static Map<String, ClientContext> cachedClientContexts =
      new ReferenceCountedHashMap<>(
          new ReferenceCountedHashMap.Callable<ClientContext>() {
            @Override
            public void call(ClientContext context) {
              for (BackgroundResource backgroundResource : context.getBackgroundResources()) {
                backgroundResource.shutdown();
              }
            }
          });

  private final BigtableDataGCJClient dataGCJClient;
  private final BigtableTableAdminSettings adminSettings;
  private final BaseBigtableTableAdminSettings baseAdminSettings;
  private BigtableTableAdminGCJClient adminGCJClient;
  private IBigtableTableAdminClient adminClientWrapper;

  private final String projectId;
  private final String instanceId;
  private final Long bulkMutateMaxRowKeyCount;

  public BigtableSessionGCJClient(Configuration configuration) throws IOException {
    BigtableDataSettings dataSettings = BigtableOptionsFactory.toDataSettings(configuration);

    String dataHostName =
        configuration.get(
            BigtableOptionsFactory.BIGTABLE_HOST_KEY, BigtableOptions.BIGTABLE_DATA_HOST_DEFAULT);
    this.projectId = dataSettings.getProjectId();
    this.instanceId = dataSettings.getInstanceId();
    this.bulkMutateMaxRowKeyCount =
        MoreObjects.firstNonNull(
            dataSettings
                .getStubSettings()
                .bulkMutateRowsSettings()
                .getBatchingSettings()
                .getFlowControlSettings()
                .getMaxOutstandingRequestBytes(),
            0L);

    // If we're using a cached channel we need to check if it's set up already. If
    //  not we need to do the setup now.
    ClientContext cachedCtx = null;
    synchronized (BigtableSession.class) {
      // If it's not set up for this specific Host we set it up and save the context for
      //  future connections
      if (!cachedClientContexts.containsKey(dataHostName)) {
        cachedCtx = ClientContext.create(dataSettings.getStubSettings());
      } else {
        cachedCtx = cachedClientContexts.get(dataHostName);
      }
      // Adding reference for reference count
      cachedClientContexts.put(dataHostName, cachedCtx);
    }

    BigtableDataSettings.Builder builder = dataSettings.toBuilder();

    // Add the executor and transport channel to the settings/options
    builder
        .stubSettings()
        .setExecutorProvider(FixedExecutorProvider.create(cachedCtx.getExecutor()))
        .setTransportChannelProvider(
            FixedTransportChannelProvider.create(
                Objects.requireNonNull(cachedCtx.getTransportChannel())))
        .setCredentialsProvider(FixedCredentialsProvider.create(cachedCtx.getCredentials()))
        .build();
    dataSettings = builder.build();
    this.dataGCJClient =
        new BigtableDataGCJClient(
            com.google.cloud.bigtable.data.v2.BigtableDataClient.create(dataSettings));

    // Defer the creation of both the tableAdminClient until we need them.
    this.adminSettings = BigtableOptionsFactory.toAdminSettings(configuration);
    this.baseAdminSettings = BaseBigtableTableAdminSettings.create(adminSettings.getStubSettings());
  }

  @Override
  public IBigtableDataClient getDataClient() {
    return dataGCJClient;
  }

  @Override
  public IBigtableTableAdminClient getTableAdminClient() throws IOException {
    if (adminGCJClient == null) {
      com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient adminClientV2 =
          com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient.create(adminSettings);
      BaseBigtableTableAdminClient baseAdminClientV2 =
          BaseBigtableTableAdminClient.create(baseAdminSettings);
      adminGCJClient = new BigtableTableAdminGCJClient(adminClientV2, baseAdminClientV2);
    }
    return adminGCJClient;
  }

  @Override
  public IBulkMutation createBulkMutation(String tableId) {
    return getDataClient().createBulkMutationBatcher(tableId);
  }

  @Override
  public BulkRead createBulkRead(String tableId) {
    BigtableTableName tableName =
        new BigtableTableName(NameUtil.formatTableName(projectId, instanceId, tableId));

    // TODO: this needs to be updated with GCJ wrappers.
    return new BulkRead(
        getDataClient(),
        tableName,
        bulkMutateMaxRowKeyCount.intValue(),
        BigtableSessionSharedThreadPools.getInstance().getBatchThreadPool());
  }

  @Override
  public BigtableInstanceClient getInstanceAdminClient() {
    // Not sure how this should be worked out
    return null;
  }
}
