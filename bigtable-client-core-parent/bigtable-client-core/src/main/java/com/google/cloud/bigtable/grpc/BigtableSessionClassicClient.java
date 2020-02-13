package com.google.cloud.bigtable.grpc;

import com.google.api.client.util.Clock;
import com.google.api.client.util.Strings;
import com.google.api.core.InternalApi;
import com.google.api.gax.core.BackgroundResource;
import com.google.api.gax.grpc.GaxGrpcProperties;
import com.google.api.gax.rpc.ApiClientHeaderProvider;
import com.google.api.gax.rpc.ClientContext;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BigtableVersionInfo;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBigtableSession;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.BulkMutationWrapper;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.grpc.async.ResourceLimiter;
import com.google.cloud.bigtable.grpc.async.ResourceLimiterStats;
import com.google.cloud.bigtable.grpc.async.ThrottlingClientInterceptor;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.io.CredentialInterceptorCache;
import com.google.cloud.bigtable.grpc.io.GoogleCloudResourcePrefixInterceptor;
import com.google.cloud.bigtable.grpc.io.HeaderInterceptor;
import com.google.cloud.bigtable.grpc.io.Watchdog;
import com.google.cloud.bigtable.grpc.io.WatchdogInterceptor;
import com.google.cloud.bigtable.util.ReferenceCountedHashMap;
import com.google.cloud.bigtable.util.ThreadUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.alts.ComputeEngineChannelBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

@InternalApi
public class BigtableSessionClassicClient implements IBigtableSession {

  private static final Logger LOG = new Logger(BigtableSession.class);
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
  private static final Map<String, ResourceLimiter> resourceLimiterMap = new HashMap<>();

  // 256 MB, server has 256 MB limit.
  private static final int MAX_MESSAGE_SIZE = 1 << 28;

  static final long DIRECT_PATH_KEEP_ALIVE_TIME_SECONDS = 3600;
  static final long DIRECT_PATH_KEEP_ALIVE_TIMEOUT_SECONDS = 20;

  @VisibleForTesting
  static final String PROJECT_ID_EMPTY_OR_NULL = "ProjectId must not be empty or null.";

  @VisibleForTesting
  static final String INSTANCE_ID_EMPTY_OR_NULL = "InstanceId must not be empty or null.";

  @VisibleForTesting
  static final String USER_AGENT_EMPTY_OR_NULL = "UserAgent must not be empty or null";

  static {
    if (!System.getProperty("BIGTABLE_SESSION_SKIP_WARMUP", "").equalsIgnoreCase("true")) {
      performWarmup();
    }
  }

  private static void performWarmup() {
    // Initialize some core dependencies in parallel.  This can speed up startup by 150+ ms.
    ExecutorService connectionStartupExecutor =
        Executors.newCachedThreadPool(
            ThreadUtil.getThreadFactory("BigtableSession-startup-%d", true));

    connectionStartupExecutor.execute(
        new Runnable() {
          @Override
          public void run() {
            // The first invocation of BigtableSessionSharedThreadPools.getInstance() is expensive.
            // Reference it so that it gets constructed asynchronously.
            BigtableSessionSharedThreadPools.getInstance();
          }
        });
    for (final String host :
        Arrays.asList(
            BigtableOptions.BIGTABLE_DATA_HOST_DEFAULT,
            BigtableOptions.BIGTABLE_ADMIN_HOST_DEFAULT)) {
      connectionStartupExecutor.execute(
          new Runnable() {
            @Override
            public void run() {
              // The first invocation of InetAddress retrieval is expensive.
              // Reference it so that it gets constructed asynchronously.
              try {
                InetAddress.getByName(host);
              } catch (UnknownHostException e) {
                // ignore.  This doesn't happen frequently, but even if it does, it's
                // inconsequential.
              }
            }
          });
    }
    connectionStartupExecutor.shutdown();
  }

  private static synchronized ResourceLimiter initializeResourceLimiter(BigtableOptions options) {
    BigtableInstanceName instanceName = options.getInstanceName();
    String key = instanceName.toString();
    ResourceLimiter resourceLimiter = resourceLimiterMap.get(key);
    if (resourceLimiter == null) {
      int maxInflightRpcs = options.getBulkOptions().getMaxInflightRpcs();
      long maxMemory = options.getBulkOptions().getMaxMemory();
      ResourceLimiterStats stats = ResourceLimiterStats.getInstance(instanceName);
      resourceLimiter = new ResourceLimiter(stats, maxMemory, maxInflightRpcs);
      BulkOptions bulkOptions = options.getBulkOptions();
      if (bulkOptions.isEnableBulkMutationThrottling()) {
        resourceLimiter.throttle(bulkOptions.getBulkMutationRpcTargetMs());
      }
      resourceLimiterMap.put(key, resourceLimiter);
    }
    return resourceLimiter;
  }

  private Watchdog watchdog;

  private final BigtableOptions options;
  private final List<ManagedChannel> managedChannels;
  @Deprecated private final List<ClientInterceptor> dataChannelInterceptors;

  private final BigtableDataClient dataClient;
  private final RequestContext dataRequestContext;

  // This BigtableDataClient has an additional throttling interceptor, which is not recommended for
  // synchronous operations.
  private final BigtableDataClient throttlingDataClient;

  private BigtableTableAdminClient tableAdminClient;
  private BigtableInstanceGrpcClient instanceAdminClient;

  BigtableSessionClassicClient(BigtableOptions opts) throws IOException {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(opts.getProjectId()), PROJECT_ID_EMPTY_OR_NULL);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(opts.getInstanceId()), INSTANCE_ID_EMPTY_OR_NULL);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(opts.getUserAgent()), USER_AGENT_EMPTY_OR_NULL);
    LOG.info(
        "Opening session for projectId %s, instanceId %s, " + "on data host %s, admin host %s.",
        opts.getProjectId(), opts.getInstanceId(), opts.getDataHost(), opts.getAdminHost());
    LOG.info("Bigtable options: %s.", opts);

    this.options = opts;
    managedChannels = new ArrayList<>();

    // BEGIN set up Data Clients
    // TODO: We should use a client wrapper factory, instead of having this large if statement.

    // Get a raw data channel pool - depending on the settings, this channel can either be
    // cached/shared or it can specific to this session. If it's specific to this session,
    // it will be added to managedChannels and cleaned up when this session is closed.
    ManagedChannel rawDataChannelPool;
    if (options.useCachedChannel()) {
      synchronized (BigtableSession.class) {
        String key = String.format("%s:%d", options.getDataHost(), options.getPort());
        rawDataChannelPool = cachedDataChannelPools.get(key);
        if (rawDataChannelPool == null) {
          rawDataChannelPool = createRawDataChannelPool(options);
          cachedDataChannelPools.put(key, rawDataChannelPool);
        }
      }
    } else {
      rawDataChannelPool = createRawDataChannelPool(options);
      managedChannels.add(rawDataChannelPool);
    }

    // TODO: stop saving the data channel interceptors as instance variables, this is here only to
    // support deprecated methods
    dataChannelInterceptors = createDataApiInterceptors(options);
    Channel dataChannel = ClientInterceptors.intercept(rawDataChannelPool, dataChannelInterceptors);

    this.dataRequestContext =
        RequestContext.create(
            options.getProjectId(), options.getInstanceId(), options.getAppProfileId());

    BigtableSessionSharedThreadPools sharedPools = BigtableSessionSharedThreadPools.getInstance();

    CallOptionsFactory.ConfiguredCallOptionsFactory callOptionsFactory =
        new CallOptionsFactory.ConfiguredCallOptionsFactory(options.getCallOptionsConfig());

    // More often than not, users want the dataClient. Create a new one in the constructor.
    this.dataClient =
        new BigtableDataGrpcClient(dataChannel, sharedPools.getRetryExecutor(), options);
    this.dataClient.setCallOptionsFactory(callOptionsFactory);

    // Async operations can run amok, so they need to have some throttling. The throttling is
    // achieved through a ThrottlingClientInterceptor.  gRPC wraps ClientInterceptors in Channels,
    // and since a new Channel is needed, a new BigtableDataGrpcClient instance is needed as well.
    //
    // Throttling should not be used in blocking operations, or streaming reads. We have not
    // tested
    // the impact of throttling on blocking operations.
    ResourceLimiter resourceLimiter = initializeResourceLimiter(options);
    Channel asyncDataChannel =
        ClientInterceptors.intercept(dataChannel, new ThrottlingClientInterceptor(resourceLimiter));
    throttlingDataClient =
        new BigtableDataGrpcClient(asyncDataChannel, sharedPools.getRetryExecutor(), options);
    throttlingDataClient.setCallOptionsFactory(callOptionsFactory);

    ManagedChannel rawAdminChannel = createNettyChannel(options.getAdminHost(), options);
    managedChannels.add(rawAdminChannel);

    Channel adminChannel =
        ClientInterceptors.intercept(rawAdminChannel, createAdminApiInterceptors(options));
    this.instanceAdminClient = new BigtableInstanceGrpcClient(adminChannel);
    this.tableAdminClient =
        new BigtableTableAdminGrpcClient(adminChannel, sharedPools.getRetryExecutor(), options);
  }

  // <editor-fold desc="Interceptors">

  static List<ClientInterceptor> createAdminApiInterceptors(BigtableOptions options)
      throws IOException {
    ImmutableList.Builder<ClientInterceptor> interceptors = ImmutableList.builder();

    // TODO: instanceName should never be null
    if (options.getInstanceName() != null) {
      interceptors.add(
          new GoogleCloudResourcePrefixInterceptor(options.getInstanceName().toString()));
    }

    interceptors.add(createGaxHeaderInterceptor());

    ClientInterceptor authInterceptor = createAuthInterceptor(options);
    if (authInterceptor != null) {
      interceptors.add(authInterceptor);
    }

    return interceptors.build();
  }

  private List<ClientInterceptor> createDataApiInterceptors(BigtableOptions options)
      throws IOException {
    ImmutableList.Builder<ClientInterceptor> interceptors = ImmutableList.builder();

    // TODO: instanceName should never be null
    if (options.getInstanceName() != null) {
      interceptors.add(
          new GoogleCloudResourcePrefixInterceptor(options.getInstanceName().toString()));
    }

    interceptors.add(createGaxHeaderInterceptor());

    interceptors.add(setupWatchdog());

    if (!BigtableOptions.isDirectPathEnabled()) {
      ClientInterceptor authInterceptor = createAuthInterceptor(options);
      if (authInterceptor != null) {
        interceptors.add(authInterceptor);
      }
    }

    return interceptors.build();
  }

  private static ClientInterceptor createGaxHeaderInterceptor() {
    return new HeaderInterceptor(
        Metadata.Key.of(
            ApiClientHeaderProvider.getDefaultApiClientHeaderKey(),
            Metadata.ASCII_STRING_MARSHALLER),
        String.format(
            "gl-java/%s %s/%s cbt/%s",
            BigtableVersionInfo.JDK_VERSION,
            GaxGrpcProperties.getGrpcTokenName(),
            GaxGrpcProperties.getGrpcVersion(),
            BigtableVersionInfo.CLIENT_VERSION));
  }

  private WatchdogInterceptor setupWatchdog() {
    Preconditions.checkState(watchdog == null, "Watchdog already setup");

    watchdog =
        new Watchdog(Clock.SYSTEM, options.getRetryOptions().getReadPartialRowTimeoutMillis());
    watchdog.start(BigtableSessionSharedThreadPools.getInstance().getRetryExecutor());

    return new WatchdogInterceptor(
        ImmutableSet.<MethodDescriptor<?, ?>>of(BigtableGrpc.getReadRowsMethod()), watchdog);
  }

  @Nullable
  private static ClientInterceptor createAuthInterceptor(BigtableOptions options)
      throws IOException {
    CredentialInterceptorCache credentialsCache = CredentialInterceptorCache.getInstance();
    RetryOptions retryOptions = options.getRetryOptions();
    CredentialOptions credentialOptions = options.getCredentialOptions();
    try {
      return credentialsCache.getCredentialsInterceptor(credentialOptions, retryOptions);
    } catch (GeneralSecurityException e) {
      throw new IOException("Could not initialize credentials.", e);
    }
  }
  // </editor-fold>

  // <editor-fold desc="Channel management">
  /**
   * @deprecated Channel creation is now considered an internal implementation detail channel
   *     creation methods will be removed from the public surface in the future
   */
  @Deprecated
  protected ManagedChannel createManagedPool(String host, int channelCount) throws IOException {
    ManagedChannel channelPool = createChannelPool(host, channelCount);
    managedChannels.add(channelPool);
    return channelPool;
  }

  /**
   * @deprecated Channel creation is now considered an internal implementation detail channel
   *     creation methods will be removed from the public surface in the future
   */
  @Deprecated
  protected ManagedChannel createChannelPool(final String hostString, int count)
      throws IOException {
    Preconditions.checkState(
        !options.useGCJClient(), "Channel pools cannot be created when using google-cloud-java");

    final ClientInterceptor[] clientInterceptorArray =
        dataChannelInterceptors.toArray(new ClientInterceptor[0]);
    ChannelPool.ChannelFactory channelFactory =
        new ChannelPool.ChannelFactory() {
          @Override
          public ManagedChannel create() throws IOException {
            return createNettyChannel(hostString, options, clientInterceptorArray);
          }
        };
    return createChannelPool(channelFactory, count);
  }

  /**
   * @deprecated Channel creation is now considered an internal implementation detail channel
   *     creation methods will be removed from the public surface in the future
   */
  @Deprecated
  @InternalApi("For internal usage only")
  protected ManagedChannel createChannelPool(
      final ChannelPool.ChannelFactory channelFactory, int count) throws IOException {
    return new ChannelPool(channelFactory, count);
  }

  /**
   * @deprecated Channel creation is now considered an internal implementation detail channel
   *     creation methods will be removed from the public surface in the future
   */
  @Deprecated
  @InternalApi("For internal usage only")
  public static ManagedChannel createChannelPool(final String host, final BigtableOptions options)
      throws IOException, GeneralSecurityException {
    return createChannelPool(host, options, 1);
  }

  /**
   * @deprecated Channel creation is now considered an internal implementation detail channel
   *     creation methods will be removed in the future
   */
  @Deprecated
  @InternalApi("For internal usage only")
  public static ManagedChannel createChannelPool(
      final String host, final BigtableOptions options, int count)
      throws IOException, GeneralSecurityException {
    final List<ClientInterceptor> interceptorList = new ArrayList<>();

    ClientInterceptor credentialsInterceptor =
        CredentialInterceptorCache.getInstance()
            .getCredentialsInterceptor(options.getCredentialOptions(), options.getRetryOptions());
    if (credentialsInterceptor != null) {
      interceptorList.add(credentialsInterceptor);
    }

    if (options.getInstanceName() != null) {
      interceptorList.add(
          new GoogleCloudResourcePrefixInterceptor(options.getInstanceName().toString()));
    }
    final ClientInterceptor[] interceptors =
        interceptorList.toArray(new ClientInterceptor[interceptorList.size()]);

    ChannelPool.ChannelFactory factory =
        new ChannelPool.ChannelFactory() {
          @Override
          public ManagedChannel create() throws IOException {
            return createNettyChannel(host, options, interceptors);
          }
        };
    return new ChannelPool(factory, count);
  }

  private static ChannelPool createRawDataChannelPool(final BigtableOptions options)
      throws IOException {
    ChannelPool.ChannelFactory channelFactory =
        new ChannelPool.ChannelFactory() {
          @Override
          public ManagedChannel create() throws IOException {
            return createNettyChannel(options.getDataHost(), options);
          }
        };
    return new ChannelPool(channelFactory, options.getChannelCount());
  }
  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public static ManagedChannel createNettyChannel(
      String host, BigtableOptions options, ClientInterceptor... interceptors) throws SSLException {

    // DirectPath is only supported for data currently
    boolean isDirectPath = BigtableOptions.isDirectPathEnabled() && !host.contains("admin");

    LOG.info("Creating new channel for %s", host);
    if (LOG.getLog().isTraceEnabled()) {
      LOG.trace(Throwables.getStackTraceAsString(new Throwable()));
    }

    ManagedChannelBuilder<?> builder;
    if (isDirectPath) {
      LOG.warn(
          "Connecting to Bigtable using DirectPath."
              + " This is currently an experimental feature and should not be used in production.");

      builder = ComputeEngineChannelBuilder.forAddress(host, options.getPort());
      builder.keepAliveTime(DIRECT_PATH_KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS);
      builder.keepAliveTimeout(DIRECT_PATH_KEEP_ALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      // When channel pooling is enabled, force the pick_first grpclb strategy.
      // This is necessary to avoid the multiplicative effect of creating channel pool with
      // `poolSize` number of `ManagedChannel`s, each with a `subSetting` number of number of
      // subchannels.
      // See the service config proto definition for more details:
      // https://github.com/grpc/grpc-proto/blob/master/grpc/service_config/service_config.proto#L182
      ImmutableMap<String, Object> pickFirstStrategy =
          ImmutableMap.<String, Object>of("pick_first", ImmutableMap.of());

      ImmutableMap<String, Object> childPolicy =
          ImmutableMap.<String, Object>of("childPolicy", ImmutableList.of(pickFirstStrategy));

      ImmutableMap<String, Object> grpcLbPolicy =
          ImmutableMap.<String, Object>of("grpclb", childPolicy);

      ImmutableMap<String, Object> loadBalancingConfig =
          ImmutableMap.<String, Object>of("loadBalancingConfig", ImmutableList.of(grpcLbPolicy));

      builder.defaultServiceConfig(loadBalancingConfig);
    } else {
      builder = ManagedChannelBuilder.forAddress(host, options.getPort());

      if (options.usePlaintextNegotiation()) {
        builder.usePlaintext();
      }
    }

    if (options.getChannelConfigurator() != null) {
      builder = options.getChannelConfigurator().configureChannel(builder, host);
    }

    return builder
        .idleTimeout(Long.MAX_VALUE, TimeUnit.SECONDS)
        .maxInboundMessageSize(MAX_MESSAGE_SIZE)
        .userAgent(BigtableVersionInfo.CORE_USER_AGENT + "," + options.getUserAgent())
        .intercept(interceptors)
        .build();
  }
  // </editor-fold>

  @Override
  public IBigtableDataClient getDataClient() {
    return new BigtableDataClientWrapper(dataClient, dataRequestContext);
  }

  @Override
  public IBigtableTableAdminClient getTableAdminClient() {
    return new BigtableTableAdminClientWrapper(tableAdminClient, options);
  }

  @Override
  public IBulkMutation createBulkMutation(String tableId) {
    BigtableTableName tableName = options.getInstanceName().toTableName(tableId);
    return new BulkMutationWrapper(
        new BulkMutation(
            tableName,
            throttlingDataClient,
            BigtableSessionSharedThreadPools.getInstance().getRetryExecutor(),
            options.getBulkOptions()));
  }

  @Override
  public BulkRead createBulkRead(String tableId) {
    BigtableTableName tableName = options.getInstanceName().toTableName(tableId);
    return new BulkRead(
        getDataClient(),
        tableName,
        options.getBulkOptions().getBulkMaxRowKeyCount(),
        BigtableSessionSharedThreadPools.getInstance().getBatchThreadPool());
  }

  @Override
  public BigtableInstanceClient getInstanceAdminClient() {
    return instanceAdminClient;
  }
}
