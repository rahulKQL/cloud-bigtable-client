/*
 * Copyright 2020 Google LLC.
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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import static com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings.defaultGrpcTransportProviderBuilder;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ADDITIONAL_RETRY_CODES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ALLOW_NO_TIMESTAMP_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.APP_PROFILE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_AUTOFLUSH_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_READ_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_TIMEOUTS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ENABLE_GRPC_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_INFLIGHT_RPCS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_SCAN_TIMEOUT_RETRIES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.PROJECT_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_PARTIAL_ROW_TIMEOUT_MS;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.grpc.internal.GrpcUtil.USER_AGENT_KEY;
import static org.threeten.bp.Duration.ofMillis;

import com.google.api.client.util.SecurityUtils;
import com.google.api.core.ApiFunction;
import com.google.api.core.InternalApi;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.stub.BigtableTableAdminStubSettings;
import com.google.cloud.bigtable.config.BigtableVersionInfo;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.cloud.bigtable.hbase.BigtableExtendedConfiguration;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.threeten.bp.Duration;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableHBaseVeneerSettings extends BigtableHBaseSettings {

  // Identifier to distinguish between CBT or GCJ adapter.
  private static final String VENEER_ADAPTER =
      BigtableVersionInfo.CORE_USER_AGENT + "," + "veneer-adapter,";

  public BigtableHBaseVeneerSettings(Configuration configuration) {
    super(configuration);
  }

  // <editor-fold desc="Public API">
  @InternalApi
  public boolean isChannelPoolCachingEnabled() {
    // This is primarily used by Dataflow where connections open and close often. This is a
    // performance optimization that will reduce the cost to open connections.
    return configuration.getBoolean(BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, false);
  }

  /**
   * Utility to convert {@link Configuration} to {@link BigtableDataSettings}.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public BigtableDataSettings getDataSettings() throws IOException {
    BigtableDataSettings.Builder dataBuilder = BigtableDataSettings.newBuilder();
    EnhancedBigtableStubSettings.Builder stubSettings = dataBuilder.stubSettings();

    dataBuilder.setProjectId(getProjectId()).setInstanceId(getInstanceId());

    String appProfileId = configuration.get(APP_PROFILE_ID_KEY);
    if (!isNullOrEmpty(appProfileId)) {
      dataBuilder.setAppProfileId(appProfileId);
    }

    String dataHostOverride = configuration.get(BIGTABLE_HOST_KEY);
    if (!isNullOrEmpty(dataHostOverride)) {
      String endpoint = dataHostOverride + ":" + getPort();
      LOG.debug("API Data endpoint hostname:portNumber is %s", endpoint);

      stubSettings.setEndpoint(endpoint);
    }

    stubSettings.setHeaderProvider(buildHeaderProvider());

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_SERVICE_ACCOUNTS_KEY))) {
      CredentialsProvider credentialsProvider = buildCredentialProvider();
      if (credentialsProvider != null) {
        stubSettings.setCredentialsProvider(credentialsProvider);
      }
    } else if (Boolean.parseBoolean(configuration.get(BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY))) {
      stubSettings.setCredentialsProvider(NoCredentialsProvider.create());
    }

    if (configuration.getBoolean(BIGTABLE_USE_PLAINTEXT_NEGOTIATION, false)) {
      stubSettings.setTransportChannelProvider(
          buildPlainTextChannelProvider(stubSettings.getEndpoint()));
    }

    String shortRpcTimeoutStr = configuration.get(BIGTABLE_RPC_TIMEOUT_MS_KEY);
    if (shortRpcTimeoutStr != null) {
      // rpcTimeout & totalTimeout for non-retry operations.
      Duration shortRpcTimeout = ofMillis(Long.valueOf(shortRpcTimeoutStr));

      stubSettings.checkAndMutateRowSettings().setSimpleTimeoutNoRetries(shortRpcTimeout);

      stubSettings.readModifyWriteRowSettings().setSimpleTimeoutNoRetries(shortRpcTimeout);
    }

    buildBulkMutationsSettings(stubSettings);

    buildBulkReadRowsSettings(stubSettings);

    buildReadRowsSettings(stubSettings);

    stubSettings
        .readRowSettings()
        .setRetryableCodes(buildRetryCodes(stubSettings.readRowSettings().getRetryableCodes()))
        .setRetrySettings(
            buildIdempotentRetrySettings(stubSettings.readRowSettings().getRetrySettings()));

    stubSettings
        .mutateRowSettings()
        .setRetryableCodes(buildRetryCodes(stubSettings.mutateRowSettings().getRetryableCodes()))
        .setRetrySettings(
            buildIdempotentRetrySettings(stubSettings.mutateRowSettings().getRetrySettings()));

    stubSettings
        .sampleRowKeysSettings()
        .setRetryableCodes(
            buildRetryCodes(stubSettings.sampleRowKeysSettings().getRetryableCodes()))
        .setRetrySettings(
            buildIdempotentRetrySettings(stubSettings.sampleRowKeysSettings().getRetrySettings()));

    String emulatorHostPort = configuration.get(BIGTABLE_EMULATOR_HOST_KEY);
    if (!isNullOrEmpty(emulatorHostPort)) {
      stubSettings
          .setCredentialsProvider(NoCredentialsProvider.create())
          .setEndpoint(emulatorHostPort)
          .setTransportChannelProvider(buildPlainTextChannelProvider(emulatorHostPort));
    }

    return dataBuilder.build();
  }

  /**
   * Utility to convert {@link Configuration} to {@link BigtableTableAdminSettings}.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public BigtableTableAdminSettings getTableAdminSettings() throws IOException {
    BigtableTableAdminSettings.Builder adminBuilder = BigtableTableAdminSettings.newBuilder();
    BigtableTableAdminStubSettings.Builder stubSettings = adminBuilder.stubSettings();

    adminBuilder.setProjectId(getProjectId()).setInstanceId(getInstanceId());

    String adminHostOverride = configuration.get(BIGTABLE_ADMIN_HOST_KEY);
    if (!isNullOrEmpty(adminHostOverride)) {
      String endpoint = adminHostOverride + ":" + getPort();
      LOG.debug("Admin endpoint host:port is %s.", endpoint);

      stubSettings.setEndpoint(endpoint);
    }

    stubSettings.setHeaderProvider(buildHeaderProvider());

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_SERVICE_ACCOUNTS_KEY))) {
      CredentialsProvider credentialsProvider = buildCredentialProvider();
      if (credentialsProvider != null) {
        stubSettings.setCredentialsProvider(credentialsProvider);
      }
    } else if (Boolean.parseBoolean(configuration.get(BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY))) {
      stubSettings.setCredentialsProvider(NoCredentialsProvider.create());
    }

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_PLAINTEXT_NEGOTIATION))) {
      stubSettings.setTransportChannelProvider(
          buildPlainTextChannelProvider(stubSettings.getEndpoint()));
    }

    String emulatorHostPort = configuration.get(BIGTABLE_EMULATOR_HOST_KEY);
    if (!isNullOrEmpty(emulatorHostPort)) {
      stubSettings
          .setCredentialsProvider(NoCredentialsProvider.create())
          .setEndpoint(emulatorHostPort)
          .setTransportChannelProvider(buildPlainTextChannelProvider(emulatorHostPort));
    }

    return adminBuilder.build();
  }

  /**
   * Utility to convert {@link Configuration} to {@link BigtableInstanceAdminSettings}.
   *
   * <p>For internal use only - public for technical reasons.
   */
  @InternalApi("For internal usage only")
  public BigtableInstanceAdminSettings getInstanceAdminSettings() throws IOException {
    Preconditions.checkState(
        isNullOrEmpty(configuration.get(BIGTABLE_EMULATOR_HOST_KEY)),
        "Instance admin does not support emulator");

    BigtableInstanceAdminSettings.Builder builder = BigtableInstanceAdminSettings.newBuilder();

    builder.setProjectId(getValue(PROJECT_ID_KEY, "Project ID"));

    String adminHostOverride = configuration.get(BIGTABLE_ADMIN_HOST_KEY);
    String endpoint = adminHostOverride + ":" + getPort();
    LOG.debug("Instance Admin endpoint host:port is %s.", endpoint);

    builder.stubSettings().setHeaderProvider(buildHeaderProvider());

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_SERVICE_ACCOUNTS_KEY))) {
      CredentialsProvider credentialsProvider = buildCredentialProvider();
      if (credentialsProvider != null) {
        builder.stubSettings().setCredentialsProvider(credentialsProvider);
      }
    } else if (Boolean.parseBoolean(configuration.get(BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY))) {
      builder.stubSettings().setCredentialsProvider(NoCredentialsProvider.create());
    }

    return builder.build();
  }
  // </editor-fold>

  // <editor-fold desc="Private Helpers">
  /** Creates {@link HeaderProvider} with VENEER_ADAPTER as prefix for user agent */
  private HeaderProvider buildHeaderProvider() {

    // This information is in addition to bigtable-client-core version, and jdk version.
    StringBuilder agentBuilder = new StringBuilder();
    agentBuilder.append("hbase-").append(VersionInfo.getVersion());
    String customUserAgent = configuration.get(CUSTOM_USER_AGENT_KEY);
    if (customUserAgent != null) {
      agentBuilder.append(',').append(customUserAgent);
    }

    return FixedHeaderProvider.create(
        USER_AGENT_KEY.name(), VENEER_ADAPTER + agentBuilder.toString());
  }

  private CredentialsProvider buildCredentialProvider() throws IOException {
    Credentials credentials = null;
    LOG.debug("Using service accounts");

    if (configuration instanceof BigtableExtendedConfiguration) {

      credentials = ((BigtableExtendedConfiguration) configuration).getCredentials();

    } else if (!isNullOrEmpty(configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY))) {

      String jsonValue = configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY);
      LOG.debug("Using json value");

      Preconditions.checkState(
          !isNullOrEmpty(jsonValue), "service account json value is null or empty");
      credentials =
          GoogleCredentials.fromStream(
              new ByteArrayInputStream(jsonValue.getBytes(StandardCharsets.UTF_8)));

    } else if (!isNullOrEmpty(
        configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY))) {

      String keyFileLocation =
          configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY);
      LOG.debug("Using json keyfile: %s", keyFileLocation);

      Preconditions.checkState(
          !isNullOrEmpty(keyFileLocation), "service account location is null or empty");
      credentials = GoogleCredentials.fromStream(new FileInputStream(keyFileLocation));

    } else if (!isNullOrEmpty(configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY))) {

      String serviceAccount = configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY);
      LOG.debug("Service account %s specified.", serviceAccount);

      String keyFileLocation = configuration.get(BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);
      Preconditions.checkState(
          !isNullOrEmpty(keyFileLocation),
          "Key file location must be specified when setting service account email");
      LOG.debug("Using p12 keyfile: %s", keyFileLocation);

      credentials = getCredentialFromPrivateKeyServiceAccount(serviceAccount, keyFileLocation);
    } else {

      LOG.debug("No credentials found with service account");
      return null;
    }

    return FixedCredentialsProvider.create(credentials);
  }

  // copied over from CredentialFactory
  // TODO: Find a better way to convert P12 key into Credentials instance
  private Credentials getCredentialFromPrivateKeyServiceAccount(
      String serviceAccountEmail, String privateKeyFile) throws IOException {
    try {
      PrivateKey privateKey =
          SecurityUtils.loadPrivateKeyFromKeyStore(
              SecurityUtils.getPkcs12KeyStore(),
              new FileInputStream(privateKeyFile),
              "notasecret",
              "privatekey",
              "notasecret");

      return ServiceAccountJwtAccessCredentials.newBuilder()
          .setClientEmail(serviceAccountEmail)
          .setPrivateKey(privateKey)
          .build();
    } catch (GeneralSecurityException exception) {
      throw new RuntimeException("exception while retrieving credentials", exception);
    }
  }

  /** Creates {@link TransportChannelProvider} for plaintext negotiation type. */
  private TransportChannelProvider buildPlainTextChannelProvider(String endpoint) {

    InstantiatingGrpcChannelProvider.Builder channelBuilder =
        defaultGrpcTransportProviderBuilder()
            .setEndpoint(endpoint)
            .setChannelConfigurator(
                new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
                  @Override
                  public ManagedChannelBuilder apply(ManagedChannelBuilder channelBuilder) {
                    return channelBuilder.usePlaintext();
                  }
                });

    String channelCount = configuration.get(BIGTABLE_DATA_CHANNEL_COUNT_KEY);
    if (!isNullOrEmpty(channelCount)) {
      channelBuilder.setPoolSize(Integer.parseInt(channelCount));
    }

    return channelBuilder.build();
  }

  /** Creates {@link Set} of {@link StatusCode.Code} from {@link Status.Code} */
  private Set<StatusCode.Code> buildRetryCodes(Set<StatusCode.Code> retryableCodes) {
    ImmutableSet.Builder<StatusCode.Code> statusCodeBuilder = ImmutableSet.builder();

    // Disables retries for all data operations
    if (!configuration.getBoolean(ENABLE_GRPC_RETRIES_KEY, true)) {
      return statusCodeBuilder.build();
    }

    statusCodeBuilder.addAll(retryableCodes);

    String retryCodes = configuration.get(ADDITIONAL_RETRY_CODES, "");

    for (String stringCode : retryCodes.split(",")) {
      String trimmed = stringCode.trim();
      if (trimmed.isEmpty()) {
        continue;
      }

      StatusCode.Code code = StatusCode.Code.valueOf(trimmed);

      Preconditions.checkNotNull(code, String.format("Unknown status code %s found", stringCode));
      statusCodeBuilder.add(code);
      LOG.debug("gRPC retry on: %s", stringCode);
    }

    return statusCodeBuilder.build();
  }

  /** Creates {@link RetrySettings} for non-streaming VENEER_ADAPTER method. */
  private RetrySettings buildIdempotentRetrySettings(RetrySettings originalRetrySettings) {
    RetrySettings.Builder retryBuilder = originalRetrySettings.toBuilder();

    if (configuration.getBoolean(ALLOW_NO_TIMESTAMP_RETRIES_KEY, false)) {
      throw new UnsupportedOperationException("Retries without Timestamp is not supported yet.");
    }

    String initialElapsedBackoffMsStr = configuration.get(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY);
    if (!isNullOrEmpty(initialElapsedBackoffMsStr)) {
      retryBuilder.setInitialRetryDelay(ofMillis(Long.parseLong(initialElapsedBackoffMsStr)));
    }

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_TIMEOUTS_KEY))) {
      String shortRpcTimeoutMsStr = configuration.get(BIGTABLE_RPC_TIMEOUT_MS_KEY);

      if (!isNullOrEmpty(shortRpcTimeoutMsStr)) {
        Duration rpcTimeoutMs = ofMillis(Long.valueOf(shortRpcTimeoutMsStr));
        retryBuilder.setInitialRpcTimeout(rpcTimeoutMs).setMaxRpcTimeout(rpcTimeoutMs);
      }
    }

    String maxElapsedBackoffMillis = configuration.get(MAX_ELAPSED_BACKOFF_MILLIS_KEY);
    if (!isNullOrEmpty(maxElapsedBackoffMillis)) {
      retryBuilder.setTotalTimeout(ofMillis(Long.valueOf(maxElapsedBackoffMillis)));
    }

    return retryBuilder.build();
  }

  private void buildBulkMutationsSettings(EnhancedBigtableStubSettings.Builder builder) {
    BatchingSettings.Builder batchMutateBuilder =
        builder.bulkMutateRowsSettings().getBatchingSettings().toBuilder();

    String autoFlushStr = configuration.get(BIGTABLE_BULK_AUTOFLUSH_MS_KEY);
    if (autoFlushStr != null) {
      long autoFlushMs = Long.valueOf(autoFlushStr);
      if (autoFlushMs > 0) {
        batchMutateBuilder.setDelayThreshold(ofMillis(autoFlushMs));
      }
    }

    long bulkMaxRowKeyCount = getBulkMaxRowCount();
    batchMutateBuilder.setElementCountThreshold(bulkMaxRowKeyCount);

    String maxInflightRpcStr = configuration.get(MAX_INFLIGHT_RPCS_KEY);
    if (!isNullOrEmpty(maxInflightRpcStr) && Integer.parseInt(maxInflightRpcStr) > 0) {

      int maxInflightRpcCount = Integer.parseInt(maxInflightRpcStr);
      FlowControlSettings.Builder flowControlBuilder =
          FlowControlSettings.newBuilder()
              // TODO: verify if it should be channelCount instead of maxRowKeyCount
              .setMaxOutstandingElementCount(maxInflightRpcCount * bulkMaxRowKeyCount);

      String maxMemory = configuration.get(BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY);
      if (!isNullOrEmpty(maxMemory)) {
        flowControlBuilder.setMaxOutstandingRequestBytes(Long.valueOf(maxMemory));
      }

      batchMutateBuilder.setFlowControlSettings(flowControlBuilder.build());
    }

    String requestByteThresholdStr = configuration.get(BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES);
    if (!isNullOrEmpty(requestByteThresholdStr)) {
      batchMutateBuilder.setRequestByteThreshold(Long.valueOf(requestByteThresholdStr));
    }

    builder
        .bulkMutateRowsSettings()
        .setBatchingSettings(batchMutateBuilder.build())
        .setRetryableCodes(buildRetryCodes(builder.bulkMutateRowsSettings().getRetryableCodes()))
        .setRetrySettings(
            buildIdempotentRetrySettings(builder.bulkMutateRowsSettings().getRetrySettings()));
  }

  private void buildBulkReadRowsSettings(EnhancedBigtableStubSettings.Builder builder) {
    BatchingSettings.Builder bulkReadBatchingBuilder =
        builder.bulkReadRowsSettings().getBatchingSettings().toBuilder();

    String bulkMaxRowKeyCountStr = configuration.get(BIGTABLE_BULK_MAX_ROW_KEY_COUNT);
    if (!isNullOrEmpty(bulkMaxRowKeyCountStr)) {
      bulkReadBatchingBuilder.setElementCountThreshold(Long.valueOf(bulkMaxRowKeyCountStr));
    }

    builder
        .bulkReadRowsSettings()
        .setBatchingSettings(bulkReadBatchingBuilder.build())
        .setRetryableCodes(buildRetryCodes(builder.bulkReadRowsSettings().getRetryableCodes()))
        .setRetrySettings(
            buildIdempotentRetrySettings(builder.bulkReadRowsSettings().getRetrySettings()));
  }

  private void buildReadRowsSettings(EnhancedBigtableStubSettings.Builder stubSettings) {
    RetrySettings.Builder retryBuilder =
        stubSettings.readRowsSettings().getRetrySettings().toBuilder();

    String initialElapsedBackoffMsStr = configuration.get(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY);
    if (!isNullOrEmpty(initialElapsedBackoffMsStr)) {
      retryBuilder.setInitialRetryDelay(ofMillis(Long.valueOf(initialElapsedBackoffMsStr)));
    }

    String maxScanTimeoutRetriesAttempts = configuration.get(MAX_SCAN_TIMEOUT_RETRIES);
    if (!isNullOrEmpty(maxScanTimeoutRetriesAttempts)) {
      LOG.debug("gRPC max scan timeout retries (count): %d", maxScanTimeoutRetriesAttempts);
      retryBuilder.setMaxAttempts(Integer.valueOf(maxScanTimeoutRetriesAttempts));
    }

    String rpcTimeoutStr = configuration.get(READ_PARTIAL_ROW_TIMEOUT_MS);
    if (!isNullOrEmpty(rpcTimeoutStr)) {
      Duration rpcTimeoutMs = ofMillis(Long.valueOf(rpcTimeoutStr));
      retryBuilder.setInitialRpcTimeout(rpcTimeoutMs).setMaxRpcTimeout(rpcTimeoutMs);
    }

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_TIMEOUTS_KEY))) {
      String readRowsRpcTimeoutMs = configuration.get(BIGTABLE_READ_RPC_TIMEOUT_MS_KEY);

      if (!isNullOrEmpty(readRowsRpcTimeoutMs)) {
        retryBuilder.setTotalTimeout(ofMillis(Long.valueOf(readRowsRpcTimeoutMs)));
      }
    } else {

      String maxElapsedBackoffMillis = configuration.get(MAX_ELAPSED_BACKOFF_MILLIS_KEY);
      if (!isNullOrEmpty(maxElapsedBackoffMillis)) {
        retryBuilder.setTotalTimeout(ofMillis(Long.valueOf(maxElapsedBackoffMillis)));
      }
    }

    stubSettings
        .readRowsSettings()
        .setRetryableCodes(buildRetryCodes(stubSettings.readRowsSettings().getRetryableCodes()))
        .setRetrySettings(retryBuilder.build());
  }
  // </editor-fold>
}
