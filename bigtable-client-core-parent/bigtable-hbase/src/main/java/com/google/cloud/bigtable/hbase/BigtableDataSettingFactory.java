/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.config.BigtableOptions.BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableOptions.BIGTABLE_DATA_HOST_DEFAULT;
import static com.google.cloud.bigtable.config.BigtableOptions.BIGTABLE_PORT_DEFAULT;
import static com.google.cloud.bigtable.config.BulkOptions.BIGTABLE_BULK_AUTOFLUSH_MS_DEFAULT;
import static com.google.cloud.bigtable.config.BulkOptions.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT;
import static com.google.cloud.bigtable.config.BulkOptions.BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT;
import static com.google.cloud.bigtable.config.CallOptionsConfig.LONG_TIMEOUT_MS_DEFAULT;
import static com.google.cloud.bigtable.config.CallOptionsConfig.SHORT_TIMEOUT_MS_DEFAULT;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_STREAMING_BUFFER_SIZE;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ADDITIONAL_RETRY_CODES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_DEFAULT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_DEFAULT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INSTANCE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.PROJECT_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_BUFFER_SIZE;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.threeten.bp.Duration;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.CredentialOptions.CredentialType;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings.Builder;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.common.base.Preconditions;

/**
 * Static methods to convert an instance of {@link Configuration} or {@link BigtableOptions} to a
 * {@link BigtableDataSettings} instance.
 */
// TODO rahulkql: Considered existing properties, did not introduce
// new or default for remaining properties of BigtableDataSettings
public class BigtableDataSettingFactory {
  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(BigtableDataSettingFactory.class);

  /**
   * <p>
   * fromConfiguration.
   * </p>
   * @param configuration a {@link Configuration} object.
   * @return a {@link BigtableDataSettings} object.
   * @throws IOException if any.
   */
  // TODO: add more properties from configuration, once mapped to BigtableDataSettings
  public static BigtableDataSettings fromConfiguration(final Configuration configuration)
      throws IOException, GeneralSecurityException {
    BigtableDataSettings.Builder builder = BigtableDataSettings.newBuilder();

    InstanceName instanceName =
        InstanceName.newBuilder().setProject(getValue(configuration, PROJECT_ID_KEY, "Project ID"))
            .setInstance(getValue(configuration, INSTANCE_ID_KEY, "Instance ID")).build();
    builder.setInstanceName(instanceName);

    String appProfileId = configuration.get(BigtableOptionsFactory.APP_PROFILE_ID_KEY, null);
    if (appProfileId != null) {
      builder.setAppProfileId(appProfileId);
    }

    String host = getHost(configuration, BigtableOptionsFactory.BIGTABLE_HOST_KEY,
      BIGTABLE_DATA_HOST_DEFAULT, "API Data");
    int port =
        configuration.getInt(BigtableOptionsFactory.BIGTABLE_PORT_KEY, BIGTABLE_PORT_DEFAULT);
    builder.setEndpoint(host + ":" + port);

    setCredentialOptions(builder, configuration);
    setBulkOptions(builder, configuration);
    setRetrySettings(builder, configuration);
    setChannelOptions(builder, configuration);
    return builder.build();
  }

  private static void setRetrySettings(BigtableDataSettings.Builder builder,
      Configuration configuration) throws IOException {
    String retryCodes = configuration.get(ADDITIONAL_RETRY_CODES, "");
    String codes[] = retryCodes.split(",");
    Set<StatusCode.Code> statusCodes = new HashSet<>();
    for (String stringCode : codes) {
      String trimmed = stringCode.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      Code code = StatusCode.Code.valueOf(trimmed);
      Preconditions.checkArgument(code != null, "Code " + stringCode + " not found.");
      LOG.debug("gRPC retry on: %s", stringCode);
      statusCodes.add(code);
    }
    builder.bulkMutationsSettings().setRetryableCodes(statusCodes);

    int initialElapsedBackoffMillis =
        configuration.getInt(BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY,
          RetryOptions.DEFAULT_INITIAL_BACKOFF_MILLIS);
    LOG.debug("gRPC retry initialElapsedBackoffMillis: %d", initialElapsedBackoffMillis);

    int maxElapsedBackoffMillis =
        configuration.getInt(BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY,
          RetryOptions.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS);
    LOG.debug("gRPC retry maxElapsedBackoffMillis: %d", maxElapsedBackoffMillis);

    double backOffMultiplier = RetryOptions.DEFAULT_BACKOFF_MULTIPLIER;
    LOG.debug("gRPC retry maxElapsedBackoffMillis: %d", maxElapsedBackoffMillis);

    int maxScanTimeoutRetries =
        configuration.getInt(BigtableOptionsFactory.MAX_SCAN_TIMEOUT_RETRIES,
          RetryOptions.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES);
    LOG.debug("gRPC max scan timeout retries (count): %d", maxScanTimeoutRetries);

    RetrySettings retrySettings = RetrySettings.newBuilder()
        .setInitialRetryDelay(Duration.ofMillis(initialElapsedBackoffMillis))
        .setRetryDelayMultiplier(backOffMultiplier)
        .setMaxRetryDelay(Duration.ofMillis(maxElapsedBackoffMillis))
        .setMaxAttempts(maxScanTimeoutRetries)
        .setInitialRpcTimeout(Duration
            .ofMillis(configuration.getLong(BIGTABLE_RPC_TIMEOUT_MS_KEY, SHORT_TIMEOUT_MS_DEFAULT)))
        .setMaxRpcTimeout(Duration.ofMillis(
          configuration.getInt(BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY, LONG_TIMEOUT_MS_DEFAULT)))
        .build();
    builder.bulkMutationsSettings().setRetrySettings(retrySettings);

  }

  private static void setCredentialOptions(BigtableDataSettings.Builder builder,
      Configuration configuration) throws IOException, GeneralSecurityException {

    if (configuration.getBoolean(BIGTABLE_USE_SERVICE_ACCOUNTS_KEY,
      BIGTABLE_USE_SERVICE_ACCOUNTS_DEFAULT)) {
      LOG.debug("Using service accounts");

      Credentials credentials = GoogleCredentials.getApplicationDefault();
      if (configuration instanceof BigtableExtendedConfiguration) {
        credentials = ((BigtableExtendedConfiguration) configuration).getCredentials();
      } else if (configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY) != null) {
        String jsonValue = configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY);
        LOG.debug("Using json value");
        credentials = CredentialFactory.getInputStreamCredential(
          new ByteArrayInputStream(jsonValue.getBytes(StandardCharsets.UTF_8)));
      } else if (configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY) != null) {
        String keyFileLocation =
            configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY);
        LOG.debug("Using json keyfile: %s", keyFileLocation);
        credentials =
            CredentialFactory.getInputStreamCredential(new FileInputStream(keyFileLocation));
      } else if (configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY) != null) {
        String serviceAccount = configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY);
        LOG.debug("Service account %s specified.", serviceAccount);
        String keyFileLocation =
            configuration.get(BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);
        Preconditions.checkState(!isNullOrEmpty(keyFileLocation),
          "Key file location must be specified when setting service account email");
        LOG.debug("Using p12 keyfile: %s", keyFileLocation);
        credentials = CredentialFactory.getCredentialFromPrivateKeyServiceAccount(serviceAccount,
          keyFileLocation);
      } else {
        LOG.debug("Using default credentials.");
        credentials = GoogleCredentials.getApplicationDefault();
      }
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    } else if (configuration.getBoolean(BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY,
      BIGTABLE_NULL_CREDENTIAL_ENABLE_DEFAULT)) {
      builder.setCredentialsProvider(NoCredentialsProvider.create());
      LOG.info("Enabling the use of null credentials. This should not be used in production.");
    } else {
      throw new IllegalStateException("Either service account or null credentials must be enabled");
    }

  }

  private static void setBulkOptions(BigtableDataSettings.Builder builder,
      Configuration configuration) throws IOException {
    long bulkMaxRowKeyCount = configuration.getLong(BIGTABLE_BULK_MAX_ROW_KEY_COUNT,
      BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT);

    long bulkMaxRequestSizeBytes = configuration.getLong(BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES,
      BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT);

    long autoFlushMs = configuration.getLong(BigtableOptionsFactory.BIGTABLE_BULK_AUTOFLUSH_MS_KEY,
      BIGTABLE_BULK_AUTOFLUSH_MS_DEFAULT);

    long maxMemory = configuration.getLong(BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY,
      BulkOptions.BIGTABLE_MAX_MEMORY_DEFAULT);

    BatchingSettings batchingSettings = BatchingSettings.newBuilder()
        .setIsEnabled(configuration.getBoolean(BigtableOptionsFactory.BIGTABLE_USE_BULK_API, true))
        .setElementCountThreshold(bulkMaxRowKeyCount)
        .setRequestByteThreshold(bulkMaxRequestSizeBytes)
        .setDelayThreshold(Duration.ofMillis(autoFlushMs))
        .setFlowControlSettings(
          FlowControlSettings.newBuilder().setLimitExceededBehavior(LimitExceededBehavior.Block)
              .setMaxOutstandingRequestBytes(maxMemory).build())
        .build();
    builder.bulkMutationsSettings().setBatchingSettings(batchingSettings);
  }

  private static void setChannelOptions(BigtableDataSettings.Builder builder,
      Configuration configuration) throws IOException {
    int channelCount =
        configuration.getInt(BIGTABLE_DATA_CHANNEL_COUNT_KEY, BIGTABLE_DATA_CHANNEL_COUNT_DEFAULT);

    int streamingBufferSize = configuration.getInt(READ_BUFFER_SIZE, DEFAULT_STREAMING_BUFFER_SIZE);
    LOG.debug("gRPC read buffer size (count): %d", streamingBufferSize);

    builder.setTransportChannelProvider(InstantiatingGrpcChannelProvider.newBuilder()
        .setChannelsPerCpu(channelCount).setMaxInboundMessageSize(streamingBufferSize)
        // TODO rahulkql:could be configured to accept pool size,
        // .setPoolSize(1)
        .build()).build();
  }

  private static String getValue(final Configuration configuration, String key, String type) {
    String value = configuration.get(key);
    Preconditions.checkArgument(!isNullOrEmpty(value),
      String.format("%s must be supplied via %s", type, key));
    LOG.debug("%s %s", type, value);
    return value;
  }

  private static String getHost(Configuration configuration, String key, String defaultVal,
      String type) {
    String hostName = configuration.get(key, defaultVal);
    LOG.debug("%s endpoint host %s.", type, hostName);
    return hostName;
  }

  /**
   * <p>
   * fromBigtableOptions.
   * </p>
   * @param configuration a {@link Configuration} object.
   * @return a {@link BigtableDataSettings} object.
   * @throws IOException if any.
   */
  // TODO: add more properties from bigtableOptions, once mapped to BigtableDataSettings
  public static BigtableDataSettings fromBigtableOptions(final BigtableOptions bigtableOptions)
      throws IOException, GeneralSecurityException {
    BigtableDataSettings.Builder builder = BigtableDataSettings.newBuilder();

    InstanceName instanceName = InstanceName.newBuilder().setProject(bigtableOptions.getProjectId())
        .setInstance(bigtableOptions.getInstanceId()).build();

    builder.setInstanceName(instanceName);
    builder.setAppProfileId(bigtableOptions.getAppProfileId());

    builder.setEndpoint(bigtableOptions.getDataHost() + ":" + bigtableOptions.getPort());

    // TODO rahulkql: BigtableOptions doesn't have credential object,
    // find better handling of credentials setting from BigtableOptions.
    CredentialType envJsonFile = bigtableOptions.getCredentialOptions().getCredentialType();
    CredentialsProvider credentialsProvider = GoogleCredentialsProvider.newBuilder().build();
    switch (envJsonFile) {
    case DefaultCredentials:
      credentialsProvider = GoogleCredentialsProvider.newBuilder().build();
      break;
    case P12:
    case SuppliedCredentials:
    case SuppliedJson:
      credentialsProvider = FixedCredentialsProvider.create(CredentialFactory
          .getInputStreamCredential(new FileInputStream(CredentialOptions.getEnvJsonFile())));
      break;
    case None:
    default:
      credentialsProvider = NoCredentialsProvider.create();
      break;
    }

    builder.setCredentialsProvider(credentialsProvider);

    setBulkOptions(builder, bigtableOptions.getBulkOptions());

    RetryOptions retryOptions = bigtableOptions.getRetryOptions();

    // Creating retrySetting from retryOptions as well callOptionsConfig
    RetrySettings retrySettings = RetrySettings.newBuilder()
        .setInitialRetryDelay(Duration.ofMillis(retryOptions.getInitialBackoffMillis()))
        .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())
        .setMaxRetryDelay(Duration.ofMillis(retryOptions.getMaxElapsedBackoffMillis()))
        .setMaxAttempts(retryOptions.getMaxScanTimeoutRetries())
        .setInitialRpcTimeout(
          Duration.ofMillis(bigtableOptions.getCallOptionsConfig().getShortRpcTimeoutMs()))
        .setMaxRpcTimeout(
          Duration.ofMillis(bigtableOptions.getCallOptionsConfig().getLongRpcTimeoutMs()))
        .build();
    builder.bulkMutationsSettings().setRetrySettings(retrySettings);

    builder.setTransportChannelProvider(InstantiatingGrpcChannelProvider.newBuilder()
        .setChannelsPerCpu(bigtableOptions.getChannelCount())
        .setMaxInboundMessageSize(retryOptions.getStreamingBufferSize()).build());

    return builder.build();
  }

  private static void setBulkOptions(Builder builder, BulkOptions bulkMutation) {
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder().setIsEnabled(bulkMutation.useBulkApi())
            .setElementCountThreshold((long) bulkMutation.getBulkMaxRowKeyCount())
            .setRequestByteThreshold(bulkMutation.getBulkMaxRequestSize())
            .setDelayThreshold(Duration.ofMillis(bulkMutation.getAutoflushMs()))
            .setFlowControlSettings(
              FlowControlSettings.newBuilder().setLimitExceededBehavior(LimitExceededBehavior.Block)
                  .setMaxOutstandingRequestBytes(bulkMutation.getMaxMemory()).build())
            .build();
    builder.bulkMutationsSettings().setBatchingSettings(batchingSettings);
  }
}
