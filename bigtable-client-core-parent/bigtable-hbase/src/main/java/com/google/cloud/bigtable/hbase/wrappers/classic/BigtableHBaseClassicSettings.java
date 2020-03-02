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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import static com.google.cloud.bigtable.config.BulkOptions.BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT;
import static com.google.cloud.bigtable.config.BulkOptions.BIGTABLE_BULK_AUTOFLUSH_MS_DEFAULT;
import static com.google.cloud.bigtable.config.BulkOptions.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT;
import static com.google.cloud.bigtable.config.BulkOptions.BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT;
import static com.google.cloud.bigtable.config.BulkOptions.BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT;
import static com.google.cloud.bigtable.config.CallOptionsConfig.LONG_TIMEOUT_MS_DEFAULT;
import static com.google.cloud.bigtable.config.CallOptionsConfig.SHORT_TIMEOUT_MS_DEFAULT;
import static com.google.cloud.bigtable.config.CallOptionsConfig.USE_TIMEOUT_DEFAULT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ADDITIONAL_RETRY_CODES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ALLOW_NO_TIMESTAMP_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.APP_PROFILE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_ASYNC_MUTATOR_COUNT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_AUTOFLUSH_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_DEFAULT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_READ_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_BATCH;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_BULK_API;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_DEFAULT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_TIMEOUTS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ENABLE_GRPC_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INSTANCE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_INFLIGHT_RPCS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_SCAN_TIMEOUT_RETRIES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.PROJECT_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_BUFFER_SIZE;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_PARTIAL_ROW_TIMEOUT_MS;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.hbase.BigtableExtendedConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.common.base.Preconditions;
import io.grpc.Status;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.VersionInfo;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableHBaseClassicSettings extends BigtableHBaseSettings {

  public BigtableHBaseClassicSettings(Configuration configuration) {
    super(configuration);
  }

  // <editor-fold desc="Public API">
  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public BigtableOptions getBigtableOptions() throws IOException {
    BigtableOptions.Builder bigtableOptionsBuilder = BigtableOptions.builder();

    bigtableOptionsBuilder.setProjectId(getValue(PROJECT_ID_KEY, "Project " + "ID"));
    bigtableOptionsBuilder.setInstanceId(getValue(INSTANCE_ID_KEY, "Instance ID"));
    String appProfileId = configuration.get(APP_PROFILE_ID_KEY);

    if (appProfileId != null) {
      bigtableOptionsBuilder.setAppProfileId(appProfileId);
    }

    String dataHostOverride = configuration.get(BIGTABLE_HOST_KEY);
    if (dataHostOverride != null) {
      LOG.debug("API Data endpoint host %s.", dataHostOverride);
      bigtableOptionsBuilder.setDataHost(dataHostOverride);
    }

    String adminHostOverride = configuration.get(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY);
    if (adminHostOverride != null) {
      LOG.debug("Admin endpoint host %s.", adminHostOverride);
      bigtableOptionsBuilder.setAdminHost(adminHostOverride);
    }

    String portOverrideStr = configuration.get(BigtableOptionsFactory.BIGTABLE_PORT_KEY);
    if (portOverrideStr != null) {
      bigtableOptionsBuilder.setPort(Integer.parseInt(portOverrideStr));
    }

    String usePlaintextStr = configuration.get(BIGTABLE_USE_PLAINTEXT_NEGOTIATION);
    if (usePlaintextStr != null) {
      bigtableOptionsBuilder.setUsePlaintextNegotiation(Boolean.parseBoolean(usePlaintextStr));
    }

    setBulkOptions(bigtableOptionsBuilder);
    setChannelOptions(bigtableOptionsBuilder);
    setClientCallOptions(bigtableOptionsBuilder);

    String emulatorHost = configuration.get(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY);
    if (emulatorHost != null) {
      bigtableOptionsBuilder.enableEmulator(emulatorHost);
    }

    String useBatchStr = configuration.get(BIGTABLE_USE_BATCH);
    if (useBatchStr != null) {
      bigtableOptionsBuilder.setUseBatch(Boolean.parseBoolean(useBatchStr));
    }

    String useGcjClientStr = configuration.get(BIGTABLE_USE_GCJ_CLIENT);
    if (useGcjClientStr != null) {
      bigtableOptionsBuilder.setUseGCJClient(Boolean.parseBoolean(useGcjClientStr));
    }
    return bigtableOptionsBuilder.build();
  }
  // </editor-fold>

  // <editor-fold desc="Private Helpers">
  private void setChannelOptions(BigtableOptions.Builder builder) throws IOException {
    setCredentialOptions(builder);

    builder.setRetryOptions(createRetryOptions());

    String channelCountStr = configuration.get(BIGTABLE_DATA_CHANNEL_COUNT_KEY);
    if (channelCountStr != null) {
      builder.setDataChannelCount(Integer.parseInt(channelCountStr));
    }

    // This is primarily used by Dataflow where connections open and close often. This is a
    // performance optimization that will reduce the cost to open connections.
    String useCachedDataPoolStr = configuration.get(BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL);
    if (useCachedDataPoolStr != null) {
      builder.setUseCachedDataPool(Boolean.parseBoolean(useCachedDataPoolStr));
    }

    // This information is in addition to bigtable-client-core version, and jdk version.
    StringBuilder agentBuilder = new StringBuilder();
    agentBuilder.append("hbase-").append(VersionInfo.getVersion());
    String customUserAgent = configuration.get(CUSTOM_USER_AGENT_KEY);
    if (customUserAgent != null) {
      agentBuilder.append(',').append(customUserAgent);
    }
    builder.setUserAgent(agentBuilder.toString());
  }

  private void setBulkOptions(BigtableOptions.Builder bigtableOptionsBuilder) {
    BulkOptions.Builder bulkOptionsBuilder = BulkOptions.builder();

    int asyncMutatorCount =
        configuration.getInt(
            BIGTABLE_ASYNC_MUTATOR_COUNT_KEY, BIGTABLE_ASYNC_MUTATOR_COUNT_DEFAULT);
    bulkOptionsBuilder.setAsyncMutatorWorkerCount(asyncMutatorCount);

    bulkOptionsBuilder.setUseBulkApi(configuration.getBoolean(BIGTABLE_USE_BULK_API, true));
    bulkOptionsBuilder.setBulkMaxRowKeyCount(
        configuration.getInt(
            BIGTABLE_BULK_MAX_ROW_KEY_COUNT, BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT));
    bulkOptionsBuilder.setBulkMaxRequestSize(
        configuration.getLong(
            BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES, BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES_DEFAULT));
    bulkOptionsBuilder.setAutoflushMs(
        configuration.getLong(BIGTABLE_BULK_AUTOFLUSH_MS_KEY, BIGTABLE_BULK_AUTOFLUSH_MS_DEFAULT));

    int defaultRpcCount =
        BIGTABLE_MAX_INFLIGHT_RPCS_PER_CHANNEL_DEFAULT
            * bigtableOptionsBuilder.getDataChannelCount();
    int maxInflightRpcs = configuration.getInt(MAX_INFLIGHT_RPCS_KEY, defaultRpcCount);
    bulkOptionsBuilder.setMaxInflightRpcs(maxInflightRpcs);

    long maxMemory =
        configuration.getLong(
            BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY, BulkOptions.BIGTABLE_MAX_MEMORY_DEFAULT);
    bulkOptionsBuilder.setMaxMemory(maxMemory);

    if (configuration.getBoolean(
        BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING,
        BulkOptions.BIGTABLE_BULK_ENABLE_THROTTLE_REBALANCE_DEFAULT)) {
      LOG.info(
          "Bigtable mutation latency throttling enabled with threshold %d",
          configuration.getInt(
              BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS,
              BulkOptions.BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT));
      bulkOptionsBuilder.enableBulkMutationThrottling();
      bulkOptionsBuilder.setBulkMutationRpcTargetMs(
          configuration.getInt(
              BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS,
              BulkOptions.BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT));
    }

    bigtableOptionsBuilder.setBulkOptions(bulkOptionsBuilder.build());
  }

  private void setCredentialOptions(BigtableOptions.Builder builder) throws FileNotFoundException {
    if (configuration.getBoolean(
        BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, BIGTABLE_USE_SERVICE_ACCOUNTS_DEFAULT)) {
      LOG.debug("Using service accounts");

      if (configuration instanceof BigtableExtendedConfiguration) {
        Credentials credentials = ((BigtableExtendedConfiguration) configuration).getCredentials();
        builder.setCredentialOptions(CredentialOptions.credential(credentials));
      } else if (configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY) != null) {
        String jsonValue = configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY);
        LOG.debug("Using json value");
        builder.setCredentialOptions(CredentialOptions.jsonCredentials(jsonValue));
      } else if (configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY) != null) {
        String keyFileLocation =
            configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY);
        LOG.debug("Using json keyfile: %s", keyFileLocation);
        builder.setCredentialOptions(
            CredentialOptions.jsonCredentials(new FileInputStream(keyFileLocation)));
      } else if (configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY) != null) {
        String serviceAccount = configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY);
        LOG.debug("Service account %s specified.", serviceAccount);
        String keyFileLocation =
            configuration.get(BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);
        Preconditions.checkState(
            !isNullOrEmpty(keyFileLocation),
            "Key file location must be specified when setting service account email");
        LOG.debug("Using p12 keyfile: %s", keyFileLocation);
        builder.setCredentialOptions(
            CredentialOptions.p12Credential(serviceAccount, keyFileLocation));
      } else {
        LOG.debug("Using default credentials.");
        builder.setCredentialOptions(CredentialOptions.defaultCredentials());
      }
    } else if (configuration.getBoolean(
        BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, BIGTABLE_NULL_CREDENTIAL_ENABLE_DEFAULT)) {
      builder.setCredentialOptions(CredentialOptions.nullCredential());
      LOG.info("Enabling the use of null credentials. This should not be used in production.");
    } else {
      throw new IllegalStateException("Either service account or null credentials must be enabled");
    }
  }

  private void setClientCallOptions(BigtableOptions.Builder bigtableOptionsBuilder) {
    CallOptionsConfig.Builder clientCallOptionsBuilder = CallOptionsConfig.builder();

    clientCallOptionsBuilder.setUseTimeout(
        configuration.getBoolean(BIGTABLE_USE_TIMEOUTS_KEY, USE_TIMEOUT_DEFAULT));
    clientCallOptionsBuilder.setShortRpcTimeoutMs(
        configuration.getInt(BIGTABLE_RPC_TIMEOUT_MS_KEY, SHORT_TIMEOUT_MS_DEFAULT));
    int longTimeoutMs =
        configuration.getInt(BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY, LONG_TIMEOUT_MS_DEFAULT);
    clientCallOptionsBuilder.setMutateRpcTimeoutMs(
        configuration.getInt(BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY, longTimeoutMs));
    clientCallOptionsBuilder.setReadRowsRpcTimeoutMs(
        configuration.getInt(BIGTABLE_READ_RPC_TIMEOUT_MS_KEY, longTimeoutMs));
    bigtableOptionsBuilder.setCallOptionsConfig(clientCallOptionsBuilder.build());
  }

  private RetryOptions createRetryOptions() {
    RetryOptions.Builder retryOptionsBuilder = RetryOptions.builder();
    boolean enableRetries =
        configuration.getBoolean(ENABLE_GRPC_RETRIES_KEY, RetryOptions.DEFAULT_ENABLE_GRPC_RETRIES);
    LOG.debug("gRPC retries enabled: %s", enableRetries);
    retryOptionsBuilder.setEnableRetries(enableRetries);

    boolean allowRetriesWithoutTimestamp =
        configuration.getBoolean(ALLOW_NO_TIMESTAMP_RETRIES_KEY, false);
    LOG.debug("allow retries without timestamp: %s", enableRetries);
    retryOptionsBuilder.setAllowRetriesWithoutTimestamp(allowRetriesWithoutTimestamp);

    String retryCodes = configuration.get(ADDITIONAL_RETRY_CODES, "");
    String codes[] = retryCodes.split(",");
    for (String stringCode : codes) {
      String trimmed = stringCode.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      Status.Code code = Status.Code.valueOf(trimmed);
      Preconditions.checkArgument(code != null, "Code " + stringCode + " not found.");
      LOG.debug("gRPC retry on: %s", stringCode);
      retryOptionsBuilder.addStatusToRetryOn(code);
    }

    boolean retryOnDeadlineExceeded =
        configuration.getBoolean(ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY, true);
    LOG.debug("gRPC retry on deadline exceeded enabled: %s", retryOnDeadlineExceeded);
    retryOptionsBuilder.setRetryOnDeadlineExceeded(retryOnDeadlineExceeded);

    int initialElapsedBackoffMillis =
        configuration.getInt(
            INITIAL_ELAPSED_BACKOFF_MILLIS_KEY, RetryOptions.DEFAULT_INITIAL_BACKOFF_MILLIS);
    LOG.debug("gRPC retry initialElapsedBackoffMillis: %d", initialElapsedBackoffMillis);
    retryOptionsBuilder.setInitialBackoffMillis(initialElapsedBackoffMillis);

    int maxElapsedBackoffMillis =
        configuration.getInt(
            MAX_ELAPSED_BACKOFF_MILLIS_KEY, RetryOptions.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS);
    LOG.debug("gRPC retry maxElapsedBackoffMillis: %d", maxElapsedBackoffMillis);
    retryOptionsBuilder.setMaxElapsedBackoffMillis(maxElapsedBackoffMillis);

    int readPartialRowTimeoutMillis =
        configuration.getInt(
            READ_PARTIAL_ROW_TIMEOUT_MS, RetryOptions.DEFAULT_READ_PARTIAL_ROW_TIMEOUT_MS);
    LOG.debug("gRPC read partial row timeout (millis): %d", readPartialRowTimeoutMillis);
    retryOptionsBuilder.setReadPartialRowTimeoutMillis(readPartialRowTimeoutMillis);

    int streamingBufferSize =
        configuration.getInt(READ_BUFFER_SIZE, RetryOptions.DEFAULT_STREAMING_BUFFER_SIZE);
    LOG.debug("gRPC read buffer size (count): %d", streamingBufferSize);
    retryOptionsBuilder.setStreamingBufferSize(streamingBufferSize);

    int maxScanTimeoutRetries =
        configuration.getInt(
            MAX_SCAN_TIMEOUT_RETRIES, RetryOptions.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES);
    LOG.debug("gRPC max scan timeout retries (count): %d", maxScanTimeoutRetries);
    retryOptionsBuilder.setMaxScanTimeoutRetries(maxScanTimeoutRetries);

    return retryOptionsBuilder.build();
  }
  // </editor-fold>
}
