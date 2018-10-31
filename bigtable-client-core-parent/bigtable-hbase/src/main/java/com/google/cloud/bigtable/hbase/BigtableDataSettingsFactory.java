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

import static org.threeten.bp.Duration.ofMillis;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.threeten.bp.Duration;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings.Builder;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.stub.BigtableStubSettings;
import com.google.common.collect.ImmutableSet;

import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.GrpcUtil;

/**
 * Static methods to convert an instance of {@link Configuration} or {@link BigtableOptions} to a
 * {@link BigtableDataSettings} instance.
 */
public class BigtableDataSettingsFactory {
  /** Constant <code>LOG</code> */
  private static final Logger LOG = new Logger(BigtableDataSettingsFactory.class);

  // NOTE: Included Code.UNAUTHENTICATED for default retryCodes
  private static final Set<Code> DEFAULT_RETRY_CODES =
      ImmutableSet.of(Code.DEADLINE_EXCEEDED, Code.UNAVAILABLE, Code.ABORTED, Code.UNAUTHENTICATED);

  // NOTE: set rpcTimeout 100ms, without retries.
  private static final Duration DEFAULT_RPC_TIMEOUT = Duration.ofMillis(100);

  /**
   * To create an instance of {@link BigtableDataSettings} from {@link BigtableOptions}.
   *
   * @param configuration a {@link BigtableOptions} object.
   * @return a {@link BigtableDataSettings} object.
   * @throws IOException if any.
   */
  public static BigtableDataSettings fromBigtableOptions(final BigtableOptions options)
      throws IOException, GeneralSecurityException {
    BigtableDataSettings.Builder builder = BigtableDataSettings.newBuilder();

    InstanceName instanceName = InstanceName.newBuilder().setProject(options.getProjectId())
        .setInstance(options.getInstanceId()).build();
    builder.setInstanceName(instanceName);
    builder.setAppProfileId(options.getAppProfileId());

    LOG.debug("endpoint host %s.", options.getDataHost());
    LOG.debug("endpoint host %s.", options.getPort());
    builder.setEndpoint(options.getDataHost() + ":" + options.getPort());

    buildCredentialProvider(builder, options.getCredentialOptions());

    buildBulkOptions(builder, options);

    buildCheckAndMutateRow(builder, options);

    buildReadModifyWrite(builder, options);

    buildReadRows(builder, options);

    buildMutateRow(builder, options);

    buildSampleRowKeys(builder, options);

    // TODO: would it map to GrpcHeaderInterceptor? or we should build userAgent
    // using ManagedChannelBuilder AND
    builder.setHeaderProvider(
      FixedHeaderProvider.create(GrpcUtil.USER_AGENT_KEY.name(), options.getUserAgent()));

    // TODO: implementation for channelCount or channelPerCPU
    ManagedChannelBuilder channelBuilder = ManagedChannelBuilder
        .forAddress(options.getDataHost(), options.getPort())//
        .userAgent(options.getUserAgent());

    if (options.usePlaintextNegotiation()) {
      channelBuilder.usePlaintext();
    }
    builder.setTransportChannelProvider(
      FixedTransportChannelProvider.create(GrpcTransportChannel.create(channelBuilder.build())));

    return builder.build();
  }

  /**
   * This method is use to build BatchSettings.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param options a {@link BigtableOptions} object.
   */
  private static void buildBulkOptions(Builder builder, BigtableOptions options) {
    BulkOptions bulkOptions = options.getBulkOptions();
    BatchingSettings.Builder batchSettingsBuilder = BatchingSettings.newBuilder();

    // NOTE: FlowControlSettings by default uses LimitExceededBehavior.Block
    FlowControlSettings.Builder flowControlBuilder =
        FlowControlSettings.newBuilder()
            .setMaxOutstandingRequestBytes(bulkOptions.getMaxMemory());

    long autoFlushMs = bulkOptions.getAutoflushMs();
    if (autoFlushMs > 0) {
      batchSettingsBuilder.setDelayThreshold(Duration.ofMillis(autoFlushMs));
    }
    long maxIn = bulkOptions.getMaxInflightRpcs();
    if(maxIn > 0) {
      flowControlBuilder.setMaxOutstandingElementCount(maxIn);
    }

    batchSettingsBuilder
        .setIsEnabled(bulkOptions.useBulkApi())
        .setElementCountThreshold(Long.valueOf(bulkOptions.getBulkMaxRowKeyCount()))
        .setRequestByteThreshold(bulkOptions.getBulkMaxRequestSize())
        .setFlowControlSettings(flowControlBuilder.build());

    // TODO: implement bulkMutationThrottling & bulkMutationRpcTargetMs, once available
    if (options.getRetryOptions().enableRetries()) {
      builder.bulkMutationsSettings()
          .setBatchingSettings(batchSettingsBuilder.build())
          .setRetryableCodes(DEFAULT_RETRY_CODES)
          .setRetrySettings(defaultRetrySettings(options));
    } else {
      builder.bulkMutationsSettings()
          .setSimpleTimeoutNoRetries(DEFAULT_RPC_TIMEOUT)
          .getRetryableCodes().clear();
    }
  }

  /**
   * To build sampleRowKey with default Settings based on retries setting.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param bulkMutation a {@link BulkOptions} object.
   */
  private static void buildSampleRowKeys(Builder builder, BigtableOptions options) {
    if (options.getRetryOptions().enableRetries()) {
      builder.sampleRowKeysSettings()
          .setRetryableCodes(DEFAULT_RETRY_CODES)
          .setRetrySettings(defaultRetrySettings(options));
    } else {
      builder.sampleRowKeysSettings()
          .setSimpleTimeoutNoRetries(DEFAULT_RPC_TIMEOUT)
          .getRetryableCodes().clear();
    }
  }

  /**
   * To build mutateRowSettings with default Settings based on retries setting.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param bulkMutation a {@link BulkOptions} object.
   */
  private static void buildMutateRow(Builder builder, BigtableOptions options) {
    if (options.getRetryOptions().enableRetries()) {
      builder.mutateRowSettings()
          .setRetryableCodes(DEFAULT_RETRY_CODES)
          .setRetrySettings(defaultRetrySettings(options));
    } else {
      builder.sampleRowKeysSettings()
          .setSimpleTimeoutNoRetries(DEFAULT_RPC_TIMEOUT)
          .getRetryableCodes().clear();
    }
  }

  /**
   * To build readRows with default Settings based on retries setting.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param bulkMutation a {@link BulkOptions} object.
   */
  private static void buildReadRows(Builder builder, BigtableOptions options) {
    // TODO: set readPartialRowTimeout for watchdog timer, taken BigtableSession#setupWatchdog()
    if(options.getRetryOptions().enableRetries()) {
      builder.readRowsSettings()
          .setIdleTimeout(ofMillis(options.getRetryOptions().getReadPartialRowTimeoutMillis()))
          .setRetryableCodes(DEFAULT_RETRY_CODES)
          .setRetrySettings(defaultRetrySettings(options));
    } else {
      builder.readRowsSettings()
          .setIdleTimeout(ofMillis(options.getRetryOptions().getReadPartialRowTimeoutMillis()))
          .setSimpleTimeoutNoRetries(DEFAULT_RPC_TIMEOUT)
          .getRetryableCodes().clear();
    }
  }

  /**
   * This method builds ReadModifyWrite with no retry configurations
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param bulkMutation a {@link BulkOptions} object.
   */
  private static void buildReadModifyWrite(Builder builder, BigtableOptions options) {
    builder.readModifyWriteRowSettings()
        .setSimpleTimeoutNoRetries(DEFAULT_RPC_TIMEOUT)
        .getRetryableCodes().clear();
  }

  /**
   * This method builds CheckAndMutateRow with no retry configurations
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param bulkMutation a {@link BulkOptions} object.
   */
  private static void buildCheckAndMutateRow(Builder builder, BigtableOptions options) {
    builder.checkAndMutateRowSettings()
        .setSimpleTimeoutNoRetries(DEFAULT_RPC_TIMEOUT)
        .getRetryableCodes().clear();
  }

  /**
   * To create default RetrySettings, for BigtableDataSettings.
   *
   * @param builder a {@link BigtableDataSettings.Builder} object.
   * @param bulkMutation a {@link BulkOptions} object.
   */
  private static RetrySettings defaultRetrySettings(BigtableOptions options) {
    RetryOptions retryOptions = options.getRetryOptions();
    if (!retryOptions.enableRetries()) {
      // TODO: limiting the retries by adding this makes sense?
      return RetrySettings.newBuilder()
          .setMaxAttempts(1).build();
    }

    // TODO: Make sure other default values are compatible with these configuration.
    RetrySettings.Builder retryBuilder = RetrySettings.newBuilder()
        .setInitialRetryDelay(ofMillis(retryOptions.getInitialBackoffMillis()))
        .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())
        .setMaxRetryDelay(ofMillis(retryOptions.getMaxElapsedBackoffMillis()));

    // TODO: verify if maxScanTimeout would cover for maxAttempts or not.
    retryBuilder.setMaxAttempts(retryOptions.getMaxScanTimeoutRetries());

    // configurations for RPC timeouts
    retryBuilder
        .setInitialRpcTimeout(ofMillis(options.getCallOptionsConfig().getShortRpcTimeoutMs()))
        .setMaxRpcTimeout(ofMillis(retryOptions.getReadPartialRowTimeoutMillis()))
        .setTotalTimeout(ofMillis(options.getCallOptionsConfig().getLongRpcTimeoutMs()));

    // TODO: an option to set RetryOptions#allowRetriesWithoutTimestamp
    return retryBuilder.build();
  }

  /**
   * To create CredentialProvider based on CredentialType of BigtableOptions
   *
   * @param builder
   * @param credentialOptions
   * @throws FileNotFoundException
   * @throws IOException
   */
  private static void buildCredentialProvider(Builder builder, CredentialOptions credentialOptions)
      throws FileNotFoundException, IOException {
    CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

    switch (credentialOptions.getCredentialType()) {
    case DefaultCredentials:
      credentialsProvider = BigtableStubSettings.defaultCredentialsProviderBuilder().build();
      break;
    case P12:
    case SuppliedCredentials:
    case SuppliedJson:
      credentialsProvider = FixedCredentialsProvider.create(CredentialFactory
          .getInputStreamCredential(new FileInputStream(CredentialOptions.getEnvJsonFile())));
      break;
    case None:
      credentialsProvider = NoCredentialsProvider.create();
      break;
    default:
      throw new IllegalStateException("Either service account or null credentials must be enabled");
    }
    LOG.debug("CredentialsProvider used is: %s", credentialsProvider);
    builder.setCredentialsProvider(credentialsProvider);
  }
}
