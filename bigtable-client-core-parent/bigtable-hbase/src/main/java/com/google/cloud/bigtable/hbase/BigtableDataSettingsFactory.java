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

import org.apache.hadoop.conf.Configuration;
import org.threeten.bp.Duration;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
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
import com.google.cloud.bigtable.data.v2.stub.BigtableStubSettings;

/**
 * Static methods to convert an instance of {@link Configuration} or {@link BigtableOptions} to a
 * {@link BigtableDataSettings} instance.
 */
public class BigtableDataSettingsFactory {
  /** Constant <code>LOG</code> */
  private static final Logger LOG = new Logger(BigtableDataSettingsFactory.class);

  /**
   * <p>
   * fromBigtableOptions.
   * </p>
   * @param configuration a {@link Configuration} object.
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

    createCredentialProvider(builder, options.getCredentialOptions());

    setBulkOptions(builder, options.getBulkOptions());

    RetryOptions retryOptions = options.getRetryOptions();
    if (retryOptions.enableRetries()) {
      // Creating retrySetting from retryOptions as well callOptionsConfig
      RetrySettings retrySettings = RetrySettings.newBuilder()
          .setInitialRetryDelay(ofMillis(retryOptions.getInitialBackoffMillis()))
          .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())
          .setMaxRetryDelay(ofMillis(retryOptions.getMaxElapsedBackoffMillis()))
          .setMaxAttempts(retryOptions.getMaxScanTimeoutRetries())
          .setInitialRpcTimeout(
            ofMillis(options.getCallOptionsConfig().getShortRpcTimeoutMs()))
          .setMaxRpcTimeout(ofMillis(options.getCallOptionsConfig().getLongRpcTimeoutMs()))
          .build();
      //TODO rahulkql:  Is this correct??, as these settings are for BigtableOptions 
      //whole but here we are just setting it for bulkMutation.
      builder.bulkMutationsSettings().setRetrySettings(retrySettings);
      builder.checkAndMutateRowSettings().setRetrySettings(retrySettings);
      builder.readModifyWriteRowSettings().setRetrySettings(retrySettings);
      builder.readRowsSettings().setRetrySettings(retrySettings);
      builder.mutateRowSettings().setRetrySettings(retrySettings);
      builder.sampleRowKeysSettings().setRetrySettings(retrySettings);
    } else {

      builder.bulkMutationsSettings().setSimpleTimeoutNoRetries(Duration.ZERO);
    }

    builder.setTransportChannelProvider(InstantiatingGrpcChannelProvider.newBuilder()
        .setChannelsPerCpu(options.getChannelCount())
        .setMaxInboundMessageSize(retryOptions.getStreamingBufferSize()).build());

    // userAgent can be set on headers.
    String userAgent = options.getUserAgent();
    if (null != userAgent)
      builder.setHeaderProvider(FixedHeaderProvider.create("USER_AGENT", userAgent));

    return builder.build();
  }

  /**
   * This method is use to set BatchSettings into BigtableDataSettings.
   * @param builder
   * @param bulkMutation
   */
  private static void setBulkOptions(Builder builder, BulkOptions bulkMutation) {
    BatchingSettings.Builder batchingSettingsBuilder =
        BatchingSettings.newBuilder().setIsEnabled(bulkMutation.useBulkApi())
            .setElementCountThreshold((long) bulkMutation.getBulkMaxRowKeyCount())
            .setRequestByteThreshold(bulkMutation.getBulkMaxRequestSize())
            .setFlowControlSettings(
              FlowControlSettings.newBuilder().setLimitExceededBehavior(LimitExceededBehavior.Block)
                  .setMaxOutstandingRequestBytes(bulkMutation.getMaxMemory()).build());

    long autoFlushMs = bulkMutation.getAutoflushMs();
    if (autoFlushMs > 0) {
      batchingSettingsBuilder.setDelayThreshold(Duration.ofMillis(autoFlushMs));
    }

    builder.bulkMutationsSettings().setBatchingSettings(batchingSettingsBuilder.build());
  }

  /**
   * To create CredentialProvider based on CredentialType of BigtableOptions
   * @param builder
   * @param credentialOptions
   * @throws FileNotFoundException
   * @throws IOException
   */
  private static void createCredentialProvider(Builder builder,
      CredentialOptions credentialOptions) throws FileNotFoundException, IOException {
    // BigtableOptions doesn't have getter for credential object,
    CredentialType credentialType = credentialOptions.getCredentialType();
    CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

    switch (credentialType) {
    case DefaultCredentials:
      // Using DEFAULT_SERVICE_SCOPES defined in BigtableStubSettings
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
