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
package com.google.cloud.bigtable.beam;

import com.google.bigtable.repackaged.com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.beam.sequencefiles.ExportJob.ExportOptions;
import com.google.cloud.bigtable.beam.sequencefiles.ImportJob.ImportOptions;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import java.io.Serializable;
import java.nio.charset.CharacterCodingException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ParseFilter;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;

/**
 * !!! DO NOT TOUCH THIS CLASS !!!
 *
 * <p>Utility needed to help setting runtime parameters in Bigtable configurations. This is needed
 * because the methods that take runtime parameters are package private and not intended for direct
 * public consumption for now.
 */
public class TemplateUtils {
  /** Builds CloudBigtableTableConfiguration from input runtime parameters for import job. */
  public static CloudBigtableTableConfiguration BuildImportConfig(ImportOptions opts) {
    CloudBigtableTableConfiguration.Builder builder =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(opts.getBigtableProject())
            .withInstanceId(opts.getBigtableInstanceId())
            .withTableId(opts.getBigtableTableId());
    if (opts.getBigtableAppProfileId() != null) {
      builder.withAppProfileId(opts.getBigtableAppProfileId());
    }

    ValueProvider enableThrottling = ValueProvider.NestedValueProvider.of(
        opts.getMutationThrottleLatencyMs(), (Integer throttleMs) -> String.valueOf(throttleMs > 0));

    builder.withConfiguration(BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING, enableThrottling);
    builder.withConfiguration(BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS,
        ValueProvider.NestedValueProvider.of(opts.getMutationThrottleLatencyMs(), String::valueOf));

    return builder.build();
  }

  /** Provides a request that is constructed with some attributes. */
  private static class RequestValueProvider
      implements ValueProvider<ReadRowsRequest>, Serializable {
    private final ValueProvider<String> start;
    private final ValueProvider<String> stop;
    private final ValueProvider<Integer> maxVersion;
    private final ValueProvider<String> filter;
    private ReadRowsRequest cachedRequest;
    private final ExportOptions options;
    private final RequestContext requestContext;

    RequestValueProvider(ExportOptions options) {
      this.start = options.getBigtableStartRow();
      this.stop = options.getBigtableStopRow();
      this.maxVersion = options.getBigtableMaxVersions();
      this.filter = options.getBigtableFilter();
      this.options = options;
      this.requestContext = RequestContext.create(
              InstanceName.of(options.getBigtableProject().get(),
                      options.getBigtableInstanceId().get()),
              options.getBigtableAppProfileId().get());
    }

    @Override
    public ReadRowsRequest get() {
      if (cachedRequest == null) {
        Scan scan = new Scan();
        if (start.get() != null && !start.get().isEmpty()) {
          scan.setStartRow(start.get().getBytes());
        }
        if (stop.get() != null && !stop.get().isEmpty()) {
          scan.setStopRow(stop.get().getBytes());
        }
        if (maxVersion.get() != null) {
          scan.setMaxVersions(maxVersion.get());
        }
        if (filter.get() != null && !filter.get().isEmpty()) {
          try {
            scan.setFilter(new ParseFilter().parseFilterString(filter.get()));
          } catch (CharacterCodingException e) {
            throw new RuntimeException(e);
          }
        }

        ReadHooks readHooks = new DefaultReadHooks();
        Query query = Query.create(options.getBigtableTableId().get());
        Adapters.SCAN_ADAPTER.adapt(scan, readHooks, query);
        readHooks.applyPreSendHook(query);
        cachedRequest = query.toProto(requestContext);
      }
      return cachedRequest;
    }

    @Override
    public boolean isAccessible() {
      return start.isAccessible()
          && stop.isAccessible()
          && maxVersion.isAccessible()
          && filter.isAccessible();
    }

    @Override
    public String toString() {
      if (isAccessible()) {
        return String.valueOf(get());
      }
      return CloudBigtableConfiguration.VALUE_UNAVAILABLE;
    }
  }

  /** Builds CloudBigtableScanConfiguration from input runtime parameters for export job. */
  public static CloudBigtableScanConfiguration BuildExportConfig(ExportOptions options) {
    ValueProvider<ReadRowsRequest> request = new RequestValueProvider(options);
    CloudBigtableScanConfiguration.Builder configBuilder =
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId())
            .withAppProfileId(options.getBigtableAppProfileId())
            .withRequest(request);

    return configBuilder.build();
  }
}
