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
package com.google.cloud.bigtable.hbase.wrappers;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_PORT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INSTANCE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.PROJECT_ID_KEY;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.classic.BigtableHBaseClassicSettings;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableHBaseVeneerSettings;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public abstract class BigtableHBaseSettings {

  protected static final Logger LOG = new Logger(BigtableOptionsFactory.class);

  protected final Configuration configuration;
  private final String projectId;
  private final String instanceId;

  public static BigtableHBaseSettings create(Configuration configuration) {
    if (configuration.getBoolean(BIGTABLE_USE_GCJ_CLIENT, false)) {
      return new BigtableHBaseVeneerSettings(configuration);
    } else {
      return new BigtableHBaseClassicSettings(configuration);
    }
  }

  public BigtableHBaseSettings(Configuration configuration) {
    this.configuration = configuration;
    this.projectId = getValue(PROJECT_ID_KEY, "Project ID");
    this.instanceId = getValue(INSTANCE_ID_KEY, "Instance ID");
  }

  public Configuration getConfiguration() {
    return new Configuration(configuration);
  }

  public String getProjectId() {
    return projectId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getDataHost() {
    String hostNameStr = configuration.get(BIGTABLE_HOST_KEY);
    return isNullOrEmpty(hostNameStr) ? BigtableConstants.BIGTABLE_DATA_HOST_DEFAULT : hostNameStr;
  }

  public String getAdminHost() {
    String adminHostStr = configuration.get(BIGTABLE_ADMIN_HOST_KEY);
    return isNullOrEmpty(adminHostStr)
        ? BigtableConstants.BIGTABLE_ADMIN_HOST_DEFAULT
        : adminHostStr;
  }

  public int getPort() {
    String portNumberStr = configuration.get(BIGTABLE_PORT_KEY);
    return isNullOrEmpty(portNumberStr)
        ? BigtableConstants.BIGTABLE_PORT_DEFAULT
        : Integer.valueOf(portNumberStr);
  }

  public int getBulkMaxRowCount() {
    String bulkMaxRowKeyCountStr = configuration.get(BIGTABLE_BULK_MAX_ROW_KEY_COUNT);
    return isNullOrEmpty(bulkMaxRowKeyCountStr)
        ? BigtableConstants.BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT
        : Integer.valueOf(bulkMaxRowKeyCountStr);
  }

  protected String getValue(String key, String type) {
    String value = configuration.get(key);
    Preconditions.checkArgument(
        !isNullOrEmpty(value), String.format("%s must be supplied via %s", type, key));
    return value;
  }
}
