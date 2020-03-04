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

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BigtableWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import java.io.IOException;

public class BigtableVeneerApi extends BigtableWrapper {

  private final BigtableHBaseVeneerSettings hbaseSettings;
  private final DataClientWrapper dataClient;
  private AdminClientWrapper adminClient;

  BigtableVeneerApi(BigtableHBaseVeneerSettings hbaseSettings) throws IOException {
    super(hbaseSettings);
    this.hbaseSettings = hbaseSettings;
    dataClient =
        new DataClientVeneerApi(BigtableDataClient.create(hbaseSettings.getDataSettings()));
  }

  @Override
  public AdminClientWrapper getAdminClientWrapper() throws IOException {
    if (adminClient == null) {
      adminClient =
          new AdminClientVeneerApi(
              BigtableTableAdminClient.create(hbaseSettings.getTableAdminSettings()));
    }
    return adminClient;
  }

  @Override
  public DataClientWrapper getDataClientWrapper() {
    return dataClient;
  }
}
