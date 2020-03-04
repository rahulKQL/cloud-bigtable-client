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

import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BigtableWrapper;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import java.io.IOException;

public class BigtableClassicApi extends BigtableWrapper {

  private final BigtableSession bigtableSession;
  private final DataClientWrapper dataClientWrapper;

  // Delaying the admin wrapper instantiation till its needed
  private AdminClientWrapper adminClientWrapper;

  public BigtableClassicApi(BigtableHBaseClassicSettings settings) throws IOException {
    super(settings);
    this.bigtableSession = new BigtableSession(settings.getBigtableOptions());
    this.dataClientWrapper = new DataClientClassicApi(bigtableSession);
  }

  @Override
  public AdminClientWrapper getAdminClientWrapper() throws IOException {
    if (adminClientWrapper == null) {
      adminClientWrapper = new AdminClientClassicApi(bigtableSession.getTableAdminClientWrapper());
    }
    return adminClientWrapper;
  }

  @Override
  public DataClientWrapper getDataClientWrapper() {
    return dataClientWrapper;
  }
}
