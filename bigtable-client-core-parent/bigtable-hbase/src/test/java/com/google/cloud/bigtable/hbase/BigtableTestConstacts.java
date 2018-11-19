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

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;

/**
 * This class contains common constants to be used between multiple test classes.
 *
 */
public class BigtableTestConstacts {

  public static final String PROJECT_ID = "test-project-id";
  public static final String INSTANCE_ID = "test-instance-id";
  public static final String TABLE_ID = "test-table-id";
  public static final String APP_PROFILE_ID = "test-app-profile-id";
  public static final RequestContext REQUEST_CONTEXT = RequestContext.create(
      InstanceName.of(PROJECT_ID, INSTANCE_ID),
      APP_PROFILE_ID
  );

  private BigtableTestConstacts() {
  }

}
