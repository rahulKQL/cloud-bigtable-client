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

import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_INSTANCE_ID;
import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_PROJECT_ID;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;

public class TestBigtableDataSettingsFactory {

  private static final String TEST_USER_AGENT = "sampleUserAgent";

  @Rule
  public ExpectedException expect = ExpectedException.none();

  private BigtableOptions bigtableOptions;

  @Before
  public void setup() {
    bigtableOptions = BigtableOptions.builder().setProjectId(TEST_PROJECT_ID)
        .setInstanceId(TEST_INSTANCE_ID).setUserAgent(TEST_USER_AGENT).build();
  }

  @Test(expected = NullPointerException.class)
  public void testProjectIdIsRequired() throws IOException, GeneralSecurityException {
    BigtableOptions options = BigtableOptions.builder().build();
    BigtableDataSettingsFactory.fromBigtableOptions(options);
  }

  @Test(expected = NullPointerException.class)
  public void testInstanceIdIsRequired() throws IOException, GeneralSecurityException {
    BigtableOptions options = BigtableOptions.builder().setProjectId(TEST_PROJECT_ID).build();
    BigtableDataSettingsFactory.fromBigtableOptions(options);
  }

  @Test
  public void testWithoutUserAgent() throws IOException, GeneralSecurityException {
    BigtableOptions options = BigtableOptions.builder().setProjectId(TEST_PROJECT_ID)
        .setInstanceId(TEST_INSTANCE_ID).build();
    BigtableDataSettings dataSettings = BigtableDataSettingsFactory.fromBigtableOptions(options);
    // TODO: Need to assert UserAgent to null & add more test cases
    Assert.assertEquals(TEST_PROJECT_ID, dataSettings.getInstanceName().getProject());
    Assert.assertEquals(TEST_INSTANCE_ID, dataSettings.getInstanceName().getInstance());
  }

  @Test
  public void testWithAllRequiredFields() throws IOException, GeneralSecurityException {
    BigtableDataSettings settings =
        BigtableDataSettingsFactory.fromBigtableOptions(bigtableOptions);
    Assert.assertNotNull(settings);
  }

  @Test
  public void testWithNullCredentials() throws IOException, GeneralSecurityException {
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID).setInstanceId(TEST_INSTANCE_ID)
            .setCredentialOptions(CredentialOptions.nullCredential())
            .setUserAgent(TEST_USER_AGENT).build();
    BigtableDataSettings settings = BigtableDataSettingsFactory.fromBigtableOptions(options);
    Assert.assertNotNull(settings.getCredentialsProvider());
  }
}
