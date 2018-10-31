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

  @Before
  public void setup() {
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
  public void testWithOnlyProjectIdAndInstanceId() throws IOException, GeneralSecurityException {
    BigtableOptions options = BigtableOptions.builder()
        .setProjectId(TEST_PROJECT_ID).setInstanceId(TEST_INSTANCE_ID)
        .setUserAgent(TEST_USER_AGENT).build();
    BigtableDataSettings settings = BigtableDataSettingsFactory.fromBigtableOptions(options);
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
