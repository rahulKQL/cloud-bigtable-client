package com.google.cloud.bigtable.hbase.temp;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.hbase.BigtableDataSettingsFactory;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_INSTANCE_ID;
import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_PROJECT_ID;

public class TestUserAgentClient {

  private static final String TEST_USER_AGENT = "sampleUserAgent";

  private BigtableDataSettings dataSettings;
  private BigtableTableAdminClient tableAdminClient;
  private int port;


  @Before
  public void setUp() throws IOException {
    BigtableOptions bigtableOptions =
        BigtableOptions.builder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setUserAgent(TEST_USER_AGENT)
            .setDataHost("127.0.0.1")
            .setPort(8086).build();
    dataSettings = BigtableDataSettingsFactory.fromBigtableOptions(bigtableOptions);

    BigtableTableAdminSettings.Builder tableAdminSettings = BigtableTableAdminSettings.newBuilder()
        .setInstanceName(com.google.bigtable.admin.v2.InstanceName.of(TEST_PROJECT_ID, TEST_INSTANCE_ID));
    tableAdminSettings.stubSettings()
        .setCredentialsProvider(dataSettings.getCredentialsProvider())
        .setTransportChannelProvider(dataSettings.getTransportChannelProvider());

    tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings.build());

    // Create a test table that can be used in tests
    tableAdminClient.createTable(
        CreateTableRequest.of("fake-table")
            .addFamily("cf")
    );

  }

  @Test
  public void testAdminMethod() {
    Assert.assertFalse(tableAdminClient.listTables().isEmpty());
  }
}
