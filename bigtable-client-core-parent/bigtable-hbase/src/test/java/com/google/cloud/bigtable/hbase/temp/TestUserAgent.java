package com.google.cloud.bigtable.hbase.temp;

import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_INSTANCE_ID;
import static com.google.cloud.bigtable.hbase.TestBigtableOptionsFactory.TEST_PROJECT_ID;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase.BigtableDataSettingsFactory;
import io.grpc.Server;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

public class TestUserAgent {
  private static final String TEST_USER_AGENT = "sampleUserAgent";
  final static BigtableTableName TABLE_NAME =
      new BigtableInstanceName(TEST_PROJECT_ID, TEST_INSTANCE_ID).toTableName("fakeTable");
  private BigtableDataSettings dataSettings;
  private Server testServer;


  @Before
  public void setUp() throws IOException  {

    BigtableOptions bigtableOptions =
        BigtableOptions.builder()
            .setDataHost("localhost")
            .setAdminHost("localhost")
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .setUserAgent(TEST_USER_AGENT)
            .setDataHost("0.0.0.0")
            .setPort(8080).build();
    dataSettings = BigtableDataSettingsFactory.fromBigtableOptions(bigtableOptions);

  }


  @Test
  public void testAdminMethod() throws IOException {
    BigtableTableAdminSettings.Builder tableAdminSettings = BigtableTableAdminSettings.newBuilder()
        .setInstanceName(com.google.bigtable.admin.v2.InstanceName.of(TEST_PROJECT_ID, TEST_INSTANCE_ID));
    tableAdminSettings.stubSettings()
        .setCredentialsProvider(dataSettings.getCredentialsProvider())
        .setTransportChannelProvider(dataSettings.getTransportChannelProvider());
    BigtableTableAdminClient tableAdminClient =
        BigtableTableAdminClient.create(tableAdminSettings.build());
    // Create a test table that can be used in tests
    //    tableAdminClient.createTable(
    //        CreateTableRequest.of("fake-table")
    //            .addFamily("cf")
    //    );

    BigtableDataClient client  = BigtableDataClient.create(dataSettings);
    client.readRows(Query.create(TABLE_NAME.getTableId())).iterator().next();

  }
}
