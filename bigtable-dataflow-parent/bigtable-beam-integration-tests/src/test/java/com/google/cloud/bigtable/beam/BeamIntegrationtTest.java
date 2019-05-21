package com.google.cloud.bigtable.beam;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INSTANCE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.PROJECT_ID_KEY;
import static org.junit.Assert.assertEquals;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.netty.handler.ssl.OpenSsl;
import io.netty.internal.tcnative.SSL;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BeamIntegrationtTest {

  private final Logger LOG = new Logger(getClass());

  private static final String STAGING_LOCATION_KEY = "dataflowStagingLocation";
  private static final String ZONE_ID_KEY = "dataflowZoneId";

  private static final String projectId = System.getProperty(PROJECT_ID_KEY);
  private static final String instanceId = System.getProperty(INSTANCE_ID_KEY);
  private static final String stagingLocation = System.getProperty(STAGING_LOCATION_KEY);
  private static final String zoneId = System.getProperty(ZONE_ID_KEY);

  private static final String workerMachineType =
      System.getProperty("workerMachineType", "n1" + "-standard-8");
  private static final String dataEndpoint = System.getProperty(BIGTABLE_HOST_KEY,
      BigtableOptions.BIGTABLE_BATCH_DATA_HOST_DEFAULT);
  private static final String adminEndpoint =
      System.getProperty(BIGTABLE_ADMIN_HOST_KEY,
      BigtableOptions.BIGTABLE_ADMIN_HOST_DEFAULT);
  private static final String TABLE_NAME_STR =
      System.getProperty("tableName", "BeamCloudBigtableIOIntegrationTest");

  private static final TableName TABLE_NAME = TableName.valueOf(TABLE_NAME_STR);
  private static final byte[] FAMILY = Bytes.toBytes("test-family");

  private static final ByteString QUALIFIER = ByteString.copyFromUtf8("test-qualifier");
  private static final int CELL_SIZE = Integer.getInteger("cell_size", 1_000);
  private static final long TOTAL_ROW_COUNT = Integer.getInteger("total_row_count", 1_000_000);
  private static final int PREFIX_COUNT = Integer.getInteger("prefix_count", 1_000);


  private static final DoFn<String, KV<ByteString, Iterable<Mutation>>> WRITE_ONE_TENTH_PERCENT =
      new DoFn<String, KV<ByteString, Iterable<Mutation>>>() {

        private static final long serialVersionUID = 1L;

        private Counter rowCounter = Metrics.counter(BeamIntegrationtTest.class, "sent_puts");

        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
          String prefix = context.element() + "_";
          int max = (int) (TOTAL_ROW_COUNT / PREFIX_COUNT);
          for (int i = 0; i < max; i++) {
            Iterable<Mutation> mutations = ImmutableList.of(Mutation.newBuilder()
                .setSetCell(Mutation.SetCell.newBuilder()
                    .setFamilyName("test-family")
                    .setColumnQualifier(QUALIFIER)
                    .setValue(ByteString.copyFrom(createRandomValue())))
                .build());
            rowCounter.inc();
            context.output(KV.of(ByteString.copyFromUtf8(prefix + i), mutations));
          }
        }
      };



  @BeforeClass
  public static void setUpConfiguration() {
    Preconditions.checkArgument(stagingLocation != null, "Set -D" + STAGING_LOCATION_KEY + ".");
    Preconditions.checkArgument(zoneId != null, "Set -D" + ZONE_ID_KEY + ".");
    Preconditions.checkArgument(projectId != null, "Set -D" + PROJECT_ID_KEY + ".");
    Preconditions.checkArgument(instanceId != null, "Set -D" + INSTANCE_ID_KEY + ".");
  }

  @Before
  public void setUp() throws IOException {
    Configuration config = BigtableConfiguration.configure(projectId, instanceId);
    config.set(BIGTABLE_HOST_KEY, dataEndpoint);
    config.set(BIGTABLE_ADMIN_HOST_KEY, adminEndpoint);
    try (Connection conn = ConnectionFactory.createConnection(config);
        Admin admin = conn.getAdmin()) {
      if (admin.tableExists(TABLE_NAME)) {
                admin.deleteTable(TABLE_NAME);
      }
        admin.createTable(new HTableDescriptor(TABLE_NAME)
              .addFamily(new HColumnDescriptor(FAMILY)));
      LOG.info("Created a table to perform batching: %s", TABLE_NAME);
    }
  }

  private void testWriteFromBigtableIO(){
    PipelineOptions options = createOptions();
    options.setJobName("testWrite-100K-InBigtableIO-" + System.currentTimeMillis());
    LOG.info("Started writeUsingBigtableIO test with jobName as: %s", options.getJobName());

    BigtableIO.Write write = BigtableIO.write()
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withTableId(TABLE_NAME.getNameAsString());

    List<String> keys = new ArrayList<>();
    for (int i = 0; i < PREFIX_COUNT; i++) {
      keys.add(RandomStringUtils.randomAlphanumeric(10));
    }

    PipelineResult.State result = Pipeline.create(options)
        .apply("Keys", Create.of(keys))
        .apply("Create Puts", ParDo.of(WRITE_ONE_TENTH_PERCENT))
        .apply("Write to BT", write)
        .getPipeline()
        .run()
        .waitUntilFinish();

    assertEquals(PipelineResult.State.DONE, result);
  }


  private void testReadFromBigtableIO(){
    PipelineOptions pipelineOptions = createOptions();
    pipelineOptions.setJobName("testRead-100k-FromBigtableIO-" + System.currentTimeMillis());
    LOG.info("Started readFromBigtableIO test with jobName as: %s", pipelineOptions.getJobName());

    BigtableIO.Read read = BigtableIO.read()
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withTableId(TABLE_NAME.getNameAsString());

    Pipeline pipeline = Pipeline.create(pipelineOptions);
    PCollection<Long> count = pipeline
        .apply("Read from BT", read)
        .apply("Count", Count.<Row> globally());

    PAssert.thatSingleton(count).isEqualTo(TOTAL_ROW_COUNT);

    PipelineResult.State result = pipeline.run().waitUntilFinish();
    assertEquals(PipelineResult.State.DONE, result);
  }

  @Test
  public void testRunner() {
    try {
      // Submitted write pipeline to mutate the Bigtable.
      testWriteFromBigtableIO();

      testReadFromBigtableIO();
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new AssertionError("Exception occurred while pipeline execution");
    }
  }

  @Test
  public void testBigtableIO(){
    System.out.println("Test started");
    ClassLoader loader =  SSL.class.getClassLoader();
    System.out.println("successfully laded");
    System.out.println(loader.getResource("java.library.path"));
    System.out.println("--");
    Assert.assertTrue(OpenSsl.isAvailable());
  }

    private static byte[] createRandomValue(){
      byte[] bytes = new byte[CELL_SIZE];
      new Random().nextBytes(bytes);
      return bytes;
    }

    private DataflowPipelineOptions createOptions() {
      DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
      options.setProject(projectId);
      options.setZone(zoneId);
      options.setStagingLocation(stagingLocation + "/stage");
      options.setTempLocation(stagingLocation + "/temp");
      options.setRunner(DataflowRunner.class);
      options.setWorkerMachineType(workerMachineType);
      return options;
    }
}
