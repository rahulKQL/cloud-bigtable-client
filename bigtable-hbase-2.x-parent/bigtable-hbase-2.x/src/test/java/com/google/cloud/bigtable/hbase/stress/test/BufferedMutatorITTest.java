/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.stress.test;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.MetricFilter;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.DropwizardMetricRegistry;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BufferedMutatorITTest {

  private static final String projectId = System.getProperty("bigtable.project");
  private static final String instanceId = System.getProperty("bigtable.instance");
  private static final Boolean useGCJClient = Boolean.getBoolean("bigtable.useGCJClient");

  private static final Logger LOG = Logger.getLogger(BufferedMutatorITTest.class.getName());
  private static final ExecutorService executor = Executors.newFixedThreadPool(4);
  private static final byte[] COL_FAMILY = "test-family".getBytes();

  private Admin admin;
  private Connection connection;
  private TableName tableName;

  @BeforeClass
  public static void setUpEnv() {
    Assume.assumeFalse(isNullOrEmpty(projectId));
    Assume.assumeFalse(isNullOrEmpty(instanceId));

    DropwizardMetricRegistry registry = new DropwizardMetricRegistry();
    BigtableClientMetrics.setMetricRegistry(registry);

    MetricFilter nonZeroMatcher =
        (name, metric) -> {
          if (metric instanceof Counting) {
            Counting counter = (Counting) metric;
            return counter.getCount() > 0;
          }
          return true;
        };

    ConsoleReporter.forRegistry(registry.getRegistry())
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(nonZeroMatcher)
        .build()
        .start(50, TimeUnit.SECONDS);
  }

  @Before
  public void setUp() throws IOException {
    Configuration conf = BigtableConfiguration.configure(projectId, instanceId);
    conf.set(BigtableOptionsFactory.BIGTABLE_USE_BULK_API, "true");
    conf.set(BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, "true");
    conf.set(BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT, useGCJClient.toString());
    connection = BigtableConfiguration.connect(conf);

    admin = connection.getAdmin();
    tableName = TableName.valueOf("test-table-" + randomAlphanumeric(8));
    admin.createTable(
        TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COL_FAMILY).build())
            .build());
    LOG.info("Table created with tableName: " + tableName.getNameAsString());
    assertTrue(admin.tableExists(tableName));
  }

  @Test
  public void stressTestOfBM() throws InterruptedException, ExecutionException {
    final Timer timer =
        BigtableClientMetrics.timer(
            BigtableClientMetrics.MetricLevel.Info, "buffer.mutator.test.timer");

    List<Future<Long>> results =
        executor.invokeAll(
            ImmutableList.of(
                new WriteToBigtable(connection, tableName, timer),
                new WriteToBigtable(connection, tableName, timer),
                new WriteToBigtable(connection, tableName, timer),
                new WriteToBigtable(connection, tableName, timer)));

    long totalTimeTaken = 0L;
    for (Future<Long> f : results) {
      totalTimeTaken += f.get();
    }

    LOG.info(String.format("Total Time taken is = %s ms", totalTimeTaken));
  }

  static class WriteToBigtable implements Callable<Long> {
    private final Connection conn;
    private final TableName tableName;
    private final Timer timer;

    WriteToBigtable(Connection conn, TableName tableName, Timer timer) {
      this.conn = conn;
      this.tableName = tableName;
      this.timer = timer;
    }

    @Override
    public Long call() throws Exception {

      long startTime = System.currentTimeMillis();
      LOG.info(String.format("Start Time: %d ms", startTime));
      try (BufferedMutator bulkMutation = conn.getBufferedMutator(tableName)) {
        final Timer.Context context = timer.time();
        for (int i = 0; i < 10; i++) {
          final String rowKeyPrefix = "test-rowKey" + randomAlphanumeric(10);
          final String qualifier = "testQualifier-" + randomAlphanumeric(5);
          final byte[] value = Bytes.toBytes("testValue-" + randomAlphanumeric(10));

          ImmutableList.Builder<Put> builder = ImmutableList.builder();
          for (int j = 0; j < 100_000; j++) {
            builder.add(
                new Put(Bytes.toBytes(rowKeyPrefix + i))
                    .addColumn(COL_FAMILY, Bytes.toBytes(qualifier + "-" + i), 10_000L, value));
          }

          bulkMutation.mutate(builder.build());

          bulkMutation.flush();
          context.close();
        }
      }

      long endTime = System.currentTimeMillis();
      LOG.info(String.format("End Time: %d ms", endTime));

      return endTime - startTime;
    }
  }

  @After
  public void tearDown() throws IOException {
    admin.deleteTable(tableName);
    connection.close();
  }
}
