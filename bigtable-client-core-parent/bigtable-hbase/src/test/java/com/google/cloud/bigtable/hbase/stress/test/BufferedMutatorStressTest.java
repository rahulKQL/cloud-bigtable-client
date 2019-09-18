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

import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.TableName;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BufferedMutatorStressTest {

  private static final String projectId = System.getProperty("bigtable.project");
  private static final String instanceId = System.getProperty("bigtable.instance");
  private static final Boolean useGCJClient = Boolean.getBoolean("bigtable.useGCJClient");

  private static final Logger LOG = Logger.getLogger(BufferedMutatorStressTest.class.getName());
  private static final ExecutorService executor = Executors.newFixedThreadPool(4);
  private static final String COL_FAMILY = "test-family";

  private BigtableSession session;
  private IBigtableTableAdminClient adminClient;
  private TableName tableName;

  @BeforeClass
  public static void setUpEnv() {
    Assume.assumeFalse(isNullOrEmpty(projectId));
    Assume.assumeFalse(isNullOrEmpty(instanceId));
  }

  @Before
  public void setUp() throws IOException {
    BigtableOptions options =
        BigtableOptions.builder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .setUserAgent("testGCJBM")
            .setUseGCJClient(useGCJClient)
            .build();

    session = new BigtableSession(options);
    adminClient = session.getTableAdminClientWrapper();
    tableName = TableName.valueOf("test-table-" + RandomStringUtils.randomAlphanumeric(8));
    adminClient.createTable(
        CreateTableRequest.of(tableName.getNameAsString()).addFamily(COL_FAMILY));
  }

  @Test
  public void stressTestOfBM() throws InterruptedException, ExecutionException {
    System.out.println("stressTestOfBM <####> Running");
    List<Future<Long>> results =
        executor.invokeAll(
            ImmutableList.of(
                new WriteToBigtable(session, tableName),
                new WriteToBigtable(session, tableName),
                new WriteToBigtable(session, tableName),
                new WriteToBigtable(session, tableName)));

    long totalTimeTaken = 0L;
    for (Future<Long> f : results) {
      totalTimeTaken += f.get();
    }

    LOG.info("Total Time taken is = " + totalTimeTaken);
  }

  static class WriteToBigtable implements Callable<Long> {
    private final BigtableSession session;
    private final TableName tableName;

    WriteToBigtable(BigtableSession session, TableName tableName) {
      this.session = session;
      this.tableName = tableName;
    }

    @Override
    public Long call() throws Exception {
      BigtableTableName bigtableTableName =
          new BigtableTableName(
              NameUtil.formatTableName(projectId, instanceId, tableName.getNameAsString()));

      long startTime = System.currentTimeMillis();
      LOG.info("Start Time: " + startTime);
      try (IBulkMutation bulkMutation = session.createBulkMutationWrapper(bigtableTableName)) {

        for (int i = 0; i < 2; i++) {
          final String rowKeyPrefix = "test-rowKey" + RandomStringUtils.randomAlphanumeric(10);
          final String qualifier = "testQualifier-" + RandomStringUtils.randomAlphanumeric(5);
          final String value = "testValue-" + RandomStringUtils.randomAlphanumeric(10);

          for (int j = 0; j < 100_000; j++) {
            bulkMutation.add(
                RowMutationEntry.create(rowKeyPrefix + "-" + i)
                    .setCell(COL_FAMILY, qualifier + "-" + i, value));
          }

          bulkMutation.flush();
        }
      }
      long endTime = System.currentTimeMillis();
      LOG.info("End Time: " + endTime);

      return endTime - startTime;
    }
  }

  @After
  public void tearDown() throws IOException {
    adminClient.deleteTable(tableName.getNameAsString());
    session.close();
  }
}
