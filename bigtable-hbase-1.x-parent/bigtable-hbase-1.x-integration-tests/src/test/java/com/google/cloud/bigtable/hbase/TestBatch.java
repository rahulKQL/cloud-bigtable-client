/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class TestBatch extends AbstractTestBatch {

  @ClassRule
  public static Timeout timeoutRule = new Timeout(8, TimeUnit.MINUTES);

  @ClassRule
  public static SharedTestEnvRule sharedTestEnvRule = SharedTestEnvRule.getInstance();

  @Test
  public void testBatchDoesntHang() throws Exception {
    Table table;
    try(Connection closedConnection = createNewConnection()) {
      table = closedConnection.getTable(sharedTestEnv.getDefaultTableName());
    }

    try {
      table.batch(Arrays.asList(new Get(Bytes.toBytes("key"))), new Object[1]);
      Assert.fail("Expected an exception");
    } catch(Exception e) {
      Assert.assertNotNull(e);
    }

  }

  protected void appendAdd(Append append, byte[] columnFamily, byte[] qualifier, byte[] value) {
    append.add(columnFamily, qualifier, value);
  }
}
