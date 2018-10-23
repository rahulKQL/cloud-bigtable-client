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
package com.google.cloud.bigtable.hbase.adapters.read;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static com.google.cloud.bigtable.hbase.BigtableTestConstacts.REQUEST_CONTEXT;
import static com.google.cloud.bigtable.hbase.BigtableTestConstacts.TABLE_ID;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.hbase.BigtableTestConstacts;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.GetAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ScanAdapter;
import com.google.common.base.Function;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for the {@link GetAdapter}
 */
@RunWith(JUnit4.class)
public class TestGetAdapter {

  private GetAdapter getAdapter =
      new GetAdapter(new ScanAdapter(FilterAdapter.buildAdapter(), new RowRangeAdapter()));
  private DataGenerationHelper dataHelper = new DataGenerationHelper();
  private ReadHooks<Query, Query> throwingReadHooks = new ReadHooks<Query, Query>() {
    @Override
    public void composePreSendHook(Function<Query, Query> newHook) {
      throw new IllegalStateException("Read hooks not supported in tests.");
    }

    @Override
    public Query applyPreSendHook(Query readRowsRequest) {
      throw new IllegalStateException("Read hooks not supported in tests.");
    }
  };

  private Get makeValidGet(byte[] rowKey) throws IOException {
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    return get;
  }

  @Test
  public void rowKeyIsSetInRequest() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    Query query = Query.create(TABLE_ID);
    getAdapter.adapt(get, throwingReadHooks, query);
    
    ByteString adaptedRowKey = query.toProto(REQUEST_CONTEXT).getRows().getRowKeys(0);
    Assert.assertEquals(
        new String(get.getRow(), StandardCharsets.UTF_8),
        adaptedRowKey.toStringUtf8());
  }

  @Test
  public void maxVersionsIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.setMaxVersions(10);
    Query query = Query.create(TABLE_ID);
    getAdapter.adapt(get, throwingReadHooks, query);
    Assert.assertEquals(
        FILTERS.limit().cellsPerColumn(10).toProto(),
        query.toProto(REQUEST_CONTEXT).getFilter());
  }

  @Test
  public void columnFamilyIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.addFamily(Bytes.toBytes("f1"));
    Query query = Query.create(TABLE_ID);
    getAdapter.adapt(get, throwingReadHooks, query);
    Assert.assertEquals(
        FILTERS.family().exactMatch("f1").toProto(),
        query.toProto(REQUEST_CONTEXT).getFilter());
  }

  @Test
  public void columnQualifierIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
    Query query = Query.create(TABLE_ID);
    getAdapter.adapt(get, throwingReadHooks, query);
    Assert.assertEquals(
        FILTERS.chain()
            .filter(FILTERS.family().regex("f1"))
            .filter(FILTERS.qualifier().regex("q1"))
            .toProto(),
        query.toProto(REQUEST_CONTEXT).getFilter());
  }

  @Test
  public void multipleQualifiersAreSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
    get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q2"));
    Query query = Query.create(TABLE_ID);
    getAdapter.adapt(get, throwingReadHooks, query);
    Assert.assertEquals(
        FILTERS.chain()
            .filter(FILTERS.family().regex("f1"))
            .filter(FILTERS.interleave()
                .filter(FILTERS.qualifier().regex("q1"))
                .filter(FILTERS.qualifier().regex("q2")))
            .toProto(),
        query.toProto(REQUEST_CONTEXT).getFilter());
  }
}
