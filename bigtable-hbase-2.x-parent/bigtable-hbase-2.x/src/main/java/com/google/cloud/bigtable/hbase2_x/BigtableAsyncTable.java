/*
d * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase2_x;

import static java.util.stream.Collectors.toList;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import io.opencensus.common.Scope;
import io.opencensus.trace.Status;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.cloud.bigtable.hbase.adapters.read.GetAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScanResultConsumer;
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.hbase.AbstractBigtableTable;
import com.google.cloud.bigtable.hbase.BatchExecutor;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.CheckAndMutateUtil;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.base.Preconditions;

import io.grpc.stub.StreamObserver;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import org.apache.hadoop.hbase.io.TimeRange;

/**
 * Bigtable implementation of {@link AsyncTable}.
 *
 * @author spollapally
 */
public class BigtableAsyncTable implements AsyncTable<ScanResultConsumer> {

  private static final Logger LOG = new Logger(AbstractBigtableTable.class);
  private static final Tracer TRACER = Tracing.getTracer();

  private static <T, R> List<R> map(List<T> list, Function<T, R> f) {
    return list.stream().map(f).collect(toList());
  }

  private final BigtableAsyncConnection asyncConnection;
  private final BigtableDataClient client;
  private final HBaseRequestAdapter hbaseAdapter;
  private final TableName tableName;
  private BatchExecutor batchExecutor;
  // Once the IBigtableDataClient interface is implemented this will be removed
  private RequestContext requestContext;

  public BigtableAsyncTable(BigtableAsyncConnection asyncConnection,
      HBaseRequestAdapter hbaseAdapter) {
    this.asyncConnection = asyncConnection;
    BigtableSession session = asyncConnection.getSession();
    this.client = new BigtableDataClient(session.getDataClient());
    this.hbaseAdapter = hbaseAdapter;
    this.tableName = hbaseAdapter.getTableName();
    // Once the IBigtableDataClient interface is implemented this will be removed
    BigtableOptions options = asyncConnection.getOptions();
    this.requestContext = RequestContext
        .create(options.getProjectId(), options.getInstanceId(), options.getAppProfileId());
  }

  protected synchronized BatchExecutor getBatchExecutor() {
    if (batchExecutor == null) {
      batchExecutor = new BatchExecutor(asyncConnection.getSession(), hbaseAdapter);
    }
    return batchExecutor;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Result> append(Append append) {
    ReadModifyWriteRowRequest request = hbaseAdapter.adapt(append).toProto(requestContext);
    Function<? super ReadModifyWriteRowResponse, ? extends Result> adaptRowFunction = response ->
        append.isReturnResults()
            ? Adapters.ROW_ADAPTER.adaptResponse(response.getRow())
            : null;
    return client.readModifyWriteRowAsync(request).thenApply(adaptRowFunction);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    // TODO: The CompletableFutures need to return Void for Put/Delete.
    return map(asyncRequests(actions), f -> (CompletableFuture<T>) f);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    return new CheckAndMutateBuilderImpl(client, hbaseAdapter, row, family);
  }

  final static class CheckAndMutateBuilderImpl implements CheckAndMutateBuilder {

    private final CheckAndMutateUtil.RequestBuilder builder;
    private final BigtableDataClient client;
    // Once the IBigtableDataClient interface is implemented this will be removed
    protected final RequestContext requestContext;

    public CheckAndMutateBuilderImpl(BigtableDataClient client, HBaseRequestAdapter hbaseAdapter,
        byte[] row, byte[] family) {
      this.client = client;
      this.builder = new CheckAndMutateUtil.RequestBuilder(hbaseAdapter, row, family);
      BigtableTableName bigtableTableName = hbaseAdapter.getBigtableTableName();
      // Once the IBigtableDataClient interface is implemented this will be removed
      this.requestContext = RequestContext
          .create(bigtableTableName.getProjectId(), bigtableTableName.getInstanceId(), "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CheckAndMutateBuilder qualifier(byte[] qualifier) {
      builder.qualifier(qualifier);
      return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CheckAndMutateBuilder ifNotExists() {
      builder.ifNotExists();
      return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CheckAndMutateBuilder ifMatches(CompareOperator compareOp, byte[] value) {
      Preconditions.checkNotNull(compareOp, "compareOp is null");
      if (compareOp != CompareOperator.NOT_EQUAL) {
        Preconditions.checkNotNull(value, "value is null for compareOperator: " + compareOp);
      }
      builder.ifMatches(BigtableTable.toCompareOp(compareOp), value);
      return this;
    }

    /**
     * {@inheritDoc}
     */
    public CheckAndMutateBuilder timeRange(TimeRange timeRange) {
      builder.timeRange(timeRange.getMin(), timeRange.getMax());
      return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Boolean> thenPut(Put put) {
      try {
        builder.withPut(put);
        return call();
      } catch (Exception e) {
        return FutureUtils.failedFuture(e);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Boolean> thenDelete(Delete delete) {
      try {
        builder.withDelete(delete);
        return call();
      } catch (Exception e) {
        return FutureUtils.failedFuture(e);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Boolean> thenMutate(RowMutations mutation) {
      try {
        builder.withMutations(mutation);
        return call();
      } catch (Exception e) {
        return FutureUtils.failedFuture(e);
      }
    }

    private CompletableFuture<Boolean> call()
        throws IOException {
      CheckAndMutateRowRequest request = builder.build().toProto(requestContext);
      return client.checkAndMutateRowAsync(request).thenApply(
        response -> CheckAndMutateUtil.wasMutationApplied(request, response));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    // figure out how to time this with Opencensus
    return client.mutateRowAsync(hbaseAdapter.adapt(delete).toProto(requestContext))
        .thenApply(r -> null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> deletes) {
    return map(asyncRequests(deletes), cf -> cf.thenApply(r -> null));
  }

  private <T> List<CompletableFuture<?>> asyncRequests(List<? extends Row> actions) {
    return map(getBatchExecutor().issueAsyncRowRequests(actions, new Object[actions.size()], null),
      FutureUtils::toCompletableFuture);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Result> get(Get get) {
    ReadRowsRequest request = hbaseAdapter.adapt(get).toProto(requestContext);
    return client.readFlatRowsAsync(request).thenApply(BigtableAsyncTable::toResult);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Boolean> exists(Get get) {
    return get(GetAdapter.setCheckExistenceOnly(get)).thenApply(r -> !r.isEmpty());
  }

  private static Result toResult(List<FlatRow> list) {
    return Adapters.FLAT_ROW_ADAPTER.adaptResponse(getSingleResult(list));
  }

  private static FlatRow getSingleResult(List<FlatRow> list) {
    switch (list.size()) {
    case 0:
      return null;
    case 1:
      return list.get(0);
    default:
      throw new IllegalStateException("Multiple responses found for Get");
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<CompletableFuture<Result>> get(List<Get> gets) {
    return map(asyncRequests(gets), (f -> (CompletableFuture<Result>) f));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<CompletableFuture<Boolean>> exists(List<Get> gets) {
    List<Get> existGets = map(gets, GetAdapter::setCheckExistenceOnly);
    return map(get(existGets), cf -> cf.thenApply(r -> !r.isEmpty()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Configuration getConfiguration() {
    return this.asyncConnection.getConfiguration(); // TODO
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TableName getName() {
    return this.tableName;
  }

  @Override
  public long getOperationTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException("getOperationTimeout"); // TODO
  }

  @Override
  public long getReadRpcTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getReadRpcTimeout"); // TODO
  }

  @Override
  public long getRpcTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getRpcTimeout"); // TODO
  }

  @Override
  public long getScanTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getScanTimeout"); // TODO
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getWriteRpcTimeout"); // TODO
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    return client.readModifyWriteRowAsync(hbaseAdapter.adapt(increment).toProto(requestContext))
        .thenApply(response -> Adapters.ROW_ADAPTER.adaptResponse(response.getRow()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> mutateRow(RowMutations rowMutations) {
    return client.mutateRowAsync(hbaseAdapter.adapt(rowMutations).toProto(requestContext))
        .thenApply(r -> null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<Void> put(Put put) {
    // figure out how to time this with Opencensus
    return client.mutateRowAsync(hbaseAdapter.adapt(put).toProto(requestContext))
        .thenApply(r -> null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<CompletableFuture<Void>> put(List<Put> puts) {
    return map(asyncRequests(puts), f -> f.thenApply(r -> null));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<List<Result>> scanAll(Scan scan) {
    if (AbstractBigtableTable.hasWhileMatchFilter(scan.getFilter())) {
      throw new UnsupportedOperationException(
          "scanAll with while match filter is not allowed");
    }
    return client.readFlatRowsAsync(hbaseAdapter.adapt(scan).toProto(requestContext))
         .thenApply(list -> map(list, Adapters.FLAT_ROW_ADAPTER::adaptResponse));
  }

  /** {@inheritDoc} */
  @Override
  public ResultScanner getScanner(Scan scan) {
    LOG.trace("getScanner(Scan)");
    final Span span = TRACER.spanBuilder("BigtableTable.scan").startSpan();
    try (Scope scope = TRACER.withSpan(span)) {
      com.google.cloud.bigtable.grpc.scanner.ResultScanner<FlatRow> scanner =
          client.getClient().readFlatRows(hbaseAdapter.adapt(scan).toProto(requestContext));
      if (AbstractBigtableTable.hasWhileMatchFilter(scan.getFilter())) {
        return Adapters.BIGTABLE_WHILE_MATCH_RESULT_RESULT_SCAN_ADAPTER.adapt(scanner, span);
      }
      return Adapters.BIGTABLE_RESULT_SCAN_ADAPTER.adapt(scanner, span);
    } catch (final Throwable throwable) {
      LOG.error("Encountered exception when executing getScanner.", throwable);
      span.setStatus(Status.UNKNOWN);
      // Close the span only when throw an exception and not on finally because if no exception
      // the span will be ended by the adapter.
      span.end();
      return new ResultScanner() {
        @Override
        public boolean renewLease() {
          return false;
        }
        @Override
        public Result next() throws IOException {
           throw throwable;
        }
        @Override
        public ScanMetrics getScanMetrics() {
          return null;
        }
        @Override
        public void close() {
        }
      };
    }
  }

  /**
   * {@inheritDoc}
   */
  public void scan(Scan scan, final ScanResultConsumer consumer) {
    if (AbstractBigtableTable.hasWhileMatchFilter(scan.getFilter())) {
      throw new UnsupportedOperationException(
          "scan with consumer and while match filter is not allowed");
    }
    ReadRowsRequest request = hbaseAdapter.adapt(scan).toProto(requestContext);
    client.getClient().readFlatRows(request, new StreamObserver<FlatRow>() {
      @Override
      public void onNext(FlatRow value) {
        consumer.onNext(Adapters.FLAT_ROW_ADAPTER.adaptResponse(value));
      }

      @Override
      public void onError(Throwable t) {
        consumer.onError(t);
      }

      @Override
      public void onCompleted() {
        consumer.onComplete();
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture coprocessorService(Function arg0, ServiceCaller arg1, byte[] arg2) {
    throw new UnsupportedOperationException("coprocessorService");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CoprocessorServiceBuilder coprocessorService(Function arg0, ServiceCaller arg1, CoprocessorCallback arg2) {
    throw new UnsupportedOperationException("coprocessorService");
  }
}
