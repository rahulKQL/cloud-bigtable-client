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

import com.google.api.core.InternalApi;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import java.io.IOException;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * Adapt a Bigtable ResultScanner to an HBase Result Scanner.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableResultScannerAdapter {

  /**
   * adapt.
   *
   * @param bigtableResultScanner a {@link com.google.cloud.bigtable.grpc.scanner.ResultScanner}
   *     object.
   * @param span A parent {@link Span} for the scan that needs to be closed when the scanning is
   *     complete. The span has an HBase specific tag, which needs to be handled by the adapter.
   * @return a {@link org.apache.hadoop.hbase.client.ResultScanner} object.
   */
  public ResultScanner adapt(
      final com.google.cloud.bigtable.grpc.scanner.ResultScanner<Result> bigtableResultScanner,
      final Span span) {
    return new AbstractClientScanner() {
      int rowCount = 0;

      @Override
      public Result next() throws IOException {
        Result row = bigtableResultScanner.next();
        if (row == null) {
          // Null signals EOF.
          span.end();
          return null;
        }
        rowCount++;
        return row;
      }

      @Override
      public void close() {
        try {
          bigtableResultScanner.close();
        } catch (IOException ioe) {
          span.setStatus(Status.UNKNOWN.withDescription(ioe.getMessage()));
          throw new RuntimeException(ioe);
        } finally {
          span.end();
        }
      }

      /**
       * This is an HBase concept that was added in HBase 1.0.2. It's not relevant for Cloud
       * Bigtable. It will not be called from the HBase code and should not be called by the user.
       */
      // Developers Note: Do not add @Override so that this can remain backwards compatible with
      // 1.0.1.
      public boolean renewLease() {
        throw new UnsupportedOperationException("renewLease");
      }
    };
  }
}
