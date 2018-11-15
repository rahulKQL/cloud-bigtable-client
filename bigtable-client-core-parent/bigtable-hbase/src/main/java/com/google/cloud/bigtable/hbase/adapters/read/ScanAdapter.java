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

<<<<<<< HEAD
import com.google.common.collect.Range;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
=======
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
>>>>>>> adding RangeSet method ScanAdapter
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.ChainFilter;
import com.google.cloud.bigtable.data.v2.models.Filters.InterleaveFilter;
import com.google.cloud.bigtable.data.v2.models.Filters.TimestampRangeFilter;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.cloud.bigtable.hbase.BigtableExtendedScan;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapterContext;
import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.base.Optional;
import com.google.common.collect.RangeSet;
<<<<<<< HEAD
=======
import com.google.common.collect.TreeRangeSet;
>>>>>>> adding RangeSet method ScanAdapter
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

/**
 * An adapter for Scan operation that makes use of the proto filter language.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ScanAdapter implements ReadOperationAdapter<Scan> {

  private static final int UNSET_MAX_RESULTS_PER_COLUMN_FAMILY = -1;
  private static final boolean OPEN_CLOSED_AVAILABLE = isOpenClosedAvailable();
  private static final boolean LIMIT_AVAILABLE = isLimitAvailable();

  /**
   * HBase supports include(Stop|Start)Row only at 1.4.0+, so check to make sure that the HBase
   * runtime dependency supports this feature.  Specifically, Beam uses HBase 1.2.0.
   */
  private static boolean isOpenClosedAvailable() {
    try {
      new Scan().includeStopRow();
      return true;
    } catch(NoSuchMethodError e) {
      return false;
    }
  }

  private static boolean isLimitAvailable() {
    try {
      new Scan().setLimit(1);
      return true;
    } catch(NoSuchMethodError e) {
      return false;
    }
  }

  private final FilterAdapter filterAdapter;
  private final RowRangeAdapter rowRangeAdapter;

  /**
   * <p>Constructor for ScanAdapter.</p>
   *
   * @param filterAdapter a {@link com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter} object.
   */
  public ScanAdapter(FilterAdapter filterAdapter, RowRangeAdapter rowRangeAdapter) {
    this.filterAdapter = filterAdapter;
    this.rowRangeAdapter = rowRangeAdapter;
  }

  /**
   * <p>throwIfUnsupportedScan.</p>
   *
   * @param scan a {@link org.apache.hadoop.hbase.client.Scan} object.
   */
  public void throwIfUnsupportedScan(Scan scan) {
    if (scan.getFilter() != null) {
      filterAdapter.throwIfUnsupportedFilter(scan, scan.getFilter());
    }

    if (scan.getMaxResultsPerColumnFamily() != UNSET_MAX_RESULTS_PER_COLUMN_FAMILY) {
      throw new UnsupportedOperationException(
          "Limiting of max results per column family is not supported.");
    }
  }

  /**
   * Given a Scan, build a RowFilter that include matching columns
   *
   * @param scan a {@link org.apache.hadoop.hbase.client.Scan} object.
   * @param hooks a {@link com.google.cloud.bigtable.hbase.adapters.read.ReadHooks} object.
   * @return a {@link com.google.bigtable.v2.RowFilter} object.
   */
  public Filters.Filter buildFilter(Scan scan, ReadHooks hooks) {
    ChainFilter chain = FILTERS.chain();
    Optional<Filters.Filter> familyFilter = createColumnFamilyFilter(scan);
    if (familyFilter.isPresent()) {
      chain.filter(familyFilter.get());
    }

    if (scan.getTimeRange() != null && !scan.getTimeRange().isAllTime()) {
      chain.filter(createTimeRangeFilter(scan.getTimeRange()));
    }

    if (scan.getMaxVersions() != Integer.MAX_VALUE) {
      chain.filter(createColumnLimitFilter(scan.getMaxVersions()));
    }

    Optional<Filters.Filter> userFilter = createUserFilter(scan, hooks);
    if (userFilter.isPresent()) {
      chain.filter(userFilter.get());
    }

    return chain;
  }

  /** {@inheritDoc} */
  @Override
  public ReadRowsRequest.Builder adapt(Scan scan, ReadHooks readHooks) {
    throwIfUnsupportedScan(scan);

<<<<<<< HEAD
<<<<<<< HEAD
    ReadRowsRequest.Builder requestBuilder = ReadRowsRequest.newBuilder()
        .setRows(toRowSet(scan))
        .setFilter(buildFilter(scan, readHooks).toProto());
=======
    rowSetToByteRange(toRowSet(scan), query);

=======
    toByteStringRange(scan, query);
>>>>>>> adding RangeSet method ScanAdapter
    query.filter(buildFilter(scan, readHooks));
>>>>>>> fix whitespace & comments  also fixed testPageFilter

    if (LIMIT_AVAILABLE && scan.getLimit() > 0) {
      requestBuilder.setRowsLimit(scan.getLimit());
    }

    return requestBuilder;
  }

  private void toByteStringRange(Scan scan, Query query) {
    RangeSet<RowKeyWrapper> rangeSet = getRangeSet(scan);

<<<<<<< HEAD
<<<<<<< HEAD
=======
  /**
   * To covert {@link RowSet} into {@link ByteStringRange} which is accepted by Query.range().
   *
   * @param rowSet
   * @param query
   */
  private void rowSetToByteRange(RowSet rowSet, Query query) {
    for(RowRange rowRange : rowSet.getRowRangesList()) {
      ByteStringRange range  = ByteStringRange.unbounded();

      switch(rowRange.getStartKeyCase()) {
        case START_KEY_OPEN:
          range.startOpen(rowRange.getStartKeyOpen());
          break;
        case START_KEY_CLOSED:
          range.startClosed(rowRange.getStartKeyClosed());
          break;
        case STARTKEY_NOT_SET:
          range.startClosed(ByteString.EMPTY);
          break;
        default:
          throw new IllegalArgumentException("Unexpected start key case: " +
              rowRange.getStartKeyCase());
      }
=======
    for (Range<RowKeyWrapper> guavaRange : rangeSet.asRanges()) {
      if (guavaRange.hasLowerBound() && guavaRange.lowerBoundType() == BoundType.CLOSED
          && guavaRange.hasUpperBound() && guavaRange.upperBoundType() == BoundType.CLOSED
          && guavaRange.lowerEndpoint().equals(guavaRange.upperEndpoint())) {
>>>>>>> adding RangeSet method ScanAdapter

        query.rowKey(guavaRange.lowerEndpoint().getKey());
      } else {
        ByteStringRange byteRange = ByteStringRange.unbounded();

        if (guavaRange.hasLowerBound()) {
          switch (guavaRange.lowerBoundType()) {
          case CLOSED:
            byteRange.startClosed(guavaRange.lowerEndpoint().getKey());
            break;
          case OPEN:
            byteRange.startOpen(guavaRange.lowerEndpoint().getKey());
            break;
          default:
            throw new IllegalArgumentException(
                "Unexpected lower bound type: " + guavaRange.lowerBoundType());
          }
        }

        // handle end key
        if (guavaRange.hasUpperBound()) {
          switch (guavaRange.upperBoundType()) {
          case CLOSED:
            byteRange.endClosed(guavaRange.upperEndpoint().getKey());
            break;
          case OPEN:
            byteRange.endOpen(guavaRange.upperEndpoint().getKey());
            break;
          default:
            throw new IllegalArgumentException(
                "Unexpected upper bound type: " + guavaRange.upperBoundType());
          }
        }
        query.range(byteRange);
      }
    }
  }

<<<<<<< HEAD
>>>>>>> fix whitespace & comments  also fixed testPageFilter
  private RowSet getRowSet(Scan scan) {
=======
  private RangeSet<RowKeyWrapper> getRangeSet(Scan scan) {
>>>>>>> adding RangeSet method ScanAdapter
    if (scan instanceof BigtableExtendedScan) {
      return rowRangeAdapter.rowSetToRangeSet(((BigtableExtendedScan) scan).getRowSet());
    } else {
      RangeSet<RowKeyWrapper> rangeSet = TreeRangeSet.create();
      if (scan.isGetScan()) {
        ByteString startRow = ByteString.copyFrom(scan.getStartRow());
        rangeSet.add(Range.singleton(new RowKeyWrapper(startRow)));
      } else {
        rangeSet.add(getRange(scan));
      }

      return narrowRangeSet(rangeSet, scan.getFilter());
    }
  }

  private Range<RowKeyWrapper> getRange(Scan scan){
    final ByteString startRow = ByteString.copyFrom(scan.getStartRow());
    final ByteString stopRow = ByteString.copyFrom(scan.getStopRow());

    final boolean startUnbounded = startRow.isEmpty();
    final boolean endUnbounded = stopRow.isEmpty();

    final BoundType startBound = (!OPEN_CLOSED_AVAILABLE || scan.includeStartRow()) ? BoundType.CLOSED: BoundType.OPEN;
    final BoundType endBound = (!OPEN_CLOSED_AVAILABLE || !scan.includeStopRow()) ? BoundType.OPEN: BoundType.CLOSED;

    if (startUnbounded && endUnbounded) {
      return Range.all();
    } else if (startUnbounded) {
      return Range.upTo(new RowKeyWrapper(stopRow), endBound);
    } else if (endUnbounded) {
      return Range.downTo(new RowKeyWrapper(startRow), startBound);
    } else {
      return Range.range(new RowKeyWrapper(startRow), startBound,
          new RowKeyWrapper(stopRow), endBound);
    }
  }

  private static ByteString quoteRegex(byte[] unquoted)  {
    try {
      return ReaderExpressionHelper.quoteRegularExpression(unquoted);
    } catch (IOException e) {
      throw new IllegalStateException(
          "IOException when writing to ByteArrayOutputStream", e);
    }
  }

  private Optional<Filters.Filter> createUserFilter(Scan scan, ReadHooks hooks) {
    if (scan.getFilter() == null) {
      return Optional.absent();
    }
    try {
      return filterAdapter
          .adaptFilter(new FilterAdapterContext(scan, hooks), scan.getFilter());
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to adapt filter", ioe);
    }
  }

<<<<<<< HEAD
<<<<<<< HEAD
  private RowSet narrowRowSet(RowSet rowSet, Filter filter) {
=======
  // TODO: find way to narrow ByteStringRange directly
  private RowSet narrowRowSet(RowSet rowSet, org.apache.hadoop.hbase.filter.Filter filter) {
>>>>>>> fix whitespace & comments  also fixed testPageFilter
=======
  private RangeSet<RowKeyWrapper> narrowRangeSet(RangeSet<RowKeyWrapper> rangeSet, Filter filter) {
>>>>>>> adding RangeSet method ScanAdapter
    if (filter == null) {
      return rangeSet;
    }
    RangeSet<RowKeyWrapper> filterRangeSet = filterAdapter.getIndexScanHint(filter);
    if (filterRangeSet.encloses(Range.<RowKeyWrapper>all())) {
      return rangeSet;
    }
    rangeSet.removeAll(filterRangeSet.complement());
    return rangeSet;
  }

  private Filters.Filter createColumnQualifierFilter(byte[] unquotedQualifier) {
    return FILTERS.qualifier().regex(quoteRegex(unquotedQualifier));
  }

  private Filters.Filter createFamilyFilter(byte[] familyName) {
    return FILTERS.family().exactMatch(new String(familyName));
  }

  private Filters.Filter createColumnLimitFilter(int maxVersionsPerColumn) {
    return FILTERS.limit().cellsPerColumn(maxVersionsPerColumn);
  }

  private Filters.Filter createTimeRangeFilter(TimeRange timeRange) {
    TimestampRangeFilter rangeBuilder = FILTERS.timestamp().range();

    rangeBuilder.startClosed(convertUnits(timeRange.getMin()));

    if (timeRange.getMax() != Long.MAX_VALUE) {
      rangeBuilder.endOpen(convertUnits(timeRange.getMax()));
    }

    return rangeBuilder;
  }

  private long convertUnits(long hbaseUnits) {
    return BigtableConstants.BIGTABLE_TIMEUNIT.convert(
        hbaseUnits, BigtableConstants.HBASE_TIMEUNIT);
  }

  private Optional<Filters.Filter> createColumnFamilyFilter(Scan scan) {
    if (!scan.hasFamilies()) {
      return Optional.absent();
    }
    // Build a filter of the form:
    // (fam1 | (qual1 + qual2 + qual3)) + (fam2 | qual1) + (fam3)
    InterleaveFilter interleave = FILTERS.interleave();
    Map<byte[],NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
    for (Map.Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
      Filters.Filter familyFilter = createFamilyFilter(entry.getKey());

      NavigableSet<byte[]> qualifiers = entry.getValue();
      // Add a qualifier filter for each specified qualifier:
      if (qualifiers != null) {
        InterleaveFilter columnFilters = FILTERS.interleave();
        for (byte[] qualifier : qualifiers) {
          columnFilters.filter(createColumnQualifierFilter(qualifier));
        }
        // Build filter of the form "family | (qual1 + qual2 + qual3)"
        interleave.filter(FILTERS.chain()
            .filter(familyFilter)
            .filter(columnFilters));
      } else {
        interleave.filter(familyFilter);
      }
    }
    return Optional.<Filters.Filter> of(interleave);
  }

}