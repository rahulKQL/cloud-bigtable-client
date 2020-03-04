/*
 * Copyright 2020 Google LLC.
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

import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.cloud.bigtable.hbase.util.TimestampConverter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

// TODO(WARNING): The implementation is not finalized. It needs to be updated accordingly.
public class ResultRowAdapter implements RowAdapter<Result> {

  @Override
  public RowBuilder<Result> createRowBuilder() {
    return new ResultRowBuilder();
  }

  @Override
  public boolean isScanMarkerRow(Result result) {
    return result.isEmpty();
  }

  @Override
  public ByteString getKey(Result result) {
    return ByteString.copyFrom(result.getRow());
  }

  public static class ResultRowBuilder implements RowBuilder<Result> {
    private ByteString currentKey;
    private String family;
    private ByteString qualifier;
    private List<String> labels;
    private long timestamp;
    private ByteString value;

    /*
     * cells contains list of {@link Cell} for all the families.
     */
    private Map<String, List<RowCell>> cells = new TreeMap<>();

    /*
     * currentFamilyCells is buffered with current family's {@link Cell}s.
     */
    private List<RowCell> currentFamilyCells = null;
    private String previousFamily;
    private int totalCellCount = 0;

    @Override
    public void startRow(ByteString byteString) {
      this.currentKey = byteString;
    }

    @Override
    public void startCell(
        String family, ByteString qualifier, long timestamp, List<String> labels, long size) {
      this.family = family;
      this.qualifier = qualifier;
      this.timestamp = timestamp;
      this.labels = labels;
      this.value = ByteString.EMPTY;
    }

    @Override
    public void cellValue(ByteString value) {
      this.value = this.value.concat(value);
    }

    @Override
    public void finishCell() {
      if (!Objects.equals(this.family, this.previousFamily)) {
        previousFamily = this.family;
        currentFamilyCells = new ArrayList<>();
        cells.put(this.family, this.currentFamilyCells);
      }

      RowCell rowCell =
          new RowCell(
              this.currentKey.toByteArray(),
              this.family.getBytes(),
              ByteStringer.extract(this.qualifier),
              // Bigtable timestamp has more granularity than HBase one. It is possible that
              // Bigtable
              // cells are deduped unintentionally here. On the other hand, if we don't dedup them,
              // HBase will treat them as duplicates.
              TimestampConverter.bigtable2hbase(this.timestamp),
              this.labels,
              ByteStringer.extract(this.value));
      this.currentFamilyCells.add(rowCell);
      totalCellCount++;
    }

    @Override
    public Result finishRow() {
      ImmutableList.Builder<RowCell> combined = ImmutableList.builder();
      for (List<RowCell> familyCellList : cells.values()) {
        RowCell previous = null;
        for (RowCell c : familyCellList) {
          if (previous == null || !c.getLabels().isEmpty() || !keysMatch(c, previous)) {
            combined.add(c);
          }
          previous = c;
        }
      }

      return Result.create(new ArrayList<Cell>(combined.build()));
    }

    @Override
    public void reset() {
      this.currentKey = null;
      this.family = null;
      this.qualifier = null;
      this.labels = null;
      this.timestamp = 0L;
      this.value = null;
      this.cells = new TreeMap<>();
      this.currentFamilyCells = null;
      this.previousFamily = null;
      this.totalCellCount = 0;
    }

    @Override
    public Result createScanMarkerRow(ByteString rowKey) {
      return new Result();
    }

    private boolean keysMatch(RowCell current, RowCell previous) {
      return current.getTimestamp() == previous.getTimestamp()
          && Arrays.equals(current.getQualifierArray(), previous.getQualifierArray())
          && Objects.equals(current.getLabels(), previous.getLabels());
    }
  }
}
