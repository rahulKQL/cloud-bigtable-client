/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc.scanner;

import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.common.collect.ImmutableList;

import com.google.protobuf.ByteString;

import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestFlatRowAdapter {

  private final FlatRowAdapter adapter = new FlatRowAdapter();
  private RowAdapter.RowBuilder<FlatRow> rowBuilder;

  @Before
  public void setUp() {
    rowBuilder = adapter.createRowBuilder();
  }

  @Test
  public void testWithSingleCellRow() {
    ByteString value = ByteString.copyFromUtf8("my-value");
    rowBuilder.startRow(ByteString.copyFromUtf8("my-key"));
    rowBuilder.startCell(
        "my-family",
        ByteString.copyFromUtf8("my-qualifier"),
        100,
        ImmutableList.of("my-label"),
        value.size());
    rowBuilder.cellValue(value);
    rowBuilder.finishCell();

    FlatRow expected = FlatRow.newBuilder()
        .withRowKey(ByteString.copyFromUtf8("my-key"))
        .addCell("my-family",
            ByteString.copyFromUtf8("my-qualifier"),
            100,
            value,
            ImmutableList.of("my-label")).build();

    Assert.assertEquals(expected, rowBuilder.finishRow());
  }

  @Test
  public void testWithMultiCell() {
    FlatRow.Builder builder = FlatRow.newBuilder()
        .withRowKey(ByteString.copyFromUtf8("my-key"));

    rowBuilder.startRow(ByteString.copyFromUtf8("my-key"));

    for (int i = 0; i < 10; i++) {
      ByteString value = ByteString.copyFromUtf8("value-" + i);
      ByteString qualifier = ByteString.copyFromUtf8("qualifier-" + i);
      rowBuilder.startCell("family", qualifier, 1000, ImmutableList.of("my-label"), value.size());
      rowBuilder.cellValue(value);
      rowBuilder.finishCell();

      builder.addCell(
          new FlatRow.Cell("family", qualifier, 1000, value, ImmutableList.of("my-label")));
    }

    Assert.assertEquals(builder.build(), rowBuilder.finishRow());
  }

  @Test
  public void testWhenSplitCell() {
    ByteString part1 = ByteString.copyFromUtf8("part1");
    ByteString part2 = ByteString.copyFromUtf8("part2");

    rowBuilder.startRow(ByteString.copyFromUtf8("my-key"));
    rowBuilder.startCell(
        "family",
        ByteString.copyFromUtf8("qualifier"),
        1000,
        ImmutableList.of("my-label"),
        part1.size() + part2.size());
    rowBuilder.cellValue(part1);
    rowBuilder.cellValue(part2);
    rowBuilder.finishCell();

    FlatRow expected = FlatRow.newBuilder()
        .withRowKey(ByteString.copyFromUtf8("my-key"))
        .addCell("family",
            ByteString.copyFromUtf8("qualifier"),
            1000,
            ByteString.copyFromUtf8("part1part2"),
            ImmutableList.of("my-label")).build();

    Assert.assertEquals(expected, rowBuilder.finishRow());
  }

  @Test
  public void testWithMarkerRow() {
    FlatRow markerRow = rowBuilder.createScanMarkerRow(ByteString.copyFromUtf8("key"));
    Assert.assertTrue(adapter.isScanMarkerRow(markerRow));

    ByteString value = ByteString.copyFromUtf8("value");
    rowBuilder.startRow(ByteString.copyFromUtf8("key"));
    rowBuilder.startCell(
        "family", ByteString.EMPTY, 1000, ImmutableList.<String>of(), value.size());
    rowBuilder.cellValue(value);
    rowBuilder.finishCell();

    Assert.assertFalse(adapter.isScanMarkerRow(rowBuilder.finishRow()));
  }

  @Test
  public void testFamilyOrdering(){
    ByteString value = ByteString.copyFromUtf8("my-value");
    ByteString qualifier = ByteString.copyFromUtf8("qualifier");
    List<String> labels = ImmutableList.of("my-label");
    String[] familyNames = {"aa", "bb", "ew", "fd", "zz"};
    rowBuilder.startRow(ByteString.copyFromUtf8("firstKey"));
    rowBuilder.startCell("zz", qualifier, 72, labels,
        value.size());
    rowBuilder.cellValue(value);
    rowBuilder.finishCell();
    rowBuilder.startCell("bb", qualifier, 2309223, labels,
        value.size());
    rowBuilder.cellValue(value);
    rowBuilder.finishCell();
    rowBuilder.startCell("aa", qualifier, 873, labels,
        value.size());
    rowBuilder.cellValue(value);
    rowBuilder.finishCell();
    rowBuilder.startCell("fd", qualifier, 726, labels,
        value.size());
    rowBuilder.cellValue(value);
    rowBuilder.finishCell();
    rowBuilder.startCell("ew", qualifier, System.currentTimeMillis(), labels,
        value.size());
    rowBuilder.cellValue(value);
    rowBuilder.finishCell();

    FlatRow row = rowBuilder.finishRow();
    Assert.assertEquals("firstKey", row.getRowKey().toStringUtf8());
    List<FlatRow.Cell> cells  = row.getCells();
    for(int i =0; i<cells.size(); i++){
      Assert.assertEquals(familyNames[i], cells.get(i).getFamily());
    }
  }
}
