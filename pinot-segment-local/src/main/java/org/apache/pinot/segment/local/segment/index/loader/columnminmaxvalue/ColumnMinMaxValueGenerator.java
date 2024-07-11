/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
import org.apache.pinot.segment.local.segment.index.readers.BigDecimalDictionary;
import org.apache.pinot.segment.local.segment.index.readers.BytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.DoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.FloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.IntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.LongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.data.FieldSpec.DataType;


public class ColumnMinMaxValueGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnMinMaxValueGenerator.class);

  private final SegmentMetadata _segmentMetadata;
  private final SegmentDirectory.Writer _segmentWriter;
  private final ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode;

  // NOTE: _segmentProperties shouldn't be used when checking whether min/max value need to be generated because at that
  //       time _segmentMetadata might not be loaded from a local file
  private PropertiesConfiguration _segmentProperties;

  private boolean _minMaxValueAdded;

  public ColumnMinMaxValueGenerator(SegmentMetadata segmentMetadata, SegmentDirectory.Writer segmentWriter,
      ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode) {
    _segmentMetadata = segmentMetadata;
    _segmentWriter = segmentWriter;
    _columnMinMaxValueGeneratorMode = columnMinMaxValueGeneratorMode;
  }

  public boolean needAddColumnMinMaxValue() {
    for (String column : getColumnsToAddMinMaxValue()) {
      if (needAddColumnMinMaxValueForColumn(column)) {
        return true;
      }
    }
    return false;
  }

  public void addColumnMinMaxValue()
      throws Exception {
    Preconditions.checkState(_columnMinMaxValueGeneratorMode != ColumnMinMaxValueGeneratorMode.NONE);
    _segmentProperties = SegmentMetadataUtils.getPropertiesConfiguration(_segmentMetadata);
    for (String column : getColumnsToAddMinMaxValue()) {
      addColumnMinMaxValueForColumn(column);
    }
    if (_minMaxValueAdded) {
      SegmentMetadataUtils.savePropertiesConfiguration(_segmentProperties, _segmentMetadata.getIndexDir());
    }
  }

  private List<String> getColumnsToAddMinMaxValue() {
    Schema schema = _segmentMetadata.getSchema();
    List<String> columnsToAddMinMaxValue = new ArrayList<>();
    
    switch (_columnMinMaxValueGeneratorMode) {
      case ALL:
        columnsToAddMinMaxValue = addAllColumns(schema);
        break;
      case NON_METRIC:
        columnsToAddMinMaxValue = addNonMetricColumns(schema);
        break;
      case TIME:
        columnsToAddMinMaxValue = addTimeColumns(schema);
        break;
      default:
        throw new IllegalStateException("Unsupported generator mode: " + _columnMinMaxValueGeneratorMode);
    }
  
    return columnsToAddMinMaxValue;
  }
  
  private List<String> addAllColumns(Schema schema) {
    List<String> columnsToAddMinMaxValue = new ArrayList<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        columnsToAddMinMaxValue.add(fieldSpec.getName());
      }
    }
    return columnsToAddMinMaxValue;
  }
  
  private List<String> addNonMetricColumns(Schema schema) {
    List<String> columnsToAddMinMaxValue = new ArrayList<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC) {
        columnsToAddMinMaxValue.add(fieldSpec.getName());
      }
    }
    return columnsToAddMinMaxValue;
  }
  
  private List<String> addTimeColumns(Schema schema) {
    List<String> columnsToAddMinMaxValue = new ArrayList<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn() && (fieldSpec.getFieldType() == FieldSpec.FieldType.TIME
          || fieldSpec.getFieldType() == FieldSpec.FieldType.DATE_TIME)) {
        columnsToAddMinMaxValue.add(fieldSpec.getName());
      }
    }
    return columnsToAddMinMaxValue;
  }

//Refactoring end