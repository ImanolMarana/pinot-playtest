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
package org.apache.pinot.segment.local.aggregator;

import java.util.List;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountCPCSketchValueAggregator implements ValueAggregator<Object, Object> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private final int _lgK;

  public DistinctCountCPCSketchValueAggregator(List<ExpressionContext> arguments) {
    // length 1 means we use the Helix default
    if (arguments.size() <= 1) {
      _lgK = CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK;
    } else {
      _lgK = arguments.get(1).getLiteral().getIntValue();
    }
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTCPCSKETCH;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public Object getInitialAggregatedValue(Object rawValue) {
    CpcUnion cpcUnion = new CpcUnion(_lgK);
    if (rawValue instanceof byte[]) { // Serialized Sketch
      byte[] bytes = (byte[]) rawValue;
      cpcUnion.update(deserializeAggregatedValue(bytes));
    } else if (rawValue instanceof byte[][]) { // Multiple Serialized Sketches
      byte[][] serializedSketches = (byte[][]) rawValue;
      for (byte[] bytes : serializedSketches) {
        cpcUnion.update(deserializeAggregatedValue(bytes));
      }
    } else {
      CpcSketch pristineSketch = empty();
      addObjectToSketch(rawValue, pristineSketch);
      cpcUnion.update(pristineSketch);
    }
    return cpcUnion;
  }

  @Override
  public Object applyRawValue(Object aggregatedValue, Object rawValue) {
    CpcUnion cpcUnion = extractUnion(aggregatedValue);
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      CpcSketch sketch = deserializeAggregatedValue(bytes);
      cpcUnion.update(sketch);
    } else {
      CpcSketch pristineSketch = empty();
      addObjectToSketch(rawValue, pristineSketch);
      cpcUnion.update(pristineSketch);
    }
    return cpcUnion;
  }

  @Override
  public Object applyAggregatedValue(Object value, Object aggregatedValue) {
    CpcUnion cpcUnion = extractUnion(aggregatedValue);
    CpcSketch sketch = extractSketch(value);
    cpcUnion.update(sketch);
    return cpcUnion;
  }

  @Override
  public Object cloneAggregatedValue(Object value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return CpcSketch.getMaxSerializedBytes(_lgK);
  }

  @Override
  public byte[] serializeAggregatedValue(Object value) {
    CpcSketch sketch = extractSketch(value);
    return CustomSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(sketch);
  }

  @Override
  public CpcSketch deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(bytes);
  }

  private CpcSketch union(CpcSketch left, CpcSketch right) {
    if (left == null && right == null) {
      return empty();
    } else if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    }

    CpcUnion union = new CpcUnion(_lgK);
    union.update(left);
    union.update(right);
    return union.getResult();
  }

  private void addObjectToSketch(Object rawValue, CpcSketch sketch) {
    if (rawValue instanceof String) {
      sketch.update((String) rawValue);
    } else if (rawValue instanceof Integer) {
      sketch.update((Integer) rawValue);
    } else if (rawValue instanceof Long) {
      sketch.update((Long) rawValue);
    } else if (rawValue instanceof Double) {
      sketch.update((Double) rawValue);
    } else if (rawValue instanceof Float) {
      sketch.update((Float) rawValue);
    } else if (rawValue instanceof Object[]) {
      addObjectsToSketch((Object[]) rawValue, sketch);
    } else {
      throw new IllegalStateException(
          "Unsupported data type for CPC Sketch aggregation: " + rawValue.getClass().getSimpleName());
    }
  }

  private void addObjectsToSketch(Object[] rawValues, CpcSketch sketch) {
    if (rawValues instanceof String[]) {
      updateSketchWithStringArray((String[]) rawValues, sketch);
    } else if (rawValues instanceof Integer[]) {
      updateSketchWithIntArray((Integer[]) rawValues, sketch);
    } else if (rawValues instanceof Long[]) {
      updateSketchWithLongArray((Long[]) rawValues, sketch);
    } else if (rawValues instanceof Double[]) {
      updateSketchWithDoubleArray((Double[]) rawValues, sketch);
    } else if (rawValues instanceof Float[]) {
      updateSketchWithFloatArray((Float[]) rawValues, sketch);
    } else {
      throw new IllegalStateException(
          "Unsupported data type for CPC Sketch aggregation: " + rawValues.getClass().getSimpleName());
    }
  }

  private void updateSketchWithStringArray(String[] stringValues, CpcSketch sketch) {
    for (String s : stringValues) {
      sketch.update(s);
    }
  }

  private void updateSketchWithIntArray(Integer[] intValues, CpcSketch sketch) {
    for (Integer i : intValues) {
      sketch.update(i);
    }
  }

  private void updateSketchWithLongArray(Long[] longValues, CpcSketch sketch) {
    for (Long l : longValues) {
      sketch.update(l);
    }
  }

  private void updateSketchWithDoubleArray(Double[] doubleValues, CpcSketch sketch) {
    for (Double d : doubleValues) {
      sketch.update(d);
    }
  }

  private void updateSketchWithFloatArray(Float[] floatValues, CpcSketch sketch) {
    for (Float f : floatValues) {
      sketch.update(f);
    }
  }

//Refactoring end

  private CpcUnion extractUnion(Object value) {
    if (value == null) {
      return new CpcUnion(_lgK);
    } else if (value instanceof CpcUnion) {
      return (CpcUnion) value;
    } else if (value instanceof CpcSketch) {
      CpcSketch sketch = (CpcSketch) value;
      CpcUnion cpcUnion = new CpcUnion(_lgK);
      cpcUnion.update(sketch);
      return cpcUnion;
    } else {
      throw new IllegalStateException(
          "Unsupported data type for CPC Sketch aggregation: " + value.getClass().getSimpleName());
    }
  }

  private CpcSketch extractSketch(Object value) {
    if (value == null) {
      return empty();
    } else if (value instanceof CpcUnion) {
      return ((CpcUnion) value).getResult();
    } else if (value instanceof CpcSketch) {
      return (CpcSketch) value;
    } else {
      throw new IllegalStateException(
          "Unsupported data type for CPC Sketch aggregation: " + value.getClass().getSimpleName());
    }
  }

  private CpcSketch empty() {
    return new CpcSketch(_lgK);
  }
}
