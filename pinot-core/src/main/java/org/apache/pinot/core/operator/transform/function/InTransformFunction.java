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
package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * The IN transform function takes one main expression (lhs) and multiple value expressions.
 * <p>For each docId, the function returns {@code true} if the set of values contains the value of the expression,
 * {@code false} otherwise.
 * <p>E.g. {@code SELECT col IN ('a','b','c') FROM myTable)}
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class InTransformFunction extends BaseTransformFunction {
  private TransformFunction _mainFunction;
  private TransformFunction[] _valueFunctions;
  private Set _valueSet;
  private boolean _valueSetContainsNull;

  @Override
  public String getName() {
    return TransformFunctionType.IN.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments >= 2, "At least 2 arguments are required for [%s] "
        + "transform function: (expression, values)", getName());
    _mainFunction = arguments.get(0);

    boolean allLiteralValues = true;
    ObjectOpenHashSet<String> stringValues = new ObjectOpenHashSet<>(numArguments - 1);
    for (int i = 1; i < numArguments; i++) {
      TransformFunction valueFunction = arguments.get(i);
      if (valueFunction instanceof LiteralTransformFunction) {
        LiteralTransformFunction literalTransformFunction = ((LiteralTransformFunction) valueFunction);
        if (literalTransformFunction.getResultMetadata().getDataType() == DataType.UNKNOWN) {
          _valueSetContainsNull = true;
        } else {
          stringValues.add(literalTransformFunction.getStringLiteral());
        }
      } else {
        allLiteralValues = false;
        break;
      }
    }

    if (allLiteralValues) {
      int numValues = stringValues.size();
      DataType storedType = _mainFunction.getResultMetadata().getDataType().getStoredType();
      switch (storedType) {
        case INT:
          IntOpenHashSet intValues = new IntOpenHashSet(numValues);
          for (String stringValue : stringValues) {
            intValues.add(Integer.parseInt(stringValue));
          }
          _valueSet = intValues;
          break;
        case LONG:
          LongOpenHashSet longValues = new LongOpenHashSet(numValues);
          for (String stringValue : stringValues) {
            longValues.add(Long.parseLong(stringValue));
          }
          _valueSet = longValues;
          break;
        case FLOAT:
          FloatOpenHashSet floatValues = new FloatOpenHashSet(numValues);
          for (String stringValue : stringValues) {
            floatValues.add(Float.parseFloat(stringValue));
          }
          _valueSet = floatValues;
          break;
        case DOUBLE:
          DoubleOpenHashSet doubleValues = new DoubleOpenHashSet(numValues);
          for (String stringValue : stringValues) {
            doubleValues.add(Double.parseDouble(stringValue));
          }
          _valueSet = doubleValues;
          break;
        case STRING:
          _valueSet = stringValues;
          break;
        case BYTES:
          ObjectOpenHashSet<ByteArray> bytesValues = new ObjectOpenHashSet<>(numValues);
          for (String stringValue : stringValues) {
            bytesValues.add(BytesUtils.toByteArray(stringValue));
          }
          _valueSet = bytesValues;
          break;
        case UNKNOWN:
          _valueSet = new ObjectOpenHashSet<>();
          break;
        default:
          throw new IllegalStateException();
      }
    } else {
      Preconditions.checkArgument(_mainFunction.getResultMetadata().isSingleValue(), "The first argument for [%s] "
          + "transform function must be single-valued when there are non-literal values", getName());
      _valueFunctions = new TransformFunction[numArguments - 1];
      for (int i = 1; i < numArguments; i++) {
        TransformFunction valueFunction = arguments.get(i);
        Preconditions.checkArgument(valueFunction.getResultMetadata().isSingleValue(), "The values for [%s] "
                + "transform function must be single-valued", getName());
        _valueFunctions[i - 1] = valueFunction;
      }
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initZeroFillingIntValuesSV(length);
    TransformResultMetadata mainFunctionMetadata = _mainFunction.getResultMetadata();
    DataType storedType = mainFunctionMetadata.getDataType().getStoredType();
    if (_valueSet != null) {
      handleValueSetCase(valueBlock, length, storedType);
    } else {
      handleValueFunctionsCase(valueBlock, length, storedType);
    }
    return _intValuesSV;
  }
  
  private void handleValueSetCase(ValueBlock valueBlock, int length, DataType storedType) {
    if (_mainFunction.getResultMetadata().isSingleValue()) {
      handleSingleValueSetCase(valueBlock, length, storedType);
    } else {
      handleMultiValueSetCase(valueBlock, length, storedType);
    }
  }
  
  private void handleValueFunctionsCase(ValueBlock valueBlock, int length, DataType storedType) {
    int numValues = _valueFunctions.length;
    switch (storedType) {
      case INT:
        handleIntValueFunctions(valueBlock, length, numValues);
        break;
      case LONG:
        handleLongValueFunctions(valueBlock, length, numValues);
        break;
      case FLOAT:
        handleFloatValueFunctions(valueBlock, length, numValues);
        break;
      case DOUBLE:
        handleDoubleValueFunctions(valueBlock, length, numValues);
        break;
      case STRING:
        handleStringValueFunctions(valueBlock, length, numValues);
        break;
      case BYTES:
        handleBytesValueFunctions(valueBlock, length, numValues);
        break;
      case UNKNOWN:
        fillResultUnknown(length);
        break;
      default:
        throw new IllegalStateException();
    }
  }
  
  private void handleSingleValueSetCase(ValueBlock valueBlock, int length, DataType storedType) {
    switch (storedType) {
      case INT:
        compareIntValuesWithSet((IntOpenHashSet) _valueSet, _mainFunction.transformToIntValuesSV(valueBlock), length);
        break;
      case LONG:
        compareLongValuesWithSet((LongOpenHashSet) _valueSet, _mainFunction.transformToLongValuesSV(valueBlock), length);
        break;
      case FLOAT:
        compareFloatValuesWithSet((FloatOpenHashSet) _valueSet, _mainFunction.transformToFloatValuesSV(valueBlock), length);
        break;
      case DOUBLE:
        compareDoubleValuesWithSet((DoubleOpenHashSet) _valueSet, _mainFunction.transformToDoubleValuesSV(valueBlock),
            length);
        break;
      case STRING:
        compareStringValuesWithSet((ObjectOpenHashSet<String>) _valueSet,
            _mainFunction.transformToStringValuesSV(valueBlock), length);
        break;
      case BYTES:
        compareBytesValuesWithSet((ObjectOpenHashSet<ByteArray>) _valueSet,
            _mainFunction.transformToBytesValuesSV(valueBlock), length);
        break;
      case UNKNOWN:
        fillResultUnknown(length);
        break;
      default:
        throw new IllegalStateException();
    }
  }
  
  private void compareIntValuesWithSet(IntOpenHashSet inIntValues, int[] intValues, int length) {
    for (int i = 0; i < length; i++) {
      if (inIntValues.contains(intValues[i])) {
        _intValuesSV[i] = 1;
      }
    }
  }
  
  private void compareLongValuesWithSet(LongOpenHashSet inLongValues, long[] longValues, int length) {
    for (int i = 0; i < length; i++) {
      if (inLongValues.contains(longValues[i])) {
        _intValuesSV[i] = 1;
      }
    }
  }
  
  private void compareFloatValuesWithSet(FloatOpenHashSet inFloatValues, float[] floatValues, int length) {
    for (int i = 0; i < length; i++) {
      if (inFloatValues.contains(floatValues[i])) {
        _intValuesSV[i] = 1;
      }
    }
  }
  
  private void compareDoubleValuesWithSet(DoubleOpenHashSet inDoubleValues, double[] doubleValues, int length) {
    for (int i = 0; i < length; i++) {
      if (inDoubleValues.contains(doubleValues[i])) {
        _intValuesSV[i] = 1;
      }
    }
  }
  
  private void compareStringValuesWithSet(ObjectOpenHashSet<String> inStringValues, String[] stringValues, int length) {
    for (int i = 0; i < length; i++) {
      if (inStringValues.contains(stringValues[i])) {
        _intValuesSV[i] = 1;
      }
    }
  }
  
  private void compareBytesValuesWithSet(ObjectOpenHashSet<ByteArray> inBytesValues, byte[][] bytesValues, int length) {
    for (int i = 0; i < length; i++) {
      if (inBytesValues.contains(new ByteArray(bytesValues[i]))) {
        _intValuesSV[i] = 1;
      }
    }
  }
  
  private void handleMultiValueSetCase(ValueBlock valueBlock, int length, DataType storedType) {
    switch (storedType) {
      case INT:
        compareIntValuesWithSetMV((IntOpenHashSet) _valueSet, _mainFunction.transformToIntValuesMV(valueBlock), length);
        break;
      case LONG:
        compareLongValuesWithSetMV((LongOpenHashSet) _valueSet, _mainFunction.transformToLongValuesMV(valueBlock),
            length);
        break;
      case FLOAT:
        compareFloatValuesWithSetMV((FloatOpenHashSet) _valueSet, _mainFunction.transformToFloatValuesMV(valueBlock),
            length);
        break;
      case DOUBLE:
        compareDoubleValuesWithSetMV((DoubleOpenHashSet) _valueSet, _mainFunction.transformToDoubleValuesMV(valueBlock),
            length);
        break;
      case STRING:
        compareStringValuesWithSetMV((ObjectOpenHashSet<String>) _valueSet,
            _mainFunction.transformToStringValuesMV(valueBlock), length);
        break;
      case UNKNOWN:
        fillResultUnknown(length);
        break;
      default:
        throw new IllegalStateException();
    }
  }
  
  private void compareIntValuesWithSetMV(IntOpenHashSet inIntValues, int[][] intValues, int length) {
    for (int i = 0; i < length; i++) {
      for (int intValue : intValues[i]) {
        if (inIntValues.contains(intValue)) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }
  
  private void compareLongValuesWithSetMV(LongOpenHashSet inLongValues, long[][] longValues, int length) {
    for (int i = 0; i < length; i++) {
      for (long longValue : longValues[i]) {
        if (inLongValues.contains(longValue)) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }

  private void compareFloatValuesWithSetMV(FloatOpenHashSet inFloatValues, float[][] floatValues, int length) {
    for (int i = 0; i < length; i++) {
      for (float floatValue : floatValues[i]) {
        if (inFloatValues.contains(floatValue)) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }
  
  private void compareDoubleValuesWithSetMV(DoubleOpenHashSet inDoubleValues, double[][] doubleValues, int length) {
    for (int i = 0; i < length; i++) {
      for (double doubleValue : doubleValues[i]) {
        if (inDoubleValues.contains(doubleValue)) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }
  
  private void compareStringValuesWithSetMV(ObjectOpenHashSet<String> inStringValues, String[][] stringValues,
      int length) {
    for (int i = 0; i < length; i++) {
      for (String stringValue : stringValues[i]) {
        if (inStringValues.contains(stringValue)) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }
  
  private void handleIntValueFunctions(ValueBlock valueBlock, int length, int numValues) {
    int[] intValues = _mainFunction.transformToIntValuesSV(valueBlock);
    int[][] inIntValues = new int[numValues][];
    for (int i = 0; i < numValues; i++) {
      inIntValues[i] = _valueFunctions[i].transformToIntValuesSV(valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (int[] inIntValue : inIntValues) {
        if (intValues[i] == inIntValue[i]) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }
  
  private void handleLongValueFunctions(ValueBlock valueBlock, int length, int numValues) {
    long[] longValues = _mainFunction.transformToLongValuesSV(valueBlock);
    long[][] inLongValues = new long[numValues][];
    for (int i = 0; i < numValues; i++) {
      inLongValues[i] = _valueFunctions[i].transformToLongValuesSV(valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (long[] inLongValue : inLongValues) {
        if (longValues[i] == inLongValue[i]) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }
  
  private void handleFloatValueFunctions(ValueBlock valueBlock, int length, int numValues) {
    float[] floatValues = _mainFunction.transformToFloatValuesSV(valueBlock);
    float[][] inFloatValues = new float[numValues][];
    for (int i = 0; i < numValues; i++) {
      inFloatValues[i] = _valueFunctions[i].transformToFloatValuesSV(valueBlock);
    }
    for (int i = 0; i < length; i++) {
      // Check int bits to be aligned with the Set (Float.equals()) behavior
      int intBits = Float.floatToIntBits(floatValues[i]);
      for (float[] inFloatValue : inFloatValues) {
        if (intBits == Float.floatToIntBits(inFloatValue[i])) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }
  
  private void handleDoubleValueFunctions(ValueBlock valueBlock, int length, int numValues) {
    double[] doubleValues = _mainFunction.transformToDoubleValuesSV(valueBlock);
    double[][] inDoubleValues = new double[numValues][];
    for (int i = 0; i < numValues; i++) {
      inDoubleValues[i] = _valueFunctions[i].transformToDoubleValuesSV(valueBlock);
    }
    for (int i = 0; i < length; i++) {
      // Check long bits to be aligned with the Set (Double.equals()) behavior
      long longBits = Double.doubleToLongBits(doubleValues[i]);
      for (double[] inDoubleValue : inDoubleValues) {
        if (longBits == Double.doubleToLongBits(inDoubleValue[i])) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }
  
  private void handleStringValueFunctions(ValueBlock valueBlock, int length, int numValues) {
    String[] stringValues = _mainFunction.transformToStringValuesSV(valueBlock);
    String[][] inStringValues = new String[numValues][];
    for (int i = 0; i < numValues; i++) {
      inStringValues[i] = _valueFunctions[i].transformToStringValuesSV(valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (String[] inStringValue : inStringValues) {
        if (stringValues[i].equals(inStringValue[i])) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }
  
  private void handleBytesValueFunctions(ValueBlock valueBlock, int length, int numValues) {
    byte[][] bytesValues = _mainFunction.transformToBytesValuesSV(valueBlock);
    byte[][][] inBytesValues = new byte[numValues][][];
    for (int i = 0; i < numValues; i++) {
      inBytesValues[i] = _valueFunctions[i].transformToBytesValuesSV(valueBlock);
    }
    for (int i = 0; i < length; i++) {
      for (byte[][] inBytesValue : inBytesValues) {
        if (Arrays.equals(bytesValues[i], inBytesValue[i])) {
          _intValuesSV[i] = 1;
          break;
        }
      }
    }
  }

//Refactoring end
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    RoaringBitmap result = new RoaringBitmap();
    RoaringBitmap mainFunctionNullBitmap = _mainFunction.getNullBitmap(valueBlock);
    if (mainFunctionNullBitmap != null) {
      result.or(mainFunctionNullBitmap);
      if (result.getCardinality() == length) {
        return result;
      }
    }
    int[] intValuesSV = transformToIntValuesSV(valueBlock);
    if (_valueSet == null) {
      RoaringBitmap valueFunctionsContainNull = new RoaringBitmap();
      RoaringBitmap[] valueFunctionNullBitmaps = new RoaringBitmap[_valueFunctions.length];
      for (int i = 0; i < _valueFunctions.length; i++) {
        valueFunctionNullBitmaps[i] = _valueFunctions[i].getNullBitmap(valueBlock);
      }
      for (int i = 0; i < length; i++) {
        for (int j = 0; j < _valueFunctions.length; j++) {
          if (valueFunctionNullBitmaps[j] != null && valueFunctionNullBitmaps[j].contains(i)) {
            valueFunctionsContainNull.add(i);
            break;
          }
        }
      }
      for (int i = 0; i < length; i++) {
        if (mainFunctionNotContainedInValues(intValuesSV[i]) && valueFunctionsContainNull.contains(i)) {
          result.add(i);
        }
      }
    } else {
      for (int i = 0; i < length; i++) {
        if (mainFunctionNotContainedInValues(intValuesSV[i]) && _valueSetContainsNull) {
          result.add(i);
        }
      }
    }
    return result;
  }

  protected boolean mainFunctionNotContainedInValues(int value) {
    return value == 0;
  }
}
