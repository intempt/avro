
package org.apache.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Optional;

/**
 * An converter adapter, for conversion switching based on logical type.
 * @author Zoltan Farkas
 */
public class MultiConverter<T> extends Conversion<T> {

  public interface Chooser {
    int chose(LogicalType type);
  }

  private final Conversion<T>[] conversions;

  private final Chooser chooser;

  @SafeVarargs
  public MultiConverter(Chooser chooser, Conversion<T>... conversions) {
    this.conversions = conversions;
    this.chooser = chooser;
  }

  @Override
  public Class<T> getConvertedType() {
    return conversions[0].getConvertedType();
  }

  @Override
  public String getLogicalTypeName() {
    return conversions[0].getLogicalTypeName();
  }

  public Optional<T> tryDirectDecode(Decoder enc, Schema schema) throws IOException {
    return conversions[chooser.chose(schema.getLogicalType())].tryDirectDecode(enc, schema);
  }

  public boolean tryDirectEncode(T object, Encoder enc, Schema schema) throws IOException {
    return conversions[chooser.chose(schema.getLogicalType())].tryDirectEncode(object, enc, schema);
  }

  public int computehashCode(T object) {
    return conversions[0].computehashCode(object);
  }

  @Override
  public IndexedRecord toRecord(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toRecord(value, schema, type);
  }

  @Override
  public Map<?, ?> toMap(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toMap(value, schema, type);
  }

  @Override
  public Collection<?> toArray(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toArray(value, schema, type);
  }

  @Override
  public ByteBuffer toBytes(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toBytes(value, schema, type);
  }

  @Override
  public GenericFixed toFixed(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toFixed(value, schema, type);
  }

  @Override
  public GenericEnumSymbol toEnumSymbol(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toEnumSymbol(value, schema, type);
  }

  @Override
  public CharSequence toCharSequence(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toCharSequence(value, schema, type);
  }

  @Override
  public Double toDouble(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toDouble(value, schema, type);
  }

  @Override
  public Float toFloat(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toFloat(value, schema, type);
  }

  @Override
  public Long toLong(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toLong(value, schema, type);
  }

  @Override
  public Integer toInt(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toInt(value, schema, type);
  }

  @Override
  public Boolean toBoolean(T value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].toBoolean(value, schema, type);
  }

  @Override
  public T fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromRecord(value, schema, type);
  }

  @Override
  public T fromMap(Map<?, ?> value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromMap(value, schema, type);
  }

  @Override
  public T fromArray(Collection<?> value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromArray(value, schema, type);
  }

  @Override
  public T fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromBytes(value, schema, type);
  }

  @Override
  public T fromFixed(GenericFixed value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromFixed(value, schema, type);
  }

  @Override
  public T fromEnumSymbol(GenericEnumSymbol value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromEnumSymbol(value, schema, type);
  }

  @Override
  public T fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromCharSequence(value, schema, type);
  }

  @Override
  public T fromDouble(Double value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromDouble(value, schema, type);
  }

  @Override
  public T fromFloat(Float value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromFloat(value, schema, type);
  }

  @Override
  public T fromLong(Long value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromLong(value, schema, type);
  }

  @Override
  public T fromInt(Integer value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromInt(value, schema, type);
  }

  @Override
  public T fromBoolean(Boolean value, Schema schema, LogicalType type) {
    return conversions[chooser.chose(type)].fromBoolean(value, schema, type);
  }

  @Override
  public String adjustAndSetValue(String varName, String valParamName) {
    return conversions[0].adjustAndSetValue(varName, valParamName);
  }

}
