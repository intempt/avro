/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro;

import org.apache.avro.util.Maps;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.data.Json;
import org.apache.avro.data.Json.RawJsonStringSerialize;

import org.apache.avro.util.internal.JacksonUtils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Function;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.ExtendedJsonDecoder;
import org.apache.avro.util.Sets;
import org.apache.avro.util.internal.Accessor;


/** An abstract data type.
 * <p>A schema may be one of:
 * <ul>
 * <li>A <i>record</i>, mapping field names to field value data;
 * <li>An <i>enum</i>, containing one of a small set of symbols;
 * <li>An <i>array</i> of values, all of the same schema;
 * <li>A <i>map</i>, containing string/value pairs, of a declared schema;
 * <li>A <i>union</i> of other schemas;
 * <li>A <i>fixed</i> sized binary object;
 * <li>A unicode <i>string</i>;
 * <li>A sequence of <i>bytes</i>;
 * <li>A 32-bit signed <i>int</i>;
 * <li>A 64-bit signed <i>long</i>;
 * <li>A 32-bit IEEE single-<i>float</i>; or
 * <li>A 64-bit IEEE <i>double</i>-float; or
 * <li>A <i>boolean</i>; or
 * <li><i>null</i>.
 * </ul>
 *
 * A schema can be constructed using one of its static <tt>createXXX</tt>
 * methods, or more conveniently using {@link SchemaBuilder}. The schema objects are
 * <i>logically</i> immutable.
 * There are only three mutating methods - {@link #setFields(List)},
 * {@link #addProp(String, String)}, and {@link #setLogicalType(LogicalType)}.
 * The following restrictions apply on these methods.
 * <ul>
 * <li> {@link #setFields(List)}, can be called at most once. This method exists
 * in order to enable clients to build recursive schemas.
 * <li> {@link #addProp(String, String)} can be called with property names
 * that are not present already. It is not possible to change or delete an
 * existing property.
 * <li> {@link #setLogicalType(LogicalType)} can be called at most once.
 * </ul>
 */
public abstract class Schema extends JsonProperties implements Serializable {

  private static final long serialVersionUID = 1L;

  protected Object writeReplace() {
    SerializableSchema ss = new SerializableSchema();
    ss.schemaString = toString();
    return ss;
  }

  private static final class SerializableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private String schemaString;

    private Object readResolve() {
      return new Schema.Parser().parse(schemaString);
    }
  }

  public static final JsonFactory FACTORY = new JsonFactory();
  public static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  static {
    FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
    FACTORY.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
    SimpleModule module = new SimpleModule("avro");
    module.addSerializer(new Json.AvroSchemaSerializer());
    module.addSerializer(new Json.AvroJsonSerializer());
    module.addSerializer(new RawJsonStringSerialize());
    module.addDeserializer(Schema.class, new Json.AvroSchemaDeserializer());
    MAPPER.registerModule(module);
    MAPPER.disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);
    FACTORY.setCodec(MAPPER);
  }

  private static final int NO_HASHCODE = Integer.MIN_VALUE;

  public static final Schema fromString(final String schemaJson) {
    return new Schema.Parser().parse(schemaJson);
  }

  public static final Schema fromString(final String schemaJson, final String ... more) {
    return new Schema.Parser().parse(schemaJson, more);
  }


  /** The type of a schema. */
  public enum Type {
    RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES,
      INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL;
    private String name;
    private Type() { this.name = this.name().toLowerCase(Locale.ENGLISH); }
    public String getName() { return name; }
  };

  private final Type type;
  private LogicalType logicalType;

  Schema(Type type) {
    super(SCHEMA_RESERVED);
    this.type = type;
    this.logicalType = null;
  }

  /** Create a schema for a primitive type. */
  public static Schema create(Type type) {
    switch (type) {
    case STRING:  return new StringSchema();
    case BYTES:   return new BytesSchema();
    case INT:     return new IntSchema();
    case LONG:    return new LongSchema();
    case FLOAT:   return new FloatSchema();
    case DOUBLE:  return new DoubleSchema();
    case BOOLEAN: return new BooleanSchema();
    case NULL:    return new NullSchema();
    default: throw new AvroRuntimeException("Can't create a: "+type);
    }
  }

  private static final Set<String> SCHEMA_RESERVED = new HashSet<String>();
  static {
    Collections.addAll(SCHEMA_RESERVED,
                       "doc", "fields", "items", "name", "namespace",
                       "size", "symbols", "values", "type", "aliases");
  }
  private static final Set<String> ENUM_RESERVED = new HashSet<>();
  static {
    ENUM_RESERVED.add("default");
    ENUM_RESERVED.addAll(SCHEMA_RESERVED);
  }

  int hashCode = NO_HASHCODE;

  public Schema withProp(String name, String value) {
    addProp(name, value);
    return this;
  }

  public Schema withProp(String name, Object value) {
    addProp(name, value);
    return this;
  }

  @Override
  public void addJsonProps(Map<String, JsonNode> xtraProps) {
    hashCode = NO_HASHCODE;
    super.addJsonProps(xtraProps);
  }

  @Override
  public void addProp(String name, Object value) {
    hashCode = NO_HASHCODE;
    super.addProp(name, value);
  }

  @Override
  void addProp(String name, JsonNode value) {
    hashCode = NO_HASHCODE;
    super.addProp(name, value);
  }

  @Override
  public void addProp(String name, String value) {
    hashCode = NO_HASHCODE;
    super.addProp(name, value);
  }


  /** Create an anonymous record schema. */
  public static Schema createRecord(List<Field> fields) {
    Schema result = createRecord(null, null, null, false);
    result.setFields(fields);
    return result;
  }

  /** Create a named record schema. */
  public static Schema createRecord(String name, String doc, String namespace,
                                    boolean isError) {
    return createRecord(name, doc, namespace, isError, true);
  }

  public static Schema createRecord(String name, String doc, String namespace,
                                    boolean isError, boolean validateName) {
    return new RecordSchema(new Name(name, namespace, validateName), doc, isError);
  }

  /** Create a named record schema with fields already set. */
  public static Schema createRecord(String name, String doc, String namespace,
                                    boolean isError, List<Field> fields) {
    return createRecord(name, doc, namespace, isError, fields, true);
  }

  /** Create a named record schema with fields already set and no name validation. */
  public static Schema createRecord(String name, String doc, String namespace,
                                    boolean isError, List<Field> fields, boolean validateName) {
    return new RecordSchema(new Name(name, namespace, validateName), doc, isError, fields);
  }

  /** Create an enum schema. */
  public static Schema createEnum(String name, String doc, String namespace,
                                  List<String> values) {
    return new EnumSchema(new Name(name, namespace), doc,
        new LockableArrayList<String>(values), null);
  }

  /** Create an enum schema. */
  public static Schema createEnum(String name, String doc, String namespace,
                                  List<String> values, @Nullable final String defaultEnumVal) {
    return new EnumSchema(new Name(name, namespace), doc,
        new LockableArrayList<String>(values), defaultEnumVal);
  }

  /** Create an array schema. */
  public static Schema createArray(Schema elementType) {
    return new ArraySchema(elementType);
  }

  /** Create a map schema. */
  public static Schema createMap(Schema valueType) {
    return new MapSchema(valueType);
  }

  /** Create a union schema. */
  public static Schema createUnion(List<Schema> types) {
    return new UnionSchema(new LockableArrayList<>(types));
  }

  /** Create a union schema. */
  public static Schema createUnion(Schema... types) {
    return createUnion(new LockableArrayList<>(types));
  }

  /** Create a union schema. */
  public static Schema createFixed(String name, String doc, String space,
      int size) {
    return new FixedSchema(new Name(name, space), doc, size);
  }

  /** Return the type of this schema. */
  @Nonnull
  public Type getType() { return type; }

  /** Return the logical type annotation for this schema */
  public LogicalType getLogicalType() { return logicalType; }

  /** Set the logical type annotation for this schema */
  void setLogicalType(LogicalType logicalType) {
    if (this.logicalType != null) {
      throw new IllegalArgumentException(
          "Cannot replace existing logical type in " + this + " with " + logicalType);
    }
    this.logicalType = logicalType;
    this.hashCode = NO_HASHCODE;
  }

  public void parseLogicalType(final boolean allowUndefinedLogicalTypes) {
    if (this.logicalType == null) {
      this.logicalType = LogicalTypes.fromSchema(this, allowUndefinedLogicalTypes);
      this.hashCode = NO_HASHCODE;
    }
  }

  /**
   * If this is a record, returns the Field with the
   * given name <tt>fieldName</tt>. If there is no field by that name, a
   * <tt>null</tt> is returned.
   */
  public Field getField(String fieldname) {
    throw new AvroRuntimeException("Not a record: " + this);
  }

  public Set<Field> getFieldsByNameOrAlias(@Nonnull String fieldnameoralias) {
    throw new AvroRuntimeException("Not a record: " + this);
  }

  /**
   * If this is a record, returns the fields in it. The returned
   * list is in the order of their positions.
   */
  public List<Field> getFields() {
    throw new AvroRuntimeException("Not a record: "+this);
  }

  /**
   * If this is a record, set its fields. The fields can be set
   * only once in a schema.
   */
  public void setFields(List<Field> fields) {
    throw new AvroRuntimeException("Not a record: "+this);
  }

  /** If this is an enum, return its symbols. */
  public List<String> getEnumSymbols() {
    throw new AvroRuntimeException("Not an enum: "+this);
  }

  @Nullable
  public String getEnumDefault() {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  public String getEnumStringSymbol(final String symbol) {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  public  Map<String, Set<String>> getEnumSymbolAliases()  {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  public List<String> getEnumStringSymbols() {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  /** If this is an enum, return a symbol's ordinal value. */
  public int getEnumOrdinal(String symbol) {
    throw new AvroRuntimeException("Not an enum: "+this);
  }

  /** If this is an enum, returns true if it contains given symbol. */
  public boolean hasEnumSymbol(String symbol) {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  public boolean hasEnumSymbolOrAlias(String symbol) {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  public int getEnumSymbolOrAliasOrdinal(String soa) {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  /** If this is a record, enum or fixed, returns its name, otherwise the name
   * of the primitive type. */
  public String getName() { return type.name; }

  /** If this is a record, enum, or fixed, returns its docstring,
   * if available.  Otherwise, returns null. */
  @Nullable
  public String getDoc() {
    return null;
  }

  /** If this is a record, enum or fixed, returns its namespace, if any. */
  public String getNamespace() {
    throw new AvroRuntimeException("Not a named type: " + this);
  }

  /** If this is a record, enum or fixed, returns its namespace-qualified name,
   * otherwise returns the name of the primitive type. */
  public String getFullName() {
    return getName();
  }

  /** If this is a record, enum or fixed, add an alias. */
  public void addAlias(String alias) {
    throw new AvroRuntimeException("Not a named type: "+this);
  }

  /** If this is a record, enum or fixed, add an alias. */
  public void addAlias(String alias, String space) {
    throw new AvroRuntimeException("Not a named type: "+this);
  }

  /** If this is a record, enum or fixed, return its aliases, if any. */
  public Set<String> getAliases() {
    throw new AvroRuntimeException("Not a named type: "+this);
  }

  /** Returns true if this record is an error type. */
  public boolean isError() {
    throw new AvroRuntimeException("Not a record: "+this);
  }

  /** If this is an array, returns its element type. */
  public Schema getElementType() {
    throw new AvroRuntimeException("Not an array: "+this);
  }

  /** If this is a map, returns its value type. */
  public Schema getValueType() {
    throw new AvroRuntimeException("Not a map: "+this);
  }

  /** If this is a union, returns its types. */
  public List<Schema> getTypes() {
    throw new AvroRuntimeException("Not a union: "+this);
  }

  /** If this is a union, return the branch with the provided full name. */
  public Integer getIndexNamed(String name) {
    throw new AvroRuntimeException("Not a union: "+this);
  }

  /** If this is fixed, returns its size. */
  public int getFixedSize() {
    throw new AvroRuntimeException("Not fixed: "+this);
  }

  /** Render this as <a href="http://json.org/">JSON</a>.*/
  @Override
  public String toString() { return toString(false); }

  /** Render this as <a href="http://json.org/">JSON</a>.
   * @param pretty if true, pretty-print JSON.
   */
  public String toString(boolean pretty) {
    try {
      StringWriter writer = new StringWriter(logicalType == null && props.isEmpty() ? 16 : 64);
      JsonGenerator gen = FACTORY.createGenerator(writer);
      if (pretty) {
        gen.useDefaultPrettyPrinter();
      }
      toJson(gen);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /**
   * Render this as <a href="https://json.org/">JSON</a>, but without inlining the
   * referenced schemas.
   *
   * @param referencedSchemas referenced schemas
   * @param pretty            if true, pretty-print JSON.
   */
  // Use at your own risk. This method should be removed with AVRO-2832.
  @Deprecated
  public String toString(Collection<Schema> referencedSchemas, boolean pretty) {
    Schema.Names names = new Schema.Names();
    if (referencedSchemas != null) {
      for (Schema s : referencedSchemas) {
        names.add(s);
      }
    }
    return toString(names, pretty);
  }

  String toString(Names names, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator gen = FACTORY.createGenerator(writer);
      if (pretty)
        gen.useDefaultPrettyPrinter();
      toJson(names, gen);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  public void toJson(final JsonGenerator gen) throws IOException {
    toJson(new Names(), gen);
  }

  public void toJson(Names names, JsonGenerator gen) throws IOException {
    if (logicalType == null && props.isEmpty()) { // no props defined
      gen.writeString(getName());                   // just write name
    } else {
      gen.writeStartObject();
      gen.writeStringField("type", getName());
      writeProps(gen);
      gen.writeEndObject();
    }
  }

  void fieldsToJson(Names names, JsonGenerator gen) throws IOException {
    throw new AvroRuntimeException("Not a record: "+this);
  }

  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    Schema that = (Schema) o;
    if (this.type != that.type) return false;
    return equalCachedHash(that)
        && props.equals(that.props);
  }
  public final int hashCode() {
    if (hashCode == NO_HASHCODE) {
      hashCode = computeHash();
    }
    return hashCode;
  }

  int computeHash() {
    return (logicalType != null ?  23 * logicalType.hashCode() : getType().hashCode());
  }

  final boolean equalCachedHash(Schema other) {
    return (hashCode == other.hashCode)
           || (hashCode == NO_HASHCODE)
           || (other.hashCode == NO_HASHCODE);
  }

  private static final Set<String> FIELD_RESERVED = new HashSet<>();
  static {
    Collections.addAll(FIELD_RESERVED,
                       "default","doc","name","order","type","aliases");
  }

  /** A field within a record. */
  public static class Field extends JsonProperties {

    static {
      Accessor.setAccessor(new Accessor.FieldAccessor() {
        @Override
        protected JsonNode defaultValue(Field field) {
          return field.defaultValue();
        }

        @Override
        protected Field createField(String name, Schema schema, String doc, JsonNode defaultValue) {
          return new Field(name, schema, doc, defaultValue, true, true, Order.ASCENDING);
        }

        @Override
        protected Field createField(String name, Schema schema, String doc, JsonNode defaultValue, boolean validate,
            Order order) {
          return new Field(name, schema, doc, defaultValue, validate, true, order);
        }
      });
    }

    /** How values of this field should be ordered when sorting records. */
    public enum Order {
      ASCENDING, DESCENDING, IGNORE;
      private String name;

      private Order() {
        this.name = this.name().toLowerCase(Locale.ENGLISH);
      }
    }

    public static final Object NULL_DEFAULT_VALUE = JsonProperties.NULL_VALUE;

    private final String name;    // name of the field.
    private int position = -1;
    private final Schema schema;
    private final String doc;
    private final JsonNode defaultValue;
    private final Object defaultJavaVal;
    private final Order order;
    private Set<String> aliases;


    Field(String name, Schema schema, String doc,
        JsonNode defaultValue, boolean validateDefault, boolean validateName, Order order) {
      super(FIELD_RESERVED);
      this.name = validateName ? validateName(name) : name;
      this.schema = schema;
      this.doc = doc;
      this.defaultValue = validateDefault ? validateDefault(name, schema, defaultValue) : defaultValue;
      this.defaultJavaVal = JacksonUtils.toObject(defaultValue, schema);
      this.order = order;
    }

    public Field(String name, Schema schema, String doc,
         Object defaultVal, boolean validateDefault, boolean validateName, Order order) {
      super(FIELD_RESERVED);
      this.name = validateName ? validateName(name) : name;
      this.schema = schema;
      this.doc = doc;
      if (defaultVal == NULL_DEFAULT_VALUE) {
        this.defaultValue =  NullNode.getInstance();
        this.defaultJavaVal = defaultVal;
      } else {
        JsonNode jsonDefault = JacksonUtils.toJsonNode(defaultVal);
        this.defaultValue = validateDefault ? validateDefault(name, schema, jsonDefault) : jsonDefault;
        this.defaultJavaVal = defaultVal;
      }
      this.order = order;
    }

    /**
     * Constructs a new Field instance with the same {@code name}, {@code doc},
     * {@code defaultValue}, and {@code order} as {@code field} has with changing
     * the schema to the specified one. It also copies all the {@code props} and
     * {@code aliases}.
     */
    public Field(Field field, Schema schema) {
      super(FIELD_RESERVED);
      this.name = field.name;
      this.schema = schema;
      this.doc = field.doc;
      this.defaultValue = field.defaultValue;
      this.defaultJavaVal = JacksonUtils.toObject(defaultValue, schema);
      this.order = field.order;
      putAll(field);
      if (field.aliases != null) {
        aliases = new LinkedHashSet<>(field.aliases);
      }
    }

    public Field(String name, Schema schema) {
      this(name, schema, (String) null, null, true, true, Order.ASCENDING);
    }

    public Field(String name, Schema schema, String doc) {
      this(name, schema, doc,  null, true, true, Order.ASCENDING);
    }

    /**
     * @param defaultValue the default value for this field specified using the mapping
     * null means no default value.
     * JsonProperties.NULL represents the null value
     *  in {@link JsonProperties}
     */
    public Field(String name, Schema schema, String doc,
        Object defaultValue) {
      this(name, schema, doc, defaultValue, Order.ASCENDING);
    }
    /**
     * @param defaultValue the default value for this field specified using the mapping
     *  in {@link JsonProperties}
     */
    public Field(String name, Schema schema, String doc,
        Object defaultValue, Order order) {
      this(name, schema, doc, defaultValue, true, true, order);
    }

    public String name() {
      return name;
    }

    /** The position of this field within the record. */
    public int pos() {
      return position;
    }

    /** This field's {@link Schema}. */
    public Schema schema() {
      return schema;
    }

    /** Field's documentation within the record, if set. May return null. */
    public String doc() {
      return doc;
    }

    /**
     * @return true if this Field has a default value set. Can be used to determine
     *         if a "null" return from defaultVal() is due to that being the default
     *         value or just not set.
     */
    public boolean hasDefaultValue() {
      return defaultValue != null;
    }

    public JsonNode defaultValue() {
      return defaultValue;
    }
    /**
     * @return the default value for this field specified using the mapping
     *  in {@link JsonProperties}
     */
    public Object defaultVal() {
      return defaultJavaVal;
    }

    public Order order() { return order; }
    @Deprecated public Map<String,String> props() { return getProps(); }
    public void addAlias(String alias) {
      if (aliases == null)
        this.aliases = new LinkedHashSet<>(2);
      aliases.add(alias);
    }
    /** Return the defined aliases as an unmodifieable Set. */
    public Set<String> aliases() {
      if (aliases == null)
        return Collections.emptySet();
      return Collections.unmodifiableSet(aliases);
    }
    public boolean equals(Object other) {
      if (other == this) return true;
      if (!(other instanceof Field)) return false;
      Field that = (Field) other;
      return (name.equals(that.name)) &&
        (schema.equals(that.schema)) &&
        defaultValueEquals(that.defaultValue, that.defaultJavaVal) &&
        (order == that.order) &&
        props.equals(that.props);
    }
    public int hashCode() { return name.hashCode() + 7 * schema.hashCode(); }

    private boolean defaultValueEquals(JsonNode thatDefaultValue, Object thatJavaDefaultValue) {
      if (defaultValue == null)
        return thatDefaultValue == null;
      if (thatDefaultValue == null)
        return false;
      if (defaultJavaVal.equals(thatJavaDefaultValue)) {
        return true;
      }
      if (Double.isNaN(defaultValue.asDouble()))
        return Double.isNaN(thatDefaultValue.asDouble());
      return defaultValue.equals(thatDefaultValue);
    }

    @Override
    public String toString() {
      return name + " type:" + schema.type + " pos:" + position;
    }
  }

  static class Name {
    private final String name;
    private final String space;
    private final String full;

    public Name(String name, String space) {
      this(name, space, true);
    }
    /**
     * @param name if name is a fully qualified name, the space parameter value is being ignored.
     * @param space namespace in case the name is not fully qualified.
     */
    public Name(String name, String space, boolean validateName) {
      if (name == null) {                         // anonymous
        this.name = this.space = this.full = null;
        return;
      }
      int lastDol = name.lastIndexOf('$');
      if (lastDol >= 0) {
        space = name.substring(0, lastDol + 1);       // ordinal space from name
        this.name = validateName ? validateName(name.substring(lastDol + 1, name.length())) : name;
      } else {
        int lastDot = name.lastIndexOf('.');
        if (lastDot < 0) {                          // unqualified name
          this.name = validateName ? validateName(name) : name;
        } else {                                    // qualified name
          space = name.substring(0, lastDot);       // ordinal space from name
          String rawName = name.substring(lastDot + 1, name.length());
          this.name =  validateName ? validateName(rawName) : rawName ;
        }
      }
      if ("".equals(space))
        space = null;
      this.space = space;
      this.full = (this.space == null) ? this.name
              : this.space.endsWith("$") ? this.space + this.name : this.space  + "." + this.name;
    }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof Name)) return false;
      Name that = (Name)o;
      return full==null ? that.full==null : full.equals(that.full);
    }
    public int hashCode() {
      return full==null ? 0 : full.hashCode();
    }
    public String toString() { return full; }
    public void writeName(Names names, JsonGenerator gen) throws IOException {
      if (name != null) gen.writeStringField("name", name);
      if (space != null) {
        if (!space.equals(names.space()))
          gen.writeStringField("namespace", space);
      } else if (names.space() != null) {         // null within non-null
        gen.writeStringField("namespace", "");
      }
    }
    public String getQualified(String defaultSpace) {
      return (space == null || space.equals(defaultSpace)) ? name : full;
    }
  }

  private static abstract class NamedSchema extends Schema {
    final Name name;
    final String doc;
    Set<Name> aliases;
    public NamedSchema(Type type, Name name, String doc) {
      super(type);
      this.name = name;
      this.doc = doc;
      if (PRIMITIVES.containsKey(name.full)) {
        throw new AvroTypeException("Schemas may not be named after primitives: " + name.full);
      }
    }
    public String getName() { return name.name; }
    public String getDoc() { return doc; }
    public String getNamespace() { return name.space; }
    public String getFullName() { return name.full; }
    public void addAlias(String alias) {
      addAlias(alias, null);
    }
    public void addAlias(String name, String space) {
      if (aliases == null)
        this.aliases = new LinkedHashSet<Name>();
      if (space == null)
        space = this.name.space;
      aliases.add(new Name(name, space));
    }
    public Set<String> getAliases() {
      Set<String> result = new LinkedHashSet<String>();
      if (aliases != null)
        for (Name alias : aliases)
          result.add(alias.full);
      return result;
    }
    public boolean writeNameRef(Names names, JsonGenerator gen)
      throws IOException {
      if (this.equals(names.get(name))) {
        gen.writeString(name.getQualified(names.space()));
        return true;
      } else if (name.name != null) {
        names.put(name, this);
      }
      return names.customWrite(this, gen);
    }
    public void writeName(Names names, JsonGenerator gen) throws IOException {
      name.writeName(names, gen);
    }
    public boolean equalNames(NamedSchema that) {
      return this.name.equals(that.name);
    }
    @Override
    int computeHash() {
      return name.hashCode();
    }
    public void aliasesToJson(JsonGenerator gen) throws IOException {
      if (aliases == null || aliases.isEmpty()) return;
      gen.writeFieldName("aliases");
      gen.writeStartArray();
      for (Name alias : aliases)
        gen.writeString(alias.getQualified(name.space));
      gen.writeEndArray();
    }

  }

  /**
   * Useful as key of {@link Map}s when traversing two schemas at the same time
   * and need to watch for recursion.
   */
  public static final class SeenPair {
    private Object s1;
    private Object s2;

    public SeenPair(final Object s1, final Object s2) {
      this.s1 = s1;
      this.s2 = s2;
    }

    public boolean equals(final Object o) {
      if (!(o instanceof SeenPair))
        return false;
      return this.s1 == ((SeenPair) o).s1 && this.s2 == ((SeenPair) o).s2;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(s1) + System.identityHashCode(s2);
    }
  }

 private static final ThreadLocal<Set> SEEN_EQUALS = ThreadLocal.withInitial(HashSet::new);


  @SuppressWarnings(value="unchecked")
  private static class RecordSchema extends NamedSchema {
    private List<Field> fields;
    private Map<String, Field> fieldMap;
    private Map<String, Set<Field>> fieldByAliasMap;
    private final boolean isError;
    public RecordSchema(Name name, String doc, boolean isError) {
      super(Type.RECORD, name, doc);
      this.isError = isError;
    }

    public RecordSchema(Name name, String doc, boolean isError,
                        List<Field> fields) {
      super(Type.RECORD, name, doc);
      this.isError = isError;
      setFields(fields);
    }

    public boolean isError() { return isError; }

    @Override
    public int computeHash() {
      int nameHash = super.computeHash();
      return nameHash == 0 ? (fields == null ? 0 : fields.size()) : nameHash;
    }

    @Override
    public Field getField(@Nonnull String fieldname) {
      if (fields == null)
        throw new AvroRuntimeException("Schema fields not set yet for " + this + ", cannot get " + fieldname);
      return fieldMap.get(fieldname);
    }

    @Override
    public Set<Field> getFieldsByNameOrAlias(@Nonnull String fieldnameoralias) {
      if (fields == null)
        throw new AvroRuntimeException("Schema fields not set yet for " + this + ", cannot get " + fieldnameoralias);
      if (fieldByAliasMap == null) {
        return Collections.singleton(fieldMap.get(fieldnameoralias));
      }
      return fieldByAliasMap.get(fieldnameoralias);
    }

    @Override
    public List<Field> getFields() {
      if (fields == null)
        throw new AvroRuntimeException("Schema fields not set yet for " + this);
      return fields;
    }

    @Override
    public void setFields(List<Field> fields) {
      if (this.fields != null) {
        throw new AvroRuntimeException("Fields are already set");
      }
      int i = 0;
      int size = fields.size();
      fieldMap = Maps.newHashMapWithExpectedSize(size);
      LockableArrayList ff = new LockableArrayList(size);
      int nrAliasses = 0;
      for (Field f : fields) {
        if (f.position != -1)
          throw new AvroRuntimeException("Field already used: " + f);
        f.position = i++;
        Field existingField = fieldMap.put(f.name(), f);
        if (existingField != null) {
          throw new AvroRuntimeException(String.format(
              "Duplicate field %s in record %s: %s and %s.",
              f.name(), name, f, existingField));
        }
        nrAliasses += f.aliases().size();
        ff.add(f);
      }
      if (nrAliasses > 0) {
        fieldByAliasMap = Maps.newHashMapWithExpectedSize(size + nrAliasses);
        for (Field f : fields) {
          fieldByAliasMap.put(f.name(), Collections.singleton(f));
          for (String alias : f.aliases()) {
            Set<Field> existingFields = fieldByAliasMap.put(alias, Collections.singleton(f));
            if (existingFields != null) {
              Set<Field> newSet = Sets.newHashSetWithExpectedSize(existingFields.size() + 1);
              newSet.add(f);
              newSet.addAll(existingFields);
              fieldByAliasMap.put(alias, newSet);
            }
          }
        }
      } else {
        fieldByAliasMap = null;
      }

      this.fields = ff.lock();
      this.hashCode = NO_HASHCODE;
    }

    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof RecordSchema)) return false;
      RecordSchema that = (RecordSchema)o;
      if (!equalCachedHash(that)) return false;
      if (!equalNames(that)) return false;
      if (!props.equals(that.props)) return false;
      Set seen = SEEN_EQUALS.get();
      SeenPair here = new SeenPair(this, o);
      if (seen.contains(here)) return true;       // prevent stack overflow
      boolean first = seen.isEmpty();
      try {
        seen.add(here);
        return Objects.equals(fields, ((RecordSchema)o).fields);
      } finally {
        if (first) seen.clear();
      }
    }

    public void toJson(Names names, JsonGenerator gen) throws IOException {
      if (writeNameRef(names, gen)) return;
      String savedSpace = names.space;            // save namespace
      gen.writeStartObject();
      gen.writeStringField("type", isError?"error":"record");
      writeName(names, gen);
      names.space = name.space;                   // set default namespace
      if (getDoc() != null)
        gen.writeStringField("doc", getDoc());

      if (fields != null) {
        gen.writeFieldName("fields");
        fieldsToJson(names, gen);
      }

      writeProps(gen);
      aliasesToJson(gen);
      gen.writeEndObject();
      names.space = savedSpace;                   // restore namespace
    }

    void fieldsToJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartArray();
      for (Field f : fields) {
        gen.writeStartObject();
        gen.writeStringField("name", f.name());
        gen.writeFieldName("type");
        f.schema().toJson(names, gen);
        String doc1 = f.doc();
        if (doc1 != null) {
          gen.writeStringField("doc", doc1);
        }
        JsonNode defaultValue = f.defaultValue();
        if (defaultValue != null) {
          gen.writeFieldName("default");
          gen.writeTree(defaultValue);
        }
        if (f.order() != Field.Order.ASCENDING)
          gen.writeStringField("order", f.order().name);
        if (f.aliases != null && !f.aliases.isEmpty()) {
          gen.writeFieldName("aliases");
          gen.writeStartArray();
          for (String alias : f.aliases)
            gen.writeString(alias);
          gen.writeEndArray();
        }
        f.writeProps(gen);
        gen.writeEndObject();
      }
      gen.writeEndArray();
    }
  }

  public static class EnumSchema extends NamedSchema {
    private final List<String> symbols;
    private final List<String> stringSymbols;
    private final Map<String,Integer> ordinals;
    private volatile Map<String, Set<String>> aliases;
    private String enumDefault;

    public EnumSchema(Name name, String doc,
        LockableArrayList<String> symbols, @Nullable  String enumDefault) {
      super(Type.ENUM, name, doc);
      this.symbols = symbols.lock();
      this.stringSymbols = new ArrayList<>(symbols);
      this.ordinals = Maps.newHashMapWithExpectedSize(symbols.size());
      int i = 0;
      for (String symbol : symbols)
        if (ordinals.put(validateName(symbol), i++) != null)
          throw new SchemaParseException("Duplicate enum symbol: "+symbol);
      if (enumDefault != null) {
        if (!ordinals.containsKey(enumDefault)) {
          throw new IllegalArgumentException("Invalid enum default/fallback value " + enumDefault
                  + " for " + name + ", symbols = " + symbols);
        }
        this.enumDefault = enumDefault;
        props.put("default", new TextNode(enumDefault));
      } else {
        this.enumDefault = null;
      }
    }


    @Override
    public List<String> getEnumSymbols() { return symbols; }

    @Override
    public List<String> getEnumStringSymbols() { return stringSymbols; }

    @Override
    public  Map<String, Set<String>> getEnumSymbolAliases() {
      return aliases;
    }

    @Deprecated
    public  Map<String, Set<String>> getSymbolAliases() {
      return aliases;
    }

    private void setAliases(JsonNode node) {
      Map<String, List<String>> sa = (Map<String, List<String>>) JacksonUtils.toObject(node);
      if (sa == null) {
        aliases = Collections.EMPTY_MAP;
      } else {
        Map<String, Set<String>> result = Maps.newHashMapWithExpectedSize(sa.size());
        for (Map.Entry<String, List<String>> entry : sa.entrySet()) {
          String symbol = entry.getKey();
          if (!symbols.contains(symbol)) {
            throw new AvroTypeException("Enum value referenced in symbolAliases not defined: " + symbol);
          }
          result.put(symbol, new HashSet<>(entry.getValue()));
        }
        aliases = result;
      }
    }

    private void setStringSymbols(JsonNode node) {
      Map<String, String> strSymbols = (Map<String, String>) JacksonUtils.toObject(node);
      if (strSymbols != null) {
        for (Map.Entry<String, String> entry : strSymbols.entrySet()) {
          final String key = entry.getKey();
          final String val = entry.getValue();
          Integer ordinal = ordinals.get(key);
          if (ordinal == null) {
            throw new AvroTypeException("enum value referenced in stringSymbol not defined: " + key);
          } else {
            stringSymbols.set(ordinal, val);
            Integer ex = ordinals.put(val, ordinal);
            if (ex != null && ex.intValue() != ordinal.intValue()) {
              throw new AvroTypeException("Same string " + val + " pointing to " + ex + " and " + ordinal);
            }
          }
        }
      }
    }

    /**
     * @deprecated use getEnumDefault() instead.
     */
    @Deprecated
    public String getFallbackSymbol() {
      return enumDefault;
    }

    @Override
    public String getEnumDefault() {
      return enumDefault;
    }

    @Deprecated
    public String getStringSymbol(final String symbol) {
      return getEnumStringSymbol(symbol);
    }

    @Override
    public String getEnumStringSymbol(final String symbol) {
      Map<String, String> strSymbols = (Map<String, String>) this.getObjectProp("stringSymbols");
      if (strSymbols == null) {
        return symbol;
      } else {
        String result = strSymbols.get(symbol);
        return (result == null) ? symbol : result;
      }
    }

    private void setFalbackSymbol(String textValue) {
      if (!symbols.contains(textValue)) {
        throw new AvroTypeException("Enum fallbackSymbol " + textValue + " must be one of " + symbols);
      }
      if (this.enumDefault == null) {
        this.enumDefault = textValue;
      } else {
         throw new AvroTypeException("Enum default symbol " + textValue + " is already set to " + this.enumDefault);
      }
    }

    @Override
    public void addProp(String name, JsonNode value) {
      switch (name) {
        case "fallbackSymbol":
        case "default":
          String  tVal = value.textValue();
          if (this.enumDefault == null) {
            setFalbackSymbol(tVal);
            break;
          } else if (this.enumDefault.equals(tVal)) {
            return;
          } else {
            throw new IllegalStateException("Denum  default already set to " + this.enumDefault
              + " cannot overwrite with " + tVal + " for " + this);
          }
        case "symbolAliases":
          setAliases(value);
          break;
        case "stringSymbols":
          setStringSymbols(value);
          break;
      }
      super.addProp(name, value);
    }

    @Override
    public void addJsonProps(Map<String, JsonNode> xtraProps) {
      for (Map.Entry<String, JsonNode> entry : xtraProps.entrySet()) {
        addProp(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public boolean hasEnumSymbol(String symbol) {
      return ordinals.containsKey(symbol);
    }

    @Override
    public boolean hasEnumSymbolOrAlias(String symbol) {
      return getEnumSymbolOrAliasOrdinal(symbol) >= 0;
    }

    @Override
    public int getEnumOrdinal(String symbol) { return ordinals.get(symbol); }

    @Override
    public int getEnumSymbolOrAliasOrdinal(String soa) {
      Integer ordinal = ordinals.get(soa);
      if (ordinal != null) {
        return ordinal;
      }
      Map<String, Set<String>> aliasses = getEnumSymbolAliases();
      if (aliasses != null) {
        for (Map.Entry<String, Set<String>> entry : aliasses.entrySet()) {
          if (entry.getValue().contains(soa)) {
            return ordinals.get(entry.getKey());
          }
        }
      }
      return -1;
    }

    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof EnumSchema)) return false;
      EnumSchema that = (EnumSchema)o;
      return equalCachedHash(that)
        && equalNames(that)
        && symbols.equals(that.symbols)
        && props.equals(that.props);
    }

    @Override
    int computeHash() {
      return super.computeHash() + 29 * symbols.size();
    }

    public void toJson(Names names, JsonGenerator gen) throws IOException {
      if (writeNameRef(names, gen)) return;
      gen.writeStartObject();
      gen.writeStringField("type", "enum");
      writeName(names, gen);
      String doc = getDoc();
      if (doc != null) {
        gen.writeStringField("doc", doc);
      }
      gen.writeArrayFieldStart("symbols");
      for (String symbol : symbols) {
        gen.writeString(symbol);
      }
      gen.writeEndArray();
      writeProps(gen);
      aliasesToJson(gen);
      gen.writeEndObject();
    }
  }

  private static class ArraySchema extends Schema {
    private final Schema elementType;
    public ArraySchema(Schema elementType) {
      super(Type.ARRAY);
      this.elementType = elementType;
    }
    public Schema getElementType() { return elementType; }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof ArraySchema)) return false;
      ArraySchema that = (ArraySchema)o;
      return equalCachedHash(that)
        && elementType.equals(that.elementType)
        && props.equals(that.props);
    }
    @Override int computeHash() {
      return  super.computeHash() + 7 * elementType.hashCode();
    }

    public void toJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeStringField("type", "array");
      gen.writeFieldName("items");
      elementType.toJson(names, gen);
      writeProps(gen);
      gen.writeEndObject();
    }
  }

  private static class MapSchema extends Schema {
    private final Schema valueType;
    public MapSchema(Schema valueType) {
      super(Type.MAP);
      this.valueType = valueType;
    }
    public Schema getValueType() { return valueType; }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof MapSchema)) return false;
      MapSchema that = (MapSchema)o;
      return equalCachedHash(that)
        && valueType.equals(that.valueType)
        && props.equals(that.props);
    }
    @Override int computeHash() {
      return super.computeHash() + 13 * valueType.hashCode();
    }

    public void toJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeStringField("type", "map");
      gen.writeFieldName("values");
      valueType.toJson(names, gen);
      writeProps(gen);
      gen.writeEndObject();
    }
  }

  private static class UnionSchema extends Schema {
    private final List<Schema> types;
    private final Map<String,Integer> indexByName;

    public UnionSchema(LockableArrayList<Schema> types) {
      super(Type.UNION);
      this.types = types.lock();
      int index = 0;
      indexByName = Maps.newHashMapWithExpectedSize(types.size());
      for (Schema type : types) {
        if (type.getType() == Type.UNION) {
          throw new AvroRuntimeException("Nested union: " + this);
        }
        String name = type.getFullName();
        if (name == null) {
          throw new AvroRuntimeException("Nameless in union:"+this);
        }
        if (indexByName.put(name, index++) != null) {
          throw new AvroRuntimeException("Duplicate in union:" + name);
        }
      }
    }

    public List<Schema> getTypes() {
      return types;
    }

    public Integer getIndexNamed(String name) {
      return indexByName.get(name);
    }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof UnionSchema)) return false;
      UnionSchema that = (UnionSchema)o;
      return equalCachedHash(that)
        && types.equals(that.types)
        && props.equals(that.props);
    }
    @Override int computeHash() {
      int hash = super.computeHash();
      for (Schema type : types)
        hash += 19 * type.hashCode();
      return hash;
    }

    @Override
    public void addProp(String name, String value) {
      throw new AvroRuntimeException("Can't set properties on a union: "+this);
    }

    public void toJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartArray();
      for (Schema type : types) {
        type.toJson(names, gen);
      }
      gen.writeEndArray();
    }
  }

  private static class FixedSchema extends NamedSchema {
    private final int size;
    public FixedSchema(Name name, String doc, int size) {
      super(Type.FIXED, name, doc);
      if (size < 0)
        throw new IllegalArgumentException("Invalid fixed size: "+size);
      this.size = size;
    }
    public int getFixedSize() { return size; }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof FixedSchema)) return false;
      FixedSchema that = (FixedSchema)o;
      return equalCachedHash(that)
        && equalNames(that)
        && size == that.size
        && props.equals(that.props);
    }

    @Override
    public void toJson(Names names, JsonGenerator gen) throws IOException {
      if (writeNameRef(names, gen)) return;
      gen.writeStartObject();
      gen.writeStringField("type", "fixed");
      writeName(names, gen);
      if (getDoc() != null)
        gen.writeStringField("doc", getDoc());
      gen.writeNumberField("size", size);
      writeProps(gen);
      aliasesToJson(gen);
      gen.writeEndObject();
    }

  }

  private static class StringSchema extends Schema {
    public StringSchema() { super(Type.STRING); }
  }

  private static class BytesSchema extends Schema {
    public BytesSchema() { super(Type.BYTES); }
  }

  private static class IntSchema extends Schema {
    public IntSchema() { super(Type.INT); }
  }

  private static class LongSchema extends Schema {
    public LongSchema() { super(Type.LONG); }
  }

  private static class FloatSchema extends Schema {
    public FloatSchema() { super(Type.FLOAT); }
  }

  private static class DoubleSchema extends Schema {
    public DoubleSchema() { super(Type.DOUBLE); }
  }

  private static class BooleanSchema extends Schema {
    public BooleanSchema() { super(Type.BOOLEAN); }
  }

  private static class NullSchema extends Schema {
    public NullSchema() { super(Type.NULL); }
  }

  /** A parser for JSON-format schemas.  Each named schema parsed with a parser
   * is added to the names known to the parser so that subsequently parsed
   * schemas may refer to it by name. */
  public static class Parser {
    private final Names names;
    private boolean validate = true;
    private boolean validateDefaults = false;

    public Parser(final Names names) {
      this.names = names;
    }

    public Parser() {
      SchemaResolver resolver = SchemaResolvers.getDefault();
      names = new AvroNamesRefResolver(resolver);
    }

    /** Adds the provided types to the set of defined, named types known to
     * this parser. */
    public Parser addTypes(Map<String,Schema> types) {
      for (Schema s : types.values())
        names.add(s);
      return this;
    }

    /** Returns the set of defined, named types known to this parser. */
    public Map<String,Schema> getTypes() {
      Collection<Schema> values = names.values();
      Map<String,Schema> result = Maps.newLinkedHashMapWithExpectedSize(values.size());
      for (Schema s : values) {
        result.put(s.getFullName(), s);
      }
      return result;
    }

    /** Enable or disable name validation. */
    public Parser setValidate(boolean validate) {
      this.validate = validate;
      return this;
    }

    /** True iff names are validated.  True by default. */
    public boolean getValidate() { return this.validate; }

    /** Enable or disable default value validation. */
    public Parser setValidateDefaults(boolean validateDefaults) {
      this.validateDefaults = validateDefaults;
      return this;
    }

    /** True iff default values are validated.  False by default. */
    public boolean getValidateDefaults() { return this.validateDefaults; }

    /** Parse a schema from the provided file.
     * If named, the schema is added to the names known to this parser. */
    public Schema parse(File file) throws IOException {
      return parse(FACTORY.createParser(file));
    }

    public Schema parse(InputStream in) throws IOException {
      return parse(in, LogicalTypes.isAllowUndefinedLogicalTypes());
    }

    public Schema parse(Reader in) throws IOException {
      return parse(in, LogicalTypes.isAllowUndefinedLogicalTypes());
    }

    public Schema parse(Reader in, boolean allowUndefinedLogicalTypes) throws IOException {
      return parse(FACTORY.createParser(in), allowUndefinedLogicalTypes);
    }

    /** Parse a schema from the provided stream.
     * If named, the schema is added to the names known to this parser. */
    public Schema parse(InputStream in, final boolean allowUndefinedLogicalTypes) throws IOException {
      return parse(FACTORY.createParser(in), allowUndefinedLogicalTypes);
    }

    /** Read a schema from one or more json strings */
    public Schema parse(String s, String... more) {
      StringBuilder b = new StringBuilder(s.length() * (more.length + 1));
      b.append(s);
      for (String part : more) {
        b.append(part);
      }
      return parse(b.toString());
    }

    public Schema parse(String s) {
      return parse(s, LogicalTypes.isAllowUndefinedLogicalTypes());
    }
    /** Parse a schema from the provided string.
     * If named, the schema is added to the names known to this parser. */
    public Schema parse(String s, final boolean allowUndefinedLogicalTypes) {
      try {
        return parse(FACTORY.createParser(new StringReader(s)), allowUndefinedLogicalTypes);
      } catch (IOException e) {
        throw new SchemaParseException(e);
      }
    }

    public Schema parse(JsonParser parser) throws IOException {
      return parse(parser, LogicalTypes.isAllowUndefinedLogicalTypes());
    }

    public Schema parse(JsonParser parser, final boolean allowUndefinedLogicalTypes) throws IOException {
      try {
        return Schema.parse(MAPPER.readTree(parser), names, allowUndefinedLogicalTypes, validate, validateDefaults);
      } catch (JsonParseException e) {
        throw new SchemaParseException(e);
      }
    }
  }

  /**
   * Constructs a Schema object from JSON schema file <tt>file</tt>.
   * The contents of <tt>file</tt> is expected to be in UTF-8 format.
   * @param file  The file to read the schema from.
   * @return  The freshly built Schema.
   * @throws IOException if there was trouble reading the contents
   * @throws JsonParseException if the contents are invalid
   * @deprecated use {@link Schema.Parser} instead.
   */
  public static Schema parse(File file) throws IOException {
    return new Parser().parse(file);
  }

  /**
   * Constructs a Schema object from JSON schema stream <tt>in</tt>.
   * The contents of <tt>in</tt> is expected to be in UTF-8 format.
   * @param in  The input stream to read the schema from.
   * @return  The freshly built Schema.
   * @throws IOException if there was trouble reading the contents
   * @throws JsonParseException if the contents are invalid
   * @deprecated use {@link Schema.Parser} instead.
   */
  public static Schema parse(InputStream in) throws IOException {
    return new Parser().parse(in);
  }

  /** Construct a schema from <a href="http://json.org/">JSON</a> text.
   * @deprecated use {@link Schema.Parser} instead.
   */
  public static Schema parse(String jsonSchema) {
    return new Parser().parse(jsonSchema);
  }

  /** Construct a schema from <a href="http://json.org/">JSON</a> text.
   * @param validate true if names should be validated, false if not.
   * @deprecated use {@link Schema.Parser} instead.
   */
  public static Schema parse(String jsonSchema, boolean validate) {
    return new Parser().setValidate(validate).parse(jsonSchema);
  }

  static final Map<String,Type> PRIMITIVES = new HashMap<String,Type>();
  static {
    PRIMITIVES.put("string",  Type.STRING);
    PRIMITIVES.put("bytes",   Type.BYTES);
    PRIMITIVES.put("int",     Type.INT);
    PRIMITIVES.put("long",    Type.LONG);
    PRIMITIVES.put("float",   Type.FLOAT);
    PRIMITIVES.put("double",  Type.DOUBLE);
    PRIMITIVES.put("boolean", Type.BOOLEAN);
    PRIMITIVES.put("null",    Type.NULL);
  }

  public static class Names extends LinkedHashMap<Name, Schema> {
    private String space;                         // default namespace

    public Names() {
      this.space = null;
    }
    public Names(String space) { this.space = space; }

    public String space() { return space; }
    public void space(String space) { this.space = space; }

    public Schema get(String o) {
      Type primitive = PRIMITIVES.get(o);
      if (primitive != null) {
        return Schema.create(primitive);
      }
      Name name = new Name(o, space);
      if (!containsKey(name)) {
        // if not in default try anonymous
        name = new Name(o, "");
      }
      return super.get(name);
    }

    @Override
    public Schema get(Object o) {
      Name name;
      if (o instanceof String) {
        return get((String) o);
      } else {
        name = (Name)o;
      }
      return super.get(name);
    }
    public boolean contains(Schema schema) {
      return get(((NamedSchema)schema).name) != null;
    }
    public void add(Schema schema) {
      put(((NamedSchema)schema).name, schema);
    }
    @Override
    public Schema put(Name name, Schema schema) {
      Schema result = super.put(name, schema);
      if (result != null) {
        throw new SchemaParseException("Can't redefine: " + name);
      }
      return result;
    }

    public Schema customRead(Function<String, JsonNode> object) {
      return null;
    }

    public boolean customWrite(Schema schema, JsonGenerator gen) throws IOException {
      return false;
    }

  }

  private static String validateName(String name) {
    int length = name.length();
    if (length == 0) {
      throw new SchemaParseException("Empty name");
    }
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      throw new SchemaParseException("Illegal initial character: " + name);
    }
    for (int i = 1; i < length; i++) {
      char c = name.charAt(i);
      if (!(Character.isLetterOrDigit(c) || c == '_')) {
        throw new SchemaParseException("Illegal character in: " + name);
      }
    }
    return name;
  }

  private static JsonNode validateDefault(String fieldName, Schema schema,
          JsonNode defaultValue) {
    if (defaultValue != null) {
      try {
        validateDefault(schema, defaultValue);
      } catch (RuntimeException e) {
        throw new AvroTypeException("Invalid default for field " + fieldName
                + ": " + defaultValue + " not a " + schema, e);     // throw exception
      }
    }
    return defaultValue;
  }

  private static void validateDefault(Schema schema, JsonNode defaultValue) {
    LogicalType lType = schema.getLogicalType();
    if (lType != null) {
      try {
        ExtendedJsonDecoder dec = new ExtendedJsonDecoder(schema, defaultValue.toString());
        GenericDatumReader reader = new GenericDatumReader(schema);
        reader.read(null, dec);
        return;
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }
    switch (schema.getType()) {
      case STRING:
      case BYTES:
      case ENUM:
      case FIXED:
        if (!defaultValue.isTextual()) {
          throw new AvroTypeException("Default value must be testual");
        }
        break;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        if (!defaultValue.isNumber()) {
          throw new AvroTypeException("Default value must be number");
        }
        break;
      case BOOLEAN:
        if (!defaultValue.isBoolean()) {
          throw new AvroTypeException("Default value must be boolean");
        }
        break;
      case NULL:
        if (!defaultValue.isNull()) {
          throw new AvroTypeException("Default value must be null");
        }
        break;
      case ARRAY:
        if (!defaultValue.isArray()) {
          throw new AvroTypeException("Default value must be array");
        }
        for (JsonNode element : defaultValue) {
          validateDefault(schema.getElementType(), element);
        }
        break;
      case MAP:
        if (!defaultValue.isObject()) {
          throw new AvroTypeException("Default value must be object");
        }
        for (JsonNode value : defaultValue) {
          validateDefault(schema.getValueType(), value);
        }
        break;
      case UNION:                                   // union default: first branch
        validateDefault(schema.getTypes().get(0), defaultValue);
        break;
      case RECORD:
        if (!defaultValue.isObject()) {
          throw new AvroTypeException("Default value must be object");
        }
        for (Field field : schema.getFields()) {
          validateDefault(field.schema(),
                  defaultValue.has(field.name())
                  ? defaultValue.get(field.name())
                  : field.defaultValue());
        }
        break;
      default:
        throw new IllegalStateException("Unsupported schema type: " + schema);
    }
  }

  public static  Schema parse(JsonNode schema, Names names) {
    return parse(schema, names,  LogicalTypes.isAllowUndefinedLogicalTypes(), true, true);
  }

  /** @see #parse(String) */
  public static Schema parse(JsonNode schema, Names names,
          final boolean allowUndefinedLogicalTypes, final boolean validateNames, boolean validateDefaults) {
    if (schema.isTextual()) {                     // name
      Schema result = names.get(schema.textValue());
      if (result == null)
        throw new SchemaParseException("Undefined name: "+schema);
      return result;
    } else if (schema.isObject()) {
      Schema custom = names.customRead(schema::get);
      if (custom != null) {
        return custom;
      }
      Schema result;
      String type = getRequiredText(schema, "type", "No type");
      Name name = null;
      String savedSpace = names.space();
      String doc = null;
      if ("record".equals(type) || "error".equals(type)
          || "enum".equals(type) || "fixed".equals(type)) {
        String space = getOptionalText(schema, "namespace");
        doc = getOptionalText(schema, "doc");
        if (space == null) {
          space = names.space();
        }
        name = new Name(getOptionalText(schema, "name"),
                        space, validateNames);
        if (name.space != null) {                 // set default namespace
          names.space(name.space);
        }
      }
      Type prType = PRIMITIVES.get(type);
      Set reserved = SCHEMA_RESERVED;
      if (prType != null) {         // primitive
        result = create(prType);
      } else if (type.equals("record") || type.equals("error")) { // record
        result = new RecordSchema(name, doc, type.equals("error"));
        if (name != null && name.name != null) names.add(result);
        JsonNode fieldsNode = schema.get("fields");
        if (fieldsNode == null || !fieldsNode.isArray())
          throw new SchemaParseException("Record has no fields: "+schema);
        List<Field> fields = new ArrayList<Field>(fieldsNode.size());
        for (JsonNode field : fieldsNode) {
          String fieldName = getRequiredText(field, "name", "No field name");
          String fieldDoc = getOptionalText(field, "doc");
          JsonNode fieldTypeNode = field.get("type");
          if (fieldTypeNode == null)
            throw new SchemaParseException("No field type: "+field);
          if (fieldTypeNode.isTextual()
              && names.get(fieldTypeNode.textValue()) == null)
            throw new SchemaParseException
              (fieldTypeNode+" is not a defined name."
               +" The type of the \""+fieldName+"\" field must be"
               +" a defined name or a {\"type\": ...} expression.");
          Schema fieldSchema = parse(fieldTypeNode, names, allowUndefinedLogicalTypes,
                  validateNames, validateDefaults);
          Field.Order order = Field.Order.ASCENDING;
          JsonNode orderNode = field.get("order");
          if (orderNode != null) {
            order = Field.Order.valueOf(orderNode.textValue().toUpperCase(Locale.ENGLISH));
          }
          JsonNode defaultValue = field.get("default");
          Type fieldType = fieldSchema.getType();
          if (defaultValue != null && (Type.FLOAT == fieldType || Type.DOUBLE == fieldType)
              && defaultValue.isTextual())
            defaultValue =
              new DoubleNode(Double.valueOf(defaultValue.textValue()));
          Field f = new Field(fieldName, fieldSchema,
                              fieldDoc, defaultValue, validateDefaults, validateNames, order);
          Iterator<String> i = field.fieldNames();
          while (i.hasNext()) {                       // add field props
            String prop = i.next();
            if (!FIELD_RESERVED.contains(prop))
              f.addProp(prop, field.get(prop));
          }
          f.aliases = parseAliases(field);
          fields.add(f);
        }
        result.setFields(fields);
      } else if ("enum".equals(type)) {           // enum
        reserved = ENUM_RESERVED;
        JsonNode symbolsNode = schema.get("symbols");
        if (symbolsNode == null || !symbolsNode.isArray())
          throw new SchemaParseException("Enum has no symbols: " + schema);
        LockableArrayList<String> symbols = new LockableArrayList<String>(symbolsNode.size());
        for (JsonNode n : symbolsNode) {
          symbols.add(n.textValue());
        }
        JsonNode enumDefault = schema.get("default");
        String defaultSymbol;
        if (enumDefault != null) {
          defaultSymbol = enumDefault.textValue();
        } else {
          enumDefault = schema.get("fallbackSymbol"); //backwards compatible
           if (enumDefault != null) {
              defaultSymbol = enumDefault.textValue();
           } else {
              defaultSymbol = null;
           }
        }
        result = new EnumSchema(name, doc, symbols, defaultSymbol);
        if (name != null) names.add(result);
      } else if ("array".equals(type)) {          // array
        JsonNode itemsNode = schema.get("items");
        if (itemsNode == null)
          throw new SchemaParseException("Array has no items type: "+schema);
        result = new ArraySchema(parse(itemsNode, names, allowUndefinedLogicalTypes,
                validateNames, validateDefaults));
      } else if ("map".equals(type)) {            // map
        JsonNode valuesNode = schema.get("values");
        if (valuesNode == null)
          throw new SchemaParseException("Map has no values type: "+schema);
        result = new MapSchema(parse(valuesNode, names, allowUndefinedLogicalTypes,
                validateNames, validateDefaults));
      } else if ("fixed".equals(type)) {          // fixed
        JsonNode sizeNode = schema.get("size");
        if (sizeNode == null || !sizeNode.isInt())
          throw new SchemaParseException("Invalid or no size: "+schema);
        result = new FixedSchema(name, doc, sizeNode.intValue());
        if (name != null) names.add(result);
      } else {
        throw new SchemaParseException("Type not supported: "+type);
      }
      Iterator<String> i = schema.fieldNames();
      while (i.hasNext()) {                       // add properties
        String prop = i.next();
        if (!reserved.contains(prop))      // ignore reserved
          result.addProp(prop, schema.get(prop));
      }
      result.parseLogicalType(allowUndefinedLogicalTypes);
      names.space(savedSpace);                  // restore space
      if (result instanceof NamedSchema) {
        Set<String> aliases = parseAliases(schema);
        if (aliases != null)                      // add aliases
          for (String alias : aliases)
            result.addAlias(alias);
      }
      return result;
    } else if (schema.isArray()) {                // union
      LockableArrayList<Schema> types =
        new LockableArrayList<Schema>(schema.size());
      for (JsonNode typeNode : schema)
        types.add(parse(typeNode, names, allowUndefinedLogicalTypes, validateNames, validateDefaults));
      return new UnionSchema(types);
    } else {
      throw new SchemaParseException("Schema not yet supported: "+schema);
    }
  }

  private static Set<String> parseAliases(JsonNode node) {
    JsonNode aliasesNode = node.get("aliases");
    if (aliasesNode == null) {
      return null;
    }
    if (!aliasesNode.isArray()) {
      throw new SchemaParseException("aliases not an array: "+node);
    }
    Set<String> aliases = Sets.newLinkedHashSetWithExpectedSize(aliasesNode.size());
    for (JsonNode aliasNode : aliasesNode) {
      if (!aliasNode.isTextual()) {
        throw new SchemaParseException("alias not a string: "+aliasNode);
      }
      aliases.add(aliasNode.textValue());
    }
    return aliases;
  }

  /** Extracts text value associated to key from the container JsonNode,
   * and throws {@link SchemaParseException} if it doesn't exist.
   *
   * @param container Container where to find key.
   * @param key Key to look for in container.
   * @param error String to prepend to the SchemaParseException.
   */
  private static String getRequiredText(JsonNode container, String key,
      String error) {
    String out = getOptionalText(container, key);
    if (null == out) {
      throw new SchemaParseException(error + ": " + container);
    }
    return out;
  }

  /** Extracts text value associated to key from the container JsonNode. */
  private static String getOptionalText(JsonNode container, String key) {
    JsonNode jsonNode = container.get(key);
    return jsonNode != null ? jsonNode.textValue() : null;
  }

  /**
   * Parses a string as Json.
   * @deprecated use {@link org.apache.avro.data.Json#parseJson(String)}
   */
  @Deprecated
  public static JsonNode parseJson(String s) {
    try {
      return MAPPER.readTree(FACTORY.createJsonParser(new StringReader(s)));
    } catch (JsonParseException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Rewrite a writer's schema using the aliases from a reader's schema.  This
   * permits reading records, enums and fixed schemas whose names have changed,
   * and records whose field names have changed.  The returned schema always
   * contains the same data elements in the same order, but with possibly
   * different names. */
  public static Schema applyAliases(Schema writer, Schema reader) {
    if (writer == reader) return writer;          // same schema

    // create indexes of names
    Map<Schema,Schema> seen = new IdentityHashMap<Schema,Schema>(1);
    Map<Name,Name> aliases = new HashMap<Name, Name>(1);
    Map<Name,Map<String,String>> fieldAliases =
      new HashMap<Name, Map<String,String>>(2);
    getAliases(reader, seen, aliases, fieldAliases);

    if (aliases.isEmpty() && fieldAliases.isEmpty())
      return writer;                              // no aliases

    seen.clear();
    return applyAliases(writer, seen, aliases, fieldAliases);
  }

  private static Schema applyAliases(Schema s, Map<Schema,Schema> seen,
                                     Map<Name,Name> aliases,
                                     Map<Name,Map<String,String>> fieldAliases){

    Name name = s instanceof NamedSchema ? ((NamedSchema)s).name : null;
    Schema result = s;
    switch (s.getType()) {
    case RECORD:
      Schema sSch = seen.get(s);
      if (sSch != null) return sSch; // break loops
      Name aname = aliases.get(name);
      if (aname != null) {
        name = aname;
      }
      result = Schema.createRecord(name.full, s.getDoc(), null, s.isError());
      seen.put(s, result);
      List<Field> fields = s.getFields();
      List<Field> newFields = new ArrayList<Field>(fields.size());
      for (Field f : fields) {
        Schema fSchema = applyAliases(f.schema, seen, aliases, fieldAliases);
        String fName = getFieldAlias(name, f.name, fieldAliases);
        Field newF = new Field(fName, fSchema, f.doc, f.defaultValue, f.order);
        newF.props.putAll(f.props);               // copy props
        newFields.add(newF);
      }
      result.setFields(newFields);
      break;


    case ENUM:
      Name aname3 = aliases.get(name);
      if (aname3 != null)
        result = Schema.createEnum(aname3.full, s.getDoc(), null, s.getEnumSymbols(), s.getEnumDefault());
      break;

    case ARRAY:
      Schema elementType = s.getElementType();
      Schema e = applyAliases(elementType, seen, aliases, fieldAliases);
      if (e != elementType)
        result = Schema.createArray(e);
      break;
    case MAP:
      Schema valueType = s.getValueType();
      Schema v = applyAliases(valueType, seen, aliases, fieldAliases);
      if (v != valueType)
        result = Schema.createMap(v);
      break;
    case UNION:
      List<Schema> types1 = s.getTypes();
      List<Schema> types = new ArrayList<Schema>(types1.size());
      for (Schema branch : types1)
        types.add(applyAliases(branch, seen, aliases, fieldAliases));
      result = Schema.createUnion(types);
      break;
    case FIXED:
      Name aname2 = aliases.get(name);
      if (aname2 != null)
        result = Schema.createFixed(aname2.full, s.getDoc(), null, s.getFixedSize());
      break;

    }
    if (result != s)
      result.props.putAll(s.props);        // copy props
    return result;
  }


  private static void getAliases(Schema schema,
                                 Map<Schema,Schema> seen,
                                 Map<Name,Name> aliases,
                                 Map<Name,Map<String,String>> fieldAliases) {
    if (schema instanceof NamedSchema) {
      NamedSchema namedSchema = (NamedSchema)schema;
      if (namedSchema.aliases != null)
        for (Name alias : namedSchema.aliases)
          aliases.put(alias, namedSchema.name);
    }
    switch (schema.getType()) {
    case RECORD:
      if (seen.containsKey(schema)) return;            // break loops
      seen.put(schema, schema);
      RecordSchema record = (RecordSchema)schema;
      for (Field field : schema.getFields()) {
        if (field.aliases != null)
          for (String fieldAlias : field.aliases) {
            Map<String,String> recordAliases = fieldAliases.get(record.name);
            if (recordAliases == null)
              fieldAliases.put(record.name,
                               recordAliases = new HashMap<String,String>());
            recordAliases.put(fieldAlias, field.name);
          }
        getAliases(field.schema, seen, aliases, fieldAliases);
      }
      Map<String, String> fas = fieldAliases.get(record.name);
      if (record.aliases != null && fas != null) {
        for (Name recordAlias : record.aliases) {
          fieldAliases.put(recordAlias, fas);
        }
      }
      break;

    case ARRAY:
      getAliases(schema.getElementType(), seen, aliases, fieldAliases);
      break;
    case MAP:
      getAliases(schema.getValueType(), seen, aliases, fieldAliases);
      break;
    case UNION:
      for (Schema s : schema.getTypes())
        getAliases(s, seen, aliases, fieldAliases);
      break;
    }
  }

  private static String getFieldAlias
    (Name record, String field, Map<Name,Map<String,String>> fieldAliases) {
    Map<String,String> recordAliases = fieldAliases.get(record);
    if (recordAliases == null)
      return field;
    String alias = recordAliases.get(field);
    if (alias == null)
      return field;
    return alias;
  }

  /**
   * No change is permitted on LockableArrayList once lock() has been
   * called on it.
   * @param <E>
   */

  /*
   * This class keeps a boolean variable <tt>locked</tt> which is set
   * to <tt>true</tt> in the lock() method. It's legal to call
   * lock() any number of times. Any lock() other than the first one
   * is a no-op.
   *
   * This class throws <tt>IllegalStateException</tt> if a mutating
   * operation is performed after being locked. Since modifications through
   * iterator also use the list's mutating operations, this effectively
   * blocks all modifications.
   */
  static class LockableArrayList<E> extends ArrayList<E> {
    private static final long serialVersionUID = 1L;
    private boolean locked = false;

    public LockableArrayList() {
    }

    public LockableArrayList(int size) {
      super(size);
    }

    public LockableArrayList(List<E> types) {
      super(types);
    }

    public LockableArrayList(E... types) {
      super(types.length);
      Collections.addAll(this, types);
    }

    public List<E> lock() {
      locked = true;
      return this;
    }

    private void ensureUnlocked() {
      if (locked) {
        throw new IllegalStateException();
      }
    }

    public boolean add(E e) {
      ensureUnlocked();
      return super.add(e);
    }

    public boolean remove(Object o) {
      ensureUnlocked();
      return super.remove(o);
    }

    public E remove(int index) {
      ensureUnlocked();
      return super.remove(index);
    }

    public boolean addAll(Collection<? extends E> c) {
      ensureUnlocked();
      return super.addAll(c);
    }

    public boolean addAll(int index, Collection<? extends E> c) {
      ensureUnlocked();
      return super.addAll(index, c);
    }

    public boolean removeAll(Collection<?> c) {
      ensureUnlocked();
      return super.removeAll(c);
    }

    public boolean retainAll(Collection<?> c) {
      ensureUnlocked();
      return super.retainAll(c);
    }

    public void clear() {
      ensureUnlocked();
      super.clear();
    }

  }

}
