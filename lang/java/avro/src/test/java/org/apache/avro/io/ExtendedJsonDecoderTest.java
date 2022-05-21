/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.ExtendedGenericDatumWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.spf4j.base.avro.LogLevel;
import org.spf4j.base.avro.LogRecord;

public class ExtendedJsonDecoderTest {


  @Test
  public void testDecoding() throws IOException {
    String data = Resources.toString(Resources.getResource("testData.json"), Charsets.UTF_8);
    String writerSchemaStr = Resources.toString(Resources.getResource("testDataWriterSchema.json"), Charsets.UTF_8);
    String readerSchemaStr = Resources.toString(Resources.getResource("testDataReaderSchema.json"), Charsets.UTF_8);
    Schema writerSchema = new Schema.Parser().parse(writerSchemaStr);
    Schema readerSchema = new Schema.Parser().parse(readerSchemaStr);
    ByteArrayInputStream bis = new ByteArrayInputStream(data.getBytes(Charsets.UTF_8));
    ExtendedJsonDecoder decoder = new ExtendedJsonDecoder(writerSchema, bis);
    GenericDatumReader reader = new GenericDatumReader(writerSchema, readerSchema);
    GenericRecord testData = (GenericRecord) reader.read(null, decoder);
    Assert.assertEquals(Long.valueOf(1L), ((Map<String, Long>) testData.get("someMap")).get("A"));
    Assert.assertEquals("caca", testData.get("someField").toString());
  }

  @Test
  public void testBooleanHandling() throws IOException {
    Schema recordSchema = SchemaBuilder.record("TestRecord").fields()
            .name("boolVal").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
            .name("mapVal").type(Schema.createMap(Schema.create(Schema.Type.BOOLEAN))).noDefault()
            .name("defBoolVal").type(Schema.create(Schema.Type.BOOLEAN)).withDefault(false)
            .endRecord();

    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("boolVal", true);
    record.put("defBoolVal", false);
    record.put("mapVal", ImmutableMap.of("a", true, "b", false,
            "c", true,  "d", false));
    Assert.assertTrue((Boolean) serDeser(record).get("boolVal"));
    Assert.assertFalse((Boolean) serDeser(record).get("defBoolVal"));
    Assert.assertFalse((Boolean)((Map<String, Boolean>) serDeser(record).get("mapVal"))
            .get(new Utf8("b")));

  }


  @Test
  public void testDoubleHandling() throws IOException {
    Schema recordSchema = SchemaBuilder.record("TestRecord").fields()
            .name("doubleVal").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .name("mapVal").type(Schema.createMap(Schema.create(Schema.Type.DOUBLE))).noDefault()
            .name("defDoubleVal").type(Schema.create(Schema.Type.DOUBLE)).withDefault(Double.NaN)
            .endRecord();

    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("doubleVal", Double.NaN);
    record.put("defDoubleVal", Double.NaN);
    record.put("mapVal", ImmutableMap.of("a", 0.1, "b", Double.NaN,
            "c", Double.POSITIVE_INFINITY, "d", 0));
    Assert.assertTrue(Double.isNaN((double) serDeser(record).get("doubleVal")));
    Assert.assertTrue(Double.isNaN((double) ((Map<String, Double>) serDeser(record).get("mapVal")).get(new Utf8("b"))));


    record = new GenericData.Record(recordSchema);
    record.put("doubleVal", Double.POSITIVE_INFINITY);
    record.put("defDoubleVal", Double.NaN);
    record.put("mapVal", ImmutableMap.of("a", 0.1, "b", Double.NaN, "c", Double.POSITIVE_INFINITY));
    double serDeser = (double) serDeser(record).get("doubleVal");
    Assert.assertTrue(Double.isInfinite(serDeser));
    Assert.assertTrue(serDeser > 0);

    record = new GenericData.Record(recordSchema);
    record.put("doubleVal", Double.NEGATIVE_INFINITY);
    record.put("defDoubleVal", Double.NaN);
    record.put("mapVal", ImmutableMap.of("a", 0.1, "b", Double.NaN, "c", Double.POSITIVE_INFINITY));
    serDeser = (double) serDeser(record).get("doubleVal");
    Assert.assertTrue(Double.isInfinite(serDeser));
    Assert.assertTrue(serDeser < 0);
  }

  public GenericRecord  serDeser(GenericData.Record record) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Schema schema = record.getSchema();
    ExtendedJsonEncoder encoder = new ExtendedJsonEncoder(schema, bos);
    GenericDatumWriter writer = new GenericDatumWriter(schema);
    writer.write(record, encoder);
    encoder.flush();
    System.out.println(bos.toString());
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ExtendedJsonDecoder decoder = new ExtendedJsonDecoder(schema, bis);
    GenericDatumReader reader = new GenericDatumReader(schema, schema);
    GenericRecord testData = (GenericRecord) reader.read(null, decoder);
    return testData;
  }


  @Test
  public void  testSchemaParsing() throws IOException {
    Assert.assertEquals("[]", LogRecord.getClassSchema().getField("xtra").defaultVal());
  }

  @Test
  public void  testDefaultValueSerialization() throws IOException {

    LogRecord rec = new LogRecord("origin", "trId", LogLevel.DEBUG,
            Instant.MIN, "logger", "thr", "message", Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(), null, Collections.emptyList());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ExtendedJsonEncoder encoder = new ExtendedJsonEncoder(rec.getSchema(), bos);
    GenericDatumWriter writer = new ExtendedGenericDatumWriter(rec.getSchema());
    writer.write(rec, encoder);
    encoder.flush();
    System.out.println(bos.toString());
    Assert.assertThat(bos.toString(), Matchers.not(Matchers.containsString("\"throwable\":null")));

  }


}
