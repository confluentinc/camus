/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoderException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroEncoder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AvroMessageDecoderTest {
  private final SchemaRegistryClient schemaRegistry = new LocalSchemaRegistryClient();
  private final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistry);
  private final KafkaAvroEncoder avroEncoder = new KafkaAvroEncoder(schemaRegistry);

  private IndexedRecord createAvroRecordVersion1() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                        "\"name\": \"User\"," +
                        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  private IndexedRecord createAvroRecordVersion2() {
    String userSchemaWithId = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                      "\"name\": \"User\"," +
                      "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}, "
                      + "{\"name\": \"id\", \"type\": \"int\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchemaWithId);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    avroRecord.put("id", 1);
    return avroRecord;
  }

  private AvroMessageDecoder createAvroDecoder(
      String topic, boolean newProducer, SchemaRegistryClient schemaRegisry ) {
    Properties props = new Properties();
    // Required by AvroMessageDecoder, but not needed by LocalSchemaRegistry
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              org.apache.kafka.common.serialization.ByteArraySerializer.class);
    props.put("schema.registry.url", "http://localhost:8081");
    if (!newProducer) {
      props.put("is.new.producer", "false");
    }
    AvroMessageDecoder decoder = new AvroMessageDecoder(schemaRegisry);
    decoder.init(props, topic);
    return decoder;
  }

  @Test
  public void testAvroDecoder() {
    String topic = "testAvro";
    Object avroRecord = createAvroRecordVersion1();
    byte[] payload = avroSerializer.serialize(topic, avroRecord);
    AvroMessageDecoder decoder = createAvroDecoder(topic, true, schemaRegistry);

    CamusWrapper<Record> wrapper = decoder.decode(payload);
    Object record = wrapper.getRecord();
    assertEquals(avroRecord, record);

    payload= avroEncoder.toBytes(avroRecord);
    AvroMessageDecoder decoder2 = createAvroDecoder(topic, false, schemaRegistry);
    wrapper = decoder2.decode(payload);
    record = wrapper.getRecord();
    assertEquals(avroRecord, record);
  }

  @Test
  public void testAvroDecoderFailure() {
    String topic = "testAvro";
    Object avroRecord = createAvroRecordVersion1();
    byte[] payload = avroSerializer.serialize(topic, avroRecord);
    AvroMessageDecoder decoder = createAvroDecoder(topic, true, schemaRegistry);
    // decode() initializes latestSchama and latestVersion in AvroMessageDecoder
    // This is needed to handle both old and new producer as we don't know teh
    // avro record name in init()
    decoder.decode(payload);
    Object avroRecord2 = createAvroRecordVersion2();
    payload = avroSerializer.serialize(topic, avroRecord2);
    try {
      decoder.decode(payload);
      fail("AvroMessageDecoder should not be able to decode Avro record with new schema version");
    } catch (MessageDecoderException e) {
      assertEquals(e.getMessage(),
                   "Producer schema is newer than the schema known to Camus");
    }
  }
}
