package com.linkedin.camus.etl.kafka.coders;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;

import com.linkedin.camus.coders.CamusWrapper;


public class LatestSchemaKafkaAvroMessageDecoder extends KafkaAvroMessageDecoder {

  @Override
  public CamusWrapper<Record> decode(byte[] payload) {
    try {
      GenericDatumReader<Record> reader = new GenericDatumReader<Record>();

      Schema schema = super.registry.getLatestSchemaByTopic(super.topicName).getSchema();

      reader.setSchema(schema);

      this.camusWrapper.set(reader.read(null, decoderFactory.jsonDecoder(schema, new String(payload,
              //Message.payloadOffset(message.magic()),
              Message.MagicOffset(), payload.length - Message.MagicOffset()))));

      return this.camusWrapper;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
