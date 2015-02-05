package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class AvroMessageDecoder extends MessageDecoder<byte[], GenericData.Record> {
  private static final byte MAGIC_BYTE = 0x0;
  private static final int idSize = 4;
  private final String SCHEMA_REGISTRY_URL = "schema.registry.url";
  private final String MAX_SCHEMAS_PER_SUBJECT = "max.schemas.per.subject";
  private final String DEFAULT_MAX_SCHEMAS_PER_SUBJECT = "1000";
  private static final Logger logger = Logger.getLogger(AvroMessageDecoder.class);
  protected DecoderFactory decoderFactory;
  private SchemaRegistryClient schemaRegistry;

  @Override
  public void init(Properties props, String topicName) {
    super.init(props, topicName);
    decoderFactory = DecoderFactory.get();
    if (props == null) {
      throw new IllegalArgumentException("Missing schema registry url!");
    }
    String url = props.getProperty(SCHEMA_REGISTRY_URL);
    if (url == null) {
      throw new IllegalArgumentException("Missing schema registry url!");
    }
    String maxSchemaObject = props.getProperty(
        MAX_SCHEMAS_PER_SUBJECT, DEFAULT_MAX_SCHEMAS_PER_SUBJECT);
    schemaRegistry = new CachedSchemaRegistryClient(url, Integer.parseInt(maxSchemaObject));
  }

  private ByteBuffer getByteBuffer(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    byte magic = buffer.get();
    logger.debug("MAGIC BYTE" + magic);
    if (magic != MAGIC_BYTE) {
      throw new MessageDecoderException("Unknown magic byte!");
    }
    return buffer;
  }

  private Object deserialize(byte[] payload) throws MessageDecoderException {
    try {
      ByteBuffer buffer = getByteBuffer(payload);
      int id = buffer.getInt();
      Schema schema = schemaRegistry.getByID(id);
      if (schema == null)
        throw new IllegalStateException("Unknown schema id: " + id);
      logger.debug(schema.toString());
      int length = buffer.limit() - 1 - idSize;
      if (schema.getType().equals(Schema.Type.BYTES)) {
        byte[] bytes = new byte[length];
        buffer.get(bytes, 0, length);
        return bytes;
      }
      int start = buffer.position() + buffer.arrayOffset();
      DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
      Object object =
          reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));

      if (schema.getType().equals(Schema.Type.STRING)) {
        object = ((Utf8) object).toString();
      }
      return object;
    } catch (IOException e) {
      throw new MessageDecoderException("Error deserializing Avro message", e);
    }
  }


  public CamusWrapper<Record> decode(byte[] payload) {
    Object object = deserialize(payload);
    if (object instanceof Record) {
      return new CamusAvroWrapper((Record) object);
    } else {
      throw new MessageDecoderException("Camus does not support Avro primitive types!");
    }
  }

  private static class CamusAvroWrapper extends CamusWrapper<Record> {
    public CamusAvroWrapper(GenericData.Record record) {
      super(record);
      GenericData.Record header = (Record) super.getRecord().get("header");
      if (header != null) {
        if (header.get("server") != null) {
          put(new Text("server"), new Text(header.get("server").toString()));
        }
        if (header.get("service") != null) {
          put(new Text("service"), new Text(header.get("service").toString()));
        }
      }
    }

    @Override
    public long getTimestamp() {
      Record header = (Record) super.getRecord().get("header");
      if (header != null && header.get("time") != null) {
        return (Long) header.get("time");
      } else if (super.getRecord().get("timestamp") != null) {
        return (Long) super.getRecord().get("timestamp");
      } else {
        return System.currentTimeMillis();
      }
    }
  }
}
