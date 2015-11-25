package com.keedio.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import org.junit.Test;

import com.linkedin.camus.etl.kafka.coders.TestMessage;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestStringMessageDecoder {

  @Test
  public void testDecodeString() {

    Properties testProperties = new Properties();

    StringMessageDecoder testDecoder = new StringMessageDecoder();
    testDecoder.init(testProperties, "testTopic");
    
    String expectedPayload = "String payload expected";
    byte[] bytePayload = expectedPayload.getBytes();
    
    CamusWrapper actualResult = testDecoder.decode(new TestMessage().setPayload(bytePayload));
    String actualPayload = actualResult.getRecord().toString();
    
    assertEquals(expectedPayload, actualPayload);

  }

}
