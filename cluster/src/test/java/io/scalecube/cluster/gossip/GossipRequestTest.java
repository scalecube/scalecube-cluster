package io.scalecube.cluster.gossip;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.scalecube.cluster.BaseTest;
import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageCodec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GossipRequestTest extends BaseTest {

  private static final String testDataQualifier = "scalecube/testData";

  private TestData testData;

  /** Setup. */
  @BeforeEach
  public void init() throws Throwable {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "123");

    testData = new TestData();
    testData.setProperties(properties);
  }

  @Test
  public void testSerializationAndDeserialization() throws Exception {

    Member from = new Member("0", Address.from("localhost:1234"));
    List<Gossip> gossips = getGossips();
    Message message =
        Message.withData(new GossipRequest(gossips, from.id())).correlationId("CORR_ID").build();

    ByteBuf bb = MessageCodec.serialize(message);

    assertTrue(bb.readableBytes() > 0);

    ByteBuf input = copiedBuffer(bb);

    Message deserializedMessage = MessageCodec.deserialize(input);

    assertNotNull(deserializedMessage);
    assertEquals(deserializedMessage.data().getClass(), GossipRequest.class);
    assertEquals("CORR_ID", deserializedMessage.correlationId());

    GossipRequest gossipRequest = deserializedMessage.data();
    assertNotNull(gossipRequest);
    assertNotNull(gossipRequest.gossips());
    assertNotNull(gossipRequest.gossips().get(0));

    Object msgData = gossipRequest.gossips().get(0).message().data();
    assertNotNull(msgData);
    assertTrue(msgData instanceof TestData, msgData.toString());
    assertEquals(testData.getProperties(), ((TestData) msgData).getProperties());
  }

  private List<Gossip> getGossips() {
    Gossip request =
        new Gossip("idGossip", Message.withData(testData).qualifier(testDataQualifier).build());
    Gossip request2 =
        new Gossip("idGossip2", Message.withData(testData).qualifier(testDataQualifier).build());
    List<Gossip> gossips = new ArrayList<>(2);
    gossips.add(request);
    gossips.add(request2);
    return gossips;
  }

  private static class TestData {

    private Map<String, String> properties;

    TestData() {}

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }
  }
}
