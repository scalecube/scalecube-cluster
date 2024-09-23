package io.scalecube.cluster.gossip;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.BaseTest;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GossipRequestTest extends BaseTest {

  private static final String NAMESPACE = "ns";

  private static final String testDataQualifier = "scalecube/testData";

  private TestData testData;
  private MessageCodec messageCodec;

  @BeforeEach
  public void init() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "123");
    testData = new TestData();
    testData.setProperties(properties);
    messageCodec = MessageCodec.INSTANCE;
  }

  @Test
  public void testSerializationAndDeserialization() throws Exception {
    Member from = new Member("0", null, "localhost:1234", NAMESPACE);
    List<Gossip> gossips = getGossips();
    Message message =
        Message.withData(new GossipRequest(gossips, from.id())).correlationId("CORR_ID").build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    messageCodec.serialize(message, out);

    assertTrue(out.size() > 0);

    Message deserializedMessage =
        messageCodec.deserialize(new ByteArrayInputStream(out.toByteArray()));

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
        new Gossip("idGossip", Message.withData(testData).qualifier(testDataQualifier).build(), 0);
    Gossip request2 =
        new Gossip("idGossip2", Message.withData(testData).qualifier(testDataQualifier).build(), 1);
    List<Gossip> gossips = new ArrayList<>(2);
    gossips.add(request);
    gossips.add(request2);
    return gossips;
  }

  private static class TestData implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, String> properties;

    public TestData() {}

    public Map<String, String> getProperties() {
      return properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }
  }
}
