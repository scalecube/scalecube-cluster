package io.scalecube.cluster;

import static io.scalecube.cluster.ClusterConfig.DEFAULT_METADATA_TIMEOUT;
import static io.scalecube.cluster.ClusterConfig.EXTERNAL_HOST_PROP_NAME;
import static io.scalecube.cluster.ClusterConfig.EXTERNAL_PORT_PROP_NAME;
import static io.scalecube.cluster.ClusterConfig.MEMBER_ALIAS_PROP_NAME;
import static io.scalecube.cluster.ClusterConfig.MEMBER_ID_PROP_NAME;
import static io.scalecube.cluster.ClusterConfig.METADATA_CODEC_PROP_NAME;
import static io.scalecube.cluster.ClusterConfig.METADATA_TIMEOUT_PROP_NAME;
import static io.scalecube.cluster.fdetector.FailureDetectorConfig.DEFAULT_PING_INTERVAL;
import static io.scalecube.cluster.fdetector.FailureDetectorConfig.DEFAULT_PING_REQ_MEMBERS;
import static io.scalecube.cluster.fdetector.FailureDetectorConfig.DEFAULT_PING_TIMEOUT;
import static io.scalecube.cluster.fdetector.FailureDetectorConfig.PING_INTERVAL_PROP_NAME;
import static io.scalecube.cluster.fdetector.FailureDetectorConfig.PING_REQ_MEMBERS_PROP_NAME;
import static io.scalecube.cluster.fdetector.FailureDetectorConfig.PING_TIMEOUT_PROP_NAME;
import static io.scalecube.cluster.gossip.GossipConfig.DEFAULT_GOSSIP_FANOUT;
import static io.scalecube.cluster.gossip.GossipConfig.DEFAULT_GOSSIP_INTERVAL;
import static io.scalecube.cluster.gossip.GossipConfig.DEFAULT_GOSSIP_REPEAT_MULT;
import static io.scalecube.cluster.gossip.GossipConfig.DEFAULT_GOSSIP_SEGMENTATION_THRESHOLD;
import static io.scalecube.cluster.gossip.GossipConfig.GOSSIP_FANOUT_PROP_NAME;
import static io.scalecube.cluster.gossip.GossipConfig.GOSSIP_INTERVAL_PROP_NAME;
import static io.scalecube.cluster.gossip.GossipConfig.GOSSIP_REPEAT_MULT_PROP_NAME;
import static io.scalecube.cluster.gossip.GossipConfig.GOSSIP_SEGMENTATION_THRESHOLD_PROP_NAME;
import static io.scalecube.cluster.membership.MembershipConfig.DEFAULT_NAMESPACE;
import static io.scalecube.cluster.membership.MembershipConfig.DEFAULT_SUSPICION_MULT;
import static io.scalecube.cluster.membership.MembershipConfig.DEFAULT_SYNC_INTERVAL;
import static io.scalecube.cluster.membership.MembershipConfig.DEFAULT_SYNC_TIMEOUT;
import static io.scalecube.cluster.membership.MembershipConfig.NAMESPACE_PROP_NAME;
import static io.scalecube.cluster.membership.MembershipConfig.SEED_MEMBERS_PROP_NAME;
import static io.scalecube.cluster.membership.MembershipConfig.SUSPICION_MULT_PROP_NAME;
import static io.scalecube.cluster.membership.MembershipConfig.SYNC_INTERVAL_PROP_NAME;
import static io.scalecube.cluster.membership.MembershipConfig.SYNC_TIMEOUT_PROP_NAME;
import static io.scalecube.cluster.transport.api.TransportConfig.CLIENT_SECURED_PROP_NAME;
import static io.scalecube.cluster.transport.api.TransportConfig.CONNECT_TIMEOUT_PROP_NAME;
import static io.scalecube.cluster.transport.api.TransportConfig.DEFAULT_CLIENT_SECURED;
import static io.scalecube.cluster.transport.api.TransportConfig.DEFAULT_CONNECT_TIMEOUT;
import static io.scalecube.cluster.transport.api.TransportConfig.DEFAULT_MAX_FRAME_LENGTH;
import static io.scalecube.cluster.transport.api.TransportConfig.DEFAULT_PORT;
import static io.scalecube.cluster.transport.api.TransportConfig.MAX_FRAME_LENGTH_PROP_NAME;
import static io.scalecube.cluster.transport.api.TransportConfig.MESSAGE_CODEC_PROP_NAME;
import static io.scalecube.cluster.transport.api.TransportConfig.PORT_PROP_NAME;
import static io.scalecube.cluster.transport.api.TransportConfig.TRANSPORT_FACTORY_PROP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.metadata.JdkMetadataCodec;
import io.scalecube.cluster.metadata.MetadataCodec;
import io.scalecube.cluster.transport.api.JdkMessageCodec;
import io.scalecube.cluster.transport.api.MessageCodec;
import io.scalecube.cluster.transport.api.TransportFactory;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ClusterConfigTest {

  private static final String[] ALL_PROP_NAMES = {
    METADATA_TIMEOUT_PROP_NAME,
    MEMBER_ID_PROP_NAME,
    MEMBER_ALIAS_PROP_NAME,
    EXTERNAL_HOST_PROP_NAME,
    EXTERNAL_PORT_PROP_NAME,
    PORT_PROP_NAME,
    CLIENT_SECURED_PROP_NAME,
    CONNECT_TIMEOUT_PROP_NAME,
    MAX_FRAME_LENGTH_PROP_NAME,
    PING_INTERVAL_PROP_NAME,
    PING_TIMEOUT_PROP_NAME,
    PING_REQ_MEMBERS_PROP_NAME,
    GOSSIP_FANOUT_PROP_NAME,
    GOSSIP_INTERVAL_PROP_NAME,
    GOSSIP_REPEAT_MULT_PROP_NAME,
    GOSSIP_SEGMENTATION_THRESHOLD_PROP_NAME,
    SEED_MEMBERS_PROP_NAME,
    SYNC_INTERVAL_PROP_NAME,
    SYNC_TIMEOUT_PROP_NAME,
    SUSPICION_MULT_PROP_NAME,
    NAMESPACE_PROP_NAME,
    METADATA_CODEC_PROP_NAME,
    MESSAGE_CODEC_PROP_NAME,
    TRANSPORT_FACTORY_PROP_NAME
  };

  @Test
  void testDefaults() {
    assertDefaults(new ClusterConfig(new Properties()));
  }

  @Test
  void testNullMarkerMeansNotSet() {
    final var properties = new Properties();
    for (String name : ALL_PROP_NAMES) {
      properties.setProperty(name, "@null");
    }
    assertDefaults(new ClusterConfig(properties));
  }

  @Test
  void testAllPropertiesAreRead() {
    final var properties = new Properties();
    properties.setProperty(METADATA_TIMEOUT_PROP_NAME, "11");
    properties.setProperty(MEMBER_ID_PROP_NAME, "id-1");
    properties.setProperty(MEMBER_ALIAS_PROP_NAME, "alias-1");
    properties.setProperty(EXTERNAL_HOST_PROP_NAME, "ext-host");
    properties.setProperty(EXTERNAL_PORT_PROP_NAME, "7070");
    properties.setProperty(PORT_PROP_NAME, "4801");
    properties.setProperty(CLIENT_SECURED_PROP_NAME, "true");
    properties.setProperty(CONNECT_TIMEOUT_PROP_NAME, "12");
    properties.setProperty(MAX_FRAME_LENGTH_PROP_NAME, "13");
    properties.setProperty(PING_INTERVAL_PROP_NAME, "14");
    properties.setProperty(PING_TIMEOUT_PROP_NAME, "15");
    properties.setProperty(PING_REQ_MEMBERS_PROP_NAME, "16");
    properties.setProperty(GOSSIP_FANOUT_PROP_NAME, "17");
    properties.setProperty(GOSSIP_INTERVAL_PROP_NAME, "18");
    properties.setProperty(GOSSIP_REPEAT_MULT_PROP_NAME, "19");
    properties.setProperty(GOSSIP_SEGMENTATION_THRESHOLD_PROP_NAME, "20");
    properties.setProperty(SEED_MEMBERS_PROP_NAME, " host1:4801, ,host2:4802 ");
    properties.setProperty(SYNC_INTERVAL_PROP_NAME, "21");
    properties.setProperty(SYNC_TIMEOUT_PROP_NAME, "22");
    properties.setProperty(SUSPICION_MULT_PROP_NAME, "23");
    properties.setProperty(NAMESPACE_PROP_NAME, "ns");

    final var config = new ClusterConfig(properties);

    assertEquals(11, config.metadataTimeout(), "metadataTimeout");
    assertEquals("id-1", config.memberId(), "memberId");
    assertEquals("alias-1", config.memberAlias(), "memberAlias");
    assertEquals("ext-host", config.externalHost(), "externalHost");
    assertEquals(7070, config.externalPort(), "externalPort");

    final var transport = config.transportConfig();
    assertEquals(4801, transport.port(), "port");
    assertEquals(true, transport.isClientSecured(), "clientSecured");
    assertEquals(12, transport.connectTimeout(), "connectTimeout");
    assertEquals(13, transport.maxFrameLength(), "maxFrameLength");

    final var fdetector = config.failureDetectorConfig();
    assertEquals(14, fdetector.pingInterval(), "pingInterval");
    assertEquals(15, fdetector.pingTimeout(), "pingTimeout");
    assertEquals(16, fdetector.pingReqMembers(), "pingReqMembers");

    final var gossip = config.gossipConfig();
    assertEquals(17, gossip.gossipFanout(), "gossipFanout");
    assertEquals(18, gossip.gossipInterval(), "gossipInterval");
    assertEquals(19, gossip.gossipRepeatMult(), "gossipRepeatMult");
    assertEquals(20, gossip.gossipSegmentationThreshold(), "gossipSegmentationThreshold");

    final var membership = config.membershipConfig();
    assertEquals(List.of("host1:4801", "host2:4802"), membership.seedMembers(), "seedMembers");
    assertEquals(21, membership.syncInterval(), "syncInterval");
    assertEquals(22, membership.syncTimeout(), "syncTimeout");
    assertEquals(23, membership.suspicionMult(), "suspicionMult");
    assertEquals("ns", membership.namespace(), "namespace");
  }

  @Test
  void testPluggablesAreInstantiatedByClassName() {
    final var properties = new Properties();
    properties.setProperty(METADATA_CODEC_PROP_NAME, JdkMetadataCodec.class.getName());
    properties.setProperty(MESSAGE_CODEC_PROP_NAME, JdkMessageCodec.class.getName());
    properties.setProperty(TRANSPORT_FACTORY_PROP_NAME, TcpTransportFactory.class.getName());

    final var config = new ClusterConfig(properties);

    assertInstanceOf(JdkMetadataCodec.class, config.metadataCodec(), "metadataCodec");
    assertInstanceOf(
        JdkMessageCodec.class, config.transportConfig().messageCodec(), "messageCodec");
    assertInstanceOf(
        TcpTransportFactory.class, config.transportConfig().transportFactory(), "transportFactory");
  }

  @Test
  void testWebsocketIsTheServiceLoaderDefault() {
    assertInstanceOf(
        WebsocketTransportFactory.class,
        new ClusterConfig(new Properties()).transportConfig().transportFactory());
  }

  @Test
  void testUnknownClassNameFailsFast() {
    final var properties = new Properties();
    properties.setProperty(TRANSPORT_FACTORY_PROP_NAME, "com.acme.NoSuchFactory");

    final var ex =
        assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(properties));
    assertTrue(ex.getMessage().contains(TRANSPORT_FACTORY_PROP_NAME), ex.getMessage());
  }

  @Test
  void testWrongTypeClassNameFailsFast() {
    final var properties = new Properties();
    properties.setProperty(MESSAGE_CODEC_PROP_NAME, JdkMetadataCodec.class.getName());

    assertThrows(IllegalArgumentException.class, () -> new ClusterConfig(properties));
  }

  @Test
  void testInjectedPropertiesWinOverSystemProperties() {
    System.setProperty(PING_INTERVAL_PROP_NAME, "111");
    try {
      final var properties = new Properties();
      properties.setProperty(PING_INTERVAL_PROP_NAME, "222");

      assertEquals(222, new ClusterConfig(properties).failureDetectorConfig().pingInterval());
      assertEquals(111, new ClusterConfig().failureDetectorConfig().pingInterval());
      assertEquals("111", System.getProperty(PING_INTERVAL_PROP_NAME), "no write-back");
    } finally {
      System.clearProperty(PING_INTERVAL_PROP_NAME);
    }
  }

  @Test
  void testSettersMutateInPlace() {
    final var config = new ClusterConfig(new Properties());
    final var transport = config.transportConfig();

    assertSame(config, config.memberAlias("a").transport(opts -> opts.port(4801)));
    assertSame(transport, config.transportConfig());
    assertEquals("a", config.memberAlias());
    assertEquals(4801, transport.port());
  }

  @Test
  void testPropertiesOverrideProgrammaticValue() {
    final var config = new ClusterConfig(new Properties()).memberAlias("code");
    final var properties = new Properties();
    properties.setProperty(MEMBER_ALIAS_PROP_NAME, "props");
    assertEquals("props", config.memberAlias(properties).memberAlias());
  }

  private static void assertDefaults(ClusterConfig config) {
    assertEquals(DEFAULT_METADATA_TIMEOUT, config.metadataTimeout(), "metadataTimeout");
    assertNull(config.memberId(), "memberId");
    assertNull(config.memberAlias(), "memberAlias");
    assertNull(config.externalHost(), "externalHost");
    assertNull(config.externalPort(), "externalPort");

    final var transport = config.transportConfig();
    assertEquals(DEFAULT_PORT, transport.port(), "port");
    assertEquals(DEFAULT_CLIENT_SECURED, transport.isClientSecured(), "clientSecured");
    assertEquals(DEFAULT_CONNECT_TIMEOUT, transport.connectTimeout(), "connectTimeout");
    assertEquals(DEFAULT_MAX_FRAME_LENGTH, transport.maxFrameLength(), "maxFrameLength");
    assertSame(MessageCodec.INSTANCE, transport.messageCodec(), "messageCodec");
    assertSame(TransportFactory.INSTANCE, transport.transportFactory(), "transportFactory");
    assertSame(MetadataCodec.INSTANCE, config.metadataCodec(), "metadataCodec");

    final var fdetector = config.failureDetectorConfig();
    assertEquals(DEFAULT_PING_INTERVAL, fdetector.pingInterval(), "pingInterval");
    assertEquals(DEFAULT_PING_TIMEOUT, fdetector.pingTimeout(), "pingTimeout");
    assertEquals(DEFAULT_PING_REQ_MEMBERS, fdetector.pingReqMembers(), "pingReqMembers");

    final var gossip = config.gossipConfig();
    assertEquals(DEFAULT_GOSSIP_FANOUT, gossip.gossipFanout(), "gossipFanout");
    assertEquals(DEFAULT_GOSSIP_INTERVAL, gossip.gossipInterval(), "gossipInterval");
    assertEquals(DEFAULT_GOSSIP_REPEAT_MULT, gossip.gossipRepeatMult(), "gossipRepeatMult");
    assertEquals(
        DEFAULT_GOSSIP_SEGMENTATION_THRESHOLD,
        gossip.gossipSegmentationThreshold(),
        "gossipSegmentationThreshold");

    final var membership = config.membershipConfig();
    assertEquals(List.of(), membership.seedMembers(), "seedMembers");
    assertEquals(DEFAULT_SYNC_INTERVAL, membership.syncInterval(), "syncInterval");
    assertEquals(DEFAULT_SYNC_TIMEOUT, membership.syncTimeout(), "syncTimeout");
    assertEquals(DEFAULT_SUSPICION_MULT, membership.suspicionMult(), "suspicionMult");
    assertEquals(DEFAULT_NAMESPACE, membership.namespace(), "namespace");
  }
}
