package io.scalecube.cluster.membership;

import static io.scalecube.cluster.ClusterConfig.DEFAULT_SUSPICION_MULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.BaseTest;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.CorrelationIdGenerator;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.fdetector.FailureDetectorImpl;
import io.scalecube.cluster.gossip.GossipProtocolImpl;
import io.scalecube.cluster.metadata.MetadataStoreImpl;
import io.scalecube.transport.Address;
import io.scalecube.transport.NetworkEmulator;
import io.scalecube.transport.Transport;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import reactor.core.Exceptions;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class MembershipProtocolTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

  public static final int TEST_SYNC_INTERVAL = 500;
  public static final int PING_INTERVAL = 200;

  private Scheduler scheduler;

  @BeforeEach
  void setUp(TestInfo testInfo) {
    scheduler = Schedulers.newSingle(testInfo.getDisplayName().replaceAll(" ", "_"), true);
  }

  @AfterEach
  void tearDown() {
    if (scheduler != null) {
      scheduler.dispose();
    }
  }

  @Test
  public void testInitialPhaseOk() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> addresses = Arrays.asList(a.address(), b.address(), c.address());

    MembershipProtocolImpl cmA = createMembership(a, addresses);
    MembershipProtocolImpl cmB = createMembership(b, addresses);
    MembershipProtocolImpl cmC = createMembership(c, addresses);

    try {
      awaitSeconds(1);

      assertTrusted(cmA, cmB.member(), cmC.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmA.member(), cmB.member());
      assertNoSuspected(cmC);
    } finally {
      stopAll(cmA, cmB, cmC);
    }
  }

  @Test
  public void testNetworkPartitionDueNoOutboundThenRecover() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> addresses = Arrays.asList(a.address(), b.address(), c.address());

    MembershipProtocolImpl cmA = createMembership(a, addresses);
    MembershipProtocolImpl cmB = createMembership(b, addresses);
    MembershipProtocolImpl cmC = createMembership(c, addresses);

    awaitSeconds(3);

    // Block traffic
    a.networkEmulator().blockOutbound(addresses);
    b.networkEmulator().blockOutbound(addresses);
    c.networkEmulator().blockOutbound(addresses);

    try {

      awaitSuspicion(addresses.size());

      assertSelfTrusted(cmA);
      assertNoSuspected(cmA);
      assertSelfTrusted(cmB);
      assertNoSuspected(cmB);
      assertSelfTrusted(cmC);
      assertNoSuspected(cmC);

      a.networkEmulator().unblockAllOutbound();
      b.networkEmulator().unblockAllOutbound();
      c.networkEmulator().unblockAllOutbound();

      awaitSeconds(TEST_SYNC_INTERVAL * 2 / 1000);

      assertTrusted(cmA, cmB.member(), cmC.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmA.member(), cmB.member());
      assertNoSuspected(cmC);
    } finally {
      stopAll(cmA, cmB, cmC);
    }
  }

  @Test
  public void testMemberLostNetworkDueNoOutboundThenRecover() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    MembershipProtocolImpl cmA = createMembership(a, members);
    MembershipProtocolImpl cmB = createMembership(b, members);
    MembershipProtocolImpl cmC = createMembership(c, members);

    try {
      awaitSeconds(1);

      // Check all trusted
      assertTrusted(cmA, cmB.member(), cmC.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmA.member(), cmB.member());
      assertNoSuspected(cmC);

      // Node b lost network
      b.networkEmulator().blockOutbound(a.address(), c.address());
      a.networkEmulator().blockOutbound(b.address());
      c.networkEmulator().blockOutbound(b.address());

      awaitSeconds(1);

      // Check partition: {b}, {a, c}
      assertTrusted(cmA, cmC.member());
      assertSuspected(cmA, cmB.member());
      assertSelfTrusted(cmB);
      assertSuspected(cmB, cmA.member(), cmC.member());
      assertTrusted(cmC, cmA.member());
      assertSuspected(cmC, cmB.member());

      // Node b recover network
      a.networkEmulator().unblockAllOutbound();
      b.networkEmulator().unblockAllOutbound();
      c.networkEmulator().unblockAllOutbound();

      awaitSeconds(1);

      // Check all trusted again
      assertTrusted(cmA, cmB.member(), cmC.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmB.member(), cmA.member());
      assertNoSuspected(cmC);
    } finally {
      stopAll(cmA, cmB, cmC);
    }
  }

  @Test
  public void testNetworkPartitionTwiceDueNoOutboundThenRecover() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> addresses = Arrays.asList(a.address(), b.address(), c.address());

    MembershipProtocolImpl cmA = createMembership(a, addresses);
    MembershipProtocolImpl cmB = createMembership(b, addresses);
    MembershipProtocolImpl cmC = createMembership(c, addresses);

    try {
      awaitSeconds(1);

      // Check all trusted
      assertTrusted(cmA, cmB.member(), cmC.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmA.member(), cmB.member());
      assertNoSuspected(cmC);

      // Node b lost network
      b.networkEmulator().blockOutbound(a.address(), c.address());
      a.networkEmulator().blockOutbound(b.address());
      c.networkEmulator().blockOutbound(b.address());

      awaitSeconds(1);

      // Check partition: {b}, {a, c}
      assertTrusted(cmA, cmC.member());
      assertSuspected(cmA, cmB.member());
      assertSelfTrusted(cmB);
      assertSuspected(cmB, cmA.member(), cmC.member());
      assertTrusted(cmC, cmA.member());
      assertSuspected(cmC, cmB.member());

      // Node a and c lost network
      a.networkEmulator().blockOutbound(c.address());
      c.networkEmulator().blockOutbound(a.address());

      awaitSeconds(1);

      // Check partition: {a}, {b}, {c}
      assertSelfTrusted(cmA);
      assertSuspected(cmA, cmB.member(), cmC.member());
      assertSelfTrusted(cmB);
      assertSuspected(cmB, cmA.member(), cmC.member());
      assertSelfTrusted(cmC);
      assertSuspected(cmC, cmB.member(), cmA.member());

      // Recover network
      a.networkEmulator().unblockAllOutbound();
      b.networkEmulator().unblockAllOutbound();
      c.networkEmulator().unblockAllOutbound();

      awaitSeconds(1);

      // Check all trusted again
      assertTrusted(cmA, cmB.member(), cmC.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmB.member(), cmA.member());
      assertNoSuspected(cmC);
    } finally {
      stopAll(cmA, cmB, cmC);
    }
  }

  @Test
  public void testNetworkLostOnAllNodesDueNoOutboundThenRecover() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> addresses = Arrays.asList(a.address(), b.address(), c.address());

    MembershipProtocolImpl cmA = createMembership(a, addresses);
    MembershipProtocolImpl cmB = createMembership(b, addresses);
    MembershipProtocolImpl cmC = createMembership(c, addresses);

    try {
      awaitSeconds(1);

      assertTrusted(cmA, cmB.member(), cmC.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmB.member(), cmA.member());
      assertNoSuspected(cmC);

      a.networkEmulator().blockOutbound(addresses);
      b.networkEmulator().blockOutbound(addresses);
      c.networkEmulator().blockOutbound(addresses);

      awaitSeconds(1);

      assertSelfTrusted(cmA);
      assertSuspected(cmA, cmB.member(), cmC.member());

      assertSelfTrusted(cmB);
      assertSuspected(cmB, cmA.member(), cmC.member());

      assertSelfTrusted(cmC);
      assertSuspected(cmC, cmB.member(), cmA.member());

      a.networkEmulator().unblockAllOutbound();
      b.networkEmulator().unblockAllOutbound();
      c.networkEmulator().unblockAllOutbound();

      awaitSeconds(1);

      assertTrusted(cmA, cmB.member(), cmC.member());
      assertNoSuspected(cmA);

      assertTrusted(cmB, cmA.member(), cmC.member());
      assertNoSuspected(cmB);

      assertTrusted(cmC, cmB.member(), cmA.member());
      assertNoSuspected(cmC);
    } finally {
      stopAll(cmA, cmB, cmC);
    }
  }

  @Test
  public void testLongNetworkPartitionDueNoOutboundThenRemoved() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> addresses = Arrays.asList(a.address(), b.address(), c.address(), d.address());

    MembershipProtocolImpl cmA = createMembership(a, addresses);
    MembershipProtocolImpl cmB = createMembership(b, addresses);
    MembershipProtocolImpl cmC = createMembership(c, addresses);
    MembershipProtocolImpl cmD = createMembership(d, addresses);

    try {
      awaitSeconds(1);

      assertTrusted(cmA, cmB.member(), cmC.member(), cmD.member());
      assertTrusted(cmB, cmA.member(), cmC.member(), cmD.member());
      assertTrusted(cmC, cmB.member(), cmA.member(), cmD.member());
      assertTrusted(cmD, cmB.member(), cmC.member(), cmA.member());

      a.networkEmulator().blockOutbound(c.address(), d.address());
      b.networkEmulator().blockOutbound(c.address(), d.address());

      c.networkEmulator().blockOutbound(a.address(), b.address());
      d.networkEmulator().blockOutbound(a.address(), b.address());

      awaitSeconds(2);

      assertTrusted(cmA, cmB.member());
      assertSuspected(cmA, cmC.member(), cmD.member());
      assertTrusted(cmB, cmA.member());
      assertSuspected(cmB, cmC.member(), cmD.member());
      assertTrusted(cmC, cmD.member());
      assertSuspected(cmC, cmB.member(), cmA.member());
      assertTrusted(cmD, cmC.member());
      assertSuspected(cmD, cmB.member(), cmA.member());

      awaitSuspicion(addresses.size());

      assertTrusted(cmA, cmB.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmD.member());
      assertNoSuspected(cmC);
      assertTrusted(cmD, cmC.member());
      assertNoSuspected(cmD);
    } finally {
      stopAll(cmA, cmB, cmC, cmD);
    }
  }

  @Test
  public void testRestartStoppedMembers() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> addresses = Arrays.asList(a.address(), b.address(), c.address(), d.address());

    MembershipProtocolImpl cmA = createMembership(a, addresses);
    MembershipProtocolImpl cmB = createMembership(b, addresses);
    MembershipProtocolImpl cmC = createMembership(c, addresses);
    MembershipProtocolImpl cmD = createMembership(d, addresses);

    Transport c_Restarted;
    Transport d_Restarted;
    MembershipProtocolImpl cmC_Restarted = null;
    MembershipProtocolImpl cmD_Restarted = null;

    try {
      awaitSeconds(1);

      assertTrusted(cmA, cmB.member(), cmC.member(), cmD.member());
      assertTrusted(cmB, cmA.member(), cmC.member(), cmD.member());
      assertTrusted(cmC, cmB.member(), cmA.member(), cmD.member());
      assertTrusted(cmD, cmB.member(), cmC.member(), cmA.member());

      ReplayProcessor<MembershipEvent> cmA_RemovedHistory = startRecordingRemoved(cmA);
      ReplayProcessor<MembershipEvent> cmB_RemovedHistory = startRecordingRemoved(cmB);

      stop(cmC);
      stop(cmD);

      awaitSeconds(1);

      assertTrusted(cmA, cmB.member());
      assertSuspected(cmA, cmC.member(), cmD.member());
      assertTrusted(cmB, cmA.member());
      assertSuspected(cmB, cmC.member(), cmD.member());

      awaitSuspicion(addresses.size());

      assertTrusted(cmA, cmB.member());
      assertNoSuspected(cmA);
      assertRemoved(cmA_RemovedHistory, cmC.member(), cmD.member());
      assertTrusted(cmB, cmA.member());
      assertNoSuspected(cmB);
      assertRemoved(cmB_RemovedHistory, cmC.member(), cmD.member());

      c_Restarted = Transport.bindAwait(true);
      d_Restarted = Transport.bindAwait(true);
      cmC_Restarted = createMembership(c_Restarted, addresses);
      cmD_Restarted = createMembership(d_Restarted, addresses);

      awaitSeconds(1);

      assertTrusted(cmC_Restarted, cmA.member(), cmB.member(), cmD_Restarted.member());
      assertNoSuspected(cmC_Restarted);
      assertTrusted(cmD_Restarted, cmA.member(), cmB.member(), cmC_Restarted.member());
      assertNoSuspected(cmD_Restarted);
      assertTrusted(cmA, cmB.member(), cmC_Restarted.member(), cmD_Restarted.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC_Restarted.member(), cmD_Restarted.member());
      assertNoSuspected(cmB);
    } finally {
      stopAll(cmA, cmB, cmC_Restarted, cmD_Restarted);
    }
  }

  @Test
  public void testLimitedSeedMembers() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    Transport e = Transport.bindAwait(true);

    MembershipProtocolImpl cmA = createMembership(a, Collections.emptyList());
    MembershipProtocolImpl cmB = createMembership(b, Collections.singletonList(a.address()));
    MembershipProtocolImpl cmC = createMembership(c, Collections.singletonList(a.address()));
    MembershipProtocolImpl cmD = createMembership(d, Collections.singletonList(b.address()));
    MembershipProtocolImpl cmE = createMembership(e, Collections.singletonList(b.address()));

    try {
      awaitSeconds(3);

      assertTrusted(cmA, cmB.member(), cmC.member(), cmD.member(), cmE.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member(), cmD.member(), cmE.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmB.member(), cmA.member(), cmD.member(), cmE.member());
      assertNoSuspected(cmC);
      assertTrusted(cmD, cmB.member(), cmC.member(), cmA.member(), cmE.member());
      assertNoSuspected(cmD);
      assertTrusted(cmE, cmB.member(), cmC.member(), cmD.member(), cmA.member());
      assertNoSuspected(cmE);
    } finally {
      stopAll(cmA, cmB, cmC, cmD, cmE);
    }
  }

  @Test
  public void testOverrideMemberAddress() throws UnknownHostException {
    String localAddress = InetAddress.getLocalHost().getHostName();

    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    Transport e = Transport.bindAwait(true);

    MembershipProtocolImpl cmA =
        createMembership(a, testConfig(Collections.emptyList()).memberHost(localAddress).build());
    MembershipProtocolImpl cmB =
        createMembership(
            b, testConfig(Collections.singletonList(a.address())).memberHost(localAddress).build());
    MembershipProtocolImpl cmC =
        createMembership(
            c, testConfig(Collections.singletonList(a.address())).memberHost(localAddress).build());
    MembershipProtocolImpl cmD =
        createMembership(
            d, testConfig(Collections.singletonList(b.address())).memberHost(localAddress).build());
    MembershipProtocolImpl cmE =
        createMembership(
            e, testConfig(Collections.singletonList(b.address())).memberHost(localAddress).build());

    try {
      awaitSeconds(3);

      assertTrusted(cmA, cmB.member(), cmC.member(), cmD.member(), cmE.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member(), cmD.member(), cmE.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmA.member(), cmB.member(), cmD.member(), cmE.member());
      assertNoSuspected(cmC);
      assertTrusted(cmD, cmA.member(), cmB.member(), cmC.member(), cmE.member());
      assertNoSuspected(cmD);
      assertTrusted(cmE, cmA.member(), cmB.member(), cmC.member(), cmD.member());
      assertNoSuspected(cmE);
    } finally {
      stopAll(cmA, cmB, cmC, cmD, cmE);
    }
  }

  @Test
  public void testNodeJoinClusterWithNoInbound() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c_noInbound = Transport.bindAwait(true);

    // Block traffic
    c_noInbound.networkEmulator().blockAllInbound();

    MembershipProtocolImpl cmA = createMembership(a, Collections.emptyList());
    MembershipProtocolImpl cmB = createMembership(b, Collections.singletonList(a.address()));
    MembershipProtocolImpl cm_noInbound =
        createMembership(c_noInbound, Collections.singletonList(a.address()));

    awaitSeconds(3);

    try {
      assertTrusted(cmA, cmB.member());
      assertTrusted(cmB, cmA.member());

      assertSelfTrusted(cm_noInbound);
      assertNoSuspected(cm_noInbound);
    } finally {
      stopAll(cmA, cmB, cm_noInbound);
    }
  }

  @Test
  public void testNodeJoinClusterWithNoInboundThenInboundRecover() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c_noInboundThenInboundOk = Transport.bindAwait(true);

    // Block traffic
    c_noInboundThenInboundOk.networkEmulator().blockAllInbound();

    MembershipProtocolImpl cmA = createMembership(a, Collections.emptyList());
    MembershipProtocolImpl cmB = createMembership(b, Collections.singletonList(a.address()));
    MembershipProtocolImpl cm_noInboundThenInboundOk =
        createMembership(c_noInboundThenInboundOk, Collections.singletonList(a.address()));

    awaitSeconds(3);

    try {
      assertTrusted(cmA, cmB.member());
      assertTrusted(cmB, cmA.member());

      assertSelfTrusted(cm_noInboundThenInboundOk);
      assertNoSuspected(cm_noInboundThenInboundOk);

      // Unblock traffic
      c_noInboundThenInboundOk.networkEmulator().unblockAllInbound();

      awaitSeconds(1);

      // Verify cluster
      assertTrusted(cmA, cmB.member(), cm_noInboundThenInboundOk.member());
      assertTrusted(cmB, cmA.member(), cm_noInboundThenInboundOk.member());
      assertTrusted(cm_noInboundThenInboundOk, cmA.member(), cmB.member());
    } finally {
      stopAll(cmA, cmB, cm_noInboundThenInboundOk);
    }
  }

  @Test
  public void testNetworkPartitionDueNoInboundThenRemoved() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);

    MembershipProtocolImpl cmA = createMembership(a, Collections.emptyList());
    MembershipProtocolImpl cmB = createMembership(b, Collections.singletonList(a.address()));
    MembershipProtocolImpl cmC = createMembership(c, Collections.singletonList(a.address()));

    try {
      awaitSeconds(3);
      // prerequisites
      assertTrusted(cmA, cmB.member(), cmC.member());
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertTrusted(cmC, cmB.member(), cmA.member());

      ReplayProcessor<MembershipEvent> cmA_RemovedHistory = startRecordingRemoved(cmA);
      ReplayProcessor<MembershipEvent> cmB_RemovedHistory = startRecordingRemoved(cmB);
      ReplayProcessor<MembershipEvent> cmC_RemovedHistory = startRecordingRemoved(cmC);

      // block inbound msgs from all
      c.networkEmulator().blockAllInbound();

      awaitSuspicion(3);

      assertTrusted(cmA, cmB.member());
      assertNoSuspected(cmA);
      assertRemoved(cmA_RemovedHistory, cmC.member());
      assertTrusted(cmB, cmA.member());
      assertNoSuspected(cmB);
      assertRemoved(cmB_RemovedHistory, cmC.member());
      assertSelfTrusted(cmC);
      assertNoSuspected(cmC);
      assertRemoved(cmC_RemovedHistory, cmA.member(), cmB.member());
    } finally {
      stopAll(cmA, cmB, cmC);
    }
  }

  @Test
  public void testNetworkPartitionDueNoInboundUntilRemovedThenInboundRecover() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);

    MembershipProtocolImpl cmA = createMembership(a, Collections.emptyList());
    MembershipProtocolImpl cmB = createMembership(b, Collections.singletonList(a.address()));
    MembershipProtocolImpl cmC = createMembership(c, Collections.singletonList(a.address()));

    try {
      awaitSeconds(3);
      // prerequisites
      assertTrusted(cmA, cmB.member(), cmC.member());
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertTrusted(cmC, cmB.member(), cmA.member());

      ReplayProcessor<MembershipEvent> cmA_RemovedHistory = startRecordingRemoved(cmA);
      ReplayProcessor<MembershipEvent> cmB_RemovedHistory = startRecordingRemoved(cmB);
      ReplayProcessor<MembershipEvent> cmC_RemovedHistory = startRecordingRemoved(cmC);

      // block inbound msgs from all
      c.networkEmulator().blockAllInbound();

      awaitSuspicion(3);

      assertTrusted(cmA, cmB.member());
      assertNoSuspected(cmA);
      assertRemoved(cmA_RemovedHistory, cmC.member());
      assertTrusted(cmB, cmA.member());
      assertNoSuspected(cmB);
      assertRemoved(cmB_RemovedHistory, cmC.member());
      assertSelfTrusted(cmC);
      assertNoSuspected(cmC);
      assertRemoved(cmC_RemovedHistory, cmA.member(), cmB.member());

      // unblock inbound msgs for all
      c.networkEmulator().unblockAllInbound();

      awaitSeconds(3);

      assertTrusted(cmA, cmB.member(), cmC.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmB.member(), cmA.member());
      assertNoSuspected(cmC);
    } finally {
      stopAll(cmA, cmB, cmC);
    }
  }

  @Test
  public void testNetworkPartitionBetweenTwoMembersDueNoInbound() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);

    MembershipProtocolImpl cmA = createMembership(a, Collections.emptyList());
    MembershipProtocolImpl cmB = createMembership(b, Collections.singletonList(a.address()));
    MembershipProtocolImpl cmC = createMembership(c, Collections.singletonList(a.address()));

    try {
      awaitSeconds(3);
      // prerequisites
      assertTrusted(cmA, cmB.member(), cmC.member());
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertTrusted(cmC, cmB.member(), cmA.member());

      // block inbound msgs from b
      c.networkEmulator().blockInbound(b.address());

      awaitSuspicion(3);

      assertTrusted(cmA, cmB.member(), cmC.member());
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertTrusted(cmC, cmB.member(), cmA.member());
    } finally {
      stopAll(cmA, cmB, cmC);
    }
  }

  @Test
  public void testNetworkPartitionBetweenTwoMembersDueNoOutbound() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);

    MembershipProtocolImpl cmA = createMembership(a, Collections.emptyList());
    MembershipProtocolImpl cmB = createMembership(b, Collections.singletonList(a.address()));
    MembershipProtocolImpl cmC = createMembership(c, Collections.singletonList(a.address()));

    try {
      awaitSeconds(3);
      // prerequisites
      assertTrusted(cmA, cmB.member(), cmC.member());
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertTrusted(cmC, cmB.member(), cmA.member());

      // block outbound msgs from b
      c.networkEmulator().blockOutbound(b.address());

      awaitSuspicion(3);

      assertTrusted(cmA, cmB.member(), cmC.member());
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertTrusted(cmC, cmA.member(), cmC.member());
    } finally {
      stopAll(cmA, cmB, cmC);
    }
  }

  @Test
  public void testNetworkPartitionBetweenTwoMembersDueNoTrafficAtAll() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);

    MembershipProtocolImpl cmA = createMembership(a, Collections.emptyList());
    MembershipProtocolImpl cmB = createMembership(b, Collections.singletonList(a.address()));
    MembershipProtocolImpl cmC = createMembership(c, Collections.singletonList(a.address()));

    try {
      awaitSeconds(3);
      // prerequisites
      assertTrusted(cmA, cmB.member(), cmC.member());
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertTrusted(cmC, cmB.member(), cmA.member());

      // block all traffic msgs from b
      c.networkEmulator().blockOutbound(b.address());
      c.networkEmulator().blockInbound(b.address());

      awaitSuspicion(3);

      assertTrusted(cmA, cmB.member(), cmC.member());
      assertTrusted(cmB, cmA.member(), cmC.member());
      assertTrusted(cmC, cmA.member(), cmB.member());
    } finally {
      stopAll(cmA, cmB, cmC);
    }
  }

  @Test
  public void testNetworkPartitionManyDueNoInboundThenRemovedThenRecover() {
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> addresses = Arrays.asList(a.address(), b.address(), c.address(), d.address());

    MembershipProtocolImpl cmA = createMembership(a, addresses);
    MembershipProtocolImpl cmB = createMembership(b, addresses);
    MembershipProtocolImpl cmC = createMembership(c, addresses);
    MembershipProtocolImpl cmD = createMembership(d, addresses);

    awaitSeconds(1);

    try {
      // Check all trusted
      assertTrusted(cmA, cmB.member(), cmC.member(), cmD.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member(), cmD.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmA.member(), cmB.member(), cmD.member());
      assertNoSuspected(cmC);
      assertTrusted(cmD, cmA.member(), cmB.member(), cmC.member());
      assertNoSuspected(cmD);

      ReplayProcessor<MembershipEvent> cmA_removedHistory = startRecordingRemoved(cmA);
      ReplayProcessor<MembershipEvent> cmB_removedHistory = startRecordingRemoved(cmB);
      ReplayProcessor<MembershipEvent> cmC_removedHistory = startRecordingRemoved(cmC);
      ReplayProcessor<MembershipEvent> cmD_removedHistory = startRecordingRemoved(cmD);

      // Split into several clusters
      Stream.of(a, b, c, d)
          .map(Transport::networkEmulator)
          .forEach(NetworkEmulator::blockAllInbound);

      awaitSeconds(1);

      // Check partition: {a}, {b}, {c}, {d}
      assertSelfTrusted(cmA);
      assertSuspected(cmA, cmB.member(), cmC.member(), cmD.member());
      assertSelfTrusted(cmB);
      assertSuspected(cmB, cmA.member(), cmC.member(), cmD.member());
      assertSelfTrusted(cmC);
      assertSuspected(cmC, cmB.member(), cmA.member(), cmD.member());
      assertSelfTrusted(cmD);
      assertSuspected(cmD, cmB.member(), cmA.member(), cmC.member());

      awaitSuspicion(addresses.size());

      assertRemoved(cmA_removedHistory, cmB.member(), cmC.member(), cmD.member());
      assertRemoved(cmB_removedHistory, cmA.member(), cmC.member(), cmD.member());
      assertRemoved(cmC_removedHistory, cmB.member(), cmA.member(), cmD.member());
      assertRemoved(cmD_removedHistory, cmB.member(), cmA.member(), cmC.member());

      // Recover network
      Stream.of(a, b, c, d)
          .map(Transport::networkEmulator)
          .forEach(NetworkEmulator::unblockAllInbound);

      awaitSeconds(3);

      // Check all trusted
      assertTrusted(cmA, cmB.member(), cmC.member(), cmD.member());
      assertNoSuspected(cmA);
      assertTrusted(cmB, cmA.member(), cmC.member(), cmD.member());
      assertNoSuspected(cmB);
      assertTrusted(cmC, cmA.member(), cmB.member(), cmD.member());
      assertNoSuspected(cmC);
      assertTrusted(cmD, cmA.member(), cmB.member(), cmC.member());
      assertNoSuspected(cmD);
    } finally {
      stopAll(cmA, cmB, cmC, cmD);
    }
  }

  private void awaitSeconds(long seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      throw Exceptions.propagate(e);
    }
  }

  private ClusterConfig.Builder testConfig(List<Address> seedAddresses) {
    // Create faster config for local testing
    return ClusterConfig.builder()
        .seedMembers(seedAddresses)
        .syncInterval(TEST_SYNC_INTERVAL)
        .syncTimeout(100)
        .pingInterval(PING_INTERVAL)
        .pingTimeout(100)
        .metadataTimeout(100);
  }

  private MembershipProtocolImpl createMembership(
      Transport transport, List<Address> seedAddresses) {
    return createMembership(transport, testConfig(seedAddresses).build());
  }

  private MembershipProtocolImpl createMembership(Transport transport, ClusterConfig config) {
    Member localMember = new Member(UUID.randomUUID().toString(), transport.address());

    DirectProcessor<MembershipEvent> membershipProcessor = DirectProcessor.create();
    FluxSink<MembershipEvent> membershipSink = membershipProcessor.sink();

    CorrelationIdGenerator cidGenerator = new CorrelationIdGenerator(localMember.id());

    FailureDetectorImpl failureDetector =
        new FailureDetectorImpl(
            localMember, transport, membershipProcessor, config, scheduler, cidGenerator);

    GossipProtocolImpl gossipProtocol =
        new GossipProtocolImpl(localMember, transport, membershipProcessor, config, scheduler);

    MetadataStoreImpl metadataStore =
        new MetadataStoreImpl(
            localMember, transport, EMPTY_BUFFER, config, scheduler, cidGenerator);

    MembershipProtocolImpl membership =
        new MembershipProtocolImpl(
            localMember,
            transport,
            failureDetector,
            gossipProtocol,
            metadataStore,
            config,
            scheduler,
            cidGenerator);

    membership.listen().subscribe(membershipSink::next);

    try {
      failureDetector.start();
      gossipProtocol.start();
      metadataStore.start();
      membership.start().block(TIMEOUT);
    } catch (Exception ex) {
      throw Exceptions.propagate(ex);
    }

    return membership;
  }

  private void stopAll(MembershipProtocolImpl... memberships) {
    for (MembershipProtocolImpl membership : memberships) {
      stop(membership);
    }
  }

  private void stop(MembershipProtocolImpl membership) {
    if (membership == null) {
      return;
    }
    membership.stop();
    membership.getMetadataStore().stop();
    membership.getGossipProtocol().stop();
    membership.getFailureDetector().stop();
    try {
      membership.getTransport().stop().block(Duration.ofSeconds(1));
    } catch (Exception ignore) {
      // ignore
    }
  }

  private void assertTrusted(MembershipProtocolImpl membership, Member... expected) {
    List<Member> actual = membersByStatus(membership, MemberStatus.ALIVE);
    List<Member> expectedList = new ArrayList<>(Arrays.asList(expected));
    expectedList.add(membership.member()); // add local since he always trusted (alive)
    assertEquals(
        expectedList.size(),
        actual.size(),
        "Expected "
            + expectedList.size()
            + " trusted members "
            + expectedList
            + ", but actual: "
            + actual);
    for (Member member : expectedList) {
      assertTrue(
          actual.contains(member), "Expected to trust " + member + ", but actual: " + actual);
    }
  }

  private void assertSuspected(MembershipProtocolImpl membership, Member... expected) {
    List<Member> actual = membersByStatus(membership, MemberStatus.SUSPECT);
    assertEquals(
        expected.length,
        actual.size(),
        "Expected "
            + expected.length
            + " suspect members "
            + Arrays.toString(expected)
            + ", but actual: "
            + actual);
    for (Member member : expected) {
      assertTrue(
          actual.contains(member), "Expected to suspect " + member + ", but actual: " + actual);
    }
  }

  private void assertRemoved(ReplayProcessor<MembershipEvent> recording, Member... expected) {
    List<Member> actual = new ArrayList<>();
    recording.map(MembershipEvent::member).subscribe(actual::add);
    assertEquals(
        expected.length,
        actual.size(),
        "Expected "
            + expected.length
            + " removed members "
            + Arrays.toString(expected)
            + ", but actual: "
            + actual);
    for (Member member : expected) {
      assertTrue(
          actual.contains(member), "Expected to be removed " + member + ", but actual: " + actual);
    }
  }

  private void assertSelfTrusted(MembershipProtocolImpl membership) {
    assertTrusted(membership);
  }

  private void assertNoRemoved(ReplayProcessor<MembershipEvent> recording) {
    assertRemoved(recording);
  }

  private void assertNoSuspected(MembershipProtocolImpl membership) {
    assertSuspected(membership);
  }

  private List<Member> membersByStatus(MembershipProtocolImpl membership, MemberStatus status) {
    return membership.getMembershipRecords().stream()
        .filter(member -> member.status() == status)
        .map(MembershipRecord::member)
        .collect(Collectors.toList());
  }

  private ReplayProcessor<MembershipEvent> startRecordingRemoved(
      MembershipProtocolImpl membership) {
    ReplayProcessor<MembershipEvent> recording = ReplayProcessor.create();
    membership.listen().filter(MembershipEvent::isRemoved).subscribe(recording);
    return recording;
  }

  private void awaitSuspicion(int clusterSize) {
    int defaultSuspicionMult = DEFAULT_SUSPICION_MULT;
    int pingInterval = PING_INTERVAL;
    long suspicionTimeoutSec =
        ClusterMath.suspicionTimeout(defaultSuspicionMult, clusterSize, pingInterval) / 1000;
    awaitSeconds(suspicionTimeoutSec + 2);
  }
}
