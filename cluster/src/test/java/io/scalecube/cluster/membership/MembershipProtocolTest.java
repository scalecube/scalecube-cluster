package io.scalecube.cluster.membership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.BaseTest;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.CorrelationIdGenerator;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.fdetector.FailureDetectorImpl;
import io.scalecube.cluster.gossip.GossipProtocolImpl;
import io.scalecube.cluster.metadata.MetadataStoreImpl;
import io.scalecube.cluster.monitor.ClusterMonitorModel;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.utils.NetworkEmulator;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import io.scalecube.net.Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import reactor.core.Exceptions;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class MembershipProtocolTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(10);

  public static final int TEST_SYNC_INTERVAL = 500;
  public static final int PING_INTERVAL = 200;

  private List<MembershipProtocolImpl> stopables;
  private Scheduler scheduler;

  @BeforeEach
  void setUp(TestInfo testInfo) {
    scheduler = Schedulers.newSingle(testInfo.getDisplayName().replaceAll(" ", "_"), true);
    stopables = new ArrayList<>();
  }

  @AfterEach
  void tearDown() {
    if (scheduler != null) {
      scheduler.dispose();
    }
    if (stopables != null) {
      for (MembershipProtocolImpl membership : stopables) {
        stop(membership);
      }
    }
  }

  @Test
  public void testLeaveCluster() {
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
    List<Address> addresses = Arrays.asList(a.address(), b.address(), c.address());

    MembershipProtocolImpl cmA = createMembership(a, addresses);
    MembershipProtocolImpl cmB = createMembership(b, addresses);
    MembershipProtocolImpl cmC = createMembership(c, addresses);

    awaitSeconds(2);

    List<MembershipEvent> cmAEvents = Collections.synchronizedList(new ArrayList<>());
    List<MembershipEvent> cmCEvents = Collections.synchronizedList(new ArrayList<>());

    cmA.listen().filter(event -> !event.isAdded()).subscribe(cmAEvents::add);
    cmC.listen().filter(event -> !event.isAdded()).subscribe(cmCEvents::add);

    try {
      cmB.leaveCluster().block(TIMEOUT);
    } finally {
      stopAll(cmB);
    }

    awaitSeconds(2);
    awaitSuspicion(3);

    assertTrue(cmAEvents.get(0).isLeaving());
    assertEquals(cmB.member(), cmAEvents.get(0).member());
    assertTrue(cmAEvents.get(1).isRemoved());
    assertEquals(cmB.member(), cmAEvents.get(1).member());

    assertTrue(cmCEvents.get(0).isLeaving());
    assertEquals(cmB.member(), cmCEvents.get(0).member());
    assertTrue(cmCEvents.get(1).isRemoved());
    assertEquals(cmB.member(), cmCEvents.get(1).member());
  }

  @Test
  public void testLeaveClusterCameBeforeAlive() {
    final NetworkEmulatorTransport a = createTransport();
    final NetworkEmulatorTransport b = createTransport();
    final Member anotherMember = new Member("leavingNodeId-1", "", Address.from("localhost:9236"));
    final List<Address> addresses = Arrays.asList(a.address(), b.address());

    final MembershipProtocolImpl cmA = createMembership(a, addresses);
    final MembershipProtocolImpl cmB = createMembership(b, addresses);

    awaitSeconds(2);

    final List<MembershipEvent> cmAEvents = Collections.synchronizedList(new ArrayList<>());
    cmA.listen().subscribe(cmAEvents::add);

    final MembershipRecord leavingRecord =
        new MembershipRecord(anotherMember, MemberStatus.LEAVING, 5);
    final Message leavingMessage =
        Message.builder()
            .qualifier(MembershipProtocolImpl.MEMBERSHIP_GOSSIP)
            .data(leavingRecord)
            .build();

    cmB.getGossipProtocol().spread(leavingMessage).block(TIMEOUT);
    awaitSeconds(3);

    final MembershipRecord addedRecord = new MembershipRecord(anotherMember, MemberStatus.ALIVE, 4);
    final Message addedMessage =
        Message.builder()
            .qualifier(MembershipProtocolImpl.MEMBERSHIP_GOSSIP)
            .data(addedRecord)
            .build();

    cmB.getGossipProtocol().spread(addedMessage).block(TIMEOUT);
    awaitSeconds(2);
    awaitSuspicion(3);

    assertTrue(cmAEvents.get(0).isAdded());
    assertEquals(anotherMember, cmAEvents.get(0).member());
    assertTrue(cmAEvents.get(1).isLeaving());
    assertEquals(anotherMember, cmAEvents.get(1).member());
    assertTrue(cmAEvents.get(2).isRemoved());
    assertEquals(anotherMember, cmAEvents.get(2).member());
  }

  @Test
  public void testLeaveClusterOnSuspectedNode() {
    final NetworkEmulatorTransport a = createTransport();
    final NetworkEmulatorTransport b = createTransport();
    final Member anotherMember = new Member("leavingNodeId-1", "", Address.from("localhost:9236"));
    final List<Address> addresses = Arrays.asList(a.address(), b.address());

    final MembershipProtocolImpl cmA = createMembership(a, addresses);
    final MembershipProtocolImpl cmB = createMembership(b, addresses);

    awaitSeconds(2);

    final List<MembershipEvent> cmAEvents = Collections.synchronizedList(new ArrayList<>());
    cmA.listen().filter(event -> !event.isAdded()).subscribe(cmAEvents::add);

    final MembershipRecord suspectedNode =
        new MembershipRecord(anotherMember, MemberStatus.SUSPECT, 5);
    final Message suspectMessage =
        Message.builder()
            .qualifier(MembershipProtocolImpl.MEMBERSHIP_GOSSIP)
            .data(suspectedNode)
            .build();

    cmB.getGossipProtocol().spread(suspectMessage).block(TIMEOUT);
    awaitSeconds(3);

    final MembershipRecord leavingRecord =
        new MembershipRecord(anotherMember, MemberStatus.LEAVING, 4);
    final Message leavingMessage =
        Message.builder()
            .qualifier(MembershipProtocolImpl.MEMBERSHIP_GOSSIP)
            .data(leavingRecord)
            .build();

    cmB.getGossipProtocol().spread(leavingMessage).block(TIMEOUT);
    awaitSeconds(2);
    awaitSuspicion(3);

    assertTrue(cmAEvents.isEmpty());
  }

  @Test
  public void testLeaveClusterOnAliveAndSuspectedNode() {
    final NetworkEmulatorTransport a = createTransport();
    final NetworkEmulatorTransport b = createTransport();
    final List<Address> addresses = Arrays.asList(a.address(), b.address());

    final MembershipProtocolImpl cmA = createMembership(a, addresses);
    final MembershipProtocolImpl cmB = createMembership(b, addresses);

    awaitSeconds(3);

    final List<MembershipEvent> cmAEvents = Collections.synchronizedList(new ArrayList<>());
    cmA.listen().filter(event -> !event.isAdded()).subscribe(cmAEvents::add);

    b.networkEmulator().blockAllInbound();
    b.networkEmulator().blockAllOutbound();

    awaitSeconds(TEST_SYNC_INTERVAL * 2 / 1000);

    try {
      b.networkEmulator().unblockAllInbound();
      b.networkEmulator().unblockAllOutbound();

      cmB.leaveCluster().block(TIMEOUT);
    } finally {
      stopAll(cmB);
    }

    awaitSeconds(3);
    awaitSuspicion(3);

    assertTrue(cmAEvents.get(0).isLeaving());
    assertEquals(cmB.member(), cmAEvents.get(0).member());
    assertTrue(cmAEvents.get(1).isRemoved());
    assertEquals(cmB.member(), cmAEvents.get(1).member());
  }

  @Test
  public void testInitialPhaseOk() {
    Transport a = createTransport();
    Transport b = createTransport();
    Transport c = createTransport();
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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
    NetworkEmulatorTransport d = createTransport();
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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
    NetworkEmulatorTransport d = createTransport();
    List<Address> addresses = Arrays.asList(a.address(), b.address(), c.address(), d.address());

    MembershipProtocolImpl cmA = createMembership(a, addresses);
    MembershipProtocolImpl cmB = createMembership(b, addresses);
    MembershipProtocolImpl cmC = createMembership(c, addresses);
    MembershipProtocolImpl cmD = createMembership(d, addresses);

    Transport c_Restarted;
    Transport d_Restarted;

    Flux.merge(
            awaitUntil(() -> assertTrusted(cmA, cmB.member(), cmC.member(), cmD.member())),
            awaitUntil(() -> assertTrusted(cmB, cmA.member(), cmC.member(), cmD.member())),
            awaitUntil(() -> assertTrusted(cmC, cmB.member(), cmA.member(), cmD.member())),
            awaitUntil(() -> assertTrusted(cmD, cmB.member(), cmC.member(), cmA.member())))
        .then()
        .block(TIMEOUT);

    ReplayProcessor<MembershipEvent> cmA_RemovedHistory = startRecordingRemoved(cmA);
    ReplayProcessor<MembershipEvent> cmB_RemovedHistory = startRecordingRemoved(cmB);

    stop(cmC);
    stop(cmD);

    Flux.merge(
            awaitUntil(() -> assertTrusted(cmA, cmB.member())),
            awaitUntil(() -> assertSuspected(cmA, cmC.member(), cmD.member())),
            awaitUntil(() -> assertTrusted(cmB, cmA.member())),
            awaitUntil(() -> assertSuspected(cmB, cmC.member(), cmD.member())))
        .then()
        .block(TIMEOUT);

    awaitSuspicion(addresses.size());

    Flux.merge(
            awaitUntil(() -> assertTrusted(cmA, cmB.member())),
            awaitUntil(() -> assertNoSuspected(cmA)),
            awaitUntil(() -> assertRemoved(cmA_RemovedHistory, cmC.member(), cmD.member())),
            awaitUntil(() -> assertTrusted(cmB, cmA.member())),
            awaitUntil(() -> assertNoSuspected(cmB)),
            awaitUntil(() -> assertRemoved(cmB_RemovedHistory, cmC.member(), cmD.member())))
        .then()
        .block(TIMEOUT);

    c_Restarted = createTransport();
    d_Restarted = createTransport();
    MembershipProtocolImpl cmC_Restarted = createMembership(c_Restarted, addresses);
    MembershipProtocolImpl cmD_Restarted = createMembership(d_Restarted, addresses);

    Flux.merge(
            awaitUntil(
                () ->
                    assertTrusted(
                        cmC_Restarted, cmA.member(), cmB.member(), cmD_Restarted.member())),
            awaitUntil(() -> assertNoSuspected(cmC_Restarted)),
            awaitUntil(
                () ->
                    assertTrusted(
                        cmD_Restarted, cmA.member(), cmB.member(), cmC_Restarted.member())),
            awaitUntil(() -> assertNoSuspected(cmD_Restarted)),
            awaitUntil(
                () ->
                    assertTrusted(
                        cmA, cmB.member(), cmC_Restarted.member(), cmD_Restarted.member())),
            awaitUntil(() -> assertNoSuspected(cmA)),
            awaitUntil(
                () ->
                    assertTrusted(
                        cmB, cmA.member(), cmC_Restarted.member(), cmD_Restarted.member())),
            awaitUntil(() -> assertNoSuspected(cmB)))
        .then()
        .block(TIMEOUT);
  }

  @Test
  public void testRestartStoppedMembersOnSameAddresses() {
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
    NetworkEmulatorTransport d = createTransport();
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

      // Verify members C and D were detected as Suspected
      assertTrusted(cmA, cmB.member());
      assertSuspected(cmA, cmC.member(), cmD.member());
      assertTrusted(cmB, cmA.member());
      assertSuspected(cmB, cmC.member(), cmD.member());

      // Restart C and D on same ports
      c_Restarted = createTransport(new TransportConfig().port(c.address().port()));
      d_Restarted = createTransport(new TransportConfig().port(d.address().port()));
      cmC_Restarted = createMembership(c_Restarted, addresses);
      cmD_Restarted = createMembership(d_Restarted, addresses);

      awaitSeconds(2);

      // new C -> A, B, new D
      assertTrusted(cmC_Restarted, cmA.member(), cmB.member(), cmD_Restarted.member());
      assertNoSuspected(cmC_Restarted);
      // new D -> A, B, new C
      assertTrusted(cmD_Restarted, cmA.member(), cmB.member(), cmC_Restarted.member());
      assertNoSuspected(cmD_Restarted);
      // A -> B, new C, new D
      // A -> removed old C, removed old D
      assertTrusted(cmA, cmB.member(), cmC_Restarted.member(), cmD_Restarted.member());
      assertNoSuspected(cmA);
      assertRemoved(cmA_RemovedHistory, cmC.member(), cmD.member());
      // B -> A, new C, new D
      // B -> removed old C, removed old D
      assertTrusted(cmB, cmA.member(), cmC_Restarted.member(), cmD_Restarted.member());
      assertNoSuspected(cmB);
      assertRemoved(cmB_RemovedHistory, cmC.member(), cmD.member());
    } finally {
      stopAll(cmA, cmB, cmC_Restarted, cmD_Restarted);
    }
  }

  @Test
  public void testLimitedSeedMembers() {
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
    NetworkEmulatorTransport d = createTransport();
    NetworkEmulatorTransport e = createTransport();

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

    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
    NetworkEmulatorTransport d = createTransport();
    NetworkEmulatorTransport e = createTransport();

    MembershipProtocolImpl cmA =
        createMembership(a, testConfig(Collections.emptyList()).memberHost(localAddress));
    MembershipProtocolImpl cmB =
        createMembership(
            b, testConfig(Collections.singletonList(a.address())).memberHost(localAddress));
    MembershipProtocolImpl cmC =
        createMembership(
            c, testConfig(Collections.singletonList(a.address())).memberHost(localAddress));
    MembershipProtocolImpl cmD =
        createMembership(
            d, testConfig(Collections.singletonList(b.address())).memberHost(localAddress));
    MembershipProtocolImpl cmE =
        createMembership(
            e, testConfig(Collections.singletonList(b.address())).memberHost(localAddress));

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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c_noInbound = createTransport();

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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c_noInboundThenInboundOk = createTransport();

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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();

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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();

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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();

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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();

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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();

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
    NetworkEmulatorTransport a = createTransport();
    NetworkEmulatorTransport b = createTransport();
    NetworkEmulatorTransport c = createTransport();
    NetworkEmulatorTransport d = createTransport();
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
          .map(NetworkEmulatorTransport::networkEmulator)
          .forEach(NetworkEmulator::blockAllInbound);

      awaitSeconds(2);

      // Check partition: {a}, {b}, {c}, {d}
      assertSelfTrusted(cmA);
      assertSuspected(cmA, cmB.member(), cmC.member(), cmD.member());
      assertSelfTrusted(cmB);
      assertSuspected(cmB, cmA.member(), cmC.member(), cmD.member());
      assertSelfTrusted(cmC);
      assertSuspected(cmC, cmA.member(), cmB.member(), cmD.member());
      assertSelfTrusted(cmD);
      assertSuspected(cmD, cmA.member(), cmB.member(), cmC.member());

      awaitSuspicion(addresses.size());

      assertRemoved(cmA_removedHistory, cmB.member(), cmC.member(), cmD.member());
      assertRemoved(cmB_removedHistory, cmA.member(), cmC.member(), cmD.member());
      assertRemoved(cmC_removedHistory, cmB.member(), cmA.member(), cmD.member());
      assertRemoved(cmD_removedHistory, cmB.member(), cmA.member(), cmC.member());

      // Recover network
      Stream.of(a, b, c, d)
          .map(NetworkEmulatorTransport::networkEmulator)
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

  private ClusterConfig testConfig(List<Address> seedAddresses) {
    // Create faster config for local testing
    return new ClusterConfig()
        .membership(
            opts ->
                opts.seedMembers(seedAddresses).syncInterval(TEST_SYNC_INTERVAL).syncTimeout(100))
        .failureDetector(opts -> opts.pingInterval(PING_INTERVAL).pingTimeout(100))
        .metadataTimeout(100);
  }

  private MembershipProtocolImpl createMembership(
      Transport transport, List<Address> seedAddresses) {
    return createMembership(transport, testConfig(seedAddresses));
  }

  private MembershipProtocolImpl createMembership(Transport transport, ClusterConfig config) {
    Member localMember = new Member(Member.generateId(), null, transport.address());

    DirectProcessor<MembershipEvent> membershipProcessor = DirectProcessor.create();
    FluxSink<MembershipEvent> membershipSink = membershipProcessor.sink();

    CorrelationIdGenerator cidGenerator = new CorrelationIdGenerator(localMember.id());

    FailureDetectorImpl failureDetector =
        new FailureDetectorImpl(
            localMember,
            transport,
            membershipProcessor,
            config.failureDetectorConfig(),
            scheduler,
            cidGenerator);

    GossipProtocolImpl gossipProtocol =
        new GossipProtocolImpl(
            localMember, transport, membershipProcessor, config.gossipConfig(), scheduler);

    MetadataStoreImpl metadataStore =
        new MetadataStoreImpl(localMember, transport, null, config, scheduler, cidGenerator);

    MembershipProtocolImpl membership =
        new MembershipProtocolImpl(
            localMember,
            transport,
            failureDetector,
            gossipProtocol,
            metadataStore,
            config,
            scheduler,
            cidGenerator,
            new ClusterMonitorModel.Builder());

    membership.listen().subscribe(membershipSink::next);

    try {
      failureDetector.start();
      gossipProtocol.start();
      metadataStore.start();
      membership.start().block(TIMEOUT);
    } catch (Exception ex) {
      throw Exceptions.propagate(ex);
    }

    stopables.add(membership);
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
    membership.getMetadataStore().stop();
    membership.stop();
    membership.getGossipProtocol().stop();
    membership.getFailureDetector().stop();
    membership.getTransport().stop().block();
  }

  private Mono<Void> awaitUntil(Runnable assertAction) {
    return Mono.<Void>fromRunnable(assertAction)
        .retryBackoff(Long.MAX_VALUE, Duration.ofMillis(100), Duration.ofSeconds(1));
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
}
