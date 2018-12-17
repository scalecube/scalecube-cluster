package io.scalecube.cluster.fdetector;

import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static io.scalecube.cluster.membership.MemberStatus.SUSPECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.BaseTest;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MemberStatus;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.transport.Address;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class FailureDetectorTest extends BaseTest {

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
  public void testTrusted() {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetectorImpl fdA = createFd(a, members);
    FailureDetectorImpl fdB = createFd(b, members);
    FailureDetectorImpl fdC = createFd(c, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fdA, fdB, fdC);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> listA = listenNextEventFor(fdA, members);
      Future<List<FailureDetectorEvent>> listB = listenNextEventFor(fdB, members);
      Future<List<FailureDetectorEvent>> listC = listenNextEventFor(fdC, members);

      assertStatus(a.address(), ALIVE, awaitEvents(listA), b.address(), c.address());
      assertStatus(b.address(), ALIVE, awaitEvents(listB), a.address(), c.address());
      assertStatus(c.address(), ALIVE, awaitEvents(listC), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testSuspected() {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetectorImpl fdA = createFd(a, members);
    FailureDetectorImpl fdB = createFd(b, members);
    FailureDetectorImpl fdC = createFd(c, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fdA, fdB, fdC);

    // block all traffic
    a.networkEmulator().block(members);
    b.networkEmulator().block(members);
    c.networkEmulator().block(members);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> listA = listenNextEventFor(fdA, members);
      Future<List<FailureDetectorEvent>> listB = listenNextEventFor(fdB, members);
      Future<List<FailureDetectorEvent>> listC = listenNextEventFor(fdC, members);

      assertStatus(a.address(), SUSPECT, awaitEvents(listA), b.address(), c.address());
      assertStatus(b.address(), SUSPECT, awaitEvents(listB), a.address(), c.address());
      assertStatus(c.address(), SUSPECT, awaitEvents(listC), a.address(), b.address());
    } finally {
      a.networkEmulator().unblockAll();
      b.networkEmulator().unblockAll();
      c.networkEmulator().unblockAll();
      stop(fdetectors);
    }
  }

  @Test
  public void testTrustedDespiteBadNetwork() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetectorImpl fdA = createFd(a, members);
    FailureDetectorImpl fdB = createFd(b, members);
    FailureDetectorImpl fdC = createFd(c, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fdA, fdB, fdC);

    // Traffic issue at connection A -> B
    a.networkEmulator().block(b.address());

    Future<List<FailureDetectorEvent>> listA = listenNextEventFor(fdA, members);
    Future<List<FailureDetectorEvent>> listB = listenNextEventFor(fdB, members);
    Future<List<FailureDetectorEvent>> listC = listenNextEventFor(fdC, members);

    try {
      start(fdetectors);

      assertStatus(a.address(), ALIVE, awaitEvents(listA), b.address(), c.address());
      assertStatus(b.address(), ALIVE, awaitEvents(listB), a.address(), c.address());
      assertStatus(c.address(), ALIVE, awaitEvents(listC), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testTrustedDespiteDifferentPingTimings() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetectorImpl fdA = createFd(a, members);
    FailureDetectorConfig fdBConfig =
        ClusterConfig.builder().pingTimeout(500).pingInterval(1000).build();
    FailureDetectorImpl fdB = createFd(b, members, fdBConfig);
    FailureDetectorImpl fdC = createFd(c, members, ClusterConfig.defaultConfig());
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fdA, fdB, fdC);

    try {
      start(fdetectors);

      Future<List<FailureDetectorEvent>> listA = listenNextEventFor(fdA, members);
      Future<List<FailureDetectorEvent>> listB = listenNextEventFor(fdB, members);
      Future<List<FailureDetectorEvent>> listC = listenNextEventFor(fdC, members);

      assertStatus(a.address(), ALIVE, awaitEvents(listA), b.address(), c.address());
      assertStatus(b.address(), ALIVE, awaitEvents(listB), a.address(), c.address());
      assertStatus(c.address(), ALIVE, awaitEvents(listC), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testSuspectedMemberWithBadNetworkGetsPartitioned() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), d.address());

    // Create failure detectors
    FailureDetectorImpl fdA = createFd(a, members);
    FailureDetectorImpl fdB = createFd(b, members);
    FailureDetectorImpl fdC = createFd(c, members);
    FailureDetectorImpl fdD = createFd(d, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fdA, fdB, fdC, fdD);

    // Block traffic on member A to all cluster members
    a.networkEmulator().block(members);

    try {
      final Future<List<FailureDetectorEvent>> listA = listenNextEventFor(fdA, members);
      final Future<List<FailureDetectorEvent>> listB = listenNextEventFor(fdB, members);
      final Future<List<FailureDetectorEvent>> listC = listenNextEventFor(fdC, members);
      final Future<List<FailureDetectorEvent>> listD = listenNextEventFor(fdD, members);

      start(fdetectors);

      assertStatus(
          a.address(),
          SUSPECT,
          awaitEvents(listA),
          b.address(),
          c.address(),
          d.address()); // node A
      // partitioned
      assertStatus(b.address(), SUSPECT, awaitEvents(listB), a.address());
      assertStatus(c.address(), SUSPECT, awaitEvents(listC), a.address());
      assertStatus(d.address(), SUSPECT, awaitEvents(listD), a.address());

      // Unblock traffic on member A
      a.networkEmulator().unblockAll();
      TimeUnit.SECONDS.sleep(4);

      final Future<List<FailureDetectorEvent>> listA0 = listenNextEventFor(fdA, members);
      final Future<List<FailureDetectorEvent>> listB0 = listenNextEventFor(fdB, members);
      final Future<List<FailureDetectorEvent>> listC0 = listenNextEventFor(fdC, members);
      final Future<List<FailureDetectorEvent>> listD0 = listenNextEventFor(fdD, members);

      // Check member A recovers

      assertStatus(a.address(), ALIVE, awaitEvents(listA0), b.address(), c.address(), d.address());
      assertStatus(b.address(), ALIVE, awaitEvents(listB0), a.address(), c.address(), d.address());
      assertStatus(c.address(), ALIVE, awaitEvents(listC0), a.address(), b.address(), d.address());
      assertStatus(d.address(), ALIVE, awaitEvents(listD0), a.address(), b.address(), c.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testSuspectedMemberWithNormalNetworkGetsPartitioned() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), d.address());

    // Create failure detectors
    FailureDetectorImpl fdA = createFd(a, members);
    FailureDetectorImpl fdB = createFd(b, members);
    FailureDetectorImpl fdC = createFd(c, members);
    FailureDetectorImpl fdD = createFd(d, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fdA, fdB, fdC, fdD);

    // Block traffic to node D on other members
    a.networkEmulator().block(d.address());
    b.networkEmulator().block(d.address());
    c.networkEmulator().block(d.address());

    try {
      final Future<List<FailureDetectorEvent>> listA = listenNextEventFor(fdA, members);
      final Future<List<FailureDetectorEvent>> listB = listenNextEventFor(fdB, members);
      final Future<List<FailureDetectorEvent>> listC = listenNextEventFor(fdC, members);
      final Future<List<FailureDetectorEvent>> listD = listenNextEventFor(fdD, members);

      start(fdetectors);

      assertStatus(a.address(), SUSPECT, awaitEvents(listA), d.address());
      assertStatus(b.address(), SUSPECT, awaitEvents(listB), d.address());
      assertStatus(c.address(), SUSPECT, awaitEvents(listC), d.address());
      assertStatus(
          d.address(),
          SUSPECT,
          awaitEvents(listD),
          a.address(),
          b.address(),
          c.address()); // node D
      // partitioned

      // Unblock traffic to member D on other members
      a.networkEmulator().unblockAll();
      b.networkEmulator().unblockAll();
      c.networkEmulator().unblockAll();
      TimeUnit.SECONDS.sleep(4);

      final Future<List<FailureDetectorEvent>> listA0 = listenNextEventFor(fdA, members);
      final Future<List<FailureDetectorEvent>> listB0 = listenNextEventFor(fdB, members);
      final Future<List<FailureDetectorEvent>> listC0 = listenNextEventFor(fdC, members);
      final Future<List<FailureDetectorEvent>> listD0 = listenNextEventFor(fdD, members);

      // Check member D recovers

      assertStatus(a.address(), ALIVE, awaitEvents(listA0), b.address(), c.address(), d.address());
      assertStatus(b.address(), ALIVE, awaitEvents(listB0), a.address(), c.address(), d.address());
      assertStatus(c.address(), ALIVE, awaitEvents(listC0), a.address(), b.address(), d.address());
      assertStatus(d.address(), ALIVE, awaitEvents(listD0), a.address(), b.address(), c.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testMemberStatusChangeAfterNetworkRecovery() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address());

    // Create failure detectors
    FailureDetectorImpl fdA = createFd(a, members);
    FailureDetectorImpl fdB = createFd(b, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fdA, fdB);

    // Traffic is blocked initially on both sides: A--X-->B, B--X-->A
    a.networkEmulator().block(b.address());
    b.networkEmulator().block(a.address());

    Future<List<FailureDetectorEvent>> listA = listenNextEventFor(fdA, members);
    Future<List<FailureDetectorEvent>> listB = listenNextEventFor(fdB, members);

    try {
      start(fdetectors);

      assertStatus(a.address(), SUSPECT, awaitEvents(listA), b.address());
      assertStatus(b.address(), SUSPECT, awaitEvents(listB), a.address());

      // Unblock A and B members: A-->B, B-->A
      a.networkEmulator().unblockAll();
      b.networkEmulator().unblockAll();
      TimeUnit.SECONDS.sleep(2);

      // Check that members recover

      listA = listenNextEventFor(fdA, members);
      listB = listenNextEventFor(fdB, members);

      assertStatus(a.address(), ALIVE, awaitEvents(listA), b.address());
      assertStatus(b.address(), ALIVE, awaitEvents(listB), a.address());
    } finally {
      stop(fdetectors);
    }
  }

  @Test
  public void testStatusChangeAfterMemberRestart() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport x = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), x.address());

    // Create failure detectors
    FailureDetectorImpl fdA = createFd(a, members);
    FailureDetectorImpl fdB = createFd(b, members);
    FailureDetectorImpl fdX = createFd(x, members);
    List<FailureDetectorImpl> fdetectors = Arrays.asList(fdA, fdB, fdX);

    Future<List<FailureDetectorEvent>> listA = listenNextEventFor(fdA, members);
    Future<List<FailureDetectorEvent>> listB = listenNextEventFor(fdB, members);
    Future<List<FailureDetectorEvent>> listX = listenNextEventFor(fdX, members);

    // Restarted member attributes are not initialized
    Transport xx;
    FailureDetectorImpl fdXx;

    try {
      start(fdetectors);

      assertStatus(a.address(), ALIVE, awaitEvents(listA), b.address(), x.address());
      assertStatus(b.address(), ALIVE, awaitEvents(listB), a.address(), x.address());
      assertStatus(x.address(), ALIVE, awaitEvents(listX), a.address(), b.address());

      // stop node X
      stop(Collections.singletonList(fdX));
      TimeUnit.SECONDS.sleep(2);

      // restart node X as XX
      xx =
          Transport.bindAwait(
              TransportConfig.builder().port(x.address().port()).useNetworkEmulator(true).build());
      assertEquals(x.address(), xx.address());
      fdetectors = Arrays.asList(fdA, fdB, fdXx = createFd(xx, members));

      // actual restart here
      fdXx.start();
      TimeUnit.SECONDS.sleep(2);

      listA = listenNextEventFor(fdA, members);
      listB = listenNextEventFor(fdB, members);
      Future<List<FailureDetectorEvent>> listXx = listenNextEventFor(fdXx, members);

      // TODO [AK]: It would be more correct to consider restarted member as a new member, so x is
      // still suspected!

      assertStatus(a.address(), ALIVE, awaitEvents(listA), b.address(), xx.address());
      assertStatus(b.address(), ALIVE, awaitEvents(listB), a.address(), xx.address());
      assertStatus(xx.address(), ALIVE, awaitEvents(listXx), a.address(), b.address());
    } finally {
      stop(fdetectors);
    }
  }

  private FailureDetectorImpl createFd(Transport transport, List<Address> members) {
    FailureDetectorConfig failureDetectorConfig =
        ClusterConfig.builder() // faster config for local testing
            .pingTimeout(100)
            .pingInterval(200)
            .pingReqMembers(2)
            .metadataTimeout(100)
            .build();
    return createFd(transport, members, failureDetectorConfig);
  }

  private FailureDetectorImpl createFd(
      Transport transport, List<Address> addresses, FailureDetectorConfig config) {

    Member localMember = new Member("member-" + transport.address().port(), transport.address());

    Flux<MembershipEvent> membershipFlux =
        Flux.fromIterable(addresses)
            .filter(address -> !transport.address().equals(address))
            .map(address -> new Member("member-" + address.port(), address))
            .map(member -> MembershipEvent.createAdded(member, Collections.emptyMap()));

    return new FailureDetectorImpl(localMember, transport, membershipFlux, config, scheduler);
  }

  private void destroyTransport(Transport transport) {
    if (transport == null || transport.isStopped()) {
      return;
    }
    try {
      transport.stop().block(Duration.ofSeconds(1));
    } catch (Exception ignore) {
      // no-op
    }
  }

  private void start(List<FailureDetectorImpl> fdetectors) {
    for (FailureDetectorImpl fd : fdetectors) {
      fd.start();
    }
  }

  private void stop(List<FailureDetectorImpl> fdetectors) {
    for (FailureDetectorImpl fd : fdetectors) {
      fd.stop();
    }
    for (FailureDetectorImpl fd : fdetectors) {
      destroyTransport(fd.getTransport());
    }
  }

  private void assertStatus(
      Address address,
      MemberStatus status,
      Collection<FailureDetectorEvent> events,
      Address... expected) {
    List<Address> actual =
        events
            .stream()
            .filter(event -> event.status() == status)
            .map(FailureDetectorEvent::member)
            .map(Member::address)
            .collect(Collectors.toList());

    String msg1 =
        String.format(
            "Node %s expected %s %s members %s, but was: %s",
            address, expected.length, status, Arrays.toString(expected), events);
    assertEquals(expected.length, actual.size(), msg1);

    for (Address member : expected) {
      String msg2 =
          String.format("Node %s expected as %s %s, but was: %s", address, status, member, events);
      assertTrue(actual.contains(member), msg2);
    }
  }

  private Future<List<FailureDetectorEvent>> listenNextEventFor(
      FailureDetectorImpl fd, List<Address> addresses) {
    addresses = new ArrayList<>(addresses);
    addresses.remove(fd.getTransport().address()); // exclude self
    if (addresses.isEmpty()) {
      throw new IllegalArgumentException();
    }

    List<CompletableFuture<FailureDetectorEvent>> resultFuture = new ArrayList<>();
    for (final Address member : addresses) {
      final CompletableFuture<FailureDetectorEvent> future = new CompletableFuture<>();
      fd.listen().filter(event -> event.member().address() == member).subscribe(future::complete);
      resultFuture.add(future);
    }

    return allOf(resultFuture);
  }

  private Collection<FailureDetectorEvent> awaitEvents(Future<List<FailureDetectorEvent>> events) {
    try {
      return events.get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futuresList) {
    CompletableFuture<Void> allFuturesResult =
        CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[futuresList.size()]));
    return allFuturesResult.thenApply(
        v -> futuresList.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }
}
