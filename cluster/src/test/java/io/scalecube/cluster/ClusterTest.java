package io.scalecube.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.membership.MembershipEvent.Type;
import io.scalecube.cluster.metadata.MetadataCodec;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public class ClusterTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(30);
  public static final int CONNECT_TIMEOUT = 3000;

  @Test
  public void testStartStopRepeatedly() throws Exception {
    Address address = Address.from("localhost:4848");

    // Start seed node
    Cluster seedNode =
        new ClusterImpl()
            .gossip(opts -> opts.gossipInterval(100))
            .failureDetector(opts -> opts.pingInterval(100))
            .membership(opts -> opts.syncInterval(100))
            .transport(opts -> opts.port(address.port()))
            .transport(opts -> opts.connectTimeout(CONNECT_TIMEOUT))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    Cluster otherNode =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(address))
            .gossip(opts -> opts.gossipInterval(100))
            .failureDetector(opts -> opts.pingInterval(100))
            .membership(opts -> opts.syncInterval(100))
            .transport(opts -> opts.connectTimeout(CONNECT_TIMEOUT))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    assertEquals(2, seedNode.members().size());
    assertEquals(2, otherNode.members().size());

    for (int i = 0; i < 10; i++) {
      seedNode.shutdown();
      seedNode.onShutdown().then(Mono.delay(Duration.ofMillis(100))).block();

      seedNode =
          new ClusterImpl()
              .gossip(opts -> opts.gossipInterval(100))
              .failureDetector(opts -> opts.pingInterval(100))
              .membership(opts -> opts.syncInterval(100))
              .transport(opts -> opts.port(address.port()))
              .transport(opts -> opts.connectTimeout(CONNECT_TIMEOUT))
              .transportFactory(TcpTransportFactory::new)
              .startAwait();

      TimeUnit.SECONDS.sleep(1);

      assertEquals(2, seedNode.members().size());
      assertEquals(2, otherNode.members().size());
    }

    shutdown(Arrays.asList(seedNode, otherNode));
  }

  @Test
  public void testMembersAccessFromScheduler() {
    // Start seed node
    Cluster seedNode = new ClusterImpl().transportFactory(TcpTransportFactory::new).startAwait();
    Cluster otherNode =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(seedNode.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    assertEquals(2, seedNode.members().size());
    assertEquals(2, otherNode.members().size());

    // Members by address

    Optional<Member> otherNodeOnSeedNode = seedNode.member(otherNode.addresses().get(0));
    Optional<Member> seedNodeOnOtherNode = otherNode.member(seedNode.addresses().get(0));

    assertEquals(otherNode.member(), otherNodeOnSeedNode.orElse(null));
    assertEquals(seedNode.member(), seedNodeOnOtherNode.orElse(null));

    shutdown(Arrays.asList(seedNode, otherNode));
  }

  @Test
  public void testJoinLocalhostIgnored() throws InterruptedException {
    InetAddress localIpAddress = Address.getLocalIpAddress();
    Address localAddressByHostname = Address.create(localIpAddress.getHostName(), 4801);
    Address localAddressByIp = Address.create(localIpAddress.getHostAddress(), 4801);
    Address[] addresses = {
      Address.from("localhost:4801"),
      Address.from("127.0.0.1:4801"),
      Address.from("127.0.1.1:4801"),
      localAddressByHostname,
      localAddressByIp
    };

    // Start seed node
    Cluster seedNode =
        new ClusterImpl()
            .transport(opts -> opts.port(4801).connectTimeout(CONNECT_TIMEOUT))
            .membership(opts -> opts.seedMembers(addresses))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    Thread.sleep(CONNECT_TIMEOUT);

    Collection<Member> otherMembers = seedNode.otherMembers();
    assertEquals(0, otherMembers.size(), "otherMembers: " + otherMembers);

    shutdown(Collections.singletonList(seedNode));
  }

  @Test
  public void testJoinLocalhostIgnoredWithOverride() throws InterruptedException {
    InetAddress localIpAddress = Address.getLocalIpAddress();
    Address localAddressByHostname = Address.create(localIpAddress.getHostName(), 7878);
    Address localAddressByIp = Address.create(localIpAddress.getHostAddress(), 7878);
    Address[] addresses = {
      Address.from("localhost:7878"),
      Address.from("127.0.0.1:7878"),
      Address.from("127.0.1.1:7878"),
      localAddressByHostname,
      localAddressByIp
    };

    // Start seed node
    Cluster seedNode =
        new ClusterImpl(new ClusterConfig().externalHosts("localhost"))
            .transport(opts -> opts.port(7878).connectTimeout(CONNECT_TIMEOUT))
            .membership(opts -> opts.seedMembers(addresses))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    Thread.sleep(CONNECT_TIMEOUT);

    Collection<Member> otherMembers = seedNode.otherMembers();
    assertEquals(0, otherMembers.size(), "otherMembers: " + otherMembers);

    shutdown(Collections.singletonList(seedNode));
  }

  @Test
  public void testJoinDynamicPort() {
    // Start seed node
    Cluster seedNode = new ClusterImpl().transportFactory(TcpTransportFactory::new).startAwait();

    int membersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(membersNum);
    try {
      // Start other nodes
      long startAt = System.currentTimeMillis();
      for (int i = 0; i < membersNum; i++) {
        otherNodes.add(
            new ClusterImpl()
                .membership(opts -> opts.seedMembers(seedNode.addresses()))
                .transportFactory(TcpTransportFactory::new)
                .startAwait());
      }
      LOGGER.info("Start up time: {} ms", System.currentTimeMillis() - startAt);
      assertEquals(membersNum + 1, seedNode.members().size());
      LOGGER.info("Cluster nodes: {}", seedNode.members());
    } finally {
      // Shutdown all nodes
      shutdown(
          Stream.concat(Stream.of(seedNode), otherNodes.stream()).collect(Collectors.toList()));
    }
  }

  @Test
  public void testUpdateMetadata() throws Exception {
    // Start seed member
    Cluster seedNode = new ClusterImpl().transportFactory(TcpTransportFactory::new).startAwait();

    Cluster metadataNode = null;
    int testMembersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(testMembersNum);
    CountDownLatch updateLatch = new CountDownLatch(testMembersNum);
    try {
      // Start member with metadata
      Map<String, String> metadata = new HashMap<>();
      metadata.put("key1", "value1");
      metadata.put("key2", "value2");
      metadataNode =
          new ClusterImpl()
              .config(opts -> opts.metadata(metadata))
              .membership(opts -> opts.seedMembers(seedNode.addresses()))
              .transportFactory(TcpTransportFactory::new)
              .startAwait();

      // Start other test members
      Flux.range(0, testMembersNum)
          .flatMap(
              integer ->
                  new ClusterImpl()
                      .membership(opts -> opts.seedMembers(seedNode.addresses()))
                      .transportFactory(TcpTransportFactory::new)
                      .handler(
                          cluster ->
                              new ClusterMessageHandler() {
                                @Override
                                public void onMembershipEvent(MembershipEvent event) {
                                  if (event.isUpdated()) {
                                    LOGGER.info("Received membership update event: {}", event);
                                    updateLatch.countDown();
                                  }
                                }
                              })
                      .start())
          .doOnNext(otherNodes::add)
          .blockLast(TIMEOUT);

      TimeUnit.SECONDS.sleep(3);

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        Member member = memberOptional.get();
        assertEquals(metadata, node.metadata(member).orElse(null));
      }

      // Update metadata
      Map<String, String> updatedMetadata = Collections.singletonMap("key1", "value3");
      metadataNode.updateMetadata(updatedMetadata).block(TIMEOUT);

      // Check all nodes had updated metadata member
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        Member member = memberOptional.get();
        assertEquals(updatedMetadata, node.metadata(member).orElse(null));
      }
    } finally {
      // Shutdown all nodes
      shutdown(
          Stream.concat(Stream.of(seedNode, metadataNode), otherNodes.stream())
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void testUpdateMetadataProperty() throws Exception {
    // Start seed member
    Cluster seedNode = new ClusterImpl().transportFactory(TcpTransportFactory::new).startAwait();

    Cluster metadataNode = null;
    int testMembersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(testMembersNum);
    CountDownLatch updateLatch = new CountDownLatch(testMembersNum);

    try {
      // Start member with metadata
      Map<String, String> metadata = new HashMap<>();
      metadata.put("key1", "value1");
      metadata.put("key2", "value2");
      metadataNode =
          new ClusterImpl()
              .config(opts -> opts.metadata(metadata))
              .membership(opts -> opts.seedMembers(seedNode.addresses()))
              .transportFactory(TcpTransportFactory::new)
              .startAwait();

      // Start other test members
      Flux.range(0, testMembersNum)
          .flatMap(
              integer ->
                  new ClusterImpl()
                      .membership(opts -> opts.seedMembers(seedNode.addresses()))
                      .transportFactory(TcpTransportFactory::new)
                      .handler(
                          cluster ->
                              new ClusterMessageHandler() {
                                @Override
                                public void onMembershipEvent(MembershipEvent event) {
                                  if (event.isUpdated()) {
                                    LOGGER.info("Received membership update event: {}", event);
                                    updateLatch.countDown();
                                  }
                                }
                              })
                      .start())
          .doOnNext(otherNodes::add)
          .blockLast(TIMEOUT);

      TimeUnit.SECONDS.sleep(3);

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        Member member = memberOptional.get();
        assertEquals(metadata, node.metadata(member).orElse(null));
      }

      // Update metadata
      Map<String, String> newMetadata = new HashMap<>(metadata);
      newMetadata.put("key2", "value3");
      metadataNode.updateMetadata(newMetadata).block(TIMEOUT);

      // Check all nodes had updated metadata member
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        Member member = memberOptional.get();
        //noinspection unchecked,OptionalGetWithoutIsPresent
        Map<String, String> actualMetadata = (Map<String, String>) node.metadata(member).get();
        assertEquals(2, actualMetadata.size());
        assertEquals("value1", actualMetadata.get("key1"));
        assertEquals("value3", actualMetadata.get("key2"));
      }
    } finally {
      // Shutdown all nodes
      shutdown(
          Stream.concat(Stream.of(seedNode, metadataNode), otherNodes.stream())
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void testRemoveMetadataProperty() throws Exception {
    // Start seed member
    Cluster seedNode = new ClusterImpl().transportFactory(TcpTransportFactory::new).startAwait();

    Cluster metadataNode = null;
    int testMembersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(testMembersNum);
    CountDownLatch updateLatch = new CountDownLatch(testMembersNum);

    try {
      // Start member with metadata
      Map<String, String> metadata = new HashMap<>();
      metadata.put("key1", "value1");
      metadata.put("key2", "value2");
      metadataNode =
          new ClusterImpl()
              .config(opts -> opts.metadata(metadata))
              .membership(opts -> opts.seedMembers(seedNode.addresses()))
              .transportFactory(TcpTransportFactory::new)
              .startAwait();

      // Start other test members
      Flux.range(0, testMembersNum)
          .flatMap(
              integer ->
                  new ClusterImpl()
                      .membership(opts -> opts.seedMembers(seedNode.addresses()))
                      .transportFactory(TcpTransportFactory::new)
                      .handler(
                          cluster ->
                              new ClusterMessageHandler() {
                                @Override
                                public void onMembershipEvent(MembershipEvent event) {
                                  if (event.isUpdated()) {
                                    LOGGER.info("Received membership update event: {}", event);
                                    updateLatch.countDown();
                                  }
                                }
                              })
                      .start())
          .doOnNext(otherNodes::add)
          .blockLast(TIMEOUT);

      TimeUnit.SECONDS.sleep(3);

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        Member member = memberOptional.get();
        assertEquals(metadata, node.metadata(member).orElse(null));
      }

      // Update metadata

      Map<String, String> newMetadata = new HashMap<>(metadata);
      newMetadata.remove("key2");
      metadataNode.updateMetadata(newMetadata).block(TIMEOUT);

      // Check all nodes had updated metadata member
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        Member member = memberOptional.get();
        //noinspection unchecked,OptionalGetWithoutIsPresent
        Map<String, String> actualMetadata = (Map<String, String>) node.metadata(member).get();
        assertEquals(1, actualMetadata.size());
        assertEquals("value1", actualMetadata.get("key1"));
        assertNull(actualMetadata.get("key2"));
      }
    } finally {
      // Shutdown all nodes
      shutdown(
          Stream.concat(Stream.of(seedNode, metadataNode), otherNodes.stream())
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void testShutdownCluster() throws Exception {
    CountDownLatch leavingLatch = new CountDownLatch(3);
    CountDownLatch removedLatch = new CountDownLatch(3);

    ClusterMessageHandler listener =
        new ClusterMessageHandler() {
          @Override
          public void onMembershipEvent(MembershipEvent event) {
            if (event.isLeaving()) {
              leavingLatch.countDown();
            }
            if (event.isRemoved()) {
              removedLatch.countDown();
            }
          }
        };

    // Start seed member
    final Cluster seedNode =
        new ClusterImpl()
            .transportFactory(TcpTransportFactory::new)
            .handler(cluster -> listener)
            .startAwait();

    // Start nodes
    final Cluster node1 =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(seedNode.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .handler(cluster -> listener)
            .startAwait();
    final Cluster node2 =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(seedNode.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .handler(cluster -> listener)
            .startAwait();
    final Cluster node3 =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(seedNode.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .handler(cluster -> listener)
            .startAwait();

    node2.shutdown();
    node2.onShutdown().block(TIMEOUT);

    assertTrue(leavingLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
    assertTrue(removedLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));

    shutdown(Arrays.asList(seedNode, node1, node3));
  }

  @Test
  public void testMemberMetadataRemoved() throws InterruptedException {
    // Start seed member
    Sinks.Many<MembershipEvent> seedEvents = Sinks.many().replay().all();
    Map<String, String> seedMetadata = new HashMap<>();
    seedMetadata.put("seed", "shmid");
    final Cluster seedNode =
        new ClusterImpl()
            .config(opts -> opts.metadata(seedMetadata))
            .transportFactory(TcpTransportFactory::new)
            .handler(
                cluster ->
                    new ClusterMessageHandler() {
                      @Override
                      public void onMembershipEvent(MembershipEvent event) {
                        seedEvents.emitNext(event, FAIL_FAST);
                      }
                    })
            .startAwait();

    // Start nodes
    // Start member with metadata
    Map<String, String> node1Metadata = new HashMap<>();
    node1Metadata.put("node", "shmod");
    Sinks.Many<MembershipEvent> node1Events = Sinks.many().replay().all();
    final Cluster node1 =
        new ClusterImpl()
            .config(opts -> opts.metadata(node1Metadata))
            .membership(opts -> opts.seedMembers(seedNode.addresses()))
            .transportFactory(TcpTransportFactory::new)
            .handler(
                cluster ->
                    new ClusterMessageHandler() {
                      @Override
                      public void onMembershipEvent(MembershipEvent event) {
                        node1Events.emitNext(event, FAIL_FAST);
                      }
                    })
            .startAwait();

    // Check events
    MembershipEvent nodeAddedEvent =
        seedEvents.asFlux().as(Mono::from).block(Duration.ofSeconds(3));
    assertEquals(Type.ADDED, nodeAddedEvent.type());

    MembershipEvent seedAddedEvent =
        node1Events.asFlux().as(Mono::from).block(Duration.ofSeconds(3));
    assertEquals(Type.ADDED, seedAddedEvent.type());

    // Check metadata
    assertEquals(node1Metadata, seedNode.metadata(node1.member()).orElse(null));
    assertEquals(seedMetadata, node1.metadata(seedNode.member()).orElse(null));

    // Remove node1 from cluster
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Map<String, String>> removedMetadata = new AtomicReference<>();
    seedEvents
        .asFlux()
        .filter(MembershipEvent::isRemoved)
        .subscribe(
            event -> {
              Object metadata = MetadataCodec.INSTANCE.deserialize(event.oldMetadata());
              //noinspection unchecked
              removedMetadata.set((Map<String, String>) metadata);
              latch.countDown();
            });

    node1.shutdown();
    node1.onShutdown().block(TIMEOUT);

    assertTrue(latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
    assertEquals(removedMetadata.get(), node1Metadata);

    shutdown(Collections.singletonList(seedNode));
  }

  @Test
  public void testJoinSeedClusterWithNoExistingSeedMember() {
    // Start seed node
    Cluster seedNode = new ClusterImpl().transportFactory(TcpTransportFactory::new).startAwait();

    List<Address> seeds = new ArrayList<>();

    seeds.add(Address.from("localhost:1234")); // Not existent
    seeds.add(Address.from("localhost:5678"));  // Not existent
    seeds.addAll(seedNode.addresses());

    Cluster otherNode =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(seeds))
            .transportFactory(TcpTransportFactory::new)
            .startAwait();

    assertEquals(otherNode.member(), seedNode.otherMembers().iterator().next());
    assertEquals(seedNode.member(), otherNode.otherMembers().iterator().next());

    shutdown(Arrays.asList(seedNode, otherNode));
  }

  private void shutdown(List<Cluster> nodes) {
    try {
      Mono.whenDelayError(
              nodes.stream()
                  .peek(Cluster::shutdown)
                  .map(Cluster::onShutdown)
                  .collect(Collectors.toList()))
          .block(TIMEOUT);
    } catch (Exception ex) {
      LOGGER.error("Exception on cluster shutdown", ex);
    }
  }

  @Test
  public void testExplicitLocalMemberId() {
    ClusterConfig config = ClusterConfig.defaultConfig().memberId("test-member");

    ClusterImpl cluster = null;
    try {
      cluster =
          (ClusterImpl)
              new ClusterImpl(config).transportFactory(TcpTransportFactory::new).startAwait();

      assertEquals("test-member", cluster.member().id());
    } finally {
      if (cluster != null) {
        shutdown(Arrays.asList(cluster));
      }
    }
  }
}
