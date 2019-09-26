package io.scalecube.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.membership.MembershipEvent.Type;
import io.scalecube.cluster.metadata.SimpleMapMetadataCodec;
import io.scalecube.net.Address;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

public class ClusterTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(30);

  @Test
  public void testMembersAccessFromScheduler() {
    // Start seed node
    Cluster seedNode = new ClusterImpl().startAwait();
    Cluster otherNode =
        new ClusterImpl().membership(opts -> opts.seedMembers(seedNode.address())).startAwait();

    assertEquals(2, seedNode.members().size());
    assertEquals(2, otherNode.members().size());

    // Members by address

    Optional<Member> otherNodeOnSeedNode = seedNode.member(otherNode.address());
    Optional<Member> seedNodeOnOtherNode = otherNode.member(seedNode.address());

    assertEquals(otherNode.member(), otherNodeOnSeedNode.orElse(null));
    assertEquals(seedNode.member(), seedNodeOnOtherNode.orElse(null));
  }

  @Test
  public void testJoinLocalhostIgnored() {
    Address[] addresses = {Address.from("localhost:4801"), Address.from("127.0.0.1:4801")};

    // Start seed node
    Cluster seedNode =
        new ClusterImpl()
            .transport(opts -> opts.port(4801).connectTimeout(500))
            .membership(opts -> opts.seedMembers(addresses))
            .startAwait();

    Collection<Member> otherMembers = seedNode.otherMembers();
    assertEquals(0, otherMembers.size(), "otherMembers: " + otherMembers);
  }

  @Test
  public void testJoinLocalhostIgnoredWithOverride() {
    // Start seed node
    Cluster seedNode =
        new ClusterImpl(new ClusterConfig().memberHost("localhost").memberPort(7878))
            .transport(opts -> opts.port(7878).connectTimeout(500))
            .membership(opts -> opts.seedMembers(Address.from("localhost:7878")))
            .startAwait();

    Collection<Member> otherMembers = seedNode.otherMembers();
    assertEquals(0, otherMembers.size(), "otherMembers: " + otherMembers);
  }

  @Test
  public void testJoinDynamicPort() {
    // Start seed node
    Cluster seedNode = new ClusterImpl().startAwait();

    int membersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(membersNum);
    try {
      // Start other nodes
      long startAt = System.currentTimeMillis();
      for (int i = 0; i < membersNum; i++) {
        otherNodes.add(
            new ClusterImpl()
                .membership(opts -> opts.seedMembers(seedNode.address()))
                .startAwait());
      }
      LOGGER.info("Start up time: {} ms", System.currentTimeMillis() - startAt);
      assertEquals(membersNum + 1, seedNode.members().size());
      LOGGER.info("Cluster nodes: {}", seedNode.members());
    } finally {
      // Shutdown all nodes
      shutdown(
          Stream.concat(
                  Stream.of(seedNode), //
                  otherNodes.stream())
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void testUpdateMetadata() throws Exception {
    // Start seed member
    Cluster seedNode = new ClusterImpl().startAwait();

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
              .membership(opts -> opts.seedMembers(seedNode.address()))
              .startAwait();

      // Start other test members
      Flux.range(0, testMembersNum)
          .flatMap(
              integer ->
                  new ClusterImpl()
                      .membership(opts -> opts.seedMembers(seedNode.address()))
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
          Stream.concat(
                  Stream.of(seedNode, metadataNode), //
                  otherNodes.stream())
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void testUpdateMetadataProperty() throws Exception {
    // Start seed member
    Cluster seedNode = new ClusterImpl().startAwait();

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
              .membership(opts -> opts.seedMembers(seedNode.address()))
              .startAwait();

      // Start other test members
      Flux.range(0, testMembersNum)
          .flatMap(
              integer ->
                  new ClusterImpl()
                      .membership(opts -> opts.seedMembers(seedNode.address()))
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
        //noinspection unchecked
        Map<String, String> actualMetadata = (Map<String, String>) node.metadata(member).get();
        assertEquals(2, actualMetadata.size());
        assertEquals("value1", actualMetadata.get("key1"));
        assertEquals("value3", actualMetadata.get("key2"));
      }
    } finally {
      // Shutdown all nodes
      shutdown(
          Stream.concat(
                  Stream.of(seedNode, metadataNode), //
                  otherNodes.stream())
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void testRemoveMetadataProperty() throws Exception {
    // Start seed member
    Cluster seedNode = new ClusterImpl().startAwait();

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
              .membership(opts -> opts.seedMembers(seedNode.address()))
              .startAwait();

      // Start other test members
      Flux.range(0, testMembersNum)
          .flatMap(
              integer ->
                  new ClusterImpl()
                      .membership(opts -> opts.seedMembers(seedNode.address()))
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
        //noinspection unchecked
        Map<String, String> actualMetadata = (Map<String, String>) node.metadata(member).get();
        assertEquals(1, actualMetadata.size());
        assertEquals("value1", actualMetadata.get("key1"));
        assertNull(actualMetadata.get("key2"));
      }
    } finally {
      // Shutdown all nodes
      shutdown(
          Stream.concat(
                  Stream.of(seedNode, metadataNode), //
                  otherNodes.stream())
              .collect(Collectors.toList()));
    }
  }

  @Test
  public void testShutdownCluster() throws Exception {
    CountDownLatch removeAfterLeavingLatch = new CountDownLatch(3);
    CountDownLatch leavingLatch = new CountDownLatch(2);

    ClusterMessageHandler listener =
        new ClusterMessageHandler() {
          private final Set<Member> leavingMembers = new HashSet<>();

          @Override
          public void onMembershipEvent(MembershipEvent event) {
            if (event.isLeaving()) {
              leavingMembers.add(event.member());
              leavingLatch.countDown();
            }

            if (event.isRemoved()) {
              if (leavingMembers.contains(event.member())) {
                removeAfterLeavingLatch.countDown();
              }
            }
          }
        };

    // Start seed member
    final Cluster seedNode = new ClusterImpl().handler(cluster -> listener).startAwait();

    // Start nodes
    final Cluster node1 =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(seedNode.address()))
            .handler(cluster -> listener)
            .startAwait();
    final Cluster node2 =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(seedNode.address()))
            .handler(cluster -> listener)
            .startAwait();
    final Cluster node3 =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(seedNode.address()))
            .handler(cluster -> listener)
            .startAwait();

    node2.shutdown();
    node2.onShutdown().block(TIMEOUT);

    assertTrue(leavingLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
    assertTrue(removeAfterLeavingLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
    assertTrue(node2.isShutdown());

    shutdown(Stream.of(seedNode, node1, node3).collect(Collectors.toList()));
  }

  @Test
  public void testMemberMetadataRemoved() throws InterruptedException {
    // Start seed member
    ReplayProcessor<MembershipEvent> seedEvents = ReplayProcessor.create();
    Map<String, String> seedMetadata = new HashMap<>();
    seedMetadata.put("seed", "shmid");
    final Cluster seedNode =
        new ClusterImpl()
            .config(opts -> opts.metadata(seedMetadata))
            .handler(
                cluster ->
                    new ClusterMessageHandler() {
                      @Override
                      public void onMembershipEvent(MembershipEvent event) {
                        seedEvents.onNext(event);
                      }
                    })
            .startAwait();

    // Start nodes
    // Start member with metadata
    Map<String, String> node1Metadata = new HashMap<>();
    node1Metadata.put("node", "shmod");
    ReplayProcessor<MembershipEvent> node1Events = ReplayProcessor.create();
    final Cluster node1 =
        new ClusterImpl()
            .config(opts -> opts.metadata(node1Metadata))
            .membership(opts -> opts.seedMembers(seedNode.address()))
            .handler(
                cluster ->
                    new ClusterMessageHandler() {
                      @Override
                      public void onMembershipEvent(MembershipEvent event) {
                        node1Events.onNext(event);
                      }
                    })
            .startAwait();

    // Check events
    MembershipEvent nodeAddedEvent = seedEvents.as(Mono::from).block(Duration.ofSeconds(3));
    assertEquals(Type.ADDED, nodeAddedEvent.type());

    MembershipEvent seedAddedEvent = node1Events.as(Mono::from).block(Duration.ofSeconds(3));
    assertEquals(Type.ADDED, seedAddedEvent.type());

    // Check metadata
    assertEquals(node1Metadata, seedNode.metadata(node1.member()).orElse(null));
    assertEquals(seedMetadata, node1.metadata(seedNode.member()).orElse(null));

    // Remove node1 from cluster
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Map<String, String>> removedMetadata = new AtomicReference<>();
    seedEvents
        .filter(MembershipEvent::isRemoved)
        .subscribe(
            event -> {
              Object metadata = SimpleMapMetadataCodec.INSTANCE.decode(event.oldMetadata());
              //noinspection unchecked
              removedMetadata.set((Map<String, String>) metadata);
              latch.countDown();
            });

    node1.shutdown();
    node1.onShutdown().block(TIMEOUT);

    assertTrue(latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
    assertEquals(removedMetadata.get(), node1Metadata);
  }

  @Test
  public void testJoinSeedClusterWithNoExistingSeedMember() {
    // Start seed node
    Cluster seedNode = new ClusterImpl().startAwait();

    Address nonExistingSeed1 = Address.from("localhost:1234");
    Address nonExistingSeed2 = Address.from("localhost:5678");
    Address[] seeds = new Address[] {nonExistingSeed1, nonExistingSeed2, seedNode.address()};

    Cluster otherNode = new ClusterImpl().membership(opts -> opts.seedMembers(seeds)).startAwait();

    assertEquals(otherNode.member(), seedNode.otherMembers().iterator().next());
    assertEquals(seedNode.member(), otherNode.otherMembers().iterator().next());
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
}
