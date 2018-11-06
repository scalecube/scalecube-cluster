package io.scalecube.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.membership.MembershipEvent;
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
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ClusterTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(5);

  @Test
  public void testJoinDynamicPort() {
    // Start seed node
    Cluster seedNode = Cluster.joinAwait();

    int membersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(membersNum);
    try {
      // Start other nodes
      long startAt = System.currentTimeMillis();
      for (int i = 0; i < membersNum; i++) {
        otherNodes.add(Cluster.joinAwait(seedNode.address()));
      }
      LOGGER.info("Start up time: {} ms", System.currentTimeMillis() - startAt);
      assertEquals(membersNum + 1, seedNode.members().size());
      LOGGER.info("Cluster nodes: {}", seedNode.members());
    } finally {
      // Shutdown all nodes
      shutdown(seedNode);
      shutdown(otherNodes);
    }
  }

  @Test
  public void testUpdateMetadata() throws Exception {
    // Start seed member
    Cluster seedNode = Cluster.joinAwait();

    Cluster metadataNode = null;
    int testMembersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(testMembersNum);
    try {
      // Start member with metadata
      Map<String, String> metadata = new HashMap<>();
      metadata.put("key1", "value1");
      metadata.put("key2", "value2");
      metadataNode = Cluster.joinAwait(metadata, seedNode.address());

      // Start other test members
      Flux.range(0, testMembersNum)
          .flatMap(integer -> Cluster.join(seedNode.address()))
          .doOnNext(otherNodes::add)
          .blockLast(TIMEOUT);

      TimeUnit.SECONDS.sleep(3);

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        assertEquals(metadata, memberOptional.get().metadata());
      }

      // Subscribe for membership update event all nodes
      CountDownLatch updateLatch = new CountDownLatch(testMembersNum);
      for (Cluster node : otherNodes) {
        node.listenMembership()
            .filter(MembershipEvent::isUpdated)
            .subscribe(
                event -> {
                  LOGGER.info("Received membership update event: {}", event);
                  updateLatch.countDown();
                });
      }

      // Update metadata
      Map<String, String> updatedMetadata = Collections.singletonMap("key1", "value3");
      metadataNode.updateMetadata(updatedMetadata).block(TIMEOUT);

      // Check all nodes had updated metadata member
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        Member member = memberOptional.get();
        assertEquals(updatedMetadata, member.metadata());
      }
    } finally {
      // Shutdown all nodes
      shutdown(seedNode);
      shutdown(metadataNode);
      shutdown(otherNodes);
    }
  }

  @Test
  public void testUpdateMetadataProperty() throws Exception {
    // Start seed member
    Cluster seedNode = Cluster.joinAwait();

    Cluster metadataNode = null;
    int testMembersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(testMembersNum);

    try {
      // Start member with metadata
      Map<String, String> metadata = new HashMap<>();
      metadata.put("key1", "value1");
      metadata.put("key2", "value2");
      metadataNode = Cluster.joinAwait(metadata, seedNode.address());

      // Start other test members
      Flux.range(0, testMembersNum)
          .flatMap(integer -> Cluster.join(seedNode.address()))
          .doOnNext(otherNodes::add)
          .blockLast(TIMEOUT);

      TimeUnit.SECONDS.sleep(3);

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        assertEquals(metadata, memberOptional.get().metadata());
      }

      // Subscribe for membership update event all nodes
      CountDownLatch updateLatch = new CountDownLatch(testMembersNum);
      for (Cluster node : otherNodes) {
        node.listenMembership()
            .filter(MembershipEvent::isUpdated)
            .subscribe(
                event -> {
                  LOGGER.info("Received membership update event: {}", event);
                  updateLatch.countDown();
                });
      }

      // Update metadata
      metadataNode.updateMetadataProperty("key2", "value3").block(TIMEOUT);

      // Check all nodes had updated metadata member
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        Member member = memberOptional.get();
        Map<String, String> mnodemetadata = member.metadata();
        assertEquals(2, member.metadata().size());
        assertEquals("value1", mnodemetadata.get("key1"));
        assertEquals("value3", mnodemetadata.get("key2"));
      }
    } finally {
      // Shutdown all nodes
      shutdown(seedNode);
      shutdown(metadataNode);
      shutdown(otherNodes);
    }
  }

  @Test
  public void testShutdownCluster() throws Exception {
    // Start seed member
    final Cluster seedNode = Cluster.joinAwait();

    // Start nodes
    final Cluster node1 = Cluster.joinAwait(seedNode.address());
    final Cluster node2 = Cluster.joinAwait(seedNode.address());
    final Cluster node3 = Cluster.joinAwait(seedNode.address());

    node2.shutdown().block(Duration.ofSeconds(30));

    assertTrue(!seedNode.members().contains(node2.member()));
    assertTrue(!node1.members().contains(node2.member()));
    assertTrue(!node3.members().contains(node2.member()));
    assertTrue(node2.isShutdown());

    Mono.when(seedNode.shutdown(), node1.shutdown(), node3.shutdown()).block(TIMEOUT);
  }

  private void shutdown(Cluster... nodes) {
    shutdown(Arrays.asList(nodes));
  }

  private void shutdown(Collection<Cluster> nodes) {
    try {
      Flux.fromIterable(nodes).flatMap(Cluster::shutdown).blockLast(TIMEOUT);
    } catch (Exception ex) {
      LOGGER.error("Exception on cluster shutdown", ex);
    }
  }
}
