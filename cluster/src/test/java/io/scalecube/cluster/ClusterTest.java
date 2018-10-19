package io.scalecube.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.membership.MembershipEvent;
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

public class ClusterTest extends BaseTest {

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
      for (int i = 0; i < testMembersNum; i++) {
        otherNodes.add(Cluster.joinAwait(seedNode.address()));
      }

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        Member member = memberOptional.get();
        assertEquals(metadata, member.metadata());
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
      metadataNode.updateMetadata(updatedMetadata);

      // Await latch
      updateLatch.await(10, TimeUnit.SECONDS);

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
      for (int i = 0; i < testMembersNum; i++) {
        otherNodes.add(Cluster.joinAwait(seedNode.address()));
      }

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> memberOptional = node.member(metadataNode.member().id());
        assertTrue(memberOptional.isPresent());
        Member member = memberOptional.get();
        assertEquals(metadata, member.metadata());
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
      metadataNode.updateMetadataProperty("key2", "value3");

      // Await latch
      updateLatch.await(10, TimeUnit.SECONDS);

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

    CountDownLatch leave = new CountDownLatch(1);
    node2.shutdown().whenComplete((done, error) -> leave.countDown());

    leave.await(5, TimeUnit.SECONDS);
    assertTrue(!seedNode.members().contains(node2.member()));
    assertTrue(!node1.members().contains(node2.member()));
    assertTrue(!node3.members().contains(node2.member()));
    assertTrue(node2.isShutdown());

    seedNode.shutdown();
    node1.shutdown();
    node3.shutdown();
  }

  private void shutdown(Cluster... nodes) {
    shutdown(Arrays.asList(nodes));
  }

  private void shutdown(Collection<Cluster> nodes) {
    for (Cluster node : nodes) {
      try {
        node.shutdown().get();
      } catch (Exception ex) {
        LOGGER.error("Exception on cluster shutdown", ex);
      }
    }
  }
}
