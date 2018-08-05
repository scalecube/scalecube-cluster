package io.scalecube.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Address;
import io.scalecube.cluster.transport.api.ServiceLoaderUtil;
import io.scalecube.cluster.transport.api.TransportFactory;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class ClusterTest extends BaseTest {

  @Parameterized.Parameters(name = "Transport={0}")
  public static Collection<Object[]> data() {
    return ServiceLoaderUtil.findAll(TransportFactory.class).stream()
        .map(factory -> new Object[] {factory})
        .collect(Collectors.toList());
  }

  public ClusterTest(TransportFactory transportFactory) {
    this.transportFactory = transportFactory;
  }

  private final TransportFactory transportFactory;

  @Test
  public void testJoinDynamicPort() {
    // Start seed node
    Cluster seedNode = cluster();

    int membersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(membersNum);
    try {
      // Start other nodes
      long startAt = System.currentTimeMillis();
      for (int i = 0; i < membersNum; i++) {
        otherNodes.add(cluster(seedNode.address()));
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
    Cluster seedNode = cluster();

    Cluster metadataNode = null;
    int testMembersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(testMembersNum);
    try {
      // Start member with metadata
      Map<String, String> metadata = new HashMap<>();
      metadata.put("key1", "value1");
      metadata.put("key2", "value2");
      metadataNode = cluster(metadata, seedNode.address());


      // Start other test members
      for (int i = 0; i < testMembersNum; i++) {
        otherNodes.add(cluster(seedNode.address()));
      }

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
        assertTrue(mNodeOpt.isPresent());
        Member mNode = mNodeOpt.get();
        assertEquals(metadata, mNode.metadata());
      }

      // Subscribe for membership update event all nodes
      CountDownLatch updateLatch = new CountDownLatch(testMembersNum);
      for (Cluster node : otherNodes) {
        node.listenMembership()
            .filter(MembershipEvent::isUpdated)
            .subscribe(event -> {
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
        Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
        assertTrue(mNodeOpt.isPresent());
        Member mNode = mNodeOpt.get();
        assertEquals(updatedMetadata, mNode.metadata());
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
    Cluster seedNode = cluster();

    Cluster metadataNode = null;
    int testMembersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(testMembersNum);

    try {
      // Start member with metadata
      Map<String, String> metadata = new HashMap<>();
      metadata.put("key1", "value1");
      metadata.put("key2", "value2");

      metadataNode = cluster(metadata, seedNode.address());

      // Start other test members
      for (int i = 0; i < testMembersNum; i++) {
        otherNodes.add(cluster(seedNode.address()));
      }

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
        assertTrue(mNodeOpt.isPresent());
        Member mNode = mNodeOpt.get();
        assertEquals(metadata, mNode.metadata());
      }

      // Subscribe for membership update event all nodes
      CountDownLatch updateLatch = new CountDownLatch(testMembersNum);
      for (Cluster node : otherNodes) {
        node.listenMembership()
            .filter(MembershipEvent::isUpdated)
            .subscribe(event -> {
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
        Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
        assertTrue(mNodeOpt.isPresent());
        Member mNode = mNodeOpt.get();
        Map<String, String> mNodeMetadata = mNode.metadata();
        assertEquals(2, mNode.metadata().size());
        assertEquals("value1", mNodeMetadata.get("key1"));
        assertEquals("value3", mNodeMetadata.get("key2"));
      }
    } finally {
      // Shutdown all nodes
      shutdown(seedNode);
      shutdown(metadataNode);
      shutdown(otherNodes);
    }
  }

  @Test
  public void testShutdownCluster() {
    // Start seed member
    Cluster seedNode = cluster();

    // Start nodes
    Cluster node1 = cluster(seedNode.address());
    Cluster node2 = cluster(seedNode.address());
    Cluster node3 = cluster(seedNode.address());

    node2.shutdown()
        .block(Duration.ofSeconds(5));

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
        node.shutdown().block();
      } catch (Exception ex) {
        LOGGER.error("Exception on cluster shutdown", ex);
      }
    }
  }

  private Cluster cluster() {
    return Cluster.joinAwait(ClusterConfig.builder()
        .transportSupplier(transportFactory::create)
        .build());
  }

  private Cluster cluster(Address... address) {
    return Cluster.joinAwait(ClusterConfig.builder()
        .seedMembers(address)
        .transportSupplier(transportFactory::create)
        .build());
  }

  private Cluster cluster(Map<String, String> metadata, Address... address) {
    return Cluster.joinAwait(ClusterConfig.builder()
        .seedMembers(address)
        .metadata(metadata)
        .transportSupplier(transportFactory::create)
        .build());
  }
}
