package io.scalecube.cluster;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.of;

import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ClusterNamespacesTest extends BaseTest {

  @ParameterizedTest
  @MethodSource("testInvalidNamespaceFormat")
  public void testInvalidNamespaceFormat(String namespace) {
    Exception actualException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new ClusterImpl()
                    .transportFactory(WebsocketTransportFactory::new)
                    .membership(opts -> opts.namespace(namespace))
                    .startAwait());
    Assertions.assertAll(
        () ->
            assertEquals(
                "Invalid cluster config: membership namespace format is invalid",
                actualException.getMessage()));
  }

  @SuppressWarnings("unused")
  public static Stream<Arguments> testInvalidNamespaceFormat() {
    return Stream.of(
        of(""),
        of("  "),
        of("/abc"),
        of("a /b /c"),
        of("a\nb\nc"),
        of(".abc"),
        of("abc."),
        of("a-/b-/c-"),
        of("a+/b+/c+"),
        of("abc/"),
        of("abc/*"),
        of("abc/."),
        of("./abc"),
        of("a./b./c."));
  }

  @Test
  public void testSeparateEmptyNamespaces() {
    Cluster root =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root"))
            .startAwait();

    Cluster root1 =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root1"))
            .membership(opts -> opts.seedMembers(root.addresses()))
            .startAwait();

    Cluster root2 =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root2"))
            .membership(opts -> opts.seedMembers(root.addresses()))
            .startAwait();

    assertThat(root.otherMembers(), iterableWithSize(0));
    assertThat(root1.otherMembers(), iterableWithSize(0));
    assertThat(root2.otherMembers(), iterableWithSize(0));
  }

  @Test
  public void testSeparateNonEmptyNamespaces() {
    Cluster root =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root"))
            .startAwait();

    Cluster bob =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root"))
            .membership(opts -> opts.seedMembers(root.addresses()))
            .startAwait();

    Cluster carol =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root"))
            .membership(opts -> opts.seedMembers(root.addresses().get(0), bob.addresses().get(0)))
            .startAwait();

    Cluster root2 =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root2"))
            .membership(opts -> opts.seedMembers(root.addresses()))
            .startAwait();

    Cluster dan =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root2"))
            .membership(
                opts ->
                    opts.seedMembers(
                        root.addresses().get(0), root2.addresses().get(0), bob.addresses().get(0), carol.addresses().get(0)))
            .startAwait();

    Cluster eve =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root2"))
            .membership(
                opts ->
                    opts.seedMembers(
                        root.addresses().get(0),
                        root2.addresses().get(0),
                        dan.addresses().get(0),
                        bob.addresses().get(0),
                        carol.addresses().get(0)))
            .startAwait();

    assertThat(root.otherMembers(), containsInAnyOrder(bob.member(), carol.member()));
    assertThat(bob.otherMembers(), containsInAnyOrder(root.member(), carol.member()));
    assertThat(carol.otherMembers(), containsInAnyOrder(root.member(), bob.member()));

    assertThat(root2.otherMembers(), containsInAnyOrder(dan.member(), eve.member()));
    assertThat(dan.otherMembers(), containsInAnyOrder(root2.member(), eve.member()));
    assertThat(eve.otherMembers(), containsInAnyOrder(root2.member(), dan.member()));
  }

  @Test
  public void testSimpleNamespacesHierarchy() {
    Cluster rootDevelop =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("develop"))
            .startAwait();

    Cluster bob =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("develop/develop"))
            .membership(opts -> opts.seedMembers(rootDevelop.addresses()))
            .startAwait();

    Cluster carol =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("develop/develop"))
            .membership(opts -> opts.seedMembers(rootDevelop.addresses().get(0), bob.addresses().get(0)))
            .startAwait();

    Cluster dan =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("develop/develop-2"))
            .membership(
                opts -> opts.seedMembers(rootDevelop.addresses().get(0), bob.addresses().get(0), carol.addresses().get(0)))
            .startAwait();

    Cluster eve =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("develop/develop-2"))
            .membership(
                opts ->
                    opts.seedMembers(
                        rootDevelop.addresses().get(0), bob.addresses().get(0), carol.addresses().get(0), dan.addresses().get(0)))
            .startAwait();

    assertThat(
        rootDevelop.otherMembers(),
        containsInAnyOrder(bob.member(), carol.member(), dan.member(), eve.member()));

    assertThat(bob.otherMembers(), containsInAnyOrder(rootDevelop.member(), carol.member()));
    assertThat(carol.otherMembers(), containsInAnyOrder(rootDevelop.member(), bob.member()));

    assertThat(dan.otherMembers(), containsInAnyOrder(rootDevelop.member(), eve.member()));
    assertThat(eve.otherMembers(), containsInAnyOrder(rootDevelop.member(), dan.member()));
  }

  @Test
  public void testIsolatedParentNamespaces() {
    Cluster parent1 =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("a/1"))
            .startAwait();

    Cluster bob =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("a/1/c"))
            .membership(opts -> opts.seedMembers(parent1.addresses()))
            .startAwait();

    Cluster carol =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("a/1/c"))
            .membership(opts -> opts.seedMembers(parent1.addresses().get(0), bob.addresses().get(0)))
            .startAwait();

    Cluster parent2 =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("a/111"))
            .startAwait();

    Cluster dan =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("a/111/c"))
            .membership(
                opts ->
                    opts.seedMembers(
                        parent1.addresses().get(0), parent2.addresses().get(0), bob.addresses().get(0), carol.addresses().get(0)))
            .startAwait();

    //noinspection unused
    Cluster eve =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("a/111/c"))
            .membership(
                opts ->
                    opts.seedMembers(
                        parent1.addresses().get(0),
                        parent2.addresses().get(0),
                        bob.addresses().get(0),
                        carol.addresses().get(0),
                        dan.addresses().get(0)))
            .startAwait();

    assertThat(parent1.otherMembers(), containsInAnyOrder(bob.member(), carol.member()));
    assertThat(bob.otherMembers(), containsInAnyOrder(parent1.member(), carol.member()));
    assertThat(carol.otherMembers(), containsInAnyOrder(parent1.member(), bob.member()));
  }
}
