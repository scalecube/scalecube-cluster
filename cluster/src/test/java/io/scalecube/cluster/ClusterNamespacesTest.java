package io.scalecube.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.of;

import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.Collection;
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
            .membership(opts -> opts.seedMembers(root.address()))
            .startAwait();

    Cluster root2 =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root2"))
            .membership(opts -> opts.seedMembers(root.address()))
            .startAwait();

    assertTrue(root.otherMembers().isEmpty());
    assertTrue(root1.otherMembers().isEmpty());
    assertTrue(root2.otherMembers().isEmpty());
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
            .membership(opts -> opts.seedMembers(root.address()))
            .startAwait();

    Cluster carol =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root"))
            .membership(opts -> opts.seedMembers(root.address(), bob.address()))
            .startAwait();

    Cluster root2 =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root2"))
            .membership(opts -> opts.seedMembers(root.address()))
            .startAwait();

    Cluster dan =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root2"))
            .membership(
                opts ->
                    opts.seedMembers(
                        root.address(), root2.address(), bob.address(), carol.address()))
            .startAwait();

    Cluster eve =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("root2"))
            .membership(
                opts ->
                    opts.seedMembers(
                        root.address(),
                        root2.address(),
                        dan.address(),
                        bob.address(),
                        carol.address()))
            .startAwait();

    assertContainsInAnyOrder(root.otherMembers(), bob.member(), carol.member());
    assertContainsInAnyOrder(bob.otherMembers(), root.member(), carol.member());
    assertContainsInAnyOrder(carol.otherMembers(), root.member(), bob.member());

    assertContainsInAnyOrder(root2.otherMembers(), dan.member(), eve.member());
    assertContainsInAnyOrder(dan.otherMembers(), root2.member(), eve.member());
    assertContainsInAnyOrder(eve.otherMembers(), root2.member(), dan.member());
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
            .membership(opts -> opts.seedMembers(rootDevelop.address()))
            .startAwait();

    Cluster carol =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("develop/develop"))
            .membership(opts -> opts.seedMembers(rootDevelop.address(), bob.address()))
            .startAwait();

    Cluster dan =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("develop/develop-2"))
            .membership(
                opts -> opts.seedMembers(rootDevelop.address(), bob.address(), carol.address()))
            .startAwait();

    Cluster eve =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("develop/develop-2"))
            .membership(
                opts ->
                    opts.seedMembers(
                        rootDevelop.address(), bob.address(), carol.address(), dan.address()))
            .startAwait();

    assertContainsInAnyOrder(
      rootDevelop.otherMembers(), bob.member(), carol.member(), dan.member(), eve.member());

    assertContainsInAnyOrder(bob.otherMembers(), rootDevelop.member(), carol.member());
    assertContainsInAnyOrder(carol.otherMembers(), rootDevelop.member(), bob.member());

    assertContainsInAnyOrder(dan.otherMembers(), rootDevelop.member(), eve.member());
    assertContainsInAnyOrder(eve.otherMembers(), rootDevelop.member(), dan.member());
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
            .membership(opts -> opts.seedMembers(parent1.address()))
            .startAwait();

    Cluster carol =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("a/1/c"))
            .membership(opts -> opts.seedMembers(parent1.address(), bob.address()))
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
                        parent1.address(), parent2.address(), bob.address(), carol.address()))
            .startAwait();

    //noinspection unused
    Cluster eve =
        new ClusterImpl()
            .transportFactory(WebsocketTransportFactory::new)
            .membership(opts -> opts.namespace("a/111/c"))
            .membership(
                opts ->
                    opts.seedMembers(
                        parent1.address(),
                        parent2.address(),
                        bob.address(),
                        carol.address(),
                        dan.address()))
            .startAwait();

    assertContainsInAnyOrder(parent1.otherMembers(), bob.member(), carol.member());
    assertContainsInAnyOrder(bob.otherMembers(), parent1.member(), carol.member());
    assertContainsInAnyOrder(carol.otherMembers(), parent1.member(), bob.member());
  }

  private static void assertContainsInAnyOrder(Collection<?> collection, Object... items) {
    assertEquals(items.length, collection.size(), "Collection size mismatch");
    for (Object item : items) {
      assertTrue(collection.contains(item), "Collection does not contain: " + item);
    }
  }
}
