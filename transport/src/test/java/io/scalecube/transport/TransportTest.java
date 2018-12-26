package io.scalecube.transport;

import static io.scalecube.transport.TransportTestUtils.createTransport;
import static io.scalecube.transport.TransportTestUtils.destroyTransport;
import static io.scalecube.transport.TransportTestUtils.send;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import reactor.netty.ChannelBindException;

public class TransportTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(10);

  // Auto-destroyed on tear down
  private Transport client;
  private Transport server;

  /** Tear down. */
  @AfterEach
  public final void tearDown() {
    destroyTransport(client);
    destroyTransport(server);
  }

  @Test
  public void testBindExceptionWithoutPortAutoIncrement() {
    TransportConfig config = TransportConfig.builder().port(6000).build();
    Transport transport1 = null;
    Transport transport2 = null;
    try {
      transport1 = Transport.bindAwait(config);
      transport2 = Transport.bindAwait(config);
      fail("Didn't get expected bind exception");
    } catch (Throwable throwable) {
      // Check that get address already in use exception
      assertTrue(
          throwable instanceof ChannelBindException
              || throwable.getMessage().contains("Address already in use"));
    } finally {
      destroyTransport(transport1);
      destroyTransport(transport2);
    }
  }

  @Test
  public void testUnresolvedHostConnection() throws Exception {
    client = createTransport();
    // create transport with wrong host
    try {
      Address address = Address.from("wronghost:49255");
      Message message = Message.withData("q").sender(client.address()).build();
      client.send(address, message).block(Duration.ofSeconds(5));
      fail();
    } catch (Exception e) {
      assertEquals(
          UnknownHostException.class, e.getCause().getClass(), "Unexpected exception class");
    }
  }

  @Test
  public void testInteractWithNoConnection(TestInfo testInfo) throws Exception {
    Address serverAddress = Address.from("localhost:49255");
    for (int i = 0; i < 10; i++) {
      LOGGER.info("####### {} : iteration = {}", testInfo.getDisplayName(), i);

      client = createTransport();

      // create transport and don't wait just send message
      try {
        Message msg = Message.withData("q").sender(client.address()).build();
        client.send(serverAddress, msg).block(Duration.ofSeconds(3));
        fail();
      } catch (Exception e) {
        assertTrue(e.getCause() instanceof IOException, "Unexpected exception type: " + e);
      }

      // send second message: no connection yet and it's clear that there's no connection
      try {
        Message msg = Message.withData("q").sender(client.address()).build();
        client.send(serverAddress, msg).block(Duration.ofSeconds(3));
        fail();
      } catch (Exception e) {
        assertTrue(e.getCause() instanceof IOException, "Unexpected exception type: " + e);
      }

      destroyTransport(client);
    }
  }

  @Test
  public void testPingPongClientTfListenAndServerTfListen() throws Exception {
    client = createTransport();
    server = createTransport();

    server
        .listen()
        .subscribe(
            message -> {
              Address address = message.sender();
              assertEquals(client.address(), address, "Expected clientAddress");
              send(server, address, Message.fromQualifier("hi client")).subscribe();
            });

    CompletableFuture<Message> messageFuture = new CompletableFuture<>();
    client.listen().subscribe(messageFuture::complete);

    send(client, server.address(), Message.fromQualifier("hello server")).subscribe();

    Message result = messageFuture.get(3, TimeUnit.SECONDS);
    assertNotNull(result, "No response from serverAddress");
    assertEquals("hi client", result.qualifier());
  }

  @Test
  public void testNetworkSettings() throws InterruptedException {
    client = createTransport();
    server = createTransport();

    int lostPercent = 50;
    int mean = 0;
    client.networkEmulator().setLinkSettings(server.address(), lostPercent, mean);

    final List<Message> serverMessageList = new ArrayList<>();
    server.listen().subscribe(serverMessageList::add);

    int total = 1000;
    for (int i = 0; i < total; i++) {
      Message message = Message.withData("q" + i).sender(client.address()).build();
      client.send(server.address(), message).subscribe();
    }

    Thread.sleep(1000);

    int expectedMax =
        total / 100 * lostPercent + total / 100 * 5; // +5% for maximum possible lost messages
    int size = serverMessageList.size();
    assertTrue(size < expectedMax, "expectedMax=" + expectedMax + ", actual size=" + size);
  }

  @Test
  public void testPingPongOnSingleChannel() throws Exception {
    server = createTransport();
    client = createTransport();

    server
        .listen()
        .buffer(2)
        .subscribe(
            messages -> {
              for (Message message : messages) {
                Message echo =
                    Message.withData("echo/" + message.qualifier())
                        .sender(server.address())
                        .build();
                server.send(message.sender(), echo).subscribe();
              }
            });

    final CompletableFuture<List<Message>> targetFuture = new CompletableFuture<>();
    client.listen().buffer(2).subscribe(targetFuture::complete);

    Message q1 = Message.withData("q1").sender(client.address()).build();
    Message q2 = Message.withData("q2").sender(client.address()).build();

    client.send(server.address(), q1).subscribe();
    client.send(server.address(), q2).subscribe();

    List<Message> target = targetFuture.get(1, TimeUnit.SECONDS);
    assertNotNull(target);
    assertEquals(2, target.size());
  }

  @Test
  public void testShouldRequestResponseSuccess() throws Exception {
    client = createTransport();
    server = createTransport();

    server
        .listen()
        .filter(req -> req.qualifier().equals("hello/server"))
        .subscribe(
            message -> {
              send(
                      server,
                      message.sender(),
                      Message.builder()
                          .correlationId(message.correlationId())
                          .data("hello: " + message.data())
                          .build())
                  .subscribe();
            });

    String result =
        client
            .requestResponse(
                Message.builder()
                    .sender(client.address())
                    .qualifier("hello/server")
                    .correlationId("123xyz")
                    .data("server")
                    .build(),
                server.address())
            .map(msg -> msg.data().toString())
            .block(Duration.ofSeconds(1));

    assertTrue("hello: server".equals(result));
  }

  @Test
  public void testPingPongOnSeparateChannel() throws Exception {
    server = createTransport();
    client = createTransport();

    server
        .listen()
        .buffer(2)
        .subscribe(
            messages -> {
              for (Message message : messages) {
                Message echo =
                    Message.withData("echo/" + message.qualifier())
                        .sender(server.address())
                        .build();
                server.send(message.sender(), echo).subscribe();
              }
            });

    final CompletableFuture<List<Message>> targetFuture = new CompletableFuture<>();
    client.listen().buffer(2).subscribe(targetFuture::complete);

    Message q1 = Message.withData("q1").sender(client.address()).build();
    Message q2 = Message.withData("q2").sender(client.address()).build();

    client.send(server.address(), q1).subscribe();
    client.send(server.address(), q2).subscribe();

    List<Message> target = targetFuture.get(1, TimeUnit.SECONDS);
    assertNotNull(target);
    assertEquals(2, target.size());
  }

  @Test
  public void testCompleteObserver() throws Exception {
    server = createTransport();
    client = createTransport();

    final CompletableFuture<Boolean> completeLatch = new CompletableFuture<>();
    final CompletableFuture<Message> messageLatch = new CompletableFuture<>();

    server
        .listen()
        .subscribe(
            messageLatch::complete,
            errorConsumer -> {
              // no-op
            },
            () -> completeLatch.complete(true));

    client
        .send(server.address(), Message.withData("q").sender(client.address()).build())
        .block(Duration.ofSeconds(1));

    assertNotNull(messageLatch.get(1, TimeUnit.SECONDS));

    server.stop().block(TIMEOUT);

    assertTrue(completeLatch.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testObserverThrowsException() throws Exception {
    server = createTransport();
    client = createTransport();

    server
        .listen()
        .subscribe(
            message -> {
              String qualifier = message.data();
              if (qualifier.startsWith("throw")) {
                throw new RuntimeException("" + message);
              }
              if (qualifier.startsWith("q")) {
                Message echo =
                    Message.withData("echo/" + message.qualifier())
                        .sender(server.address())
                        .build();
                server.send(message.sender(), echo).subscribe();
              }
            },
            Throwable::printStackTrace);

    // send "throw" and raise exception on server subscriber
    final CompletableFuture<Message> messageFuture0 = new CompletableFuture<>();
    client.listen().subscribe(messageFuture0::complete);
    Message message = Message.withData("throw").sender(client.address()).build();
    client.send(server.address(), message).subscribe();
    Message message0 = null;
    try {
      message0 = messageFuture0.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // ignore since expected behavior
    }
    assertNull(message0);

    // send normal message and check whether server subscriber is broken (no response)
    final CompletableFuture<Message> messageFuture1 = new CompletableFuture<>();
    client.listen().subscribe(messageFuture1::complete);
    client.send(server.address(), Message.withData("q").sender(client.address()).build());
    Message transportMessage1 = null;
    try {
      transportMessage1 = messageFuture1.get(1, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // ignore since expected behavior
    }
    assertNull(transportMessage1);
  }

  @Test
  public void testBlockAndUnblockTraffic() throws Exception {
    client = createTransport();
    server = createTransport();

    server.listen().subscribe(message -> server.send(message.sender(), message).subscribe());

    final List<Message> resp = new ArrayList<>();
    client.listen().subscribe(resp::add);

    // test at unblocked transport
    send(client, server.address(), Message.fromQualifier("q/unblocked")).subscribe();

    // then block client->server messages
    Thread.sleep(1000);
    client.networkEmulator().block(server.address());
    send(client, server.address(), Message.fromQualifier("q/blocked")).subscribe();

    Thread.sleep(1000);
    assertEquals(1, resp.size());
    assertEquals("q/unblocked", resp.get(0).qualifier());
  }
}
