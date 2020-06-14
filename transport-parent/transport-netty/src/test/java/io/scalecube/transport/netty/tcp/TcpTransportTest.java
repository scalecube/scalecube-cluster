package io.scalecube.transport.netty.tcp;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.BaseTest;
import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

public class TcpTransportTest extends BaseTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(10);

  // Auto-destroyed on tear down
  private NetworkEmulatorTransport client;
  private NetworkEmulatorTransport server;

  /** Tear down. */
  @AfterEach
  public final void tearDown() {
    destroyTransport(client);
    destroyTransport(server);
  }

  @Test
  public void testUnresolvedHostConnection() {
    client = createTcpTransport();
    // create transport with wrong host
    try {
      Address address = Address.from("wronghost:49255");
      Message message = Message.withData("q").build();
      client.send(address, message).block(Duration.ofSeconds(5));
      fail("fail");
    } catch (Exception e) {
      assertEquals(
          UnknownHostException.class, e.getCause().getClass(), "Unexpected exception class");
    }
  }

  @Test
  public void testInteractWithNoConnection(TestInfo testInfo) {
    Address serverAddress = Address.from("localhost:49255");
    for (int i = 0; i < 10; i++) {
      LOGGER.debug("####### {} : iteration = {}", testInfo.getDisplayName(), i);

      client = createTcpTransport();

      // create transport and don't wait just send message
      try {
        Message msg = Message.withData("q").build();
        client.send(serverAddress, msg).block(Duration.ofSeconds(3));
        fail("fail");
      } catch (Exception e) {
        assertTrue(e.getCause() instanceof IOException, "Unexpected exception type: " + e);
      }

      // send second message: no connection yet and it's clear that there's no connection
      try {
        Message msg = Message.withData("q").build();
        client.send(serverAddress, msg).block(Duration.ofSeconds(3));
        fail("fail");
      } catch (Exception e) {
        assertTrue(e.getCause() instanceof IOException, "Unexpected exception type: " + e);
      }

      destroyTransport(client);
    }
  }

  @Test
  public void testPingPongClientTfListenAndServerTfListen() throws Exception {
    client = createTcpTransport();
    server = createTcpTransport();

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
  public void testNetworkSettings() {
    client = createTcpTransport();
    server = createTcpTransport();

    int lostPercent = 50;
    int mean = 0;
    client.networkEmulator().outboundSettings(server.address(), lostPercent, mean);

    final List<Message> serverMessageList = new ArrayList<>();
    server.listen().subscribe(serverMessageList::add);

    int total = 1000;
    Flux.range(0, total)
        .flatMap(i -> client.send(server.address(), Message.withData("q" + i).build()))
        .onErrorContinue((th, o) -> {})
        .blockLast(TIMEOUT);

    int expectedMax =
        total / 100 * lostPercent + total / 100 * 5; // +5% for maximum possible lost messages
    int size = serverMessageList.size();
    assertTrue(size < expectedMax, "expectedMax=" + expectedMax + ", actual size=" + size);
  }

  @Test
  public void testPingPongOnSingleChannel() throws Exception {
    server = createTcpTransport();
    client = createTcpTransport();

    server
        .listen()
        .buffer(2)
        .subscribe(
            messages -> {
              for (Message message : messages) {
                Message echo = Message.withData("echo/" + message.qualifier()).build();
                server
                    .send(message.sender(), echo)
                    .subscribe(null, th -> LOGGER.error("Failed to send message", th));
              }
            });

    final CompletableFuture<List<Message>> targetFuture = new CompletableFuture<>();
    client.listen().buffer(2).subscribe(targetFuture::complete);

    Message q1 = Message.withData("q1").build();
    Message q2 = Message.withData("q2").build();

    client
        .send(server.address(), q1)
        .subscribe(null, th -> LOGGER.error("Failed to send message", th));
    client
        .send(server.address(), q2)
        .subscribe(null, th -> LOGGER.error("Failed to send message", th));

    List<Message> target = targetFuture.get(1, TimeUnit.SECONDS);
    assertNotNull(target);
    assertEquals(2, target.size());
  }

  @Test
  public void testShouldRequestResponseSuccess() {
    client = createTcpTransport();
    server = createTcpTransport();

    server
        .listen()
        .filter(req -> req.qualifier().equals("hello/server"))
        .subscribe(
            message ->
                send(
                        server,
                        message.sender(),
                        Message.builder()
                            .correlationId(message.correlationId())
                            .data("hello: " + message.data())
                            .build())
                    .subscribe());

    String result =
        client
            .requestResponse(
                server.address(),
                Message.builder()
                    .qualifier("hello/server")
                    .correlationId("123xyz")
                    .data("server")
                    .build())
            .map(msg -> msg.data().toString())
            .block(Duration.ofSeconds(1));

    assertEquals("hello: server", result);
  }

  @Test
  public void testPingPongOnSeparateChannel() throws Exception {
    server = createTcpTransport();
    client = createTcpTransport();

    server
        .listen()
        .buffer(2)
        .subscribe(
            messages -> {
              for (Message message : messages) {
                Message echo = Message.withData("echo/" + message.qualifier()).build();
                server
                    .send(message.sender(), echo)
                    .subscribe(null, th -> LOGGER.error("Failed to send message", th));
              }
            });

    final CompletableFuture<List<Message>> targetFuture = new CompletableFuture<>();
    client.listen().buffer(2).subscribe(targetFuture::complete);

    Message q1 = Message.withData("q1").build();
    Message q2 = Message.withData("q2").build();

    client
        .send(server.address(), q1)
        .subscribe(null, th -> LOGGER.error("Failed to send message", th));
    client
        .send(server.address(), q2)
        .subscribe(null, th -> LOGGER.error("Failed to send message", th));

    List<Message> target = targetFuture.get(1, TimeUnit.SECONDS);
    assertNotNull(target);
    assertEquals(2, target.size());
  }

  @Test
  public void testCompleteObserver() throws Exception {
    server = createTcpTransport();
    client = createTcpTransport();

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

    client.send(server.address(), Message.withData("q").build()).block(Duration.ofSeconds(1));

    assertNotNull(messageLatch.get(1, TimeUnit.SECONDS));

    server.stop().block(TIMEOUT);

    assertTrue(completeLatch.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testObserverThrowsException() throws Exception {
    server = createTcpTransport();
    client = createTcpTransport();

    server
        .listen()
        .subscribe(
            message -> {
              String qualifier = message.data();
              if (qualifier.startsWith("throw")) {
                throw new RuntimeException("" + message);
              }
              if (qualifier.startsWith("q")) {
                Message echo = Message.withData("echo/" + message.qualifier()).build();
                server
                    .send(message.sender(), echo)
                    .subscribe(null, th -> LOGGER.error("Failed to send message", th));
              }
            },
            Throwable::printStackTrace);

    // send "throw" and raise exception on server subscriber
    final CompletableFuture<Message> messageFuture0 = new CompletableFuture<>();
    client.listen().subscribe(messageFuture0::complete);
    Message message = Message.withData("throw").build();
    client
        .send(server.address(), message)
        .subscribe(null, th -> LOGGER.error("Failed to send message", th));
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
    client.send(server.address(), Message.withData("q").build());
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
    client = createTcpTransport();
    server = createTcpTransport();

    server.listen().subscribe(message -> server.send(message.sender(), message).subscribe());

    ReplayProcessor<Message> responses = ReplayProcessor.create();
    client.listen().subscribe(responses);

    // test at unblocked transport
    send(client, server.address(), Message.fromQualifier("q/unblocked")).subscribe();

    // then block client->server messages
    Thread.sleep(1000);
    client.networkEmulator().blockOutbound(server.address());
    send(client, server.address(), Message.fromQualifier("q/blocked")).subscribe();

    StepVerifier.create(responses)
        .assertNext(message -> assertEquals("q/unblocked", message.qualifier()))
        .expectNoEvent(Duration.ofMillis(300))
        .thenCancel()
        .verify(TIMEOUT);
  }

  @Test
  public void testNoChannelLeaksWhenFailedToConnect() {
    client = createTcpTransport();

    for (int i = 0; i <= Math.pow(2, 16); i++) {
      LockSupport.parkNanos(1000);
      try {
        client
            .send(
                Address.create(UUID.randomUUID().toString(), 4801),
                Message.builder().data("hello").build())
            .block(TIMEOUT);
      } catch (Exception ex) {
        Throwable unwrap = Exceptions.unwrap(ex);
        MatcherAssert.assertThat(unwrap, instanceOf(UnknownHostException.class));
      }
    }
  }

  @Test
  public void testNoChannelLeaksWhenSuccessfullyConnected() {
    server = createTcpTransport();
    server
        .listen()
        .doOnNext(
            message -> {
              Address sender = message.sender();
              server.send(sender, message).subscribe();
            })
        .subscribe();

    for (int i = 0; i <= Math.pow(2, 16); i++) {
      LockSupport.parkNanos(1000);
      client = createTcpTransport();
      client
          .requestResponse(
              server.address(),
              Message.withData("hello").correlationId(UUID.randomUUID().toString()).build())
          .block(TIMEOUT);
      client.stop().block(TIMEOUT);
    }
  }
}
