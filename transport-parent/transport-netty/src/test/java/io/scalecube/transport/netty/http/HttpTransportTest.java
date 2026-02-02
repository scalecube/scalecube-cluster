package io.scalecube.transport.netty.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import io.scalecube.transport.netty.BaseTest;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class HttpTransportTest extends BaseTest {

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
  public void testInteractWithNoConnection(TestInfo testInfo) {
    String serverAddress = "localhost:49255";
    for (int i = 0; i < 10; i++) {
      LOGGER.debug("####### {} : iteration = {}", testInfo.getDisplayName(), i);

      client = createHttpTransport();

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
    client = createHttpTransport();
    server = createHttpTransport();

    server
        .listen()
        .subscribe(
            message -> {
              String address = message.sender();
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
  public void testPingPongOnSingleChannel() throws Exception {
    server = createHttpTransport();
    client = createHttpTransport();

    server
        .listen()
        .buffer(2)
        .subscribe(
            messages -> {
              for (Message message : messages) {
                Message echo = Message.withData("echo/" + message.qualifier()).build();
                server
                    .send(message.sender(), echo)
                    .subscribe(null, th -> LOGGER.debug("Failed to send message", th));
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

    List<Message> target = targetFuture.get(3, TimeUnit.SECONDS);
    assertNotNull(target);
    assertEquals(2, target.size());
  }

  @Test
  public void testShouldRequestResponseSuccess() {
    client = createHttpTransport();
    server = createHttpTransport();

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
}
