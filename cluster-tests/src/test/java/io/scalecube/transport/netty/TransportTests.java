package io.scalecube.transport.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.TransportWrapper;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.transport.api.TransportFactory;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class TransportTests {

  public static final Logger LOGGER = LoggerFactory.getLogger(TransportTests.class);

  public static final TransportConfig TRANSPORT_CONFIG = TransportConfig.defaultConfig();
  public static final String NS = "namespace";
  public static final Duration TIMEOUT = Duration.ofSeconds(3);

  private Context context;

  @BeforeEach
  public final void baseSetUp(TestInfo testInfo) {
    LOGGER.info("***** Test started  : " + testInfo.getDisplayName() + " *****");
  }

  @AfterEach
  void afterEach(TestInfo testInfo) {
    if (context != null) {
      context.close();
    }

    LOGGER.info("***** Test finished : " + testInfo.getDisplayName() + " *****");
  }

  @ParameterizedTest
  @MethodSource("transportContexts")
  public void testUnresolvedHostConnection(Context context) {
    this.context = context;

    try {
      Address address = Address.from("wronghost:49255");
      Message message = Message.builder().sender(createMember(address)).data("q").build();
      context.sender.send(address, message).block(Duration.ofSeconds(20));
      fail("fail");
    } catch (Exception e) {
      assertEquals(
          UnknownHostException.class, e.getCause().getClass(), "Unexpected exception class");
    }
  }

  @ParameterizedTest
  @MethodSource("transportContexts")
  public void testInteractWithNoConnection(Context context) {
    this.context = context;

    Address serverAddress = Address.from("localhost:49255");
    Member sender = createMember(serverAddress);

    for (int i = 0; i < 10; i++) {
      Transport transport = context.createTransport();

      // create transport and don't wait just send message
      try {
        Message msg = Message.builder().sender(sender).data("q").build();
        transport.send(serverAddress, msg).block(TIMEOUT);
        fail("fail");
      } catch (Exception e) {
        assertTrue(e.getCause() instanceof IOException, "Unexpected exception type: " + e);
      }

      // send second message: no connection yet and it's clear that there's no connection
      try {
        Message msg = Message.builder().sender(sender).data("q").build();
        transport.send(serverAddress, msg).block(TIMEOUT);
        fail("fail");
      } catch (Exception e) {
        assertTrue(e.getCause() instanceof IOException, "Unexpected exception type: " + e);
      }

      transport.stop().block(TIMEOUT);
    }
  }

  @ParameterizedTest
  @MethodSource("transportContexts")
  public void testConnect(Context context) {
    this.context = context;

    // Create client and try send msg to not-yet existing server

    final int serverPort = 4801;
    final Transport client = context.createTransport();
    final Member clientMember =
        new Member("client", null, Collections.singletonList(client.address()), NS);
    final Member serverMember =
        new Member(
            "server", null, Collections.singletonList(Address.create("localhost", serverPort)), NS);

    // Verify error

    StepVerifier.create(
            new TransportWrapper(client)
                .send(serverMember, Message.builder().sender(clientMember).data("Hola!").build())
                .retry(2))
        .verifyError();

    // Start server

    final Transport server =
        Transport.bindAwait(
            TransportConfig.defaultConfig()
                .port(serverPort)
                .transportFactory(context.transportFactory));

    // Verify success

    StepVerifier.create(
            new TransportWrapper(client)
                .send(serverMember, Message.builder().sender(clientMember).data("Hola!").build()))
        .verifyComplete();

    client.stop().block(TIMEOUT);
    server.stop().block(TIMEOUT);
  }

  @ParameterizedTest
  @MethodSource("transportContexts")
  public void testPingPong(Context context) {
    this.context = context;

    final Transport receiver = context.receiver;
    final Member senderMember = context.senderMember;
    final Member receiverMember = context.receiverMember;

    receiver
        .listen()
        .subscribe(
            message -> {
              final Member sender = message.sender();
              final List<Address> addresses = sender.addresses();

              assertEquals(senderMember, sender, "sender");

              receiver
                  .send(
                      addresses.get(0),
                      Message.builder().sender(receiverMember).data("hi client").build())
                  .subscribe();
            });

    final Transport sender = context.sender;
    CompletableFuture<Message> messageFuture = new CompletableFuture<>();
    sender.listen().subscribe(messageFuture::complete);

    sender
        .send(
            receiver.address(), Message.builder().sender(senderMember).data("hello server").build())
        .subscribe();

    Message result = Mono.fromFuture(messageFuture).block(TIMEOUT);
    assertNotNull(result, "No response from serverAddress");
    assertEquals("hi client", result.data());
    assertEquals(receiverMember, result.sender(), "sender");
  }

  @ParameterizedTest
  @MethodSource("transportContexts")
  public void testPingPongWithTransportWrapper(Context context) {
    this.context = context;

    final Transport receiver = context.receiver;
    final Member senderMember = context.senderMember;
    final TransportWrapper receiverWrapper = context.receiverWrapper;
    final Member receiverMember = context.receiverMember;

    receiver
        .listen()
        .subscribe(
            message -> {
              assertEquals(senderMember, message.sender(), "sender");

              receiverWrapper
                  .send(
                      message.sender(),
                      Message.builder().sender(receiverMember).data("hi client").build())
                  .subscribe();
            });

    CompletableFuture<Message> messageFuture = new CompletableFuture<>();
    context.sender.listen().subscribe(messageFuture::complete);

    final Message ping = Message.builder().sender(senderMember).data("hello server").build();
    context.senderWrapper.send(receiverMember, ping).subscribe();

    Message result = Mono.fromFuture(messageFuture).block(TIMEOUT);
    assertNotNull(result, "No response from serverAddress");
    assertEquals("hi client", result.data());
    assertEquals(receiverMember, result.sender(), "sender");
  }

  @ParameterizedTest
  @MethodSource("transportContexts")
  public void testRequestResponse(Context context) {
    this.context = context;

    final Transport receiver = context.receiver;
    final Member senderMember = context.senderMember;
    final Member receiverMember = context.receiverMember;

    receiver
        .listen()
        .filter(req -> req.qualifier().equals("hello/server"))
        .subscribe(
            message -> {
              final Member sender = message.sender();
              final List<Address> addresses = sender.addresses();

              assertEquals(senderMember, sender, "sender");

              receiver
                  .send(
                      addresses.get(0),
                      Message.builder()
                          .correlationId(message.correlationId())
                          .sender(receiverMember)
                          .data("hello: " + message.data())
                          .build())
                  .subscribe();
            });

    Message result =
        context
            .sender
            .requestResponse(
                receiver.address(),
                Message.builder()
                    .qualifier("hello/server")
                    .correlationId("" + System.nanoTime())
                    .sender(senderMember)
                    .data("server")
                    .build())
            .block(TIMEOUT);

    //noinspection ConstantConditions
    assertEquals("hello: server", result.data().toString(), "data");
    assertEquals(receiverMember, result.sender(), "sender");
  }

  @ParameterizedTest
  @MethodSource("transportContexts")
  public void testRequestResponseWithTransportWrapper(Context context) {
    this.context = context;

    final Transport receiver = context.receiver;
    final Member senderMember = context.senderMember;
    final TransportWrapper receiverWrapper = context.receiverWrapper;
    final Member receiverMember = context.receiverMember;

    receiver
        .listen()
        .filter(req -> req.qualifier().equals("hello/server"))
        .subscribe(
            message -> {
              assertEquals(senderMember, message.sender(), "sender");

              receiverWrapper
                  .send(
                      message.sender(),
                      Message.builder()
                          .correlationId(message.correlationId())
                          .sender(receiverMember)
                          .data("hello: " + message.data())
                          .build())
                  .subscribe();
            });

    Message result =
        context
            .sender
            .requestResponse(
                receiver.address(),
                Message.builder()
                    .qualifier("hello/server")
                    .correlationId("" + System.nanoTime())
                    .sender(senderMember)
                    .data("server")
                    .build())
            .block(TIMEOUT);

    //noinspection ConstantConditions
    assertEquals("hello: server", result.data().toString(), "data");
    assertEquals(receiverMember, result.sender(), "sender");
  }

  private static Stream<Arguments> transportContexts() {
    final Builder<Arguments> builder = Stream.builder();

    builder.add(Arguments.of(new Context(new TcpTransportFactory())));
    builder.add(Arguments.of(new Context(new WebsocketTransportFactory())));

    return builder.build();
  }

  private static Member createMember(Address address) {
    return new Member("0", null, address, "NAMESPACE");
  }

  private static class Context implements AutoCloseable {

    private final Transport receiver;
    private final Transport sender;
    private final Member receiverMember;
    private final Member senderMember;
    private final TransportFactory transportFactory;
    private final TransportWrapper receiverWrapper;
    private final TransportWrapper senderWrapper;

    public Context(TransportFactory transportFactory) {
      this.transportFactory = transportFactory;
      receiver = Transport.bindAwait(TRANSPORT_CONFIG.transportFactory(transportFactory));
      sender = Transport.bindAwait(TRANSPORT_CONFIG.transportFactory(transportFactory));

      receiverMember =
          new Member("receiver", null, Collections.singletonList(receiver.address()), NS);
      senderMember = new Member("sender", null, Collections.singletonList(sender.address()), NS);

      receiverWrapper = new TransportWrapper(receiver);
      senderWrapper = new TransportWrapper(sender);
    }

    private Transport createTransport() {
      return Transport.bindAwait(TRANSPORT_CONFIG.transportFactory(transportFactory));
    }

    @Override
    public void close() {
      receiver.stop().block(TIMEOUT);
      sender.stop().block(TIMEOUT);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Context.class.getSimpleName() + "[", "]")
          .add("transportFactory=" + transportFactory.getClass().getSimpleName())
          .toString();
    }
  }
}
