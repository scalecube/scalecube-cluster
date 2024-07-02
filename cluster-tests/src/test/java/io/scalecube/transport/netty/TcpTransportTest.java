//package io.scalecube.transport.netty.tcp;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertNotNull;
//import static org.junit.jupiter.api.Assertions.assertNull;
//import static org.junit.jupiter.api.Assertions.assertTrue;
//import static org.junit.jupiter.api.Assertions.fail;
//
//import io.scalecube.cluster.Member;
//import io.scalecube.cluster.TransportWrapper;
//import io.scalecube.cluster.transport.api.Message;
//import io.scalecube.cluster.utils.NetworkEmulatorTransport;
//import io.scalecube.net.Address;
//import io.scalecube.transport.netty.BaseTest;
//import java.io.IOException;
//import java.net.UnknownHostException;
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.TestInfo;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Sinks;
//import reactor.test.StepVerifier;
//
//public class TcpTransportTest extends BaseTest {
//
//  public static final Duration TIMEOUT = Duration.ofSeconds(10);
//
//  // Auto-destroyed on tear down
//  private NetworkEmulatorTransport client;
//  private NetworkEmulatorTransport server;
//
//  /** Tear down. */
//  @AfterEach
//  public final void tearDown() {
//    destroyTransport(client);
//    destroyTransport(server);
//  }
//
//  @Test
//  public void testNetworkSettings() {
//    client = createTcpTransport();
//    server = createTcpTransport();
//
//    int lostPercent = 50;
//    int mean = 0;
//    final Address address = server.address();
//    client.networkEmulator().outboundSettings(address, lostPercent, mean);
//
//    final List<Message> serverMessageList = new ArrayList<>();
//    server.listen().subscribe(serverMessageList::add);
//
//    int total = 1000;
//    Flux.range(0, total)
//        .flatMap(
//            i ->
//                client.send(
//                    address, Message.builder().sender(createMember(address)).data("q" + i).build()))
//        .onErrorContinue((th, o) -> {})
//        .blockLast(TIMEOUT);
//
//    int expectedMax =
//        total / 100 * lostPercent + total / 100 * 5; // +5% for maximum possible lost messages
//    int size = serverMessageList.size();
//    assertTrue(size < expectedMax, "expectedMax=" + expectedMax + ", actual size=" + size);
//  }
//
//  @Test
//  public void testBlockAndUnblockTraffic() throws Exception {
//    client = createTcpTransport();
//    server = createTcpTransport();
//
//    server
//        .listen()
//        .subscribe(message -> TransportWrapper.send(server, message.sender(), message).subscribe());
//
//    Sinks.Many<Message> responses = Sinks.many().replay().all();
//    client
//        .listen()
//        .subscribe(responses::tryEmitNext, responses::tryEmitError, responses::tryEmitComplete);
//
//    // test at unblocked transport
//    send(client, server.address(), Message.fromQualifier("q/unblocked")).subscribe();
//
//    // then block client->server messages
//    Thread.sleep(1000);
//    client.networkEmulator().blockOutbound(server.address());
//    send(client, server.address(), Message.fromQualifier("q/blocked")).subscribe();
//
//    StepVerifier.create(responses.asFlux())
//        .assertNext(message -> assertEquals("q/unblocked", message.qualifier()))
//        .expectNoEvent(Duration.ofMillis(300))
//        .thenCancel()
//        .verify(TIMEOUT);
//  }
//}
