package io.scalecube.cluster.gossip;

import io.scalecube.cluster.BaseTest;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class GossipProtocolStopTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private final Scheduler scheduler = Schedulers.newSingle("gossip-stop-test", true);
  private Transport transport;
  private GossipProtocolImpl gossipProtocol;

  @BeforeEach
  void setUp() {
    transport = createTransport();
    gossipProtocol =
        new GossipProtocolImpl(
            new Member("member", null, transport.address(), "ns"),
            transport,
            Flux.never(),
            // Long interval: gossip never gets "disseminated" during the test
            new GossipConfig().gossipInterval(60_000),
            scheduler);
    gossipProtocol.start();
  }

  @AfterEach
  void tearDown() {
    gossipProtocol.stop();
    transport.stop().block(TIMEOUT);
    scheduler.dispose();
  }

  @Test
  void testPendingSpreadFailsOnStop() {
    StepVerifier.create(gossipProtocol.spread(message()))
        .then(() -> awaitPendingFutures(1))
        .then(gossipProtocol::stop)
        .expectError(IllegalStateException.class)
        .verify(TIMEOUT);
  }

  @Test
  void testSpreadAfterStopFails() {
    gossipProtocol.stop();

    StepVerifier.create(gossipProtocol.spread(message()))
        .expectError(IllegalStateException.class)
        .verify(TIMEOUT);
  }

  @Test
  void testCancelledSpreadIsRemoved() {
    var subscription = gossipProtocol.spread(message()).subscribe();
    awaitPendingFutures(1);

    subscription.dispose();

    // Removal may land on the scheduler thread right after dispose(), so await it
    awaitPendingFutures(0);
  }

  private static Message message() {
    return Message.builder().qualifier("test").data("data").build();
  }

  private void awaitPendingFutures(int expected) {
    var deadline = System.nanoTime() + TIMEOUT.toNanos();
    Map<String, ?> futures = getField(gossipProtocol, "futures");
    while (futures.size() != expected) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("expected " + expected + " pending futures: " + futures);
      }
      Thread.onSpinWait();
    }
  }
}
