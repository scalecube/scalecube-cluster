package io.scalecube.cluster.gossip;

import static io.scalecube.cluster.ClusterMath.gossipConvergencePercent;
import static io.scalecube.cluster.ClusterMath.gossipDisseminationTime;
import static io.scalecube.cluster.ClusterMath.maxMessagesPerGossipPerNode;
import static io.scalecube.cluster.ClusterMath.maxMessagesPerGossipTotal;
import static io.scalecube.cluster.transport.api.Transport.parsePort;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.BaseTest;
import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class GossipProtocolTest extends BaseTest {

  private static final Logger LOGGER = System.getLogger(GossipProtocolTest.class.getName());

  private static final List<int[]> experiments =
      Arrays.asList(
          new int[][] {
            // N , L , D // N - num of nodes, L - msg loss percent, D - msg mean delay (ms)
            {2, 0, 2}, // warm up
            {2, 0, 2},
            {3, 0, 2},
            {5, 0, 2},
            {10, 0, 2},
            {10, 10, 2},
            {10, 25, 2},
            {10, 25, 100},
            {10, 50, 2},
            {50, 0, 2},
            {50, 10, 2},
            {50, 10, 100},
          });

  // Makes tests run longer since always awaits for maximum gossip lifetime, but performs more
  // checks
  private static final boolean awaitFullCompletion = true;

  // Allow to configure gossip settings other than defaults
  private static final long gossipInterval /* ms */ = GossipConfig.DEFAULT_GOSSIP_INTERVAL;
  private static final int gossipFanout = GossipConfig.DEFAULT_GOSSIP_FANOUT;
  private static final int gossipRepeatMultiplier = GossipConfig.DEFAULT_GOSSIP_REPEAT_MULT;

  private static final String NAMESPACE = "ns";

  // Uncomment and modify params to run single experiment repeatedly
  // static {
  // int repeatCount = 1000;
  // int membersNum = 10;
  // int lossPercent = 50; //%
  // int meanDelay = 2; //ms
  // experiments = new ArrayList<>(repeatCount + 1);
  // experiments.add(new Object[] {2, 0, 2}); // add warm up experiment
  // for (int i = 0; i < repeatCount; i++) {
  // experiments.add(new Object[] {membersNum, lossPercent, meanDelay});
  // }
  // }

  @SuppressWarnings("unused")
  static Stream<Arguments> experiment() {
    return experiments.stream().map(objects -> Arguments.of(objects[0], objects[1], objects[2]));
  }

  private Scheduler scheduler;

  @BeforeEach
  void setUp() {
    scheduler = Schedulers.newSingle("scheduler", true);
  }

  @AfterEach
  void tearDown() {
    if (scheduler != null) {
      scheduler.dispose();
    }
  }

  @ParameterizedTest(name = "N={0}, Ploss={1}%, Tmean={2}ms")
  @MethodSource("experiment")
  void testGossipProtocol(int membersNum, int lossPercent, int meanDelay) throws Exception {
    // Init gossip protocol instances
    List<GossipProtocolImpl> gossipProtocols =
        initGossipProtocols(membersNum, lossPercent, meanDelay);

    // Subscribe on gossips
    long disseminationTime = 0;
    LongSummaryStatistics messageSentStatsDissemination = null;
    LongSummaryStatistics messageLostStatsDissemination = null;
    LongSummaryStatistics messageSentStatsOverall = null;
    LongSummaryStatistics messageLostStatsOverall = null;
    long gossipTimeout =
        ClusterMath.gossipTimeoutToSweep(gossipRepeatMultiplier, membersNum, gossipInterval);
    try {
      final String gossipData = "test gossip - " + ThreadLocalRandom.current().nextLong();
      final CountDownLatch latch = new CountDownLatch(membersNum - 1);
      final Map<Member, Member> receivers = new ConcurrentHashMap<>();
      final AtomicBoolean doubleDelivery = new AtomicBoolean(false);
      for (final GossipProtocolImpl gossipProtocol : gossipProtocols) {
        gossipProtocol
            .listen()
            .subscribe(
                gossip -> {
                  final Member localMember = BaseTest.getField(gossipProtocol, "localMember");
                  final Transport transport = BaseTest.getField(gossipProtocol, "transport");

                  if (gossipData.equals(gossip.data())) {
                    boolean firstTimeAdded = receivers.put(localMember, localMember) == null;
                    if (firstTimeAdded) {
                      latch.countDown();
                    } else {
                      LOGGER.log(
                          Level.ERROR, "Delivered gossip twice to: {0}", transport.address());
                      doubleDelivery.set(true);
                    }
                  }
                });
      }

      // Spread gossip, measure and verify delivery metrics
      long start = System.currentTimeMillis();
      gossipProtocols.get(0).spread(Message.fromData(gossipData)).subscribe();
      latch.await(2 * gossipTimeout, TimeUnit.MILLISECONDS); // Await for double gossip timeout
      disseminationTime = System.currentTimeMillis() - start;
      messageSentStatsDissemination = computeMessageSentStats(gossipProtocols);
      if (lossPercent > 0) {
        messageLostStatsDissemination = computeMessageLostStats(gossipProtocols);
      }
      assertEquals(membersNum - 1, receivers.size(), "Not all members received gossip");
      assertTrue(
          disseminationTime < gossipTimeout,
          "Too long dissemination time "
              + disseminationTime
              + "ms (timeout "
              + gossipTimeout
              + "ms)");

      // Await gossip lifetime plus few gossip intervals too ensure gossip is fully spread
      if (awaitFullCompletion) {
        long awaitCompletionTime = gossipTimeout - disseminationTime + 3 * gossipInterval;
        Thread.sleep(awaitCompletionTime);

        messageSentStatsOverall = computeMessageSentStats(gossipProtocols);
        if (lossPercent > 0) {
          messageLostStatsOverall = computeMessageLostStats(gossipProtocols);
        }
      }
      assertFalse(doubleDelivery.get(), "Delivered gossip twice to same member");
    } finally {
      // Print theoretical results
      LOGGER.log(
          Level.INFO,
          "Experiment params: "
              + "N={0}, Gfanout={1}, Grepeat_mult={2}, Tgossip={3}ms Ploss={4}%, Tmean={5}ms",
          membersNum,
          gossipFanout,
          gossipRepeatMultiplier,
          gossipInterval,
          lossPercent,
          meanDelay);
      double convergProb =
          gossipConvergencePercent(gossipFanout, gossipRepeatMultiplier, membersNum, lossPercent);
      long expDissemTime =
          gossipDisseminationTime(gossipRepeatMultiplier, membersNum, gossipInterval);
      int maxMsgPerNode =
          maxMessagesPerGossipPerNode(gossipFanout, gossipRepeatMultiplier, membersNum);
      int maxMsgTotal = maxMessagesPerGossipTotal(gossipFanout, gossipRepeatMultiplier, membersNum);
      LOGGER.log(
          Level.INFO,
          "Expected dissemination time is {0}ms with probability {1}%",
          expDissemTime,
          convergProb);
      LOGGER.log(
          Level.INFO, "Max messages sent per node {0} and total {1}", maxMsgPerNode, maxMsgTotal);

      // Print actual results
      LOGGER.log(
          Level.INFO,
          "Actual dissemination time: {0}ms (timeout {1}ms)",
          disseminationTime,
          gossipTimeout);
      LOGGER.log(Level.INFO, "Messages sent stats (diss.): {0}", messageSentStatsDissemination);
      if (lossPercent > 0) {
        LOGGER.log(Level.INFO, "Messages lost stats (diss.): {0}", messageLostStatsDissemination);
      }
      if (awaitFullCompletion) {
        LOGGER.log(Level.INFO, "Messages sent stats (total): {0}", messageSentStatsOverall);
        if (lossPercent > 0) {
          LOGGER.log(Level.INFO, "Messages lost stats (total): {0}", messageLostStatsOverall);
        }
      }

      // Destroy gossip protocol instances
      destroyGossipProtocols(gossipProtocols);
    }
  }

  private LongSummaryStatistics computeMessageSentStats(List<GossipProtocolImpl> gossipProtocols) {
    List<Long> messageSentPerNode = new ArrayList<>(gossipProtocols.size());
    for (GossipProtocolImpl gossipProtocol : gossipProtocols) {
      final NetworkEmulatorTransport transport = BaseTest.getField(gossipProtocol, "transport");
      messageSentPerNode.add(transport.networkEmulator().totalMessageSentCount());
    }
    return messageSentPerNode.stream().mapToLong(v -> v).summaryStatistics();
  }

  private LongSummaryStatistics computeMessageLostStats(List<GossipProtocolImpl> gossipProtocols) {
    List<Long> messageLostPerNode = new ArrayList<>(gossipProtocols.size());
    for (GossipProtocolImpl gossipProtocol : gossipProtocols) {
      final NetworkEmulatorTransport transport = BaseTest.getField(gossipProtocol, "transport");
      messageLostPerNode.add(transport.networkEmulator().totalOutboundMessageLostCount());
    }
    return messageLostPerNode.stream().mapToLong(v -> v).summaryStatistics();
  }

  private List<GossipProtocolImpl> initGossipProtocols(int count, int lostPercent, int meanDelay) {
    final List<Transport> transports = initTransports(count, lostPercent, meanDelay);
    List<String> members = new ArrayList<>();
    for (Transport transport : transports) {
      members.add(transport.address());
    }
    List<GossipProtocolImpl> gossipProtocols = new ArrayList<>();
    for (Transport transport : transports) {
      gossipProtocols.add(initGossipProtocol(transport, members));
    }
    return gossipProtocols;
  }

  private List<Transport> initTransports(int count, int lostPercent, int meanDelay) {
    List<Transport> transports = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      NetworkEmulatorTransport transport = createTransport();
      transport.networkEmulator().setDefaultOutboundSettings(lostPercent, meanDelay);
      transports.add(transport);
    }
    return transports;
  }

  private GossipProtocolImpl initGossipProtocol(Transport transport, List<String> members) {
    GossipConfig gossipConfig =
        new GossipConfig()
            .gossipFanout(gossipFanout)
            .gossipInterval(gossipInterval)
            .gossipRepeatMult(gossipRepeatMultiplier);

    Member localMember =
        new Member(
            "member-" + parsePort(transport.address()), null, transport.address(), NAMESPACE);

    Flux<MembershipEvent> membershipFlux =
        Flux.fromIterable(members)
            .filter(address -> !transport.address().equals(address))
            .map(address -> new Member("member-" + parsePort(address), null, address, NAMESPACE))
            .map(member -> MembershipEvent.createAdded(member, null, 0));

    GossipProtocolImpl gossipProtocol =
        new GossipProtocolImpl(localMember, transport, membershipFlux, gossipConfig, scheduler);
    gossipProtocol.start();
    return gossipProtocol;
  }

  private static void destroyGossipProtocols(List<GossipProtocolImpl> gossipProtocols) {
    // Stop all gossip protocols
    for (GossipProtocolImpl gossipProtocol : gossipProtocols) {
      gossipProtocol.stop();
    }

    // Stop all transports
    List<Mono<Void>> futures = new ArrayList<>();
    for (GossipProtocolImpl gossipProtocol : gossipProtocols) {
      futures.add(BaseTest.<Transport>getField(gossipProtocol, "transport").stop());
    }

    try {
      Mono.when(futures).block(Duration.ofSeconds(30));
    } catch (Exception ex) {
      LOGGER.log(Level.WARNING, "Failed to await transport termination: " + ex);
    }

    // Await a bit
    try {
      Thread.sleep(gossipProtocols.size() * 20L);
    } catch (InterruptedException ignore) {
      // ignore
    }
  }
}
