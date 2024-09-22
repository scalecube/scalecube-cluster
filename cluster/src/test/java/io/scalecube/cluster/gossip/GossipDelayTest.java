package io.scalecube.cluster.gossip;

import static io.scalecube.cluster.transport.api.Transport.parsePort;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.cluster.BaseTest;
import io.scalecube.cluster.ClusterMath;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class GossipDelayTest extends BaseTest {

  private static final String NAMESPACE = "ns";

  private static final long gossipInterval = GossipConfig.DEFAULT_GOSSIP_INTERVAL;
  private static final int gossipFanout = GossipConfig.DEFAULT_GOSSIP_FANOUT;
  private static final int gossipRepeatMultiplier = GossipConfig.DEFAULT_GOSSIP_REPEAT_MULT;

  private final Scheduler scheduler = Schedulers.newSingle("scheduler", true);

  @Test
  public void testMessageDelayMoreThanGossipSweepTime() throws InterruptedException {
    final NetworkEmulatorTransport transport1 = getNetworkEmulatorTransport(0, 3000);
    final NetworkEmulatorTransport transport2 = getNetworkEmulatorTransport(0, 3000);
    final NetworkEmulatorTransport transport3 = getNetworkEmulatorTransport(0, 100);

    final GossipProtocolImpl gossipProtocol1 =
        initGossipProtocol(
            transport1,
            Arrays.asList(transport1.address(), transport2.address(), transport3.address()));
    final GossipProtocolImpl gossipProtocol2 =
        initGossipProtocol(
            transport2,
            Arrays.asList(transport1.address(), transport2.address(), transport3.address()));
    final GossipProtocolImpl gossipProtocol3 =
        initGossipProtocol(
            transport3,
            Arrays.asList(transport1.address(), transport2.address(), transport3.address()));

    final AtomicInteger protocol1GossipCounter = new AtomicInteger(0);
    final AtomicInteger protocol2GossipCounter = new AtomicInteger(0);
    final AtomicInteger protocol3GossipCounter = new AtomicInteger(0);

    gossipProtocol1.listen().subscribe(message -> protocol1GossipCounter.incrementAndGet());
    gossipProtocol2.listen().subscribe(message -> protocol2GossipCounter.incrementAndGet());
    gossipProtocol3.listen().subscribe(message -> protocol3GossipCounter.incrementAndGet());

    for (int i = 0; i < 3; i++) {
      gossipProtocol1.spread(Message.fromData("message: " + i)).subscribe();
    }

    TimeUnit.MILLISECONDS.sleep(
        2 * (ClusterMath.gossipTimeoutToSweep(gossipRepeatMultiplier, 3, gossipInterval) + 6000));

    assertEquals(0, protocol1GossipCounter.get());
    assertEquals(3, protocol2GossipCounter.get());
    assertEquals(3, protocol3GossipCounter.get());
  }

  private NetworkEmulatorTransport getNetworkEmulatorTransport(int lostPercent, int meanDelay) {
    NetworkEmulatorTransport transport = createTransport();
    transport.networkEmulator().setDefaultOutboundSettings(lostPercent, meanDelay);
    return transport;
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
}
