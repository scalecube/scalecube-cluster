package io.scalecube.cluster2.gossip;

import static io.scalecube.cluster2.UUIDCodec.uuid;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.GossipRequestDecoder;
import io.scalecube.cluster2.sbe.GossipRequestDecoder.GossipsDecoder;
import io.scalecube.cluster2.sbe.MembershipEventDecoder;
import io.scalecube.cluster2.sbe.MembershipEventType;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import reactor.core.publisher.MonoSink;

public class GossipProtocol extends AbstractAgent {

  private final GossipConfig config;
  private final Member localMember;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final GossipRequestDecoder gossipRequestDecoder = new GossipRequestDecoder();
  private final MembershipEventDecoder membershipEventDecoder = new MembershipEventDecoder();
  private final GossipCodec codec = new GossipCodec();
  private final MemberCodec memberCodec = new MemberCodec();
  private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();
  private final String roleName;
  private long currentPeriod = 0;
  private long gossipCounter = 0;
  private final Map<UUID, SequenceIdCollector> sequenceIdCollectors = new HashMap<>();
  private final Map<String, GossipState> gossips = new HashMap<>();
  private final Map<String, MonoSink<String>> futures = new HashMap<>();
  private final List<Member> remoteMembers = new ArrayList<>();
  private int remoteMembersIndex = -1;

  public GossipProtocol(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      GossipConfig config,
      Member localMember) {
    super(
        transport,
        messageTx,
        messageRxSupplier,
        epochClock,
        Duration.ofMillis(config.gossipInterval()));
    this.config = config;
    this.localMember = localMember;
    roleName = "gossip@" + localMember.address();
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  protected void onTick() {
    // TODO
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
    headerDecoder.wrap(buffer, index);
    final int templateId = headerDecoder.templateId();
    switch (templateId) {
      case GossipRequestDecoder.TEMPLATE_ID:
        onGossipRequest(gossipRequestDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case MembershipEventDecoder.TEMPLATE_ID:
        onMembershipEvent(membershipEventDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      default:
        // no-op
    }
  }

  private void onGossipRequest(GossipRequestDecoder decoder) {
    final long period = this.currentPeriod;
    final UUID from = uuid(decoder.from());

    for (GossipsDecoder gossipsDecoder : decoder.gossips()) {
      final Gossip gossip = codec.gossip(gossipsDecoder::wrapGossip);
      GossipState gossipState = gossips.get(gossip.gossipId());
      if (ensureSequence(gossip.gossiperId()).add(gossip.sequenceId())) {
        if (gossipState == null) { // new gossip
          gossipState = new GossipState(gossip, period);
          gossips.put(gossip.gossipId(), gossipState);
          emitGossipMessage(gossip.message());
        }
      }
      if (gossipState != null) {
        gossipState.addToInfected(from);
      }
    }
  }

  private void emitGossipMessage(byte[] message) {
    unsafeBuffer.wrap(message);
    messageTx.transmit(1, unsafeBuffer, 0, unsafeBuffer.capacity());
  }

  private SequenceIdCollector ensureSequence(UUID key) {
    return sequenceIdCollectors.computeIfAbsent(key, s -> new SequenceIdCollector());
  }

  private void onMembershipEvent(MembershipEventDecoder decoder) {
    final MembershipEventType type = decoder.type();
    final Member member = memberCodec.member(decoder::wrapMember);
    decoder.sbeSkip();

    if (localMember.equals(member)) {
      return;
    }

    switch (type) {
      case REMOVED:
      case LEAVING:
        remoteMembers.remove(member);
        sequenceIdCollectors.remove(member.id());
        break;
      case ADDED:
        if (!remoteMembers.contains(member)) {
          remoteMembers.add(member);
        }
        break;
      default:
        // no-op
    }
  }
}
