package io.scalecube.cluster2.gossip;

import static io.scalecube.cluster2.UUIDCodec.uuid;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.ClusterMath;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.GossipDecoder;
import io.scalecube.cluster2.sbe.GossipMessageDecoder;
import io.scalecube.cluster2.sbe.GossipRequestDecoder;
import io.scalecube.cluster2.sbe.MembershipEventDecoder;
import io.scalecube.cluster2.sbe.MembershipEventType;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public class GossipProtocol extends AbstractAgent {

  private final GossipConfig config;
  private final Member localMember;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final GossipMessageDecoder gossipMessageDecoder = new GossipMessageDecoder();
  private final GossipRequestDecoder gossipRequestDecoder = new GossipRequestDecoder();
  private final GossipDecoder gossipDecoder = new GossipDecoder();
  private final MembershipEventDecoder membershipEventDecoder = new MembershipEventDecoder();
  private final GossipRequestCodec gossipRequestCodec = new GossipRequestCodec();
  private final MemberCodec memberCodec = new MemberCodec();
  private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();
  private final String roleName;
  private long currentPeriod = 0;
  private long gossipCounter = 0;
  private final Map<UUID, SequenceIdCollector> sequenceIdCollectors = new Object2ObjectHashMap<>();
  private final ArrayList<Gossip> gossips = new ArrayList<>();
  private final ArrayList<Member> remoteMembers = new ArrayList<>();
  private final ArrayList<Member> gossipMembers = new ArrayList<>();
  private final IntArrayList gossipsToSend = new IntArrayList();
  private final IntArrayList gossipsToRemove = new IntArrayList();

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
    long period = ++currentPeriod;

    checkGossipSegmentation();

    if (gossips.isEmpty()) {
      return;
    }

    // Spread gossips

    nextGossipMembers();

    for (int n = gossipMembers.size() - 1, i = n; i >= 0; i--) {
      final Member member = gossipMembers.get(i);
      ArrayListUtil.fastUnorderedRemove(gossipMembers, i);
      spreadGossips(period, member);
    }

    // Sweep gossips

    nextGossipsToRemove(period);

    for (int n = gossipsToRemove.size() - 1, i = n; i >= 0; i--) {
      final int index = gossipsToRemove.fastUnorderedRemove(i);
      ArrayListUtil.fastUnorderedRemove(gossips, index);
    }
  }

  private void nextGossipMembers() {
    gossipMembers.clear();

    final int demand = config.gossipFanout();
    final int size = remoteMembers.size();
    if (demand == 0 || size == 0) {
      return;
    }

    for (int i = 0, limit = Math.min(demand, size); i < limit; ) {
      final Member member = remoteMembers.get(random.nextInt(remoteMembers.size()));
      if (!gossipMembers.contains(member)) {
        gossipMembers.add(member);
        i++;
      }
    }
  }

  private void nextGossipsToRemove(long period) {
    gossipsToRemove.clear();

    final int periodsToSweep =
        ClusterMath.gossipPeriodsToSweep(config.gossipRepeatMult(), remoteMembers.size() + 1);

    for (int i = 0, n = gossips.size(); i < n; i++) {
      final Gossip gossip = gossips.get(i);
      if (period > gossip.infectionPeriod() + periodsToSweep) {
        gossipsToRemove.addInt(i);
      }
    }
  }

  private void checkGossipSegmentation() {
    final int intervalsThreshold = config.gossipSegmentationThreshold();
    for (Entry<UUID, SequenceIdCollector> entry : sequenceIdCollectors.entrySet()) {
      // Size of sequenceIdCollector could grow only if we never received some messages.
      // Which is possible only if current node wasn't available(suspected) for some time
      // or network issue
      final SequenceIdCollector sequenceIdCollector = entry.getValue();
      if (sequenceIdCollector.size() > intervalsThreshold) {
        sequenceIdCollector.clear();
      }
    }
  }

  private void spreadGossips(long period, Member member) {
    nextGossipsToSend(period, member);

    final String address = member.address();
    final UUID from = localMember.id();

    for (int n = gossipsToSend.size() - 1, i = n; i >= 0; i--) {
      final Gossip gossip = gossips.get(gossipsToSend.fastUnorderedRemove(i));
      transport.send(
          address, gossipRequestCodec.encode(from, gossip), 0, gossipRequestCodec.encodedLength());
    }
  }

  private void nextGossipsToSend(long period, Member member) {
    gossipsToSend.clear();

    final int periodsToSpread =
        ClusterMath.gossipPeriodsToSpread(config.gossipRepeatMult(), remoteMembers.size() + 1);

    for (int i = 0, n = gossips.size(); i < n; i++) {
      final Gossip gossip = gossips.get(i);
      if (gossip.infectionPeriod() + periodsToSpread >= period && !gossip.isInfected(member.id())) {
        gossipsToSend.addInt(i);
      }
    }
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
    headerDecoder.wrap(buffer, index);
    final int templateId = headerDecoder.templateId();
    switch (templateId) {
      case GossipMessageDecoder.TEMPLATE_ID:
        onGossipMessage(gossipMessageDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
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

  private void onGossipMessage(GossipMessageDecoder decoder) {
    final long period = currentPeriod;
    final long sequenceId = ++gossipCounter;

    final int messageLength = decoder.messageLength();
    final byte[] message = new byte[messageLength];
    decoder.getMessage(message, 0, messageLength);

    gossips.add(new Gossip(localMember.id(), sequenceId, message, period));
    ensureSequence(localMember.id()).add(sequenceId);
  }

  private void onGossipRequest(GossipRequestDecoder decoder) {
    final long period = currentPeriod;
    final UUID from = uuid(decoder.from());

    decoder.wrapGossip(unsafeBuffer);

    gossipDecoder.wrapAndApplyHeader(unsafeBuffer, 0, headerDecoder);

    final UUID gossiperId = uuid(gossipDecoder.gossiperId());
    final long sequenceId = gossipDecoder.sequenceId();

    final int messageLength = gossipDecoder.messageLength();
    final byte[] message = new byte[messageLength];
    gossipDecoder.getMessage(message, 0, messageLength);

    Gossip gossip = getGossip(gossiperId, sequenceId);

    if (ensureSequence(gossiperId).add(sequenceId)) {
      if (gossip == null) { // new gossip
        gossip = new Gossip(gossiperId, sequenceId, message, period);
        gossips.add(gossip);
        emitMessage(message); // TODO: why emit is happening here? is it correct?
      }
    }

    if (gossip != null) {
      gossip.addToInfected(from);
    }
  }

  private Gossip getGossip(UUID gossiperId, long sequenceId) {
    Gossip result = null;
    for (int i = 0, n = gossips.size(); i < n; i++) {
      final Gossip gossip = gossips.get(i);
      if (gossip.gossiperId() == gossiperId && gossip.sequenceId() == sequenceId) {
        result = gossip;
        break;
      }
    }
    return result;
  }

  private void emitMessage(byte[] message) {
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
