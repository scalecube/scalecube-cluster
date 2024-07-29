package io.scalecube.cluster2.payload;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.ClusterConfig;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.AddMemberDecoder;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.RemoveMemberDecoder;
import io.scalecube.cluster2.sbe.SetPayloadDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.function.Supplier;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2LongHashMap;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public class PayloadProtocol extends AbstractAgent {

  private final Member localMember;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final PayloadCodec payloadCodec = new PayloadCodec();
  private final AddMemberDecoder addMemberDecoder = new AddMemberDecoder();
  private final RemoveMemberDecoder removeMemberDecoder = new RemoveMemberDecoder();
  private final MemberCodec memberCodec = new MemberCodec();
  private final SetPayloadDecoder setPayloadDecoder = new SetPayloadDecoder();
  private final String roleName;
  private final ArrayList<Member> remoteMembers = new ArrayList<>();
  private final Object2LongHashMap<UUID> payloadIndex = new Object2LongHashMap<>(Long.MIN_VALUE);
  private final ExpandableArrayBuffer payloadBuffer = new ExpandableArrayBuffer();
  private int payloadLength;
  private long generation;

  public PayloadProtocol(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      ClusterConfig config,
      Member localMember) {
    super(
        transport,
        messageTx,
        messageRxSupplier,
        epochClock,
        Duration.ofMillis(config.payloadInterval()));
    this.localMember = localMember;
    roleName = "payload@" + localMember.address();
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  protected void onTick() {
    // no-op
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
    headerDecoder.wrap(buffer, index);

    final int templateId = headerDecoder.templateId();

    switch (templateId) {
      case SetPayloadDecoder.TEMPLATE_ID:
        onSetPayload(setPayloadDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case AddMemberDecoder.TEMPLATE_ID:
        onAddMember(addMemberDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case RemoveMemberDecoder.TEMPLATE_ID:
        onRemoveMember(removeMemberDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      default:
        // no-op
    }
  }

  private void onSetPayload(SetPayloadDecoder decoder) {
    payloadLength = decoder.payloadLength();

    decoder.getPayload(payloadBuffer, 0, payloadLength);

    messageTx.transmit(
        1,
        payloadCodec.encodePayloadGenerationUpdated(localMember.id(), ++generation, payloadLength),
        0,
        payloadCodec.encodedLength());
  }

  private void onAddMember(AddMemberDecoder decoder) {
    final Member member = memberCodec.member(decoder::wrapMember);
    decoder.sbeSkip();

    if (localMember.equals(member)) {
      return;
    }

    if (!remoteMembers.contains(member)) {
      remoteMembers.add(member);
    }
  }

  private void onRemoveMember(RemoveMemberDecoder decoder) {
    final Member member = memberCodec.member(decoder::wrapMember);
    decoder.sbeSkip();

    if (localMember.equals(member)) {
      return;
    }

    remoteMembers.remove(member);
  }
}
