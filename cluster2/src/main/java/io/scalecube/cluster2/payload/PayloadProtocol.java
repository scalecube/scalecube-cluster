package io.scalecube.cluster2.payload;

import static io.scalecube.cluster2.UUIDCodec.uuid;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.ClusterConfig;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.AddMemberDecoder;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.PayloadGenerationEventDecoder;
import io.scalecube.cluster2.sbe.RemoveMemberDecoder;
import java.util.ArrayList;
import java.util.UUID;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public class PayloadProtocol extends AbstractAgent {

  private final Member localMember;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final AddMemberDecoder addMemberDecoder = new AddMemberDecoder();
  private final RemoveMemberDecoder removeMemberDecoder = new RemoveMemberDecoder();
  private final PayloadGenerationEventDecoder payloadGenerationEventDecoder =
      new PayloadGenerationEventDecoder();
  private final MemberCodec memberCodec = new MemberCodec();
  private final String roleName;
  private final ArrayList<Member> remoteMembers = new ArrayList<>();
  private final MutableDirectBuffer payload;
  private int payloadLength;

  public PayloadProtocol(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      ClusterConfig config,
      Member localMember) {
    super(transport, messageTx, messageRxSupplier, epochClock, null);
    this.localMember = localMember;
    payload = config.payload();
    payloadLength = config.payloadLength();
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
      case AddMemberDecoder.TEMPLATE_ID:
        onAddMember(addMemberDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case RemoveMemberDecoder.TEMPLATE_ID:
        onRemoveMember(removeMemberDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case PayloadGenerationEventDecoder.TEMPLATE_ID:
        onPayloadGenerationEvent(
            payloadGenerationEventDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      default:
        // no-op
    }
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

  private void onPayloadGenerationEvent(PayloadGenerationEventDecoder decoder) {
    final UUID memberId = uuid(decoder.memberId());
    final int payloadLength = decoder.payloadLength();
  }
}
