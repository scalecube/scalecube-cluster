package io.scalecube.cluster2.membership;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.FailureDetectorEventDecoder;
import io.scalecube.cluster2.sbe.MembershipRecordDecoder;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.SyncAckDecoder;
import io.scalecube.cluster2.sbe.SyncDecoder;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public class MembershipProtocol extends AbstractAgent {

  private final MembershipConfig config;
  private final Member localMember;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final SyncDecoder syncDecoder = new SyncDecoder();
  private final SyncAckDecoder syncAckDecoder = new SyncAckDecoder();
  private final FailureDetectorEventDecoder failureDetectorEventDecoder =
      new FailureDetectorEventDecoder();
  private final MembershipRecordDecoder membershipRecordDecoder = new MembershipRecordDecoder();
  private final MemberCodec memberCodec = new MemberCodec();
  private final String roleName;
  private List<String> seedMembers;

  public MembershipProtocol(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      MembershipConfig config,
      Member localMember) {
    super(
        transport,
        messageTx,
        messageRxSupplier,
        epochClock,
        Duration.ofMillis(config.syncInterval()));
    this.config = config;
    this.localMember = localMember;
    roleName = "membership@" + localMember.address();
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
      case SyncDecoder.TEMPLATE_ID:
        onSync(syncDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case SyncAckDecoder.TEMPLATE_ID:
        onSyncAck(syncAckDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case FailureDetectorEventDecoder.TEMPLATE_ID:
        onFailureDetectorEvent(
            failureDetectorEventDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case MembershipRecordDecoder.TEMPLATE_ID:
        onMembershipRecord(
            membershipRecordDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      default:
        // no-op
    }
  }

  private void onSync(SyncDecoder decoder) {}

  private void onSyncAck(SyncAckDecoder decoder) {}

  private void onFailureDetectorEvent(FailureDetectorEventDecoder decoder) {}

  private void onMembershipRecord(MembershipRecordDecoder decoder) {}
}
