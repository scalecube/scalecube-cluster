package io.scalecube.cluster2.payload;

import static io.scalecube.cluster2.ShuffleUtil.shuffle;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.ClusterConfig;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.MemberActionDecoder;
import io.scalecube.cluster2.sbe.MemberActionType;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2LongHashMap;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public class PayloadProtocol extends AbstractAgent {

  private final Member localMember;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final PayloadCodec payloadCodec = new PayloadCodec();
  private final MemberActionDecoder memberActionDecoder = new MemberActionDecoder();
  private final MemberCodec memberCodec = new MemberCodec();
  private final String roleName;
  private long period = 0;
  private final MemberSelector memberSelector;
  private final ArrayList<Member> remoteMembers = new ArrayList<>();
  private final Object2LongHashMap<UUID> payloadIndex = new Object2LongHashMap<>(Long.MIN_VALUE);

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
    memberSelector = new MemberSelector(remoteMembers);
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  protected void onTick() {
    period++;

    final Member member = memberSelector.nextMember();
    if (member == null) {
      return;
    }

    // TODO implement
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {}

  private void onMemberAction(MemberActionDecoder decoder) {
    final MemberActionType actionType = decoder.actionType();
    final Member member = memberCodec.member(decoder::wrapMember);
    decoder.sbeSkip();

    if (localMember.equals(member)) {
      return;
    }

    switch (actionType) {
      case REMOVE_MEMBER:
        onRemoveMember(member);
        break;
      case ADD_MEMBER:
        onAddMember(member);
        break;
      default:
        // no-op
    }
  }

  private void onRemoveMember(Member member) {
    remoteMembers.remove(member);
  }

  private void onAddMember(Member member) {
    if (!remoteMembers.contains(member)) {
      remoteMembers.add(member);
    }
  }

  static class MemberSelector {

    private final ArrayList<Member> remoteMembers;

    private final Random random = new Random();
    private int index;

    MemberSelector(ArrayList<Member> remoteMembers) {
      this.remoteMembers = remoteMembers;
    }

    Member nextMember() {
      final int size = remoteMembers.size();
      if (size == 0) {
        return null;
      }

      final int i;
      if (index >= size) {
        i = index = 0;
        shuffle(remoteMembers, random);
      } else {
        i = index++;
      }

      return remoteMembers.get(i);
    }
  }
}
