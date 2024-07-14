package io.scalecube.cluster2.membership;

import static io.scalecube.cluster2.ShuffleUtil.shuffle;
import static io.scalecube.cluster2.sbe.MemberActionType.ADD_MEMBER;
import static io.scalecube.cluster2.sbe.MemberStatus.ALIVE;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberActionCodec;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.FailureDetectorEventDecoder;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.MembershipRecordDecoder;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.SyncAckDecoder;
import io.scalecube.cluster2.sbe.SyncDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public class MembershipProtocol extends AbstractAgent {

  private final MembershipConfig config;
  private final MembershipRecord localRecord;
  private final Member localMember;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final SyncDecoder syncDecoder = new SyncDecoder();
  private final SyncAckDecoder syncAckDecoder = new SyncAckDecoder();
  private final FailureDetectorEventDecoder failureDetectorEventDecoder =
      new FailureDetectorEventDecoder();
  private final MembershipRecordDecoder membershipRecordDecoder = new MembershipRecordDecoder();
  private final MemberCodec memberCodec = new MemberCodec();
  private final MemberActionCodec memberActionCodec = new MemberActionCodec();
  private final MembershipRecordCodec membershipRecordCodec = new MembershipRecordCodec();
  private final String roleName;
  private long period = 0;
  private final MemberSelector memberSelector;
  private List<String> seedMembers;
  private final ArrayList<Member> remoteMembers = new ArrayList<>();
  private final ArrayList<Member> nonSeedMembers = new ArrayList<>();
  private final Map<UUID, MembershipRecord> recordMap = new Object2ObjectHashMap<>();

  public MembershipProtocol(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      MembershipConfig config,
      MembershipRecord localRecord) {
    super(
        transport,
        messageTx,
        messageRxSupplier,
        epochClock,
        Duration.ofMillis(config.syncInterval()));
    this.config = config;
    this.localRecord = localRecord;
    this.localMember = localRecord.member();
    roleName = "membership@" + localMember.address();
    seedMembers = config.seedMembers();
    memberSelector = new MemberSelector(seedMembers, remoteMembers, nonSeedMembers);
    recordMap.put(localMember.id(), localRecord);
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  protected void onTick() {
    period++;

    final String seedMember = memberSelector.nextSeedMember();
    if (seedMember != null) {
      doSync(seedMember);
    }

    final Member remoteMember = memberSelector.nextRemoteMember();
    if (remoteMember != null) {
      doSync(remoteMember.address());
    }
  }

  private void doSync(String address) {
    // transport.send();
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

  private void putRecord(MembershipRecord record) {
    final Member member = record.member();
    final UUID key = member.id();
    final MembershipRecord oldRecord = recordMap.get(key);
    if (oldRecord == null) {
      recordMap.put(key, record);
      emitAddMember(member);
      return;
    }

    if (record.incarnation() < oldRecord.incarnation()) {
      return;
    }

    if (localMember.equals(member) && record.status() != ALIVE) {
      localRecord.incarnation(localRecord.incarnation() + 1);
      emitGossip(localRecord);
      return;
    }

    if (record.incarnation() > oldRecord.incarnation()) {
      recordMap.put(key, record);
      return;
    }

    if (record.status() != ALIVE) {
      recordMap.put(key, record);
    }
  }

  private void putRecord(Member member, MemberStatus status) {
    final MembershipRecord record = recordMap.get(member.id());
    if (record != null && record.status() != status) {
      record.status(status);
      emitGossip(record);
    }
  }

  private void forEach(Consumer<MembershipRecord> consumer) {
    recordMap.values().forEach(consumer);
  }

  private void emitAddMember(final Member member) {
    messageTx.transmit(
        1, memberActionCodec.encode(ADD_MEMBER, member), 0, memberActionCodec.encodedLength());
  }

  private void emitGossip(MembershipRecord record) {
    messageTx.transmit(
        1, membershipRecordCodec.encode(record), 0, membershipRecordCodec.encodedLength());
  }

  static class MemberSelector {

    private final List<String> seedMembers;
    private final ArrayList<Member> remoteMembers;
    private final ArrayList<Member> nonSeedMembers;

    private final Random random = new Random();
    private int index;

    MemberSelector(
        List<String> seedMembers,
        ArrayList<Member> remoteMembers,
        ArrayList<Member> nonSeedMembers) {
      this.seedMembers = seedMembers;
      this.remoteMembers = remoteMembers;
      this.nonSeedMembers = nonSeedMembers;
    }

    String nextSeedMember() {
      final int size = seedMembers.size();
      if (size == 0) {
        return null;
      }

      final int i;
      if (index >= size) {
        i = index = 0;
        shuffle(seedMembers, random);
      } else {
        i = index++;
      }

      return seedMembers.get(i);
    }

    Member nextRemoteMember() {
      nonSeedMembers.clear();

      final int size = remoteMembers.size();
      if (size == 0) {
        return null;
      }

      for (int i = 0; i < size; i++) {
        final Member member = remoteMembers.get(i);
        if (!seedMembers.contains(member.address())) {
          nonSeedMembers.add(member);
        }
      }

      final int n = nonSeedMembers.size();
      if (n == 0) {
        return null;
      }

      shuffle(nonSeedMembers, random);

      return nonSeedMembers.get(random.nextInt(n));
    }
  }
}
