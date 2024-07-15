package io.scalecube.cluster2.membership;

import static io.scalecube.cluster2.ShuffleUtil.shuffle;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.fdetector.FailureDetectorConfig;
import io.scalecube.cluster2.sbe.FailureDetectorEventDecoder;
import io.scalecube.cluster2.sbe.GossipInputMessageDecoder;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.SyncAckDecoder;
import io.scalecube.cluster2.sbe.SyncDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableLong;
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
  private final GossipInputMessageDecoder gossipInputMessageDecoder =
      new GossipInputMessageDecoder();
  private final MembershipRecordCodec membershipRecordCodec = new MembershipRecordCodec();
  private final MemberCodec memberCodec = new MemberCodec();
  private final SyncCodec syncCodec = new SyncCodec();
  private final String roleName;
  private final MutableLong period = new MutableLong();
  private final MemberSelector memberSelector;
  private List<String> seedMembers;
  private final ArrayList<Member> remoteMembers = new ArrayList<>();
  private final ArrayList<Member> nonSeedMembers = new ArrayList<>();
  private final MembershipTable membershipTable;

  public MembershipProtocol(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      MembershipConfig config,
      FailureDetectorConfig fdetectorConfig,
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
    membershipTable =
        new MembershipTable(
            epochClock,
            messageTx,
            localRecord,
            config.suspicionMult(),
            fdetectorConfig.pingInterval());
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  public int doWork() {
    return super.doWork() + membershipTable.doWork();
  }

  @Override
  protected void onTick() {
    period.increment();

    final String seedMember = memberSelector.nextSeedMember();
    if (seedMember != null) {
      doSync(seedMember);
    }

    final Member remoteMember = memberSelector.nextRemoteMember();
    if (remoteMember != null) {
      doSync(remoteMember.address());
    }
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
      case GossipInputMessageDecoder.TEMPLATE_ID:
        onGossipInputMessage(
            gossipInputMessageDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case FailureDetectorEventDecoder.TEMPLATE_ID:
        onFailureDetectorEvent(
            failureDetectorEventDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      default:
        // no-op
    }
  }

  private void onSync(SyncDecoder decoder) {
    final long period = decoder.period();
    final String from = decoder.from();

    if (decoder.membershipRecordLength() != 0) {
      membershipTable.put(membershipRecordCodec.membershipRecord(decoder::wrapMembershipRecord));
    } else {
      doSyncAck(period, from);
    }
  }

  private void onSyncAck(SyncAckDecoder decoder) {
    final long period = decoder.period();

    if (this.period.get() != period) {
      return;
    }

    membershipTable.put(membershipRecordCodec.membershipRecord(decoder::wrapMembershipRecord));
  }

  private void onGossipInputMessage(GossipInputMessageDecoder decoder) {
    membershipTable.put(membershipRecordCodec.membershipRecord(decoder::wrapMessage));
  }

  private void onFailureDetectorEvent(FailureDetectorEventDecoder decoder) {
    final MemberStatus status = decoder.status();
    final Member member = memberCodec.member(decoder::wrapMember);
    membershipTable.put(member, status);
  }

  private void doSync(String address) {
    membershipTable.forEach(
        record ->
            transport.send(
                address,
                syncCodec.encodeSync(Long.MIN_VALUE, null, record),
                0,
                syncCodec.encodedLength()));

    // Complete sync (so that remote side could start SyncAck)

    transport.send(
        address,
        syncCodec.encodeSync(period.get(), localMember.address(), null),
        0,
        syncCodec.encodedLength());
  }

  private void doSyncAck(long period, String address) {
    membershipTable.forEach(
        record ->
            transport.send(
                address, syncCodec.encodeSyncAck(period, record), 0, syncCodec.encodedLength()));
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
