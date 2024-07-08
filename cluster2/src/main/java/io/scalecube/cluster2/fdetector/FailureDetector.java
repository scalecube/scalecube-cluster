package io.scalecube.cluster2.fdetector;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.CallbackInvoker;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.MemberCodec;
import io.scalecube.cluster2.sbe.MemberStatus;
import io.scalecube.cluster2.sbe.MembershipEventDecoder;
import io.scalecube.cluster2.sbe.MembershipEventType;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.PingAckDecoder;
import io.scalecube.cluster2.sbe.PingDecoder;
import io.scalecube.cluster2.sbe.PingRequestDecoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Random;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public class FailureDetector extends AbstractAgent {

  private final FailureDetectorConfig config;
  private final Member localMember;

  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final PingDecoder pingDecoder = new PingDecoder();
  private final PingRequestDecoder pingRequestDecoder = new PingRequestDecoder();
  private final PingAckDecoder pingAckDecoder = new PingAckDecoder();
  private final MembershipEventDecoder membershipEventDecoder = new MembershipEventDecoder();
  private final FailureDetectorCodec codec = new FailureDetectorCodec();
  private final MemberCodec memberCodec = new MemberCodec();
  private final Member from = new Member();
  private final Member target = new Member();
  private final Member issuer = new Member();
  private final String roleName;
  private final MemberSelector memberSelector;
  private final ArrayList<Member> pingMembers = new ArrayList<>();
  private final ArrayList<Member> pingReqMembers = new ArrayList<>();
  private long period = 0;
  private long cid = 0;
  private Member pingMember;
  private MemberStatus memberStatus;

  public FailureDetector(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      CallbackInvoker callbackInvoker,
      FailureDetectorConfig config,
      Member localMember) {
    super(
        transport,
        messageTx,
        messageRxSupplier,
        epochClock,
        callbackInvoker,
        Duration.ofMillis(config.pingInterval()));
    this.localMember = localMember;
    this.config = config;
    roleName = "fdetector@" + localMember.address();
    memberSelector = new MemberSelector(config.pingReqMembers(), pingMembers, pingReqMembers);
  }

  public long period() {
    return period;
  }

  public long cid() {
    return cid;
  }

  @Override
  public String roleName() {
    return roleName;
  }

  @Override
  protected void onTick() {
    period++;

    // Conclude prev. period

    if (pingMember != null && memberStatus == null) {
      emitMemberStatus(pingMember, MemberStatus.SUSPECTED);
    }

    // Init current period

    memberStatus = null;
    pingMember = memberSelector.nextPingMember();
    if (pingMember == null) {
      return;
    }

    // Do ping

    cid++;

    transport.send(
        pingMember.address(),
        codec.encodePing(cid, period, localMember, pingMember, null),
        0,
        codec.encodedLength());

    callbackInvoker.addCallback(cid, config.pingInterval(), this::onResponse);

    // Do ping request

    memberSelector.nextPingReqMembers(pingMember);

    doPingRequest(pingMember);
  }

  private void onResponse(Member target) {
    if (target == null) {
      return;
    }

    if (!Objects.equals(pingMember, target)) {
      throw new AgentTerminationException(
          "PingMember mismatch -- period: "
              + period
              + "pingMember: "
              + pingMember
              + ", target: "
              + target);
    }

    if (memberStatus == null) {
      memberStatus = MemberStatus.ALIVE;
      emitMemberStatus(target, memberStatus);
    }
  }

  private void emitMemberStatus(Member target, final MemberStatus memberStatus) {
    messageTx.transmit(
        1, codec.encodeFailureDetectorEvent(target, memberStatus), 0, codec.encodedLength());
  }

  private void doPingRequest(Member pingMember) {
    for (int n = pingReqMembers.size(), i = n - 1; i >= 0; i--) {
      final Member member = pingReqMembers.get(i);
      ArrayListUtil.fastUnorderedRemove(pingReqMembers, i);

      cid++;

      transport.send(
          member.address(),
          codec.encodePingRequest(cid, period, localMember, pingMember),
          0,
          codec.encodedLength());

      callbackInvoker.addCallback(cid, config.pingInterval(), this::onResponse);
    }
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
    headerDecoder.wrap(buffer, index);

    final int templateId = headerDecoder.templateId();

    switch (templateId) {
      case PingDecoder.TEMPLATE_ID:
        onPing(pingDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case PingRequestDecoder.TEMPLATE_ID:
        onPingRequest(pingRequestDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case PingAckDecoder.TEMPLATE_ID:
        onPingAck(pingAckDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      case MembershipEventDecoder.TEMPLATE_ID:
        onMembershipEvent(membershipEventDecoder.wrapAndApplyHeader(buffer, index, headerDecoder));
        break;
      default:
        // no-op
    }
  }

  private void onPing(PingDecoder decoder) {
    final long cid = decoder.cid();
    final long period = decoder.period();
    final Member from = memberCodec.member(decoder::wrapFrom, this.from);
    final Member target = memberCodec.member(decoder::wrapTarget, this.target);
    final Member issuer = memberCodec.member(decoder::wrapIssuer, this.issuer);

    if (!localMember.equals(target)) {
      return;
    }

    transport.send(
        from.address(),
        codec.encodePingAck(cid, period, from, target, issuer),
        0,
        codec.encodedLength());
  }

  private void onPingRequest(PingRequestDecoder decoder) {
    final long cid = decoder.cid();
    final long period = decoder.period();
    final Member from = memberCodec.member(decoder::wrapFrom, this.from);
    final Member target = memberCodec.member(decoder::wrapTarget, this.target);
    decoder.skipIssuer();

    transport.send(
        target.address(),
        codec.encodePing(cid, period, localMember, target, from),
        0,
        codec.encodedLength());
  }

  private void onPingAck(PingAckDecoder decoder) {
    final long cid = decoder.cid();
    final long period = decoder.period();
    final Member from = memberCodec.member(decoder::wrapFrom, this.from);
    final Member target = memberCodec.member(decoder::wrapTarget, this.target);
    final Member issuer = memberCodec.member(decoder::wrapIssuer, this.issuer);

    // Transit PingAck

    if (issuer != null) {
      transport.send(
          issuer.address(),
          codec.encodePingAck(cid, period, issuer, target, null),
          0,
          codec.encodedLength());
      return;
    }

    // Normal PingAck

    if (this.period == period && localMember.equals(from)) {
      callbackInvoker.invokeCallback(cid, target);
    }
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
        pingMembers.remove(member);
        break;
      case ADDED:
        if (!pingMembers.contains(member)) {
          pingMembers.add(member);
        }
        break;
      default:
        // no-op
    }
  }

  static class MemberSelector {

    private final int pingReqMembersNum;
    private final ArrayList<Member> pingMembers;
    private final ArrayList<Member> pingReqMembers;

    private final Random random = new Random();
    private int index;

    MemberSelector(
        int pingReqMembersNum, ArrayList<Member> pingMembers, ArrayList<Member> pingReqMembers) {
      this.pingReqMembersNum = pingReqMembersNum;
      this.pingMembers = pingMembers;
      this.pingReqMembers = pingReqMembers;
    }

    Member nextPingMember() {
      final int size = pingMembers.size();
      if (size == 0) {
        return null;
      }

      final int i;
      if (index >= size) {
        i = index = 0;
        shuffle();
      } else {
        i = index++;
      }

      return pingMembers.get(i);
    }

    void nextPingReqMembers(Member pingMember) {
      pingReqMembers.clear();

      final int demand = pingReqMembersNum;
      final int size = pingMembers.size();
      if (size <= 1) {
        return;
      }

      for (int i = 0, limit = demand < size ? demand : size - 1; i < limit; ) {
        final Member member = pingMembers.get(random.nextInt(size));
        if (!pingMember.equals(member) && !pingReqMembers.contains(member)) {
          pingReqMembers.add(member);
          i++;
        }
      }
    }

    void shuffle() {
      for (int i = 0, n = pingMembers.size(); i < n; i++) {
        final Member current = pingMembers.get(i);
        final int k = random.nextInt(n);
        final Member member = pingMembers.get(k);
        if (i != k) {
          pingMembers.set(i, member);
          pingMembers.set(k, current);
        }
      }
    }
  }
}
