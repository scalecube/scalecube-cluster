package io.scalecube.cluster2.membership;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
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
    // TODO
  }
}
