package io.scalecube.cluster2.fdetector;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.Member;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;

public class FailureDetector extends AbstractAgent {

  private final Member localMember;

  private final FailureDetectorCodec codec = new FailureDetectorCodec();
  private final List<Member> pingMembers = new ArrayList<>();

  public FailureDetector(
      Transport transport,
      BroadcastTransmitter messageTx,
      AtomicBuffer messageBuffer,
      EpochClock epochClock,
      Duration tickInterval,
      Member localMember) {
    super(transport, messageTx, messageBuffer, epochClock, tickInterval);
    this.localMember = localMember;
  }

  @Override
  public String roleName() {
    return null;
  }

  @Override
  protected void onTick() {
    Member pingMember = nextPingMember();
    if (pingMember == null) {
      return;
    }
  }

  private Member nextPingMember() {
    return pingMembers.size() > 0 ? pingMembers.get(random.nextInt(pingMembers.size())) : null;
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {}
}
