package io.scalecube.cluster2.payload;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import io.scalecube.cluster2.ClusterConfig;
import io.scalecube.cluster2.Member;
import java.time.Duration;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public class PayloadProtocol extends AbstractAgent {

  private final String roleName;
  private final Member localMember;

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
  protected void onTick() {}

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {}
}
