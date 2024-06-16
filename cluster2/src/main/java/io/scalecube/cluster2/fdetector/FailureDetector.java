package io.scalecube.cluster2.fdetector;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster2.AbstractAgent;
import java.time.Duration;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;

public class FailureDetector extends AbstractAgent {

  public FailureDetector(
      EpochClock epochClock,
      Transport transport,
      AtomicBuffer messageBuffer,
      Duration tickInterval) {
    super(transport, messageBuffer, epochClock, tickInterval);
  }

  @Override
  public String roleName() {
    return null;
  }

  @Override
  public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {}

  @Override
  protected void onTick() {}
}
