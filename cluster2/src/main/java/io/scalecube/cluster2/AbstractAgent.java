package io.scalecube.cluster2;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster.transport.api2.Transport.MessagePoller;
import java.time.Duration;
import java.util.function.Supplier;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public abstract class AbstractAgent implements Agent, MessageHandler {

  protected final Transport transport;
  protected final BroadcastTransmitter messageTx;
  protected final Supplier<CopyBroadcastReceiver> messageRxSupplier;
  protected final EpochClock epochClock;

  protected final MessagePoller messagePoller;
  protected final CopyBroadcastReceiver messageRx;
  protected final Delay tickDelay;

  public AbstractAgent(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      Duration tickInterval) {
    this.transport = transport;
    this.messageTx = messageTx;
    this.messageRxSupplier = messageRxSupplier;
    this.epochClock = epochClock;
    messagePoller = transport.newMessagePoller();
    messageRx = messageRxSupplier.get();
    tickDelay = tickInterval != null ? new Delay(epochClock, tickInterval.toMillis()) : null;
  }

  @Override
  public int doWork() {
    int workCount = 0;

    workCount += pollMessage();
    workCount += receiveMessage();
    workCount += processTick();

    return workCount;
  }

  private int pollMessage() {
    try {
      return messagePoller.poll(this);
    } catch (Exception ex) {
      throw new AgentTerminationException(ex);
    }
  }

  private int receiveMessage() {
    try {
      return messageRx.receive(this);
    } catch (Exception ex) {
      throw new AgentTerminationException(ex);
    }
  }

  private int processTick() {
    if (tickDelay != null && tickDelay.isOverdue()) {
      tickDelay.delay();
      onTick();
      return 1;
    }
    return 0;
  }

  protected abstract void onTick();
}
