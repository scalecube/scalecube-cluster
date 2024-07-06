package io.scalecube.cluster2;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster.transport.api2.Transport.MessagePoller;
import java.time.Duration;
import java.util.Random;
import java.util.function.Supplier;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public abstract class AbstractAgent implements Agent, MessageHandler {

  protected final Transport transport;
  protected final BroadcastTransmitter messageTx;
  protected final Supplier<CopyBroadcastReceiver> messageRxSupplier;
  protected final EpochClock epochClock;
  protected final CallbackInvoker callbackInvoker;

  protected MessagePoller messagePoller;
  protected CopyBroadcastReceiver messageRx;
  protected final Delay tickDelay;
  protected final Random random = new Random();
  protected long currentCid;

  public AbstractAgent(
      Transport transport,
      BroadcastTransmitter messageTx,
      Supplier<CopyBroadcastReceiver> messageRxSupplier,
      EpochClock epochClock,
      CallbackInvoker callbackInvoker,
      Duration tickInterval) {
    this.transport = transport;
    this.messageTx = messageTx;
    this.messageRxSupplier = messageRxSupplier;
    this.epochClock = epochClock;
    this.callbackInvoker = callbackInvoker;
    messagePoller = transport.newMessagePoller();
    messageRx = messageRxSupplier.get();
    tickDelay = new Delay(epochClock, tickInterval.toMillis());
  }

  @Override
  public int doWork() {
    int workCount = 0;

    workCount += pollMessage();
    workCount += receiveMessage();
    workCount += processTick();
    workCount += callbackInvoker.doWork();

    return workCount;
  }

  private int pollMessage() {
    int workCount = 0;
    try {
      workCount = messagePoller.poll(this);
    } catch (Exception ex) {
      messagePoller = transport.newMessagePoller();
    }
    return workCount;
  }

  private int receiveMessage() {
    int workCount = 0;
    try {
      workCount = messageRx.receive(this);
    } catch (Exception ex) {
      messageRx = messageRxSupplier.get();
    }
    return workCount;
  }

  private int processTick() {
    if (tickDelay.isOverdue()) {
      tickDelay.delay();
      onTick();
      return 1;
    }
    return 0;
  }

  protected abstract void onTick();

  protected long nextCid() {
    return ++currentCid;
  }

  public long currentCid() {
    return currentCid;
  }
}
