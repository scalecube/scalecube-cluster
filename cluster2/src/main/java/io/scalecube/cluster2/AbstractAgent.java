package io.scalecube.cluster2;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster.transport.api2.Transport.MessagePoller;
import java.time.Duration;
import java.util.Random;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

public abstract class AbstractAgent implements Agent, MessageHandler {

  protected final Transport transport;
  protected final AtomicBuffer messageBuffer;

  protected MessagePoller messagePoller;
  protected BroadcastTransmitter messageTx;
  protected CopyBroadcastReceiver messageRx;
  protected final Delay tickDelay;
  protected final Random random = new Random();

  public AbstractAgent(
      Transport transport,
      AtomicBuffer messageBuffer,
      EpochClock epochClock,
      Duration tickInterval) {
    this.transport = transport;
    this.messageBuffer = messageBuffer;
    tickDelay = new Delay(epochClock, tickInterval.toMillis());
  }

  @Override
  public void onStart() {
    messagePoller = transport.newMessagePoller();
    messageTx = new BroadcastTransmitter(messageBuffer);
    messageRx = new CopyBroadcastReceiver(new BroadcastReceiver(messageBuffer));
  }

  @Override
  public int doWork() {
    int workCount = 0;

    workCount += pollMessage();
    workCount += receiveMessage();

    if (tickDelay.isOverdue()) {
      tickDelay.delay();
      onTick();
    }

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
      messageRx = new CopyBroadcastReceiver(new BroadcastReceiver(messageBuffer));
    }
    return workCount;
  }

  protected abstract void onTick();
}
