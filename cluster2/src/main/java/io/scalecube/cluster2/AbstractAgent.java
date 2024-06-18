package io.scalecube.cluster2;

import io.scalecube.cluster.transport.api2.Transport;
import io.scalecube.cluster.transport.api2.Transport.MessagePoller;
import java.time.Duration;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2LongHashMap.EntryIterator;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
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

  protected MessagePoller messagePoller;
  protected CopyBroadcastReceiver messageRx;
  protected final Delay tickDelay;
  protected final Random random = new Random();
  protected final Long2LongHashMap deadlineByCid = new Long2LongHashMap(Long.MIN_VALUE);
  protected final Long2ObjectHashMap<LongFunction<Consumer<?>>> callbackByCid =
      new Long2ObjectHashMap<>();
  protected final LongArrayList expiredCalls = new LongArrayList();
  protected long currentCid;

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
    tickDelay = new Delay(epochClock, tickInterval.toMillis());
  }

  @Override
  public int doWork() {
    int workCount = 0;

    workCount += pollMessage();
    workCount += receiveMessage();
    workCount += processTick();
    workCount += processCalls();

    return workCount;
  }

  private int pollMessage() {
    int workCount = 0;
    if (messagePoller == null) {
      messagePoller = transport.newMessagePoller();
    }
    try {
      workCount = messagePoller.poll(this);
    } catch (Exception ex) {
      messagePoller = transport.newMessagePoller();
    }
    return workCount;
  }

  private int receiveMessage() {
    int workCount = 0;
    if (messageRx == null) {
      messageRx = messageRxSupplier.get();
    }
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

  private int processCalls() {
    int workCount = 0;

    if (deadlineByCid.size() > 0) {
      final long now = epochClock.time();

      for (final EntryIterator it = deadlineByCid.entrySet().iterator(); it.hasNext(); ) {
        it.next();
        final long cid = it.getLongKey();
        final long deadline = it.getLongValue();
        if (now > deadline) {
          it.remove();
          expiredCalls.add(cid);
          workCount++;
        }
      }

      for (int n = expiredCalls.size() - 1, i = n; i >= 0; i--) {
        final long cid = expiredCalls.fastUnorderedRemove(i);
        final LongFunction<Consumer<?>> callback = callbackByCid.remove(cid);
        callback.apply(cid).accept(null);
      }
    }

    return workCount;
  }

  protected abstract void onTick();

  protected long nextCid() {
    return ++currentCid;
  }
}
