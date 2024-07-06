package io.scalecube.cluster2;

import java.util.function.Consumer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2LongHashMap.EntryIterator;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;

public class CallbackInvoker {

  private final EpochClock epochClock;

  private final Long2LongHashMap deadlineByCid = new Long2LongHashMap(Long.MIN_VALUE);
  private final Long2ObjectHashMap<Consumer<?>> callbackByCid = new Long2ObjectHashMap<>();
  private final LongArrayList expiredCalls = new LongArrayList();

  public CallbackInvoker(EpochClock epochClock) {
    this.epochClock = epochClock;
  }

  public int doWork() {
    int workCount = 0;

    if (deadlineByCid.isEmpty()) {
      return workCount;
    }

    final long now = epochClock.time();

    for (final EntryIterator it = deadlineByCid.entrySet().iterator(); it.hasNext(); ) {
      it.next();
      final long cid = it.getLongKey();
      final long deadline = it.getLongValue();
      if (now > deadline) {
        it.remove();
        expiredCalls.addLong(cid);
        workCount++;
      }
    }

    for (int n = expiredCalls.size(), i = n - 1; i >= 0; i--) {
      invokeCallback(expiredCalls.fastUnorderedRemove(i), null);
    }

    return workCount;
  }

  public <T> void addCallback(long cid, long timeout, Consumer<T> callback) {
    final long prevDeadline = deadlineByCid.put(cid, epochClock.time() + timeout);
    if (prevDeadline != Long.MIN_VALUE) {
      throw new AgentTerminationException("prevDeadline exists, cid=" + cid);
    }
    final Consumer<?> prevCallback = callbackByCid.put(cid, callback);
    if (prevCallback != null) {
      throw new AgentTerminationException("prevCallback exists, cid=" + cid);
    }
  }

  public void invokeCallback(long cid, Object response) {
    deadlineByCid.remove(cid);
    //noinspection unchecked
    final Consumer<Object> callback = (Consumer<Object>) callbackByCid.remove(cid);
    if (callback != null) {
      callback.accept(response);
    }
  }
}
