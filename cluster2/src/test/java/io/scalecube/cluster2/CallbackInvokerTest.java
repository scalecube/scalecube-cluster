package io.scalecube.cluster2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import java.util.function.Consumer;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class CallbackInvokerTest {

  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final CallbackInvoker callbackInvoker = new CallbackInvoker(epochClock);

  @Test
  void shouldDoNothing() {
    assertEquals(0, callbackInvoker.doWork());
  }

  @Test
  void shouldExpireCallback() {
    final int cid = 100;
    final int timeout = 10;
    final Consumer consumer = mock(Consumer.class);
    callbackInvoker.addCallback(cid, timeout, consumer);

    epochClock.advance(timeout + 1);

    assertEquals(1, callbackInvoker.doWork());
    verify(consumer).accept(null);
  }

  @Test
  void shouldExpireOneCallbackAndDontExpireAnother() {
    final int cid1 = 100;
    final int timeout1 = 10;
    final Consumer consumer1 = mock(Consumer.class);
    callbackInvoker.addCallback(cid1, timeout1, consumer1);

    final int cid2 = 200;
    final int timeout2 = 20;
    final Consumer consumer2 = mock(Consumer.class);
    callbackInvoker.addCallback(cid2, timeout2, consumer2);

    epochClock.advance(timeout1 + 1);

    assertEquals(1, callbackInvoker.doWork());
    verify(consumer1).accept(null);
    verify(consumer2, never()).accept(any());
  }

  @Test
  void shouldInvokeCallback() {
    final int cid = 100;
    final int timeout = 10;
    final Consumer consumer = mock(Consumer.class);
    callbackInvoker.addCallback(cid, timeout, consumer);

    assertEquals(0, callbackInvoker.doWork());
    verify(consumer, never()).accept(any());

    reset(consumer);
    final long response = System.currentTimeMillis();
    callbackInvoker.invokeCallback(cid, response);

    verify(consumer).accept(response);

    epochClock.update(timeout + 1);
    assertEquals(0, callbackInvoker.doWork());
  }
}
