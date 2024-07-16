package io.scalecube.cluster2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.function.LongConsumer;
import org.agrona.concurrent.CachedEpochClock;
import org.junit.jupiter.api.Test;

class TimerInvokerTest {

  private final CachedEpochClock epochClock = new CachedEpochClock();
  private final TimerInvoker timerInvoker = new TimerInvoker(epochClock);

  @Test
  void shouldDoNothing() {
    assertEquals(0, timerInvoker.poll(null));
  }

  @Test
  void shouldExpireTimer() {
    final int timeout = 10;
    final LongConsumer consumer = mock(LongConsumer.class);
    final long timerId = timerInvoker.scheduleTimer(timeout);

    epochClock.advance(timeout + 1);

    assertEquals(1, timerInvoker.poll(consumer));
    verify(consumer).accept(timerId);
  }

  @Test
  void shouldExpireOneTimerAndDontExpireAnother() {
    final int timeout1 = 10;
    final LongConsumer consumer1 = mock(LongConsumer.class);
    final long timerId1 = timerInvoker.scheduleTimer(timeout1);

    final int timeout2 = 20;
    final LongConsumer consumer2 = mock(LongConsumer.class);
    timerInvoker.scheduleTimer(timeout2);

    epochClock.advance(timeout1 + 1);

    assertEquals(1, timerInvoker.poll(consumer1));
    verify(consumer1).accept(timerId1);
    verify(consumer2, never()).accept(anyLong());
  }

  @Test
  void shouldCancelTimer() {
    final int timeout = 10;
    final LongConsumer consumer = mock(LongConsumer.class);
    final long timerId = timerInvoker.scheduleTimer(timeout);
    timerInvoker.cancelTimer(timerId);

    epochClock.advance(timeout + 1);

    assertEquals(0, timerInvoker.poll(consumer));
    verify(consumer, never()).accept(anyLong());
  }
}
