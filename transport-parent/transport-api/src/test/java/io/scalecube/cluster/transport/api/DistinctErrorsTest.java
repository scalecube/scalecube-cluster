package io.scalecube.cluster.transport.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class DistinctErrorsTest {

  @Test
  void repeatInsideWindowIsSeenAndRepeatAfterWindowIsNew() throws InterruptedException {
    DistinctErrors distinctErrors = new DistinctErrors(Duration.ofMillis(200));
    RuntimeException error = new RuntimeException("error");

    assertFalse(distinctErrors.contains(error), "first observation");
    assertTrue(distinctErrors.contains(error), "repeat inside window");
    assertTrue(distinctErrors.contains(error), "repeat inside window");

    Thread.sleep(300);

    assertFalse(distinctErrors.contains(error), "repeat after window");
    assertTrue(distinctErrors.contains(error), "repeat inside new window");
  }

  @Test
  void olderErrorIsFoundAmongSeveral() {
    DistinctErrors distinctErrors = new DistinctErrors(Duration.ofMinutes(1));
    RuntimeException error1 = new RuntimeException("error1");
    RuntimeException error2 = new RuntimeException("error2");

    assertFalse(distinctErrors.contains(error1));
    assertFalse(distinctErrors.contains(error2));
    assertTrue(distinctErrors.contains(error1), "older error inside window");
    assertTrue(distinctErrors.contains(error2), "newer error inside window");
  }

  @Test
  void expiredEntryIsEvictedWhileLiveOneIsKept() throws InterruptedException {
    DistinctErrors distinctErrors = new DistinctErrors(Duration.ofMillis(200));
    RuntimeException expired = new RuntimeException("expired");
    RuntimeException live = new RuntimeException("live");

    assertFalse(distinctErrors.contains(expired));
    Thread.sleep(150);
    assertFalse(distinctErrors.contains(live));
    Thread.sleep(100);

    assertTrue(distinctErrors.contains(live), "live entry kept");
    assertFalse(distinctErrors.contains(expired), "expired entry evicted");
  }

  @Test
  void defaultConstructorNeverExpires() {
    DistinctErrors distinctErrors = new DistinctErrors();
    RuntimeException error1 = new RuntimeException("error1");
    RuntimeException error2 = new RuntimeException("error2");

    assertFalse(distinctErrors.contains(error1));
    assertFalse(distinctErrors.contains(error2));
    assertTrue(distinctErrors.contains(error1), "repeat");
    assertTrue(distinctErrors.contains(error2), "repeat");
  }
}
