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
}
