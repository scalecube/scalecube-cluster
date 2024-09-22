package io.scalecube.cluster.transport.api;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DistinctErrors {

  private final List<DistinctObservation> distinctObservations = new ArrayList<>();
  private final long evictionInterval;

  /** Constructor. */
  public DistinctErrors() {
    this(null);
  }

  /**
   * Constructor.
   *
   * @param evictionInterval optional, how long consider incoming observation as unique.
   */
  public DistinctErrors(Duration evictionInterval) {
    this.evictionInterval =
        evictionInterval != null && evictionInterval.toMillis() > 0
            ? evictionInterval.toMillis()
            : Long.MAX_VALUE;
  }

  /**
   * Return true if there is an observation (or at least in the eviction time window) of this error
   * type for a stack trace. Otherwise a new entry will be created and kept.
   *
   * @param observation an error observation
   * @return true if such observation exists.
   */
  public boolean contains(Throwable observation) {
    synchronized (this) {
      final long now = System.currentTimeMillis();
      DistinctObservation distinctObservation = find(now, distinctObservations, observation);

      if (distinctObservation == null) {
        distinctObservations.add(new DistinctObservation(observation, now + evictionInterval));
        return false;
      }

      if (distinctObservation.deadline > now) {
        distinctObservation.resetDeadline(now + evictionInterval);
        return false;
      }
    }

    return true;
  }

  private static DistinctObservation find(
      long now, List<DistinctObservation> existingObservations, Throwable observation) {
    DistinctObservation existingObservation = null;

    for (int lastIndex = existingObservations.size() - 1, i = lastIndex; i >= 0; i--) {
      final DistinctObservation o = existingObservations.get(lastIndex);

      if (equals(o.throwable, observation)) {
        existingObservation = o;
        break;
      }

      if (o.deadline > now) {
        if (i == lastIndex) {
          existingObservations.remove(i);
        } else {
          existingObservations.set(i, existingObservations.remove(lastIndex));
        }
        lastIndex--;
      }
    }

    return existingObservation;
  }

  private static boolean equals(Throwable lhs, Throwable rhs) {
    while (true) {
      if (lhs == rhs) {
        return true;
      }

      if (lhs.getClass() == rhs.getClass()
          && Objects.equals(lhs.getMessage(), rhs.getMessage())
          && equals(lhs.getStackTrace(), rhs.getStackTrace())) {
        lhs = lhs.getCause();
        rhs = rhs.getCause();

        if (null == lhs && null == rhs) {
          return true;
        } else if (null != lhs && null != rhs) {
          continue;
        }
      }

      return false;
    }
  }

  private static boolean equals(
      StackTraceElement[] lhsStackTrace, StackTraceElement[] rhsStackTrace) {
    if (lhsStackTrace.length != rhsStackTrace.length) {
      return false;
    }

    for (int i = 0, length = lhsStackTrace.length; i < length; i++) {
      final StackTraceElement lhs = lhsStackTrace[i];
      final StackTraceElement rhs = rhsStackTrace[i];

      if (lhs.getLineNumber() != rhs.getLineNumber()
          || !lhs.getClassName().equals(rhs.getClassName())
          || !Objects.equals(lhs.getMethodName(), rhs.getMethodName())
          || !Objects.equals(lhs.getFileName(), rhs.getFileName())) {
        return false;
      }
    }

    return true;
  }

  private static final class DistinctObservation {

    private final Throwable throwable;
    private long deadline;

    DistinctObservation(Throwable throwable, long deadline) {
      this.throwable = throwable;
      this.deadline = deadline;
    }

    void resetDeadline(long deadline) {
      this.deadline = deadline;
    }
  }
}
