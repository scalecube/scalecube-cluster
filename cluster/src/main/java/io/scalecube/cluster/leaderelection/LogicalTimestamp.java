package io.scalecube.cluster.leaderelection;

import java.io.Serializable;

/**
 * This class represents specific time value of logical clock at the given moment of time. This
 * class is immutable. It provides convenient operations for working with logical time.
 *
 * <p>
 * Logical timestamps assigned to each event in the distributed system obey causality, but they can
 * not distinguish concurrent events. Logical timestamps not guaranteed to be ordered or unequal for
 * concurrent events:
 * 
 * <pre>
 * E1 -> E2 => timestamp(E1) < timestamp(E2), but
 * timestamp(E1) < timestamp(E2) => {E1 -> E2} or {E1 and E2 are concurrent}
 * </pre>
 *
 * <p>
 * In order to identify concurrent events see
 * {@link com.antonkharenko.logicalclocks.VectorTimestamp}.
 *
 * <p>
 * See also Leslie Lamport's paper <a
 * href="http://research.microsoft.com/en-us/um/people/lamport/pubs/time-clocks.pdf"> Time, Clocks,
 * and the Ordering of Events in a Distributed System</a> for more info.
 *
 */
public final class LogicalTimestamp implements Comparable<LogicalTimestamp>, Serializable {

  private static final long serialVersionUID = -919934135310565056L;

  private static final int LONG_BYTES = 8;

  private final long cyclicTime;

  /**
   * Creates instance of logical timestamp at the initial moment of time.
   */
  public LogicalTimestamp() {
    this(0L);
  }

  /**
   * Creates instance of logical timestamp at the given logical time. Time value passed as argument
   * should be considered to be represented in cyclic time.
   *
   * @param cyclicTime logical time counter value
   */
  private LogicalTimestamp(long cyclicTime) {
    this.cyclicTime = cyclicTime;
  }

  /**
   * Returns timestamp which is next in time comparing to this timestamp instance.
   */
  public LogicalTimestamp nextTimestamp() {
    return new LogicalTimestamp(cyclicTime + 1);
  }

  /**
   * Converts given byte array into corresponding logical timestamp. It is supposed that given byte
   * array was produced by {@link LogicalTimestamp#toBytes()} method.
   */
  public static LogicalTimestamp fromBytes(byte[] bytes) {
    long count = 0;
    for (byte aByte : bytes) {
      count <<= Byte.SIZE;
      count += aByte & 0xFF;
    }
    return new LogicalTimestamp(count);
  }

  /**
   * Converts given long value into corresponding logical timestamp. It is supposed that given long
   * was produced by {@link LogicalTimestamp#toLong()} method.
   */
  public static LogicalTimestamp fromLong(long longValue) {
    return new LogicalTimestamp(longValue);
  }

  /**
   * Converts this timestamp into a byte array representation. It can be converted back by
   * {@link LogicalTimestamp#fromBytes(byte[])} method.
   */
  public byte[] toBytes() {
    byte[] bytes = new byte[LONG_BYTES];
    for (int i = 0; i < LONG_BYTES; i++) {
      bytes[i] = (byte) (cyclicTime >> (LONG_BYTES - i - 1 << 3));
    }
    return bytes;
  }

  /**
   * Converts this timestamp into a long value. It can be converted back by
   * {@link LogicalTimestamp#fromBytes(byte[])} method.
   */
  public long toLong() {
    return cyclicTime;
  }

  /**
   * Returns true if the given timestamp is smaller than this timestamp.
   */
  public boolean isBefore(LogicalTimestamp timestamp) {
    return compareTo(timestamp) < 0;
  }

  /**
   * Returns true if the given timestamp is bigger than this timestamp.
   */
  public boolean isAfter(LogicalTimestamp timestamp) {
    return compareTo(timestamp) > 0;
  }

  /**
   * Compares two logical timestamps.
   *
   * @param that the logical timestamp to be compared.
   * @return the value {@code 0} if this timestamp is happens at same logical time to the argument
   *         timestamp; a value less than {@code 0} if this timestamp is smaller than the argument
   *         timestamp; and a value greater than {@code 0} if this timestamp is bigger than the
   *         argument timestamp.
   */
  @Override
  public int compareTo(LogicalTimestamp that) {
    if ((this.cyclicTime < 0) == (that.cyclicTime < 0)) {
      return Long.compare(this.cyclicTime, that.cyclicTime);
    } else {
      return Long.compare(that.cyclicTime & Long.MAX_VALUE, this.cyclicTime & Long.MAX_VALUE);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LogicalTimestamp that = (LogicalTimestamp) o;
    return this.cyclicTime == that.cyclicTime;
  }

  @Override
  public int hashCode() {
    return (int) (cyclicTime ^ (cyclicTime >>> 32));
  }

  @Override
  public String toString() {
    return Long.toString(cyclicTime);
  }
}