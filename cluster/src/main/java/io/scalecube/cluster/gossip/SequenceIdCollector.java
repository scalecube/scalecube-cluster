package io.scalecube.cluster.gossip;

import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * A collector that efficiently store sequence of incremented numbers which go in a row without
 * missed elements.
 */
public class SequenceIdCollector {

  // store closed intervals [a, b] without intersections
  // if there is an intersection between tow intervals then they will be merged into one
  private final TreeMap<Long, Long> processedInterval = new TreeMap<>();

  // [a, b]
  private static boolean isInClosedRange(Entry<Long, Long> range, long element) {
    return range != null && range.getKey() <= element && element <= range.getValue();
  }

  private static boolean isNextToClosedRange(Entry<Long, Long> range, long element) {
    return range != null && (element + 1 == range.getKey() || element - 1 == range.getValue());
  }

  /**
   * Returns <tt>true</tt> if this set contains the specified element.
   *
   * @param sequenceId element whose presence in this set is to be tested
   * @return <tt>true</tt> if this set contains the specified element
   */
  public boolean contains(long sequenceId) {
    // floor: returns the entry for the greatest key less than the specified key
    return isInClosedRange(processedInterval.floorEntry(sequenceId), sequenceId);
  }

  /**
   * Adds the specified element to this holder if it is not already present.
   *
   * @param sequenceId element to be added
   * @return <tt>true</tt> if this holder did not already contain the specified element
   */
  public boolean add(long sequenceId) {
    // floor: returns the entry for the greatest key less than the specified key
    final Entry<Long, Long> floorEntry = processedInterval.floorEntry(sequenceId);

    if (isInClosedRange(floorEntry, sequenceId)) {
      return false;
    }

    // ceiling: returns the entry for the least key greater than the specified key
    final Entry<Long, Long> ceilingEntry = processedInterval.ceilingEntry(sequenceId);

    final boolean nextToFloor = isNextToClosedRange(floorEntry, sequenceId);
    final boolean nextToCeiling = isNextToClosedRange(ceilingEntry, sequenceId);

    if (nextToFloor && nextToCeiling) {
      processedInterval.remove(floorEntry.getKey());
      processedInterval.remove(ceilingEntry.getKey());
      processedInterval.put(floorEntry.getKey(), ceilingEntry.getValue());
    } else if (nextToFloor) {
      processedInterval.remove(floorEntry.getKey());
      processedInterval.put(floorEntry.getKey(), sequenceId);
    } else if (nextToCeiling) {
      processedInterval.remove(ceilingEntry.getKey());
      processedInterval.put(sequenceId, ceilingEntry.getValue());
    } else {
      processedInterval.put(sequenceId, sequenceId);
    }

    return true;
  }

  /**
   * Returns the number of intervals in this collector.
   *
   * @return the number of intervals in this collector
   */
  public int size() {
    return processedInterval.size();
  }

  /** Removes all of the elements from this collector. */
  public void clear() {
    processedInterval.clear();
  }

  @Override
  public String toString() {
    return processedInterval.entrySet().stream()
        .map(entry -> "[" + entry.getKey() + "," + entry.getValue() + "]")
        .collect(Collectors.joining(",", "{", "}"));
  }
}
