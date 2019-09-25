package io.scalecube.cluster.gossip;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SequenceIdCollectorTest {

  private SequenceIdCollector sequenceIdCollector;

  @BeforeEach
  public void before() {
    sequenceIdCollector = new SequenceIdCollector();
  }

  @Test
  public void testEmpty() {
    assertFalse(sequenceIdCollector.contains(0));
  }

  @Test
  public void testOneElement() {
    assertTrue(sequenceIdCollector.add(10));
    assertEquals(1, sequenceIdCollector.size());
    assertTrue(sequenceIdCollector.contains(10));
  }

  @Test
  public void testIsHeldNotExistedElements() {
    assertTrue(sequenceIdCollector.add(10));
    assertFalse(sequenceIdCollector.contains(9));
    assertFalse(sequenceIdCollector.contains(11));
  }

  @Test
  public void testAddExistedElement() {
    assertTrue(sequenceIdCollector.add(10));
    assertFalse(sequenceIdCollector.add(10));
  }

  @Test
  public void testClear() {
    assertTrue(sequenceIdCollector.add(10));

    sequenceIdCollector.clear();

    assertFalse(sequenceIdCollector.contains(10));
  }

  @Test
  public void testLowestAndHighestElementInRange() {
    for (int i = 0; i < 10; i++) {
      assertTrue(sequenceIdCollector.add(i));
    }

    assertTrue(sequenceIdCollector.contains(0));
    assertTrue(sequenceIdCollector.contains(9));
    assertFalse(sequenceIdCollector.add(0));
    assertFalse(sequenceIdCollector.add(9));
  }

  @Test
  public void testJoinLowerRange() {
    assertTrue(sequenceIdCollector.add(10));
    assertTrue(sequenceIdCollector.add(11));
    assertEquals(1, sequenceIdCollector.size());

    for (int i = 20; i < 30; i++) {
      assertTrue(sequenceIdCollector.add(i));
    }
    assertEquals(2, sequenceIdCollector.size());

    assertTrue(sequenceIdCollector.add(19));
    assertEquals(2, sequenceIdCollector.size());
  }

  @Test
  public void testJoinUpperRange() {
    assertTrue(sequenceIdCollector.add(10));
    assertTrue(sequenceIdCollector.add(9));
    assertEquals(1, sequenceIdCollector.size());

    for (int i = 20; i < 30; i++) {
      assertTrue(sequenceIdCollector.add(i));
    }
    assertEquals(2, sequenceIdCollector.size());

    assertTrue(sequenceIdCollector.add(30));
    assertEquals(2, sequenceIdCollector.size());
  }

  @Test
  public void testJoinTwoRange() {
    assertTrue(sequenceIdCollector.add(10));
    assertTrue(sequenceIdCollector.add(12));
    assertEquals(2, sequenceIdCollector.size());

    assertTrue(sequenceIdCollector.add(11));
    assertEquals(1, sequenceIdCollector.size());

    for (int i = 20; i < 25; i++) {
      assertTrue(sequenceIdCollector.add(i));
    }
    for (int i = 26; i < 30; i++) {
      assertTrue(sequenceIdCollector.add(i));
    }
    assertEquals(3, sequenceIdCollector.size());

    sequenceIdCollector.add(25);
    assertEquals(2, sequenceIdCollector.size());
  }
}
