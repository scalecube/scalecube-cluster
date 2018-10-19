package io.scalecube.cluster.membership;

import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static io.scalecube.cluster.membership.MemberStatus.DEAD;
import static io.scalecube.cluster.membership.MemberStatus.SUSPECT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.BaseTest;
import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;
import org.junit.jupiter.api.Test;

public class MembershipRecordTest extends BaseTest {

  private final Member member = new Member("0", Address.from("localhost:1234"));
  private final Member anotherMember = new Member("1", Address.from("localhost:4567"));

  private final MembershipRecord r0Null = null;

  private final MembershipRecord r0Alive0 = new MembershipRecord(member, ALIVE, 0);
  private final MembershipRecord r0Alive1 = new MembershipRecord(member, ALIVE, 1);
  private final MembershipRecord r0Alive2 = new MembershipRecord(member, ALIVE, 2);

  private final MembershipRecord r0Suspect0 = new MembershipRecord(member, SUSPECT, 0);
  private final MembershipRecord r0Suspect1 = new MembershipRecord(member, SUSPECT, 1);
  private final MembershipRecord r0Suspect2 = new MembershipRecord(member, SUSPECT, 2);

  private final MembershipRecord r0Dead0 = new MembershipRecord(member, DEAD, 0);
  private final MembershipRecord r0Dead1 = new MembershipRecord(member, DEAD, 1);
  private final MembershipRecord r0Dead2 = new MembershipRecord(member, DEAD, 2);

  @Test
  public void testCantCompareDifferentMembers() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          MembershipRecord r0 = new MembershipRecord(member, ALIVE, 0);
          MembershipRecord r1 = new MembershipRecord(anotherMember, ALIVE, 0);

          r1.isOverrides(r0); // throws exception
        });
  }

  @Test
  public void testDeadOverride() {
    MembershipRecord r1Dead1 = new MembershipRecord(member, DEAD, 1);

    assertFalse(r1Dead1.isOverrides(r0Null));

    assertTrue(r1Dead1.isOverrides(r0Alive0));
    assertTrue(r1Dead1.isOverrides(r0Alive1));
    assertTrue(r1Dead1.isOverrides(r0Alive2));

    assertTrue(r1Dead1.isOverrides(r0Suspect0));
    assertTrue(r1Dead1.isOverrides(r0Suspect1));
    assertTrue(r1Dead1.isOverrides(r0Suspect2));

    assertFalse(r1Dead1.isOverrides(r0Dead0));
    assertFalse(r1Dead1.isOverrides(r0Dead1));
    assertFalse(r1Dead1.isOverrides(r0Dead2));
  }

  @Test
  public void testAliveOverride() {
    MembershipRecord r1Alive1 = new MembershipRecord(member, ALIVE, 1);

    assertTrue(r1Alive1.isOverrides(r0Null));

    assertTrue(r1Alive1.isOverrides(r0Alive0));
    assertFalse(r1Alive1.isOverrides(r0Alive1));
    assertFalse(r1Alive1.isOverrides(r0Alive2));

    assertTrue(r1Alive1.isOverrides(r0Suspect0));
    assertFalse(r1Alive1.isOverrides(r0Suspect1));
    assertFalse(r1Alive1.isOverrides(r0Suspect2));

    assertFalse(r1Alive1.isOverrides(r0Dead0));
    assertFalse(r1Alive1.isOverrides(r0Dead1));
    assertFalse(r1Alive1.isOverrides(r0Dead2));
  }

  @Test
  public void testSuspectOverride() {
    MembershipRecord r1Suspect1 = new MembershipRecord(member, SUSPECT, 1);

    assertFalse(r1Suspect1.isOverrides(r0Null));

    assertTrue(r1Suspect1.isOverrides(r0Alive0));
    assertTrue(r1Suspect1.isOverrides(r0Alive1));
    assertFalse(r1Suspect1.isOverrides(r0Alive2));

    assertTrue(r1Suspect1.isOverrides(r0Suspect0));
    assertFalse(r1Suspect1.isOverrides(r0Suspect1));
    assertFalse(r1Suspect1.isOverrides(r0Suspect2));

    assertFalse(r1Suspect1.isOverrides(r0Dead0));
    assertFalse(r1Suspect1.isOverrides(r0Dead1));
    assertFalse(r1Suspect1.isOverrides(r0Dead2));
  }

  @Test
  public void testEqualRecordNotOverriding() {
    assertFalse(r0Alive1.isOverrides(r0Alive1));
    assertFalse(r0Suspect1.isOverrides(r0Suspect1));
    assertFalse(r0Dead1.isOverrides(r0Dead1));
  }
}
