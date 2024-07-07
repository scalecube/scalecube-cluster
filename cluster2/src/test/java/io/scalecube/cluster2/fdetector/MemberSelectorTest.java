package io.scalecube.cluster2.fdetector;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.fdetector.FailureDetector.MemberSelector;
import java.util.ArrayList;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class MemberSelectorTest {

  private final Member fooMember = new Member(UUID.randomUUID(), "foo:1");
  private final Member barMember = new Member(UUID.randomUUID(), "bar:2");
  private final Member bazMember = new Member(UUID.randomUUID(), "baz:3");
  private final Member aliceMember = new Member(UUID.randomUUID(), "alice:4");
  private final Member bobMember = new Member(UUID.randomUUID(), "bob:5");
  private final Member johnMember = new Member(UUID.randomUUID(), "john:6");
  private final Member eveMember = new Member(UUID.randomUUID(), "eve:7");

  private final ArrayList<Member> pingMembers = new ArrayList<>();
  private final ArrayList<Member> pingReqMembers = new ArrayList<>();

  @Test
  void testPingMemberWhenNoMembers() {
    final MemberSelector memberSelector = new MemberSelector(1, pingMembers, pingReqMembers);
    assertNull(memberSelector.nextPingMember());
  }

  @Test
  void testPingMemberWhenOneMember() {
    pingMembers.add(fooMember);

    final MemberSelector memberSelector = new MemberSelector(1, pingMembers, pingReqMembers);

    final Member pingMember = memberSelector.nextPingMember();
    assertEquals(fooMember, pingMember);

    memberSelector.nextPingReqMembers(pingMember);
    assertEquals(0, pingReqMembers.size());
  }

  @Test
  void testPingReqMembersWhenOnePingReqMember() {
    pingMembers.add(fooMember);
    pingMembers.add(barMember);

    final MemberSelector memberSelector = new MemberSelector(1, pingMembers, pingReqMembers);

    for (int i = 0; i < 10; i++) {
      final Member pingMember = memberSelector.nextPingMember();
      assertThat(pingMember, isOneOf(fooMember, barMember));

      memberSelector.nextPingReqMembers(pingMember);

      assertThat(pingReqMembers, hasItems(isOneOf(fooMember, barMember)));
      assertEquals(1, pingReqMembers.size());
    }
  }

  @Test
  void testPingReqMembersWhenTwoPingReqMembers() {
    pingMembers.add(fooMember);
    pingMembers.add(barMember);
    pingMembers.add(bazMember);

    final MemberSelector memberSelector = new MemberSelector(2, pingMembers, pingReqMembers);

    for (int i = 0; i < 10; i++) {
      final Member pingMember = memberSelector.nextPingMember();
      assertThat(pingMember, isOneOf(fooMember, barMember, bazMember));

      memberSelector.nextPingReqMembers(pingMember);

      assertThat(pingReqMembers, hasItems(isOneOf(fooMember, barMember, bazMember)));
      assertEquals(2, pingReqMembers.size());
    }
  }

  @Test
  void testPingReqMembersWhenDemandLessThanSize() {
    pingMembers.add(fooMember);
    pingMembers.add(barMember);
    pingMembers.add(bazMember);
    pingMembers.add(aliceMember);
    pingMembers.add(bobMember);
    pingMembers.add(johnMember);
    pingMembers.add(eveMember);

    final MemberSelector memberSelector = new MemberSelector(3, pingMembers, pingReqMembers);

    for (int i = 0; i < 10; i++) {
      final Member pingMember = memberSelector.nextPingMember();
      assertThat(
          pingMember,
          isOneOf(fooMember, barMember, bazMember, aliceMember, bobMember, johnMember, eveMember));

      memberSelector.nextPingReqMembers(pingMember);

      assertThat(
          pingReqMembers,
          hasItems(
              isOneOf(
                  fooMember, barMember, bazMember, aliceMember, bobMember, johnMember, eveMember)));
      assertEquals(3, pingReqMembers.size());
    }
  }

  @Test
  void testPingReqMembersWhenDemandMoreThanSize() {
    pingMembers.add(aliceMember);
    pingMembers.add(bobMember);
    pingMembers.add(johnMember);
    pingMembers.add(eveMember);

    final MemberSelector memberSelector = new MemberSelector(10, pingMembers, pingReqMembers);

    for (int i = 0; i < 10; i++) {
      final Member pingMember = memberSelector.nextPingMember();
      assertThat(pingMember, isOneOf(aliceMember, bobMember, johnMember, eveMember));

      memberSelector.nextPingReqMembers(pingMember);

      assertThat(pingReqMembers, hasItems(isOneOf(aliceMember, bobMember, johnMember, eveMember)));
      assertEquals(3, pingReqMembers.size());
    }
  }
}
