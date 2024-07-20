package io.scalecube.cluster2.payload;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.payload.PayloadProtocol.MemberSelector;
import java.util.ArrayList;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MemberSelectorTest {

  private final Member aliceMember = new Member(UUID.randomUUID(), "alice:4");
  private final Member bobMember = new Member(UUID.randomUUID(), "bob:5");
  private final Member johnMember = new Member(UUID.randomUUID(), "john:6");
  private final Member eveMember = new Member(UUID.randomUUID(), "eve:7");

  private final ArrayList<Member> pingMembers = new ArrayList<>();
  private final MemberSelector memberSelector = new MemberSelector(pingMembers);

  @Test
  void testPingMemberWhenNoMembers() {
    assertNull(memberSelector.nextPingMember());
  }

  @Test
  void testPingMemberWhenOneMember() {
    pingMembers.add(aliceMember);

    for (int i = 0; i < 10; i++) {
      assertEquals(aliceMember, memberSelector.nextPingMember());
    }
  }

  @Test
  void testPingMember() {
    pingMembers.add(aliceMember);
    pingMembers.add(bobMember);
    pingMembers.add(johnMember);
    pingMembers.add(eveMember);

    for (int i = 0; i < 10; i++) {
      final Member member = memberSelector.nextPingMember();
      assertThat(member, isOneOf(aliceMember, bobMember, johnMember, eveMember));
    }
  }
}
