package io.scalecube.cluster2.membership;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.membership.MembershipProtocol.MemberSelector;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class MemberSelectorTest {

  private final Member fooMember = new Member(UUID.randomUUID(), "foo:1");
  private final Member barMember = new Member(UUID.randomUUID(), "bar:2");
  private final Member bazMember = new Member(UUID.randomUUID(), "baz:3");
  private final Member aliceMember = new Member(UUID.randomUUID(), "alice:4");
  private final Member bobMember = new Member(UUID.randomUUID(), "bob:5");
  private final Member johnMember = new Member(UUID.randomUUID(), "john:6");

  private final List<String> seedMembers = new ArrayList<>();
  private final ArrayList<Member> remoteMembers = new ArrayList<>();
  private final ArrayList<Member> nonSeedMembers = new ArrayList<>();
  private final MemberSelector memberSelector =
      new MemberSelector(seedMembers, remoteMembers, nonSeedMembers);

  @Test
  void testSelectNothingWhenEmpty() {
    assertNull(memberSelector.nextSeedMember());
    assertNull(memberSelector.nextRemoteMember());
  }

  @Test
  void testSelectOnlySeedMember() {
    seedMembers.add(aliceMember.address());
    seedMembers.add(bobMember.address());
    seedMembers.add(johnMember.address());

    assertThat(
        memberSelector.nextSeedMember(),
        isOneOf(aliceMember.address(), bobMember.address(), johnMember.address()));

    assertNull(memberSelector.nextRemoteMember());
  }

  @Test
  void testSelectOnlyRemoteMember() {
    remoteMembers.add(fooMember);
    remoteMembers.add(barMember);
    remoteMembers.add(bazMember);

    assertNull(memberSelector.nextSeedMember());
    assertThat(memberSelector.nextRemoteMember(), isOneOf(fooMember, barMember, bazMember));
  }

  @Test
  void testNoRemoteMemberWhenTheyAreSeedMembers() {
    seedMembers.add(aliceMember.address());
    seedMembers.add(bobMember.address());
    seedMembers.add(johnMember.address());

    remoteMembers.add(aliceMember);
    remoteMembers.add(bobMember);
    remoteMembers.add(johnMember);

    assertThat(
        memberSelector.nextSeedMember(),
        isOneOf(aliceMember.address(), bobMember.address(), johnMember.address()));

    assertNull(memberSelector.nextRemoteMember());
  }

  @Test
  void testSelectSeedMemberAndRemoteMember() {
    seedMembers.add(aliceMember.address());
    seedMembers.add(bobMember.address());
    seedMembers.add(johnMember.address());

    remoteMembers.add(fooMember);
    remoteMembers.add(barMember);
    remoteMembers.add(bazMember);

    assertThat(
        memberSelector.nextSeedMember(),
        isOneOf(aliceMember.address(), bobMember.address(), johnMember.address()));

    assertThat(memberSelector.nextRemoteMember(), isOneOf(fooMember, barMember, bazMember));
  }
}
