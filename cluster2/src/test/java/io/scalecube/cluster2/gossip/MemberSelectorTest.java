package io.scalecube.cluster2.gossip;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.cluster2.Member;
import io.scalecube.cluster2.gossip.GossipProtocol.MemberSelector;
import java.util.ArrayList;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class MemberSelectorTest {

  private final Member fooMember = new Member(UUID.randomUUID(), "foo:1");
  private final Member barMember = new Member(UUID.randomUUID(), "bar:2");
  private final Member bazMember = new Member(UUID.randomUUID(), "baz:3");
  private final Member aliceMember = new Member(UUID.randomUUID(), "alice:4");
  private final Member bobMember = new Member(UUID.randomUUID(), "bob:5");
  private final Member johnMember = new Member(UUID.randomUUID(), "john:6");
  private final Member eveMember = new Member(UUID.randomUUID(), "eve:7");
  private final Member abcMember = new Member(UUID.randomUUID(), "abc:8");
  private final Member xyzMember = new Member(UUID.randomUUID(), "abc:9");

  private final ArrayList<Member> remoteMembers = new ArrayList<>();
  private final ArrayList<Member> gossipMembers = new ArrayList<>();

  @Test
  void testSelectNothing() {
    final MemberSelector memberSelector = new MemberSelector(3, remoteMembers, gossipMembers);
    memberSelector.nextGossipMembers();
    assertEquals(0, gossipMembers.size());
  }

  @Test
  void testSelectWhenLessThanFanout() {
    remoteMembers.add(fooMember);
    remoteMembers.add(barMember);
    remoteMembers.add(bazMember);

    final MemberSelector memberSelector = new MemberSelector(5, remoteMembers, gossipMembers);

    for (int i = 0; i < 10; i++) {
      memberSelector.nextGossipMembers();
      assertEquals(remoteMembers.size(), gossipMembers.size());
      assertThat(gossipMembers, hasItems(isOneOf(fooMember, barMember, bazMember)));
    }
  }

  @Test
  void testSelectWhenEqualToFanout() {
    remoteMembers.add(fooMember);
    remoteMembers.add(barMember);
    remoteMembers.add(bazMember);

    final MemberSelector memberSelector = new MemberSelector(3, remoteMembers, gossipMembers);

    for (int i = 0; i < 10; i++) {
      memberSelector.nextGossipMembers();
      assertEquals(remoteMembers.size(), gossipMembers.size());
      assertThat(gossipMembers, hasItems(isOneOf(fooMember, barMember, bazMember)));
    }
  }

  @Test
  void testSelectWhenGreaterThanFanout() {
    remoteMembers.add(fooMember);
    remoteMembers.add(barMember);
    remoteMembers.add(bazMember);
    remoteMembers.add(aliceMember);
    remoteMembers.add(bobMember);
    remoteMembers.add(johnMember);
    remoteMembers.add(eveMember);
    remoteMembers.add(abcMember);
    remoteMembers.add(xyzMember);

    final int gossipFanout = 3;
    final MemberSelector memberSelector =
        new MemberSelector(gossipFanout, remoteMembers, gossipMembers);

    for (int i = 0; i < 10; i++) {
      memberSelector.nextGossipMembers();
      assertEquals(gossipFanout, gossipMembers.size());
      assertThat(
          gossipMembers,
          hasItems(
              isOneOf(
                  fooMember,
                  barMember,
                  bazMember,
                  aliceMember,
                  bobMember,
                  johnMember,
                  eveMember,
                  abcMember,
                  xyzMember)));
    }
  }

  @Test
  void testSelectWhenGreaterThanFanoutWithReminder() {
    remoteMembers.add(fooMember);
    remoteMembers.add(barMember);
    remoteMembers.add(bazMember);
    remoteMembers.add(aliceMember);
    remoteMembers.add(bobMember);
    remoteMembers.add(johnMember);
    remoteMembers.add(eveMember);
    remoteMembers.add(abcMember);
    remoteMembers.add(xyzMember);

    final int gossipFanout = 4;
    final MemberSelector memberSelector =
        new MemberSelector(gossipFanout, remoteMembers, gossipMembers);

    for (int i = 0; i < 10; i++) {
      memberSelector.nextGossipMembers();
      assertEquals(gossipFanout, gossipMembers.size());
      assertThat(
          gossipMembers,
          hasItems(
              isOneOf(
                  fooMember,
                  barMember,
                  bazMember,
                  aliceMember,
                  bobMember,
                  johnMember,
                  eveMember,
                  abcMember,
                  xyzMember)));
    }
  }
}
