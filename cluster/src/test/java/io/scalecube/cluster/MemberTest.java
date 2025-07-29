package io.scalecube.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.junit.jupiter.api.Test;

public class MemberTest {

  @Test
  public void testMemberString() {
    UUID id = UUID.fromString("879162b5-0300-401a-9df3-18ca1f7df990");
    Member member = new Member(id.toString(), "alias", "address", "namespace");
    assertEquals("namespace:alias:879162b50300401a@address", member.toString());
  }
}
