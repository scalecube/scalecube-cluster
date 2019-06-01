package io.scalecube.cluster.transport.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class AddressTest {

  @Test
  public void testParseHostPort() {
    Address address1 = Address.from("localhost:5810");
    assertEquals(5810, address1.port());
    assertEquals(Address.getLocalIpAddress().getHostAddress(), address1.host());

    Address address2 = Address.from("127.0.0.1:5810");
    assertEquals(5810, address1.port());
    assertEquals(Address.getLocalIpAddress().getHostAddress(), address2.host());

    assertEquals(address1, address2);
  }

  @Test
  public void testParseUnknownHostPort() {
    Address address = Address.from("host:1111");
    assertEquals(1111, address.port());
    assertEquals("host", address.host());
  }
}
