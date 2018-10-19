package io.scalecube.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import org.junit.jupiter.api.Test;

public class NetworkEmulatorTest extends BaseTest {

  @Test
  public void testResolveLinkSettingsBySocketAddress() throws UnknownHostException {
    // Init network emulator
    Address address = Address.from("localhost:1234");
    NetworkEmulator networkEmulator = new NetworkEmulator(address, true);
    networkEmulator.setLinkSettings(Address.create("localhost", 5678), 25, 10);
    networkEmulator.setLinkSettings(Address.create("192.168.0.1", 8765), 10, 20);
    networkEmulator.setDefaultLinkSettings(0, 2);

    // Check resolve by hostname:port
    InetSocketAddress addr1 = new InetSocketAddress("localhost", 5678);
    NetworkLinkSettings link1 = networkEmulator.getLinkSettings(addr1);
    assertEquals(25, link1.lossPercent());
    assertEquals(10, link1.meanDelay());

    // Check resolve by ipaddr:port
    byte[] byteAddr = new byte[] {(byte) 192, (byte) 168, 0, 1};
    InetSocketAddress addr2 =
        new InetSocketAddress(InetAddress.getByAddress("localhost", byteAddr), 8765);
    NetworkLinkSettings link2 = networkEmulator.getLinkSettings(addr2);
    assertEquals(10, link2.lossPercent());
    assertEquals(20, link2.meanDelay());

    // Check default link settings
    InetSocketAddress addr3 = new InetSocketAddress("localhost", 8765);
    NetworkLinkSettings link3 = networkEmulator.getLinkSettings(addr3);
    assertEquals(0, link3.lossPercent());
    assertEquals(2, link3.meanDelay());
  }
}
