package io.scalecube.cluster.utils;

import io.scalecube.cluster.utils.NetworkEmulator.OutboundSettings;
import io.scalecube.net.Address;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NetworkEmulatorTest extends BaseTest {

  @Test
  public void testResolveLinkSettingsBySocketAddress() {
    // Init network emulator
    Address address = Address.from("localhost:1234");
    NetworkEmulator networkEmulator = new NetworkEmulator(address);
    networkEmulator.outboundSettings(Address.create("localhost", 5678), 25, 10);
    networkEmulator.outboundSettings(Address.create("192.168.0.1", 8765), 10, 20);
    networkEmulator.setDefaultOutboundSettings(0, 2);

    // Check resolve by hostname:port
    OutboundSettings link1 = networkEmulator.outboundSettings(Address.create("localhost", 5678));
    Assertions.assertEquals(25, link1.lossPercent());
    Assertions.assertEquals(10, link1.meanDelay());

    // Check resolve by ipaddr:port
    OutboundSettings link2 = networkEmulator.outboundSettings(Address.create("192.168.0.1", 8765));
    Assertions.assertEquals(10, link2.lossPercent());
    Assertions.assertEquals(20, link2.meanDelay());

    // Check default link settings
    OutboundSettings link3 = networkEmulator.outboundSettings(Address.create("localhost", 8765));
    Assertions.assertEquals(0, link3.lossPercent());
    Assertions.assertEquals(2, link3.meanDelay());
  }
}
