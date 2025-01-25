package io.scalecube.cluster.utils;

import io.scalecube.cluster.utils.NetworkEmulator.OutboundSettings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NetworkEmulatorTest {

  @Test
  public void testResolveLinkSettingsBySocketAddress() {
    // Init network emulator
    NetworkEmulator networkEmulator = new NetworkEmulator("localhost:1234");
    networkEmulator.outboundSettings("localhost:" + 5678, 25, 10);
    networkEmulator.outboundSettings("192.168.0.1:" + 8765, 10, 20);
    networkEmulator.setDefaultOutboundSettings(0, 2);

    // Check resolve by hostname:port
    OutboundSettings link1 = networkEmulator.outboundSettings("localhost:" + 5678);
    Assertions.assertEquals(25, link1.lossPercent());
    Assertions.assertEquals(10, link1.meanDelay());

    // Check resolve by ipaddr:port
    OutboundSettings link2 = networkEmulator.outboundSettings("192.168.0.1:" + 8765);
    Assertions.assertEquals(10, link2.lossPercent());
    Assertions.assertEquals(20, link2.meanDelay());

    // Check default link settings
    OutboundSettings link3 = networkEmulator.outboundSettings("localhost:" + 8765);
    Assertions.assertEquals(0, link3.lossPercent());
    Assertions.assertEquals(2, link3.meanDelay());
  }
}
