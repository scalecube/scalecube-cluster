package io.scalecube.cluster;

import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.membership.MembershipProtocolTest;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;

/** Base test class. */
public class BaseTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

  @BeforeEach
  public final void baseSetUp(TestInfo testInfo) {
    LOGGER.info("***** Test started  : " + testInfo.getDisplayName() + " *****");
  }

  @AfterEach
  public final void baseTearDown(TestInfo testInfo) {
    LOGGER.info("***** Test finished : " + testInfo.getDisplayName() + " *****");
  }

  public static <T> T getField(Object obj, String fieldName) {
    try {
      final Field field = obj.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      //noinspection unchecked
      return (T) field.get(obj);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void awaitSeconds(long seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      throw Exceptions.propagate(e);
    }
  }

  public static void awaitSuspicion(int clusterSize) {
    int defaultSuspicionMult = MembershipConfig.DEFAULT_SUSPICION_MULT;
    int pingInterval = MembershipProtocolTest.PING_INTERVAL;
    long suspicionTimeoutSec =
        ClusterMath.suspicionTimeout(defaultSuspicionMult, clusterSize, pingInterval) / 1000;
    awaitSeconds(suspicionTimeoutSec + 2);
  }

  public static NetworkEmulatorTransport createTransport() {
    return createTransport(TransportConfig.defaultConfig());
  }

  public static NetworkEmulatorTransport createTransport(TransportConfig transportConfig) {
    return new NetworkEmulatorTransport(
        Transport.bindAwait(transportConfig.transportFactory(new TcpTransportFactory())));
  }

  public static void destroyTransport(Transport transport) {
    if (transport == null || transport.isStopped()) {
      return;
    }
    try {
      transport.stop().block(Duration.ofSeconds(1));
    } catch (Exception ignore) {
      // no-op
    }
  }
}
