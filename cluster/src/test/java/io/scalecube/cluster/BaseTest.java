package io.scalecube.cluster;

import static io.scalecube.cluster.ClusterConfig.DEFAULT_SUSPICION_MULT;

import io.scalecube.cluster.membership.MembershipProtocolTest;
import io.scalecube.cluster.transport.api.SenderAwareTransport;
import io.scalecube.cluster.transport.api.Transport;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.cluster.utils.NetworkEmulatorTransport;
import io.scalecube.transport.netty.TransportImpl;
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

  protected void awaitSeconds(long seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      throw Exceptions.propagate(e);
    }
  }

  protected void awaitSuspicion(int clusterSize) {
    //noinspection UnnecessaryLocalVariable
    int defaultSuspicionMult = DEFAULT_SUSPICION_MULT;
    int pingInterval = MembershipProtocolTest.PING_INTERVAL;
    long suspicionTimeoutSec =
        ClusterMath.suspicionTimeout(defaultSuspicionMult, clusterSize, pingInterval) / 1000;
    awaitSeconds(suspicionTimeoutSec + 2);
  }

  protected NetworkEmulatorTransport createTransport() {
    return createTransport(TransportConfig.defaultConfig());
  }

  protected NetworkEmulatorTransport createTransport(TransportConfig transportConfig) {
    return new NetworkEmulatorTransport(
        new SenderAwareTransport(TransportImpl.bindAwait(transportConfig)));
  }

  protected void destroyTransport(Transport transport) {
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
