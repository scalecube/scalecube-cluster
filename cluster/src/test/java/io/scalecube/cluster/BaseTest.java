package io.scalecube.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base test class. */
public class BaseTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

  /** Setup. */
  @BeforeEach
  public final void baseSetUp(TestInfo testInfo) {
    LOGGER.info("***** Test started  : " + testInfo.getDisplayName() + " *****");
  }

  /** Tear down. */
  @AfterEach
  public final void baseTearDown(TestInfo testInfo) {
    LOGGER.info("***** Test finished : " + testInfo.getDisplayName() + " *****");
  }
}
