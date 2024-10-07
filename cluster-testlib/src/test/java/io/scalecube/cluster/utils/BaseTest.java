package io.scalecube.cluster.utils;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/** Base test class. */
public class BaseTest {

  protected static final Logger LOGGER = System.getLogger(BaseTest.class.getName());

  @BeforeEach
  public final void baseSetUp(TestInfo testInfo) {
    LOGGER.log(Level.INFO, "***** Test started  : " + testInfo.getDisplayName() + " *****");
  }

  @AfterEach
  public final void baseTearDown(TestInfo testInfo) {
    LOGGER.log(Level.INFO, "***** Test finished : " + testInfo.getDisplayName() + " *****");
  }
}
