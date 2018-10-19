package io.scalecube.cluster.membership;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class IdGeneratorTest {

  private static final int ATTEMPTS = (int) 2e+6;

  @Test
  public void generatorUniquenessTest() {
    Map<String, Integer> previds = new HashMap<>(ATTEMPTS);

    for (int attemptNumber = 0; attemptNumber < ATTEMPTS; attemptNumber++) {
      String id = generateId();
      if (previds.containsKey(id)) {
        fail(
            "Found key duplication on attempt "
                + attemptNumber
                + " same id = "
                + id
                + " as at attempt "
                + previds.get(id));
      } else {
        previds.put(id, attemptNumber);
      }
    }
  }

  private String generateId() {
    return IdGenerator.generateId(10);
  }
}
