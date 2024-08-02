package io.scalecube.cluster2.payload;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PayloadStoreTest {

  @TempDir private Path tempDir;

  @Test
  void testAddGeneration() throws IOException {
    final PayloadStore payloadStore =
        new PayloadStore(tempDir.resolve("" + System.currentTimeMillis()).toFile());
    for (int i = 0; i < 10; i++) {
      payloadStore.addGeneration(UUID.randomUUID(), 128);
    }
  }
}
