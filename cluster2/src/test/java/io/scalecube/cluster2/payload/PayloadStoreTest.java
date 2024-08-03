package io.scalecube.cluster2.payload;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PayloadStoreTest {

  @TempDir private Path tempDir;

  @Test
  void testAddGeneration() throws IOException {
    final File storeFile = tempDir.resolve("" + System.currentTimeMillis()).toFile();
    final PayloadStore payloadStore = new PayloadStore(storeFile);
    final int n = 100;
    final int payloadLength = 128;

    for (int i = 0; i < n; i++) {
      payloadStore.addGeneration(UUID.randomUUID(), payloadLength);
    }

    assertEquals(storeFile.length(), n * payloadLength, "storeFile.length");
  }

  @Test
  void testRemoveGeneration() throws IOException {
    final File storeFile = tempDir.resolve("" + System.currentTimeMillis()).toFile();
    final PayloadStore payloadStore = new PayloadStore(storeFile);
    final int n = 100;
    final int payloadLength = 128;

    UUID memberId = null;
    for (int i = 0; i < n; i++) {
      memberId = UUID.randomUUID();
      payloadStore.addGeneration(memberId, payloadLength);
    }

    payloadStore.removeGeneration(memberId);

    assertEquals(n - 1, payloadStore.size());
  }
}
