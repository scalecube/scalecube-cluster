package io.scalecube.cluster2.payload;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;
import org.agrona.concurrent.UnsafeBuffer;
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

    assertEquals(n - 1, payloadStore.size(), "payloadStore.size");
    assertNull(payloadStore.readPayload(memberId), "readPayload");
  }

  @Test
  void testReadPayload() throws IOException {
    final File storeFile = tempDir.resolve("" + System.currentTimeMillis()).toFile();
    final PayloadStore payloadStore = new PayloadStore(storeFile);

    final UUID memberId = UUID.randomUUID();
    final int payloadLength = 1032;

    payloadStore.addGeneration(memberId, payloadLength);

    assertNull(payloadStore.readPayload(memberId), "readPayload");

    final Random random = new Random();
    byte[] bytes = new byte[payloadLength];
    random.nextBytes(bytes);
    final UnsafeBuffer buffer = new UnsafeBuffer(bytes);

    assertFalse(payloadStore.putPayload(memberId, 0, buffer, 0, 768), "putPayload");
    assertFalse(payloadStore.putPayload(memberId, 768, buffer, 768, 255), "putPayload");
    assertTrue(payloadStore.putPayload(memberId, 1023, buffer, 1023, 9), "putPayload");

    final ByteBuffer payload = payloadStore.readPayload(memberId);
    assertEquals(payloadLength, payload.limit(), "payloadLength");
    byte[] payloadBytes = new byte[payloadLength];
    payload.get(payloadBytes);

    assertArrayEquals(bytes, payloadBytes, "payloadBytes");
  }

  @Test
  void testPutPayloadPayloadNotFound() throws IOException {
    final File storeFile = tempDir.resolve("" + System.currentTimeMillis()).toFile();
    final PayloadStore payloadStore = new PayloadStore(storeFile);

    final UUID memberId = UUID.randomUUID();
    final int payloadLength = 1032;

    final Random random = new Random();
    byte[] bytes = new byte[payloadLength];
    random.nextBytes(bytes);
    final UnsafeBuffer buffer = new UnsafeBuffer(bytes);

    payloadStore.addGeneration(memberId, payloadLength);

    assertThrows(
        IllegalArgumentException.class,
        () -> payloadStore.putPayload(UUID.randomUUID(), 0, buffer, 0, 768));
  }

  @Test
  void testPutPayloadInvalidPayloadOffset() throws IOException {
    final File storeFile = tempDir.resolve("" + System.currentTimeMillis()).toFile();
    final PayloadStore payloadStore = new PayloadStore(storeFile);

    final UUID memberId = UUID.randomUUID();
    final int payloadLength = 1032;

    final Random random = new Random();
    byte[] bytes = new byte[payloadLength];
    random.nextBytes(bytes);
    final UnsafeBuffer buffer = new UnsafeBuffer(bytes);

    payloadStore.addGeneration(memberId, payloadLength);

    assertThrows(
        IllegalArgumentException.class,
        () -> payloadStore.putPayload(memberId, 768, buffer, 0, 768));
  }

  @Test
  void testPutPayloadInvalidChunk() throws IOException {
    final File storeFile = tempDir.resolve("" + System.currentTimeMillis()).toFile();
    final PayloadStore payloadStore = new PayloadStore(storeFile);

    final UUID memberId = UUID.randomUUID();
    final int payloadLength = 1032;

    payloadStore.addGeneration(memberId, payloadLength);

    assertNull(payloadStore.readPayload(memberId), "readPayload");

    final Random random = new Random();
    byte[] bytes = new byte[payloadLength];
    random.nextBytes(bytes);
    final UnsafeBuffer buffer = new UnsafeBuffer(bytes);

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          assertFalse(payloadStore.putPayload(memberId, 0, buffer, 0, 768), "putPayload");
          assertFalse(payloadStore.putPayload(memberId, 768, buffer, 768, 255), "putPayload");
          payloadStore.putPayload(memberId, 1023, buffer, 1023, 10);
        });
  }
}
