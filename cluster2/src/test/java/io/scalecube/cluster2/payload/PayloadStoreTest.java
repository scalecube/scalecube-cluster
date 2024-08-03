package io.scalecube.cluster2.payload;

import static io.scalecube.cluster2.payload.PayloadStore.HEADER_LENGTH;
import static org.junit.jupiter.api.Assertions.*;

import io.scalecube.cluster2.UUIDCodec;
import io.scalecube.cluster2.sbe.PayloadGenerationHeaderDecoder;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.UUID;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PayloadStoreTest {

  @TempDir private Path tempDir;

  private final PayloadGenerationHeaderDecoder headerDecoder = new PayloadGenerationHeaderDecoder();
  private final MutableDirectBuffer headerBuffer = new ExpandableDirectByteBuffer(HEADER_LENGTH);

  @Test
  void testAddGeneration() throws IOException {
    final File storeFile = tempDir.resolve("" + System.currentTimeMillis()).toFile();
    final PayloadStore payloadStore = new PayloadStore(storeFile);
    final int n = 100;
    final int payloadLength = 128;

    for (int i = 0; i < n; i++) {
      payloadStore.addGeneration(UUID.randomUUID(), payloadLength);
    }

    assertEquals(storeFile.length(), n * (HEADER_LENGTH + payloadLength), "storeFile.length");
  }

  @Test
  void removeGeneration() throws IOException {
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

    long lastRecordPosition = storeFile.length() - (HEADER_LENGTH + payloadLength);
    final RandomAccessFile randomAccessFile = new RandomAccessFile(storeFile, "r");
    final FileChannel fileChannel = randomAccessFile.getChannel();

    //noinspection RedundantCast
    final ByteBuffer buffer = (ByteBuffer) headerBuffer.byteBuffer().clear();
    do {
      fileChannel.read(buffer, lastRecordPosition);
    } while (buffer.hasRemaining());

    headerDecoder.wrap(headerBuffer, 0, HEADER_LENGTH, 0);

    assertEquals(UUIDCodec.uuid(headerDecoder.memberId()), memberId, "memberId");
    assertEquals(payloadLength, headerDecoder.payloadLength(), "payloadLength");
    assertEquals(Integer.MIN_VALUE, headerDecoder.payloadOffset(), "payloadOffset");
  }
}
