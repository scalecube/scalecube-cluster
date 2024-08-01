package io.scalecube.cluster2.payload;

import static io.scalecube.cluster2.UUIDCodec.encodeUUID;

import io.scalecube.cluster2.sbe.PayloadGenerationHeaderEncoder;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.UUID;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2LongHashMap;

public class PayloadStore {

  private final PayloadGenerationHeaderEncoder headerEncoder = new PayloadGenerationHeaderEncoder();
  private final Object2LongHashMap<UUID> payloadIndex = new Object2LongHashMap<>(Long.MIN_VALUE);
  private final MutableDirectBuffer headerBuffer = new ExpandableArrayBuffer();
  private FileChannel fileChannel;
  private RandomAccessFile file;

  public PayloadStore() {}

  public void addGeneration(UUID memberId, int payloadLength) throws IOException {
    payloadIndex.put(memberId, fileChannel.position());

    headerEncoder.wrap(headerBuffer, 0);
    encodeUUID(memberId, headerEncoder.memberId());
    headerEncoder.payloadLength(payloadLength).payloadOffset(0);

    file.write(headerBuffer.byteArray());

    for (int i = 0; i < payloadLength; i++) {
      file.write(0);
    }
  }

  public void removeGeneration(UUID memberId) {}
}
