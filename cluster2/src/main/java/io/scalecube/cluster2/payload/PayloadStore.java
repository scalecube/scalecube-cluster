package io.scalecube.cluster2.payload;

import static io.scalecube.cluster2.UUIDCodec.encodeUUID;

import io.scalecube.cluster2.sbe.PayloadGenerationHeaderEncoder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.UUID;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2LongHashMap;

public class PayloadStore {

  private final PayloadGenerationHeaderEncoder headerEncoder = new PayloadGenerationHeaderEncoder();
  private final MutableDirectBuffer headerBuffer = new ExpandableArrayBuffer();
  private final Object2LongHashMap<UUID> payloadIndex = new Object2LongHashMap<>(Long.MIN_VALUE);
  private RandomAccessFile payloadStore;

  public PayloadStore(File payloadFile) {
    try {
      payloadStore = new RandomAccessFile(payloadFile, "rw");
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public void addGeneration(UUID memberId, int payloadLength) throws IOException {
    payloadIndex.put(memberId, payloadStore.getFilePointer());

    headerEncoder.wrap(headerBuffer, 0);
    encodeUUID(memberId, headerEncoder.memberId());
    headerEncoder.payloadLength(payloadLength).payloadOffset(0);

    payloadStore.write(headerBuffer.byteArray());

    for (int i = 0; i < payloadLength; i++) {
      payloadStore.write(0);
    }
  }

  public void removeGeneration(UUID memberId) throws IOException {
    final long position = payloadIndex.getValue(memberId);
    if (position != Long.MIN_VALUE) {
      payloadStore.read(headerBuffer.byteArray());
      headerEncoder.wrap(headerBuffer, 0);
      headerEncoder.payloadOffset(Integer.MIN_VALUE);

      payloadStore.write(headerBuffer.byteArray());

      payloadIndex.removeKey(memberId);
    }
  }
}
