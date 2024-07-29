package io.scalecube.cluster2;

import io.scalecube.cluster2.sbe.UUIDDecoder;
import io.scalecube.cluster2.sbe.UUIDEncoder;
import java.util.UUID;

public class UUIDCodec {

  private UUIDCodec() {}

  public static void encodeUUID(UUID value, UUIDEncoder encoder) {
    if (value == null) {
      encoder
          .mostSignificantBits(UUIDEncoder.mostSignificantBitsNullValue())
          .leastSignificantBits(UUIDEncoder.leastSignificantBitsNullValue());
    } else {
      encoder
          .mostSignificantBits(value.getMostSignificantBits())
          .leastSignificantBits(value.getLeastSignificantBits());
    }
  }

  public static UUID uuid(UUIDDecoder decoder) {
    if (decoder == null) {
      // that was created without such a field, has no decoder.
      return null;
    }

    final long mostSigBits = decoder.mostSignificantBits();
    final long leastSigBits = decoder.leastSignificantBits();

    if (mostSigBits == UUIDDecoder.mostSignificantBitsNullValue()
        && leastSigBits == UUIDDecoder.leastSignificantBitsNullValue()) {
      return null;
    }

    return new UUID(mostSigBits, leastSigBits);
  }
}
