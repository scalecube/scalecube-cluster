package io.scalecube.cluster2;

import io.scalecube.cluster2.sbe.MessageHeaderDecoder;
import io.scalecube.cluster2.sbe.MessageHeaderEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public abstract class AbstractCodec {

  protected final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  protected final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

  protected final UnsafeBuffer unsafeBuffer = new UnsafeBuffer();
  protected final ExpandableArrayBuffer encodedBuffer = new ExpandableArrayBuffer();
  protected int encodedLength;

  public final int encodedLength() {
    return encodedLength;
  }
}
