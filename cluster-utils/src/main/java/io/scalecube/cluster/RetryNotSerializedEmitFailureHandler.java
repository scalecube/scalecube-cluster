package io.scalecube.cluster;

import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.EmitResult;

public class RetryNotSerializedEmitFailureHandler implements EmitFailureHandler {

  public static final RetryNotSerializedEmitFailureHandler RETRY_NOT_SERIALIZED =
      new RetryNotSerializedEmitFailureHandler();

  @Override
  public boolean onEmitFailure(SignalType signalType, EmitResult emitResult) {
    return emitResult == EmitResult.FAIL_NON_SERIALIZED;
  }
}
