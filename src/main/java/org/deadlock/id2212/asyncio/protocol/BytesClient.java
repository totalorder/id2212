package org.deadlock.id2212.asyncio.protocol;

import java.util.concurrent.CompletionStage;

public interface BytesClient {
  CompletionStage<Void> send(final byte[]... bytes);

  CompletionStage<byte[]> receive();
}
