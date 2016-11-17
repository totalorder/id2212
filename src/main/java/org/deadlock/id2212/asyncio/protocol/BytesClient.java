package org.deadlock.id2212.asyncio.protocol;

import java.util.concurrent.CompletionStage;

public interface BytesClient extends Client<byte[]> {
  CompletionStage<Void> send(final byte[]... bytes);
}
