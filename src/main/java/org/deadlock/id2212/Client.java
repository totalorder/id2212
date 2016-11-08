package org.deadlock.id2212;

import java.util.concurrent.CompletionStage;

public interface Client {
  CompletionStage<Void> send(final byte[] bytes);

  CompletionStage<byte[]> receieve();
}
