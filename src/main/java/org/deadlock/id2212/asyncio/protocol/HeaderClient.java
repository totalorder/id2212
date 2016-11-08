package org.deadlock.id2212.asyncio.protocol;

import java.util.concurrent.CompletionStage;

public interface HeaderClient<Header> {
  CompletionStage<Void> send(final Header header, byte[] bytes);

  CompletionStage<HeadedMessage<Header>> receive();
}
