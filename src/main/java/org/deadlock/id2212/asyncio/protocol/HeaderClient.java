package org.deadlock.id2212.asyncio.protocol;

import java.util.concurrent.CompletionStage;

public interface HeaderClient<Header> extends Client<HeadedMessage<Header>> {
  CompletionStage<Void> send(final Header header, byte[] bytes);
}
