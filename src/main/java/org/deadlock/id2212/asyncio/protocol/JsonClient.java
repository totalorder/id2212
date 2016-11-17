package org.deadlock.id2212.asyncio.protocol;

import java.util.concurrent.CompletionStage;

public interface JsonClient<MessageType> extends Client<MessageType> {
  CompletionStage<Void> send(final Object serializable);
}
