package org.deadlock.id2212.asyncio.protocol;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public interface JsonClient<MessageType> extends Client<MessageType> {
  CompletionStage<UUID> send(final Object serializable);
}
