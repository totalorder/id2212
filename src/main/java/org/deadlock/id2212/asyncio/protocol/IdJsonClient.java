package org.deadlock.id2212.asyncio.protocol;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public interface IdJsonClient extends JsonClient<IdJsonMessage> {
  CompletionStage<UUID> send(Object serializable, UUID uuid);
  CompletionStage<IdJsonMessage> receive(UUID uuid);
}
