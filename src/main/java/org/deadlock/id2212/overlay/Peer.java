package org.deadlock.id2212.overlay;

import org.deadlock.id2212.asyncio.protocol.Client;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public interface Peer extends Client<IdJsonMessage> {
  CompletionStage<UUID> send(Object object);
  CompletionStage<UUID> send(Object object, UUID uuid);
  CompletionStage<IdJsonMessage> receive(UUID uuid);

  int getUUID();
}
