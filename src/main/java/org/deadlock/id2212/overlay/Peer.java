package org.deadlock.id2212.overlay;

import org.deadlock.id2212.asyncio.protocol.Client;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;

import java.util.UUID;
import java.util.concurrent.CompletionStage;

public interface Peer extends Client<IdJsonMessage> {
  CompletionStage<Void> send(Object object);

  UUID getUUID();
}
