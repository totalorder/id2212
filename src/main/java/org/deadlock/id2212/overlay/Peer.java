package org.deadlock.id2212.overlay;

import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;

import java.util.concurrent.CompletionStage;

public interface Peer {
  CompletionStage<Void> send(Object object);

  CompletionStage<IdJsonMessage> receive();
}
