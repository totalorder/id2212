package org.deadlock.id2212.overlay;

import org.deadlock.id2212.asyncio.protocol.Client;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public interface Peer extends Client<IdJsonMessage>, Closeable {
  CompletionStage<UUID> send(Object object);
  CompletionStage<UUID> send(Object object, UUID uuid);
  CompletionStage<IdJsonMessage> receive(UUID uuid);
  InetSocketAddress getListeningAddress();

  int getUUID();
}
