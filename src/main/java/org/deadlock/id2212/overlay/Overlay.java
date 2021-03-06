package org.deadlock.id2212.overlay;

import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface Overlay extends Closeable {
  CompletionStage<Peer> connect(InetSocketAddress inetSocketAddress);

  CompletionStage<Void> waitForConnectedPeers(int numberOfPeers);

  CompletionStage<Void> start(int port);

  int getListeningPort();

  List<CompletionStage<Peer>> broadcast(Object message);

  int registerType(final Class clazz);

  void setOnMessageReceivedCallback(final BiConsumer<Peer, IdJsonMessage> callback);

  void setOnPeerAcceptedCallback(final Consumer<Peer> callback);

  void setOnPeerConnectedCallback(final Consumer<Peer> callback);

  UUID getUUID();
}
