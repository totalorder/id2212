package org.deadlock.id2212.overlay;

import org.deadlock.id2212.asyncio.AsyncQueue;
import org.deadlock.id2212.asyncio.protocol.IdJsonClient;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.overlay.messages.KnownPeers;
import org.deadlock.id2212.overlay.messages.PeerId;
import org.deadlock.id2212.overlay.messages.PeerInfo;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

public class PeerExchangePeer implements Peer {
  private final UUID uuid;
  private final int listeningPort;
  private final IdJsonClient jsonClient;
  private final BiConsumer<Peer, IdJsonMessage> onMessageReceivedCallback;
  private final AsyncQueue<IdJsonMessage> messages = new AsyncQueue<>();
  private final AsyncQueue<IdJsonMessage> internalMessages = new AsyncQueue<>();
  private volatile CompletionStage<Void> receiving = null;

  public PeerExchangePeer(final UUID uuid, int listeningPort, final IdJsonClient jsonClient, BiConsumer<Peer, IdJsonMessage> onMessageReceivedCallback) {
    this.uuid = uuid;
    this.listeningPort = listeningPort;
    this.jsonClient = jsonClient;
    this.onMessageReceivedCallback = onMessageReceivedCallback;
  }

  @Override
  public CompletionStage<Void> send(Object object) {
    return jsonClient.send(object);
  }

  @Override
  public UUID getUUID() {
    return uuid;
  }

  private void ensureReceiving() {
    if (receiving == null) {
      jsonClient.receive().thenApply(message -> {
            ensureReceiving();
            onMessageReceived(message);
        return null;
          });
    }
  }

  private synchronized Void onMessageReceived(final IdJsonMessage message) {
    if (message.isClass(KnownPeers.class, PeerId.class, PeerInfo.class)) {
      internalMessages.add(message);
    } else {

      if (onMessageReceivedCallback != null) {
        onMessageReceivedCallback.accept(this, message);
      }
      messages.add(message);
    }

    return null;
  }

  @Override
  public CompletionStage<IdJsonMessage> receive() {
    ensureReceiving();
    return messages.remove();
  }

  public CompletionStage<IdJsonMessage> receiveInternal() {
    ensureReceiving();
    return internalMessages.remove();
  }

  public PeerInfo getPeerInfo() {
    final InetSocketAddress listeningAddress = new InetSocketAddress(
        jsonClient.getAddress().getAddress(), listeningPort);
    return new PeerInfo(uuid, listeningAddress);
  }
}
