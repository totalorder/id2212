package org.deadlock.id2212.overlay;

import org.deadlock.id2212.asyncio.AsyncQueue;
import org.deadlock.id2212.asyncio.protocol.IdJsonClient;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.overlay.messages.KnownPeers;
import org.deadlock.id2212.overlay.messages.PeerId;
import org.deadlock.id2212.overlay.messages.PeerInfo;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Receive messages and put overlay-specific messages in a specific queue, while
 * exposing other messages to the client.
 */
public class PeerExchangePeer implements Peer {
  private final UUID uuid;
  private final int listeningPort;
  private final IdJsonClient jsonClient;
  private Consumer<IdJsonMessage> onMessageReceivedCallback;
  private final AsyncQueue<IdJsonMessage> messages = new AsyncQueue<>();
  private final AsyncQueue<IdJsonMessage> internalMessages = new AsyncQueue<>();
  private Consumer<IdJsonMessage> onOverlayMessageReceivedCallback;
  private List<IdJsonMessage> callbackQueue = new ArrayList<>();
  private List<IdJsonMessage> overlayCallbackQueue = new ArrayList<>();

  public PeerExchangePeer(final UUID uuid, int listeningPort, final IdJsonClient jsonClient) {
    this.uuid = uuid;
    this.listeningPort = listeningPort;
    this.jsonClient = jsonClient;
    jsonClient.setOnMessageReceivedCallback(this::onMessageReceived);

  }

  @Override
  public CompletionStage<Void> send(Object object) {
    return jsonClient.send(object);
  }

  @Override
  public UUID getUUID() {
    return uuid;
  }


  @Override
  public void setOnMessageReceivedCallback(final Consumer<IdJsonMessage> callback) {
    onMessageReceivedCallback = callback;
    ArrayList<IdJsonMessage> callbackQueueCopy;
    synchronized (callbackQueue) {
      callbackQueueCopy = new ArrayList<>(callbackQueue);
      callbackQueue.clear();
    }
    callbackQueueCopy.stream().forEach(callback::accept);
  }

  public void setOnOverlayMessageReceivedCallback(final Consumer<IdJsonMessage> callback) {
    onOverlayMessageReceivedCallback = callback;
    ArrayList<IdJsonMessage> callbackQueueCopy;
    synchronized (callbackQueue) {
      callbackQueueCopy = new ArrayList<>(callbackQueue);
      callbackQueue.clear();
    }
    callbackQueueCopy.stream().forEach(callback::accept);
  }

  @Override
  public InetSocketAddress getAddress() {
    return jsonClient.getAddress();
  }

  private synchronized Void onMessageReceived(final IdJsonMessage message) {
    // Put internal messages on internalMessages queue, others on messages queue
    if (message.isClass(KnownPeers.class, PeerId.class, PeerInfo.class)) {
      if (onOverlayMessageReceivedCallback != null) {
        onOverlayMessageReceivedCallback.accept(message);
      } else {
        synchronized (overlayCallbackQueue) {
          overlayCallbackQueue.add(message);
        }
      }

      internalMessages.add(message);
    } else {
      // Notify client that message is received
      if (onMessageReceivedCallback != null) {
        onMessageReceivedCallback.accept(message);
      } else {
        synchronized (callbackQueue) {
          callbackQueue.add(message);
        }
      }
      messages.add(message);
    }

    return null;
  }

  public CompletionStage<IdJsonMessage> receive() {
    return messages.remove();
  }

  public CompletionStage<IdJsonMessage> receiveInternal() {
    return internalMessages.remove();
  }

  public PeerInfo getPeerInfo() {
    final InetSocketAddress listeningAddress = new InetSocketAddress(
        jsonClient.getAddress().getAddress(), listeningPort);
    return new PeerInfo(uuid, listeningAddress);
  }
}
