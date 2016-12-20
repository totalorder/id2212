package org.deadlock.id2212.overlay;

import org.deadlock.id2212.asyncio.AsyncQueue;
import org.deadlock.id2212.asyncio.protocol.IdJsonClient;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.overlay.messages.KnownPeers;
import org.deadlock.id2212.overlay.messages.PeerId;
import org.deadlock.id2212.overlay.messages.PeerInfo;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Receive messages and put overlay-specific messages in a specific queue, while
 * exposing other messages to the client.
 */
public class PeerExchangePeer implements Peer {
  private final int uuid;
  private final InetSocketAddress listeningAddress;
  private final IdJsonClient jsonClient;
  private Consumer<IdJsonMessage> onMessageReceivedCallback;
  private final AsyncQueue<IdJsonMessage> messages = new AsyncQueue<>();
  private final AsyncQueue<IdJsonMessage> internalMessages = new AsyncQueue<>();
  private Consumer<IdJsonMessage> onOverlayMessageReceivedCallback;
  private List<IdJsonMessage> callbackQueue = new ArrayList<>();
  private List<IdJsonMessage> overlayCallbackQueue = new ArrayList<>();
  private Runnable onBrokenPipeCallback;

  public PeerExchangePeer(final int uuid, InetSocketAddress listeningAddress, final IdJsonClient jsonClient) {
    this.uuid = uuid;
    this.listeningAddress = listeningAddress;
    this.jsonClient = jsonClient;
    jsonClient.setOnMessageReceivedCallback(this::onMessageReceived);
    jsonClient.setOnBrokenPipeCallback(this::onBrokenPipe);
  }

  @Override
  public CompletionStage<UUID> send(Object object) {
    return jsonClient.send(object);
  }

  @Override
  public CompletionStage<UUID> send(final Object object, final UUID uuid) {
    return jsonClient.send(object, uuid);
  }

  @Override
  public CompletionStage<IdJsonMessage> receive(final UUID uuid) {
    return jsonClient.receive(uuid);
  }

  @Override
  public int getUUID() {
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

  @Override
  public void setOnBrokenPipeCallback(final Runnable callback) {
    onBrokenPipeCallback = callback;
  }

  @Override
  public void onBrokenPipe() {
    if (onBrokenPipeCallback != null) {
      onBrokenPipeCallback.run();
    }
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

  @Override
  public InetSocketAddress getListeningAddress() {
    return listeningAddress;
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
    return new PeerInfo(uuid, listeningAddress);
  }

  @Override
  public void close() throws IOException {
    jsonClient.close();
  }
}
