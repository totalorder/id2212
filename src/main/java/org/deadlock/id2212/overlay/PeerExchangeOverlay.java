package org.deadlock.id2212.overlay;

import org.deadlock.id2212.asyncio.protocol.IdJsonClient;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.asyncio.protocol.JsonProtocol;
import org.deadlock.id2212.overlay.messages.KnownPeers;
import org.deadlock.id2212.overlay.messages.PeerId;
import org.deadlock.id2212.overlay.messages.PeerInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Peer-to-peer overlay that automatically connects all discoverable
 * peers in a mesh. Enables broadcasting. Supports notification when
 * N peers are discovered with waitForConnectedPeers().
 */
public class PeerExchangeOverlay implements Overlay {
  private final JsonProtocol jsonProtocol;
  private final List<PeerExchangePeer> peers = new LinkedList<>();
  private final int uuid;
  private final List<CompletionStage<Void>> peersReceivingFutures = new LinkedList<>();
  private final Map<Integer, CompletableFuture<Void>> waitingForConnections = new HashMap<>();
  private volatile CompletableFuture<Void> started = null;
  private CompletableFuture<Void> acceptingForeverFuture;
  private ScheduledExecutorService scheduledExecutorService;
  private BiConsumer<Peer, IdJsonMessage> onMessageReceivedCallback;
  private Consumer<Peer> onPeerAcceptedCallback;
  private Consumer<Peer> onPeerConnectedCallback;

  public PeerExchangeOverlay(final JsonProtocol jsonProtocol) {
    this.jsonProtocol = jsonProtocol;
    jsonProtocol.registerType(PeerId.class);
    jsonProtocol.registerType(KnownPeers.class);
    uuid = new Random().nextInt();
  }

  @Override
  public CompletionStage<Peer> connect(InetSocketAddress inetSocketAddress) {
    // Connect, exchange ids, start receiving
    return jsonProtocol.connect(inetSocketAddress)
      .thenCompose(this::exchangePeerId)
      .thenApply(peer -> {
        if (onPeerConnectedCallback != null) {
          onPeerConnectedCallback.accept(peer);
        }
        return peer;
      });
  }

  public CompletionStage<Peer> accept() {
    // Accept, exchange ids, start receiving
    return jsonProtocol.accept()
        .thenCompose(this::exchangePeerId)
        .thenApply(peer -> {
          if (onPeerAcceptedCallback != null) {
            onPeerAcceptedCallback.accept(peer);
          }
          return peer;
        });
  }

  /**
   * Exchange peer-id with remote peer
   */
  private CompletionStage<PeerExchangePeer> exchangePeerId(final IdJsonClient jsonClient) {
    // Send local peer id to remote peer
    return jsonClient.send(getPeerId()).thenCompose(ignored -> {
      final CompletableFuture<PeerExchangePeer> peerFuture = new CompletableFuture<>();
      jsonClient.setOnMessageReceivedCallback(jsonMessage ->
          peerFuture.complete(createPeer(jsonMessage.getObject(PeerId.class), jsonClient)));
      return peerFuture;
    });
  }

  /**
   * Announce the known peers to all other peers
   */
  public CompletionStage<Void> announceKnownPeers() {
    List<CompletableFuture<UUID>> peerAnnouncedToFutures;
    synchronized (peers) {
      // Announce known peers to all connected peers
      peerAnnouncedToFutures = peers.stream()
          .map(this::announceKnownPeers)
          .map(CompletionStage::toCompletableFuture)
          .collect(Collectors.toList());
    }
    return CompletableFuture.allOf(
        peerAnnouncedToFutures.toArray(new CompletableFuture[peerAnnouncedToFutures.size()]));
  }

  private CompletionStage<UUID> announceKnownPeers(final Peer peer) {
    return peer.send(getKnownPeers());
  }

  private void onOverlayMessageReceived(final PeerExchangePeer peer, final IdJsonMessage jsonMessage) {
    // Handle KnownPeers message, ignore others
    if (jsonMessage.isClass(KnownPeers.class)) {
      acceptPeerAnnouncement(jsonMessage.getObject(KnownPeers.class));
    } else {
      new RuntimeException("Unsupported message type!").printStackTrace();
    }
  }

  public CompletionStage<Void> start(final int port) {
    if (started != null) {
      return started;
    }

    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    started = new CompletableFuture<>();
    return listen(port).thenCompose(ignored -> {
      acceptingForeverFuture = acceptForever().toCompletableFuture();
      announceKnownPeersForever();
      return started;
    });
  }

  private CompletionStage<Void> acceptForever() {
    return accept().thenCompose(ignored -> acceptForever());
  }

  private void announceKnownPeersForever() {
    // Announce known peers to all connected peers every second
    scheduledExecutorService.schedule(() -> {
      if (started != null) {
        started.complete(null);
      }

      try {
        announceKnownPeers().toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      } finally {
        announceKnownPeersForever();
      }
    }, 1, TimeUnit.SECONDS);
  }

  /**
   * Return a future that completes when N peers are connected
   */
  @Override
  public CompletionStage<Void> waitForConnectedPeers(final int numberOfPeers) {
    synchronized (waitingForConnections) {
      // Get or create a future corresponding to numberOfPeers
      if (waitingForConnections.get(numberOfPeers) == null) {
        final CompletableFuture<Void> peersConnected = new CompletableFuture<>();
        waitingForConnections.put(numberOfPeers, peersConnected);
        synchronized (peers) {
          // Complete immediately if already correct number of peers
          if (peers.size() == numberOfPeers) {
            peersConnected.complete(null);
          }
        }
      }
      return waitingForConnections.get(numberOfPeers);
    }
  }

  /**
   * Find any new peers that are not connected. If local uuid > remote uuid, connect to
   * the remote peer.
   */
  private CompletionStage<Void> acceptPeerAnnouncement(final KnownPeers knownPeers) {
          final Set<PeerInfo> newKnownPeers = knownPeers.peerInfos
              // Filter out self
              .stream().filter(peerInfo -> !(peerInfo.uuid == uuid)).collect(Collectors.toSet());
          final Set<PeerInfo> existingKnownPeers = getKnownPeers().peerInfos;

          // Find new peers not known before
          newKnownPeers.removeAll(existingKnownPeers);

          // Filter out all peers with a lower uuid than self, to avoid connecting from
          // both sides
          final Set<PeerInfo> peersToConnectTo = newKnownPeers.stream()
              .filter(peerInfo -> peerInfo.uuid > uuid)
              .collect(Collectors.toSet());

          if (peersToConnectTo.size() == 0) {
            return completedFuture(null);
          }

          // Connect to all new peers
          final List<CompletionStage<Peer>> newPeerFutures = peersToConnectTo.stream()
              .map(peerInfo -> connect(peerInfo.address)).collect(Collectors.toList());

          // Complete when all new are connected
          return CompletableFuture.allOf(
              newPeerFutures.toArray(new CompletableFuture[newKnownPeers.size()]));
  }

  public CompletionStage<Void> listen(final int port) {
    return jsonProtocol.startServer(port);
  }

  private PeerExchangePeer createPeer(final PeerId peerId, final IdJsonClient jsonClient) {
    final PeerExchangePeer peer = new PeerExchangePeer(peerId.uuid, peerId.listeningPort, jsonClient);
    peer.setOnOverlayMessageReceivedCallback(message -> onOverlayMessageReceived(peer, message));
    peer.setOnMessageReceivedCallback(message -> onMessageReceivedCallback.accept(peer, message));
    synchronized (peers) {
      peers.add(peer);
      // Notify any listeners that N peers are connected
      final CompletableFuture<Void> peersConnected = waitingForConnections.get(peers.size());
      if (peersConnected != null) {
        peersConnected.complete(null);
      }
    }
    return peer;
  }

  public KnownPeers getKnownPeers() {
    synchronized (peers) {
      return new KnownPeers( peers.stream().map(peer -> peer.getPeerInfo()).collect(Collectors.toSet()));
    }
  }

  public int getUUID() {
    return uuid;
  }

  private PeerId getPeerId() {
    return new PeerId(uuid, getListeningPort());
  }

  public int getListeningPort() {
    return jsonProtocol.getListeningPort();
  }

  public InetSocketAddress getListeningAddress() {
    return jsonProtocol.getListeningAddress();
  }

  @Override
  public List<CompletionStage<Peer>> broadcast(final Object message) {
    synchronized (peers) {
      return peers.stream()
          .map(peer -> peer.send(message)
              .thenApply(ignored -> (Peer) peer))
          .collect(Collectors.toList());
    }
  }

  @Override
  public int registerType(Class clazz) {
    return jsonProtocol.registerType(clazz);
  }

  @Override
  public void setOnMessageReceivedCallback(BiConsumer<Peer, IdJsonMessage> callback) {
    this.onMessageReceivedCallback = callback;
  }

  @Override
  public void setOnPeerAcceptedCallback(Consumer<Peer> callback) {
    this.onPeerAcceptedCallback = callback;
  }

  @Override
  public void setOnPeerConnectedCallback(Consumer<Peer> callback) {
    this.onPeerConnectedCallback = callback;
  }

  @Override
  public void close() throws IOException {
    if (started == null) {
      return;
    }

    scheduledExecutorService.shutdownNow();

    jsonProtocol.close();
    synchronized (peersReceivingFutures) {
      peersReceivingFutures.stream()
          .map(future -> future.toCompletableFuture().cancel(true))
          .collect(Collectors.toList());
    }

    acceptingForeverFuture.cancel(true);
  }

  public static PeerExchangeOverlay createDefault() {
    return new PeerExchangeOverlay(JsonProtocol.createDefault());
  }
}
