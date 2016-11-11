package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.protocol.IdJsonClient;
import org.deadlock.id2212.asyncio.protocol.JsonProtocol;
import org.deadlock.id2212.messages.KnownPeers;
import org.deadlock.id2212.messages.PeerId;
import org.deadlock.id2212.messages.PeerInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class PeerExchangeOverlay implements Overlay {
  private final JsonProtocol jsonProtocol;
  private final List<Peer> peers = new LinkedList<>();
  private final UUID uuid;
  private final List<CompletionStage<Void>> peersReceivingFutures = new LinkedList<>();
  private final Map<Integer, CompletableFuture<Void>> waitingForConnections = new HashMap<>();
  private volatile CompletableFuture<Void> started = null;
  private CompletableFuture<Void> acceptingForeverFuture;
  private ScheduledExecutorService scheduledExecutorService;

  public PeerExchangeOverlay(final JsonProtocol jsonProtocol) {
    this.jsonProtocol = jsonProtocol;
    jsonProtocol.registerType(PeerId.class);
    jsonProtocol.registerType(KnownPeers.class);
    uuid = UUID.randomUUID();
  }

  @Override
  public CompletionStage<Peer> connect(InetSocketAddress inetSocketAddress) {
    return jsonProtocol.connect(inetSocketAddress)
      .thenCompose(this::exchangePeerId)
      .thenApply(this::startReceivingMessages);
  }

  public CompletionStage<Peer> accept() {
    return jsonProtocol.accept()
        .thenCompose(this::exchangePeerId)
        .thenApply(this::startReceivingMessages);
  }

  private CompletionStage<Peer> exchangePeerId(final IdJsonClient jsonClient) {
    // Send local peer id to remote peer
    return jsonClient.send(getPeerId())
        .thenCompose(ignored ->
            // Receive remote peer id
            jsonClient.receive())
        .thenApply(jsonMessage ->
            // Create peer with received peer id
            createPeer(jsonMessage.getObject(PeerId.class), jsonClient));
  }

  public CompletionStage<Void> announceKnownPeers() {
    List<CompletableFuture<Void>> peerAnnouncedToFutures;
    synchronized (peers) {
      peerAnnouncedToFutures = peers.stream()
          .map(this::announceKnownPeers)
          .map(CompletionStage::toCompletableFuture)
          .collect(Collectors.toList());
    }
    return CompletableFuture.allOf(
        peerAnnouncedToFutures.toArray(new CompletableFuture[peerAnnouncedToFutures.size()]));
  }

  private CompletionStage<Void> announceKnownPeers(final Peer peer) {
    return peer.send(getKnownPeers());
  }

  private Peer startReceivingMessages(final Peer peer) {
    final CompletionStage<Void> receivingForeverFuture = receiveMessagesForever(peer);
    synchronized (peersReceivingFutures) {
      peersReceivingFutures.add(receivingForeverFuture);
    }
    return peer;
  }

  private CompletionStage<Void> receiveMessagesForever(final Peer peer) {
    return peer.receive().thenCompose(jsonMessage -> {
      if (jsonMessage.isClass(KnownPeers.class)) {
        return acceptPeerAnnouncement(jsonMessage.getObject(KnownPeers.class))
            .thenCompose(ignored -> receiveMessagesForever(peer));
      } else {
        new RuntimeException("Unsupported message type!").printStackTrace();
        return receiveMessagesForever(peer);
      }
    });
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
    scheduledExecutorService.schedule(() -> {
      try {
        announceKnownPeers().toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      } finally {
        announceKnownPeersForever();
      }
    }, 1, TimeUnit.SECONDS);
  }

  @Override
  public CompletionStage<Void> waitForConnectedPeers(final int numberOfPeers) {
    synchronized (waitingForConnections) {
      if (waitingForConnections.get(numberOfPeers) == null) {
        final CompletableFuture<Void> peersConnected = new CompletableFuture<>();
        waitingForConnections.put(numberOfPeers, peersConnected);
        synchronized (peers) {
          if (peers.size() == numberOfPeers) {
            peersConnected.complete(null);
          }
        }
      }
      return waitingForConnections.get(numberOfPeers);
    }
  }

  private CompletionStage<Void> acceptPeerAnnouncement(final KnownPeers knownPeers) {
          final Set<PeerInfo> newKnownPeers = knownPeers.peerInfos
              // Filter out self
              .stream().filter(peerInfo -> !peerInfo.uuid.equals(uuid)).collect(Collectors.toSet());
          final Set<PeerInfo> existingKnownPeers = getKnownPeers().peerInfos;

          // Find new peers not known before
          newKnownPeers.removeAll(existingKnownPeers);

          final Set<PeerInfo> peersToConnectTo = newKnownPeers.stream()
              .filter(peerInfo -> peerInfo.uuid.compareTo(uuid) > 0)
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

  private Peer createPeer(final PeerId peerId, final IdJsonClient jsonClient) {
    final Peer peer = new Peer(peerId.uuid, peerId.listeningPort, jsonClient);
    synchronized (peers) {
      peers.add(peer);
      final CompletableFuture<Void> peersConnected = waitingForConnections.get(peers.size());
      if (peersConnected != null) {
        peersConnected.complete(null);
      }
    }
    return peer;
  }

  public KnownPeers getKnownPeers() {
    synchronized (peers) {
      return new KnownPeers( peers.stream().map(Peer::getPeerInfo).collect(Collectors.toSet()));
    }
  }

  public UUID getUUID() {
    return uuid;
  }

  private PeerId getPeerId() {
    return new PeerId(uuid, getListeningPort());
  }

  public int getListeningPort() {
    return jsonProtocol.getListeningPort();
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
