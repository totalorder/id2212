package org.deadlock.id2212;

import com.google.common.collect.Lists;
import org.apache.commons.codec.digest.DigestUtils;
import org.deadlock.id2212.asyncio.AsyncIO;
import org.deadlock.id2212.asyncio.protocol.IdJsonMessage;
import org.deadlock.id2212.messages.GetKeyRequest;
import org.deadlock.id2212.messages.GetKeyResponse;
import org.deadlock.id2212.messages.HeartbeatRequest;
import org.deadlock.id2212.messages.HeartbeatResponse;
import org.deadlock.id2212.messages.PredecessorNotification;
import org.deadlock.id2212.messages.PredecessorRequest;
import org.deadlock.id2212.messages.PredecessorResponse;
import org.deadlock.id2212.messages.ReplicationRequest;
import org.deadlock.id2212.messages.RingProbe;
import org.deadlock.id2212.messages.SetKeyRequest;
import org.deadlock.id2212.messages.SetKeyResponse;
import org.deadlock.id2212.messages.SuccessorRequest;
import org.deadlock.id2212.messages.SuccessorResponse;
import org.deadlock.id2212.overlay.Overlay;
import org.deadlock.id2212.overlay.Peer;
import org.deadlock.id2212.overlay.PeerExchangeOverlay;
import org.deadlock.id2212.util.Timeout;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.deadlock.id2212.util.CompletableExtras.async;

/**
 * Find a common time among N peers in schedule X
 *
 * Automatically discovers other peers in the overlay after connecting
 * to a bootstrap node.
 */
public class DHT implements Closeable {
  private final Overlay overlay;
  private final LinkedList<Peer> fingers = new LinkedList<>();
  private final int numFingers = 10;
  private int localUUID;
  private Peer predecessor;
  private Peer successor;
  private final Map<Integer, Peer> connectedPeers = new HashMap<>();
  private int nextFingerToFix;
  private static final int size = (int)Math.pow(2, 10);
  private static final int K = (int)Math.pow(size, 1f / 10);

  private ScheduledExecutorService scheduledExecutorService;
  private CompletableFuture<Void> isStable = new CompletableFuture<>();
  private CompletableFuture<Void> fingersFixed = new CompletableFuture<>();
  private boolean running = false;
  private CompletableFuture<Void> stopped;
  private CompletableFuture<Void> isAlone = new CompletableFuture<>();
  private CompletableFuture<RingProbe> lastProbeReceieved = new CompletableFuture<>();
  private boolean logMessages = false;
  private Timeout timeout = new Timeout();
  private Map<Integer, String> store = new HashMap<>();
  private Instant lastLog = Instant.now();

  public DHT(final Overlay overlay) {
    overlay.setOnMessageReceivedCallback(this::onMessageReceived);
    overlay.setOnPeerAcceptedCallback(this::onPeerAccepted);
    overlay.setOnPeerConnectedCallback(this::onPeerConnected);
    overlay.setOnPeerBrokenPipeCallback(this::onPeerBrokenPipe);

    overlay.registerType(GetKeyRequest.class);
    overlay.registerType(GetKeyResponse.class);
    overlay.registerType(HeartbeatRequest.class);
    overlay.registerType(HeartbeatResponse.class);
    overlay.registerType(PredecessorNotification.class);
    overlay.registerType(PredecessorRequest.class);
    overlay.registerType(PredecessorResponse.class);
    overlay.registerType(ReplicationRequest.class);
    overlay.registerType(RingProbe.class);
    overlay.registerType(SetKeyRequest.class);
    overlay.registerType(SetKeyResponse.class);
    overlay.registerType(SuccessorRequest.class);
    overlay.registerType(SuccessorResponse.class);
    this.overlay = overlay;
    localUUID = overlay.getUUID();
    log("N: %s", size);
    log("K: %s", K);
  }

  private int hash(final String key) {
    return new BigInteger(DigestUtils.sha1(key.getBytes())).mod(BigInteger.valueOf(size)).intValue();
  }

  /**
   * If a peer disconnects, remove it from successor, predecessor and fingers
   *
   * @param peer The peer that disconnected
   */
  private void onPeerBrokenPipe(final Peer peer) {
    log("peer disconnected: %s", peer.getUUID());
    if (successor == peer) {
      successor = null;
    }

    if (predecessor == peer) {
      predecessor = null;
    }

    while (fingers.remove(peer)) {
      fingers.addFirst(me);
    }
  }

  /**
   * Receive messages and execute the relevant callbacks
   * @param peer
   * @param message
   */
  private void onMessageReceived(final Peer peer, final IdJsonMessage message) {
    if (!running) {
      return;
    }

    // Receive all types of messages
    if (message.isClass(PredecessorRequest.class)) {
      onPredecessorRequest(peer, message);
    } else if (message.isClass(SuccessorRequest.class)) {
      final SuccessorRequest successorRequest = message.getObject(SuccessorRequest.class);
      onSuccessorRequest(peer, message, successorRequest);
    } else if (message.isClass(PredecessorNotification.class)) {
      final PredecessorNotification predecessorNotification = message.getObject(PredecessorNotification.class);
      onPredecessorNotification(peer, predecessorNotification);
    } else  if (message.isClass(HeartbeatRequest.class)) {
      onHeartbeatRequest(peer, message);
    } else if (message.isClass(RingProbe.class)) {
      onRingProbe(message.getObject(RingProbe.class));
    } else if (message.isClass(GetKeyRequest.class)) {
      onGetKeyRequest(peer, message, message.getObject(GetKeyRequest.class));
    } else if (message.isClass(SetKeyRequest.class)) {
      final SetKeyRequest setKeyRequest = message.getObject(SetKeyRequest.class);
      onSetKeyRequest(peer, message, setKeyRequest);
    } else if (message.isClass(ReplicationRequest.class)) {
      onReplicationRequest(message.getObject(ReplicationRequest.class));
    }
  }

  /**
   * When a HeartbeatRequest is received, reply with a HeartbeatResponse
   */
  private void onHeartbeatRequest(final Peer peer, final IdJsonMessage message) {
    peer.send(new HeartbeatResponse(), message.getUUID());
  }

  /**
   * When a PredecessorRequest is received, reply with the our predecessor or null
   */
  private void onPredecessorRequest(final Peer peer, final IdJsonMessage message) {
    if (predecessor != null) {
      peer.send(new PredecessorResponse(predecessor.getUUID(), predecessor.getListeningAddress()), message.getUUID());
    } else {
      peer.send(new PredecessorResponse(null, null), message.getUUID());
    }
  }

  /**
   * When a SetKeyRequest is received, set the key locally
   */
  private void onSetKeyRequest(final Peer peer, final IdJsonMessage message, final SetKeyRequest setKeyRequest) {
    setKey(setKeyRequest.key, setKeyRequest.value);
    peer.send(new SetKeyResponse(), message.getUUID());
  }

  /**
   * When a GetKeyRequest is received, reply with the value of the key
   */
  private void onGetKeyRequest(final Peer peer, final IdJsonMessage message, final GetKeyRequest getKeyRequest) {
    peer.send(new GetKeyResponse(getKey(getKeyRequest.key)), message.getUUID()).exceptionally(throwable -> {
      throwable.printStackTrace();
      return null;
    });
  }

  /**
   * When a ReplicationRequest is received, update the local store with the data
   */
  private void onReplicationRequest(final ReplicationRequest replicationRequest) {
    replicationRequest.store.entrySet().forEach(entry -> {
        store.put(entry.getKey(), entry.getValue());
    });
  }

  /**
   * When a RingProbe is received, complete lastProbeReceived if it came from us, otherwise
   * add our id and pass it on
   */
  private void onRingProbe(final RingProbe ringProbe) {
    if (ringProbe.nodes.get(0) == localUUID) {
      lastProbeReceieved.complete(ringProbe);
      lastProbeReceieved = new CompletableFuture<>();
    } else if (successor != null && successor != me){
      ringProbe.nodes.add(localUUID);
      successor.send(ringProbe);
    }
  }

  /**
   * When a PredecessorNotification is received, update the predecessor if it's in the range, or current is null
   */
  private void onPredecessorNotification(final Peer peer, final PredecessorNotification predecessorNotification) {
    if (predecessor == null || isBetween(predecessorNotification.uuid, predecessor.getUUID() + 1, localUUID)) {
      predecessor = peer;
    }
  }

  /**
   * When a SuccessorRequest is made, reply with the successor for that uuid, possibly asking another node
   * to find the answer
   */
  private void onSuccessorRequest(final Peer peer, final IdJsonMessage message, final SuccessorRequest successorRequest) {
    findSuccessor(successorRequest.uuid)
        .thenCompose(successorReceived ->
            peer.send(new SuccessorResponse(
                successorReceived.getUUID(), successorReceived.getListeningAddress()), message.getUUID()))
        .exceptionally(throwable -> {
          throwable.printStackTrace();
          return null;
        });
  }


  private String getKey(final int key) {
    log("get key %s", key);
    if (predecessor == null || isBetween(key, predecessor.getUUID() + 1, localUUID)) {
      return store.get(key);
    } else {
      return null;
    }
  }

  private void setKey(final int key, final String value) {
    log("set key %s %s", key, value);
    if (predecessor == null || isBetween(key, predecessor.getUUID() + 1, localUUID)) {
      store.put(key, value);
    } else {
      throw new RuntimeException("Invalid key!");
    }
  }

  /**
   * Find the successor of a node.
   * If we are the predecessor of the successor node, answer immediately, otherwise ask another node.
   */
  private CompletionStage<Peer> findSuccessor(int uuid) {
    // We are the only node in the ring, return "me"
    if (predecessor == null && (successor == null || successor == me)) {
      return completedFuture(me);
    }

    // Our successor is the successor node, return our successor
    if (successor != null && isBetween(uuid, localUUID + 1, successor.getUUID()) || successor == me) {
      return completedFuture(successor);
    } else {
      // Find the closest finger
      final Peer closestFinger = getClosestPreceedingFinger(uuid);

      // The closest predecessor is ourselves, no need to ask around
      if (closestFinger == me) {
        return completedFuture(me);
      }

      // Ask the closest finger for the the successor of uuid
      return timeout.catchTimeout(closestFinger.send(new SuccessorRequest(uuid))
          .thenComposeAsync(closestFinger::receive)
          .thenCompose(reply -> {
            // Return the connected successor
            final SuccessorResponse successorResponse = reply.getObject(SuccessorResponse.class);
            return getOrConnectPeer(successorResponse.successor, successorResponse.address);
          }), () -> {
        // No response, return self
        log("successor request timeout for %s to %s", uuid, closestFinger.getUUID());
            return me;
        });
    }
  }

  /**
   * Keep track of connected peers
   */
  private void onPeerAccepted(final Peer peer) {
    synchronized (connectedPeers) {
      connectedPeers.put(peer.getUUID(), peer);
    }
    log("Accepted connection from " + peer.getUUID());
  }

  /**
   * Keep track of connected peers
   */
  private void onPeerConnected(final Peer peer) {
    synchronized (connectedPeers) {
      connectedPeers.put(peer.getUUID(), peer);
    }
    log("Connected to " + peer.getUUID());
  }

  /**
   * Start the DHT overlay, listening on <port>
   */
  public CompletionStage<Void> start(final int port) {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    return overlay.start(port).thenApply(ignored -> {
      log("Started on " + overlay.getListeningAddress());
      return null;
    });
  }

  /**
   * Return the address on which we are listening for incoming connections
   */
  public InetSocketAddress getListeningAddress() {
    return overlay.getListeningAddress();
  }

  /**
   * Become the first node of the ring
   */
  public void initiate() {
    running = true;
    predecessor = null;
    successor = me;

    // Initialize all fingers to "me"
    while (fingers.size() < numFingers) {
      fingers.add(me);
    }

    // Start stabilizing
    stabilizeForever();
  }

  /**
   * Join an overlay. Set all fingers to "me", connect and
   * ask the connected peer about our successor
   */
  public CompletionStage<Void> join(final InetSocketAddress inetSocketAddress) {
    // Initialize all fingers to "me"
    running = true;
    while (fingers.size() < numFingers) {
      fingers.add(me);
    }
    predecessor = null;

    // Start stabilization
    stabilizeForever();

    return async(() ->
        // Connect to remote peer
        overlay.connect(inetSocketAddress)
            .thenApply(peer -> {
              synchronized (connectedPeers) {
                connectedPeers.put(peer.getUUID(), peer);
              }
              return peer;
            })
            // Find our successor
            .thenCompose(peer -> findSuccessor(peer, localUUID)
                .thenApply(successor -> {
                  log("Found successor " + successor.getUUID());
                  this.successor = successor;
                  return null;
                })));
  }

  /**
   * Ask a specific peer for a successor. Used for finding the initial successor
   */
  private CompletionStage<Peer> findSuccessor(final Peer peer, final int uuid) {
    log("initial findSuccessor at " + peer.getUUID() + " for " + uuid);
    return peer.send(new SuccessorRequest(uuid))
        .thenCompose(peer::receive)
        .thenCompose(reply -> {
          final SuccessorResponse successorResponse = reply.getObject(SuccessorResponse.class);
          return getOrConnectPeer(successorResponse.successor, successorResponse.address);
        });
  }

  /**
   * Send a heartbeat to our predecessor. If the predecessor does not answer, set predecessor to null
   */
  public CompletionStage<Void> heartbeat() {
    return handleErrors(async(() -> {
      // There is a predecessor, send it a heartbeat
      if (predecessor != null && predecessor != me) {
        final CompletableFuture<IdJsonMessage> replyFuture = predecessor.send(new HeartbeatRequest())
            .thenCompose(predecessor::receive)
            .toCompletableFuture();

        // Fiddle with type inference
        final CompletableFuture<Void> voidReplyFuture = replyFuture.<Void>thenApplyAsync(ignoredAgain -> null);

        // Wait for a timeout
        return timeout.timeout(voidReplyFuture, 500).exceptionally(throwable -> {
          if (throwable instanceof Timeout.TimeoutException) {
            log("heartbeat timeout!");
            // Timeout reached, reset predecessor to null
            predecessor = null;
            return null;
          } else {
            throw new RuntimeException(throwable);
          }
        });
      }
      return completedFuture(null);
    }), "heartbeat");
  }


  /**
   * Replicate our keys to our successor
   */
  public CompletionStage<Void> replicate() {
    return handleErrors(async(() -> {
      if (successor != null && successor != me) {
        return successor.send(new ReplicationRequest(store)).thenApply(ignored -> null);
      }
      return completedFuture(null);
    }), "replicate");
  }

  /**
   * Run stabilization.
   */
  public CompletionStage<Void> stabilize() {
    return handleErrors(async(() -> {
      // Notify that we are alone in the ring
      if (successor == me && predecessor == null && !isAlone.isDone()) {
        isAlone.complete(null);
      }

      // We just started, try to find a successor
      if (successor == null) {
        return findSuccessor(localUUID).thenApplyAsync(peer -> {
          successor = peer;
          return null;
        });
      }

      // We are alone in the ring. Nothing to do
      if (successor == me && (predecessor == me || predecessor == null)) {
        return completedFuture(null);
      }

      // We are our own successor, ask ourselves about our predecessor
      if (successor == me) {
        return updateSuccessorAndSendNotification(predecessor);
      }

      // Notify that we have a successor and predecessor that's not ourselves
      if (predecessor != null && predecessor != me && !isStable.isDone()) {
        isStable.complete(null);
      }

      // Send a predecessor request to our successor
      // If our successors predecessor are in between us, update the predecessor
      return successor.send(new PredecessorRequest())
          .thenCompose(successor::receive)
          .thenCompose(reply -> {
            final PredecessorResponse predecessorResponse = reply.getObject(PredecessorResponse.class);
            return getOrConnectPeer(predecessorResponse.predecessor, predecessorResponse.address)
                .thenCompose(this::updateSuccessorAndSendNotification);
          });
    }), "stabilize");
  }

  private <T> CompletionStage<T> handleErrors(final CompletionStage<T> stage, final String actionName) {
    return stage.exceptionally(throwable -> {
      if (throwable instanceof Timeout.TimeoutException || throwable.getCause() instanceof Timeout.TimeoutException) {
        log("%s timed out!", actionName);
        return null;
      } else if (throwable.getCause() instanceof AsyncIO.BrokenPipeException) {
        log("%s broke its pipe!", actionName);
        return null;
      } else {
        throw new RuntimeException(throwable);
      }
    });
  }

  /**
   * If our successors predecessor is between us and our successor, update successor and notify
   * the successor that we are its predecessor
   */
  private CompletionStage<Void> updateSuccessorAndSendNotification(final Peer successorsPredecessor) {
    // Update successor since the received predecessor is between
    if (successorsPredecessor != null && isBetween(successorsPredecessor.getUUID(), localUUID + 1, successor.getUUID())) {
      successor = successorsPredecessor;
    }

    // We are our own sucessor, no notification needed
    if (successor == me) {
      return completedFuture(null);
    }

    // Notify our successor that we are its predecessor
    return successor.send(new PredecessorNotification(localUUID, overlay.getListeningAddress()))
        .thenApplyAsync(ignored -> null);
  }

  /**
   * Fix fingers, one by one by finding the successor of uuid+K^index
   */
  public CompletionStage<Void> fixFingers() {
    return handleErrors(async(() -> {
      if (successor == null) {
        return completedFuture(null);
      }

      // Find the next finger to fix
      if (nextFingerToFix > fingers.size() - 1) {

        // Notify that all fingers are fixed
        if (!fingersFixed.isDone()) {
          fingersFixed.complete(null);
        }
        nextFingerToFix = 0;
      }

      final int nextPosition = (localUUID + (int) Math.pow(K, nextFingerToFix)) % size;

      // Update finger
      return findSuccessor(nextPosition).thenApply(successor -> {
        fingers.set(nextFingerToFix, successor);
        nextFingerToFix = nextFingerToFix + 1;
        return null;
      });
    }), "fix fingers");
  }

  /**
   * Get the closest finger to a given uuid by traversing the finger
   * table from the end to the beginning
   */
  private Peer getClosestPreceedingFinger(final int uuid) {
    if (successor == null) {
      return me;
    }

    // Get the last node that's between us and the uuid
    for (final Peer peer : Lists.reverse(fingers)) {
      // We are not interested in ourselves
      if (peer == me) {
        continue;
      }

      if (isBetween(peer.getUUID(), localUUID, uuid - 1)) {
        return peer;
      }
    }

    // None found, return "me"
    return me;
  }

  /**
   * Get <key> from DHT
   */
  public CompletionStage<String> get(final String key) {
    return async(() -> {
      final int hash = hash(key);
      return findSuccessor(hash).thenCompose(peer -> {
          if (peer == me) {
            return completedFuture(getKey(hash));
          } else {
            return peer.send(new GetKeyRequest(hash))
                .thenCompose(peer::receive)
                .thenApply(reply ->
                    reply.getObject(GetKeyResponse.class).value);
          }
      });
    });
  }

  /**
   * Set <key> to <value> in DHT
   */
  public CompletionStage<Void> set(final String key, final String value) {
    return async(() -> {
      final int hash = hash(key);
      return findSuccessor(hash).thenCompose(peer -> {
        if (peer == me) {
          setKey(hash, value);
          return completedFuture(null);
        } else {
          return peer.send(new SetKeyRequest(hash, value))
              .thenCompose(peer::receive)
              .thenApply(reply -> null);
        }
      });
    });
  }

  /**
   * Return whether uuid is between from and to, inclusive, on the ring
   */
  private boolean isBetween(final int uuid, final int from, final int to) {
    if (to == from) {
      return true;
    }
    if (to > from) {
      return uuid >= from && uuid <= to;
    } else {
      return uuid >= from || uuid <= to;
    }
  }

  /**
   * Run heartbeat(), stabilize(), replicate() and fixFingers() every second
   */
  private void stabilizeForever() {
    // Announce known peers to all connected peers every second
    scheduledExecutorService.schedule(() -> {
      try {
        heartbeat().toCompletableFuture().get();
        stabilize().toCompletableFuture().get(15000, TimeUnit.MILLISECONDS);
        replicate().toCompletableFuture().get();
        fixFingers().toCompletableFuture().get();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        // Log interesting data every 10 seconds
        if (lastLog.isBefore(Instant.now().minus(10, ChronoUnit.SECONDS))) {
          lastLog = Instant.now();
          printFingerTable();
        }

        // Stop if we are asked to
        if (running) {
          stabilizeForever();
        } else {
          if (!stopped.isDone()) {
            stopped.complete(null);
          }
        }
      }
    }, 1, TimeUnit.SECONDS);
  }

  /**
   * Stop the DHT, disconnecting all clients and not responding to incoming messages
   */
  public CompletableFuture<Void> stop() {
    log("stopping");
    return async(() -> {
      connectedPeers.entrySet().forEach(entry -> {
        try {
          entry.getValue().close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      connectedPeers.clear();
      stopped = new CompletableFuture<>();
      running = false;
      return stopped;
    }).toCompletableFuture();
  }

  /**
   * Send a probe around the ring to figure out the topology
   */
  public CompletionStage<RingProbe> probeRing() {
    return async(() -> {
      lastProbeReceieved = new CompletableFuture<>();
      probeRingUntilResponse();
      return timeout.timeout(lastProbeReceieved, 1000);
    });
  }

  /**
   * Send a probe around the ring to figure out the topology. Repeat until we get a response
   */
  private void probeRingUntilResponse() {
    if (successor != null && successor != me) {
      log("sending ring probe");
      final RingProbe ringProbe = new RingProbe();
      ringProbe.nodes.add(localUUID);

      final CompletionStage<RingProbe> replyFuture = successor.send(ringProbe)
          .thenCompose(ignored -> lastProbeReceieved);
      timeout.timeout(replyFuture, 1000, () -> {
      });
    }
  }

  /**
   * Return a Peer object for a given uuid and address. If the peer is not already connected, it
   * will be connected to. If it is, the peer is immediately returned
   */
  private CompletionStage<Peer> getOrConnectPeer(final Integer uuid, final InetSocketAddress address) {
    if (uuid == null) {
      return completedFuture(null);
    }

    if (uuid == localUUID) {
      return completedFuture(me);
    }

    Peer peer;
    synchronized (connectedPeers) {
      peer = connectedPeers.get(uuid);
    }

    if (peer != null) {
      return completedFuture(peer);
    } else {
      log("peer " + uuid + " not found. Connecting...");
      return overlay.connect(address);
    }
  }

  public CompletionStage<Void> waitForStable() {
    isStable = new CompletableFuture<>();
    return isStable;
  }

  public CompletionStage<Void> waitForAlone() {
    isAlone = new CompletableFuture<>();
    return async(() -> isAlone);
  }

  public CompletionStage<Void> waitForFingersFixed() {
    nextFingerToFix = 0;
    fingersFixed = new CompletableFuture<>();
    return fingersFixed;
  }

  public CompletionStage<Peer> getSuccessor(int uuid) {
    return async(() -> findSuccessor(uuid));
  }

  public CompletionStage<Peer> getSuccessor(final String key) {
    return async(() -> findSuccessor(hash(key)));
  }

  private void log(final String log, final Object... args) {
    System.out.println(localUUID + " " + String.format(log, args));
  }

  public void printFingerTable() {
    log("predecessor: " + (predecessor != null ? predecessor.getUUID() : "null"));
    log("successor: " + (successor != null ? successor.getUUID() : "null"));
    log("keys: " + store.size());
    for (final Peer finger : fingers) {
          System.out.println(localUUID + " finger: " + finger.getUUID());
    }
  }

  public int getUUID() {
    return localUUID;
  }

  public static DHT createDefault() {
    return new DHT(PeerExchangeOverlay.createDefault(new Random().nextInt(size)));
  }

  public static DHT createDefault(final int uuid) {
    return new DHT(PeerExchangeOverlay.createDefault(uuid));
  }

  @Override
  public void close() throws IOException {
    overlay.close();
  }

  private final Peer me = new Peer() {
    @Override
    public void close() throws IOException {
    }

    @Override
    public CompletionStage<UUID> send(final Object object) {
      throw new RuntimeException("This is me");
    }

    @Override
    public CompletionStage<UUID> send(final Object object, final UUID uuid) {
      throw new RuntimeException("This is me");
    }

    @Override
    public CompletionStage<IdJsonMessage> receive(final UUID uuid) {
      throw new RuntimeException("This is me");
    }

    @Override
    public InetSocketAddress getListeningAddress() {
      return overlay.getListeningAddress();
    }

    @Override
    public int getUUID() {
      return localUUID;
    }

    @Override
    public void setOnMessageReceivedCallback(final Consumer<IdJsonMessage> callback) {
      throw new RuntimeException("This is me");
    }

    @Override
    public void setOnBrokenPipeCallback(final Runnable callback) {
    }

    @Override
    public InetSocketAddress getAddress() {
      throw new RuntimeException("This is me");
    }

    @Override
    public void onBrokenPipe() {
    }
  };
}
