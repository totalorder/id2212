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
import static org.deadlock.id2212.util.CompletableExtras.asyncApply;

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
//  private final double K = 2;
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

  private void onPeerBrokenPipe(final Peer peer) {
//    log("broken pipe: " + peer.getUUID());
    if (successor == peer) {
      successor = null;
    }

    if (predecessor == peer) {
      predecessor = null;
    }

    while (fingers.remove(peer)) {
      fingers.addFirst(me);
    }
    log("done breaking");
  }

  private void onMessageReceived(final Peer peer, final IdJsonMessage message) {
    if (!running) {
      return;
    }

    if (logMessages) {
      log("received " + message);
    }

//    System.out.println(localUUID + " received " + message);
    if (message.isClass(PredecessorRequest.class)) {
      if (predecessor != null) {
//        log("Sending predecessor response: %s, %s", predecessor.getUUID(), predecessor.getListeningAddress());
        peer.send(new PredecessorResponse(predecessor.getUUID(), predecessor.getListeningAddress()), message.getUUID());
      } else {
        peer.send(new PredecessorResponse(null, null), message.getUUID());
      }
    } else if (message.isClass(SuccessorRequest.class)) {
      final SuccessorRequest successorRequest = message.getObject(SuccessorRequest.class);
      onSuccessorRequest(peer, message, successorRequest);
    } else if (message.isClass(PredecessorNotification.class)) {
      final PredecessorNotification predecessorNotification = message.getObject(PredecessorNotification.class);
      onPredecessorNotification(peer, message, predecessorNotification);
    } else  if (message.isClass(HeartbeatRequest.class)) {
      peer.send(new HeartbeatResponse(), message.getUUID());
    } else if (message.isClass(RingProbe.class)) {
      onRingProbe(message.getObject(RingProbe.class));
    } else if (message.isClass(GetKeyRequest.class)) {
      onGetKeyRequest(peer, message, message.getObject(GetKeyRequest.class));
    } else if (message.isClass(SetKeyRequest.class)) {
      final SetKeyRequest setKeyRequest = message.getObject(SetKeyRequest.class);
      setKey(setKeyRequest.key, setKeyRequest.value);
      peer.send(new SetKeyResponse(), message.getUUID());
    } else if (message.isClass(ReplicationRequest.class)) {
      onReplicationRequest(message.getObject(ReplicationRequest.class));
    }
  }

  private void onGetKeyRequest(final Peer peer, final IdJsonMessage message, final GetKeyRequest getKeyRequest) {
    peer.send(new GetKeyResponse(getKey(getKeyRequest.key)), message.getUUID()).exceptionally(throwable -> {
      throwable.printStackTrace();
      return null;
    });
  }

  private void onReplicationRequest(final ReplicationRequest replicationRequest) {
//    log("onReplicationRequest %s", replicationRequest.store.size());
    replicationRequest.store.entrySet().forEach(entry -> {

//      if (predecessor == null || !isBetween(entry.getKey(), predecessor.getUUID() + 1, localUUID)) {
//        log("storing key %s", entry.getKey());
        store.put(entry.getKey(), entry.getValue());
//      } else {
//        log("not storing key %s", entry.getKey());
//      }
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

  private void onRingProbe(final RingProbe ringProbe) {
    if (ringProbe.nodes.get(0) == localUUID) {
      lastProbeReceieved.complete(ringProbe);
      lastProbeReceieved = new CompletableFuture<>();
    } else if (successor != null && successor != me){
      ringProbe.nodes.add(localUUID);
      successor.send(ringProbe);
    }
  }

  private void onPredecessorNotification(final Peer peer, final IdJsonMessage message, final PredecessorNotification predecessorNotification) {
//    log("received predecessor notif from " + peer.getUUID());
    if (predecessor == null || isBetween(predecessorNotification.uuid, predecessor.getUUID() + 1, localUUID)) {
      predecessor = peer;
    }
  }

  private void onSuccessorRequest(final Peer peer, final IdJsonMessage message, final SuccessorRequest successorRequest) {
      findSuccessor(successorRequest.uuid).thenCompose(successorReceived -> {
//        log("remote successor response reply for %s to %s with uuid %s", successorRequest.uuid, peer.getUUID(), message.getUUID());
        return peer.send(new SuccessorResponse(successorReceived.getUUID(), successorReceived.getListeningAddress()), message.getUUID());
      }).exceptionally(throwable -> { throwable.printStackTrace(); return null; });
//    if (isBetween(successorRequest.uuid, localUUID + 1, successor.getUUID())) {
//      log("local A to successor response for %s with %s to %s", successorRequest.uuid, successor.getUUID(), peer.getUUID());
//      if (successor == me) {
//        peer.send(new SuccessorResponse(localUUID, getListeningAddress()), message.getUUID());
//      } else {
//        peer.send(new SuccessorResponse(successor.getUUID(), successor.getListeningAddress()), message.getUUID());
//      }
//    } else {
//      log("remote to successor response for %s", successorRequest.uuid);
//      findSuccessor(successorRequest.uuid).thenCompose(successorReceived -> {
//        log("remote successor response reply for %s to %s with uuid %s", successorRequest.uuid, peer.getUUID(), message.getUUID());
//        return peer.send(new SuccessorResponse(successorReceived.getUUID(), successorReceived.getListeningAddress()), message.getUUID());
//      });
//      final Peer closestFinger = getClosestPreceedingFinger(successorRequest.uuid);
//      if (closestFinger != me) {
//        closestFinger.send(successorRequest)
//            .thenCompose(peer::receive)
//            .thenCompose(reply -> {
//              log("remote successor response reply for %s to %s", successorRequest.uuid, peer);
//              return peer.send(reply, message.getUUID());
//            });
//      } else {
//        log("local B to successor response for %s with %s to ", successorRequest.uuid, successor.getUUID());
//        peer.send(new SuccessorResponse(localUUID, getListeningAddress()), message.getUUID());
//      }
//    }
  }

  public CompletionStage<Peer> getSuccessor(int uuid) {
    return async(() -> findSuccessor(uuid));
  }

  public CompletionStage<Peer> getSuccessor(final String key) {
    return async(() -> findSuccessor(hash(key)));
  }

//  public CompletionStage<Peer> lookup(int uuid) {
//    return async(() -> findSuccessor(uuid));
//  }

  private CompletionStage<Peer> findSuccessor(int uuid) {
    if (predecessor == null && (successor == null || successor == me)) {
      return completedFuture(me);
    }
//    log("findSuccessor %s", uuid);

    if (successor != null && isBetween(uuid, localUUID + 1, successor.getUUID()) || successor == me) {
//      log("findSuccessor %s is between successor", uuid);
      return completedFuture(successor);
    } else {
//      log("%s is not between %s and %s", uuid, localUUID + 1, successor != null ? successor.getUUID() : null);
      final Peer closestFinger = getClosestPreceedingFinger(uuid);
      if (closestFinger == me) {
//        log("findSuccessor %s closestFinger == me", uuid);
        if (successor == null) {
          return completedFuture(me);
        }
        return completedFuture(successor);
      }

//      log("findSuccessor %s asking successor %s", uuid, closestFinger.getUUID());
      return timeout.catchTimeout(closestFinger.send(new SuccessorRequest(uuid))
          .thenApply(uuid1 -> {
//            log("sent successor find with uuid %s", uuid1);
            return uuid1;
          })
          .thenComposeAsync(closestFinger::receive)
          .thenCompose(reply -> {
            final SuccessorResponse successorResponse = reply.getObject(SuccessorResponse.class);
//            log("got remote reply %s for %s", successorResponse.successor, uuid);
            return getOrConnectPeer(successorResponse.successor, successorResponse.address);
          }), () -> {
        log("successor request timeout for %s to %s", uuid, closestFinger.getUUID());
        return me;
//            fingers.remove(closestFinger);
//            fingers.addFirst(me);
//            throw new RuntimeException("Successor request timeout!");
//            return me;
          });
    }
  }

  private void onPeerAccepted(final Peer peer) {
    synchronized (connectedPeers) {
      connectedPeers.put(peer.getUUID(), peer);
    }
    log("Accepted connection from " + peer.getUUID());
  }

  private void onPeerConnected(final Peer peer) {
    synchronized (connectedPeers) {
      connectedPeers.put(peer.getUUID(), peer);
    }
    log("Connected to " + peer.getUUID());
  }

  public static DHT createDefault() {
    return new DHT(PeerExchangeOverlay.createDefault(new Random().nextInt(size)));
  }

  public static DHT createDefault(final int uuid) {
    return new DHT(PeerExchangeOverlay.createDefault(uuid));
  }

  public CompletionStage<Void> start(final int port) {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    return overlay.start(port).thenApply(ignored -> {
      log("Started on " + overlay.getListeningAddress());
//      System.out.println("UUID: " + overlay.getUUID());
//      System.out.println("Listening on port " + port + "...");
      return null;
    });
  }

  public InetSocketAddress getListeningAddress() {
    return overlay.getListeningAddress();
  }

//  public CompletionStage<Void> connect(final InetSocketAddress inetSocketAddress) {
//    return overlay.connect(inetSocketAddress).thenApply(ignored -> null);
//  }

  public CompletionStage<Void> join(final InetSocketAddress inetSocketAddress) {
    running = true;
    while (fingers.size() < numFingers) {
      fingers.add(me);
    }

    predecessor = null;

    stabilizeForever();

    return async(() -> {
      return overlay.connect(inetSocketAddress)
          .thenApply(peer -> {
            synchronized (connectedPeers) {
              connectedPeers.put(peer.getUUID(), peer);
            }
            return peer;
          })
          .thenCompose(peer -> findSuccessor(peer, localUUID)
              .thenApply(successor -> {
                log("Found successor " + successor.getUUID());
                this.successor = successor;
//                fingers.clear();
//                while (fingers.size() < numFingers) {
//                  fingers.add(successor);
//                }
                return null;
              }));
    });
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

  private CompletionStage<Peer> findSuccessor(final Peer peer, final int uuid) {
    log("initial findSuccessor at " + peer.getUUID() + " for " + uuid);
    return peer.send(new SuccessorRequest(uuid))
        .thenCompose(peer::receive)
        .thenCompose(reply -> {
          final SuccessorResponse successorResponse = reply.getObject(SuccessorResponse.class);
          return getOrConnectPeer(successorResponse.successor, successorResponse.address);
        });
  }

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

  public CompletionStage<Void> heartbeat() {
    return handleErrors(async(() -> {
//      log("heartbeat init!");
      if (predecessor != null && predecessor != me) {
//      System.out.println(localUUID + " Sending heartbeat!");
        final CompletableFuture<IdJsonMessage> replyFuture = predecessor.send(new HeartbeatRequest())
            .thenCompose(predecessor::receive)
            .toCompletableFuture();

        final CompletableFuture<Void> voidReplyFuture = replyFuture.<Void>thenApplyAsync(ignoredAgain -> null);
        return timeout.timeout(voidReplyFuture, 500).exceptionally(throwable -> {
          if (throwable instanceof Timeout.TimeoutException) {
            log("heartbeat timeout!");
            predecessor = null;
            return null;
          } else {
            throw new RuntimeException(throwable);
          }
        });
      } else {
//        log("Sending NO heartbeat!");
      }
      return completedFuture(null);
    }), "heartbeat");
  }

  private void log(final String log, final Object... args) {
//    System.out.println(Thread.currentThread().getName() + " " + localUUID + " " + String.format(log, args));
    System.out.println(localUUID + " " + String.format(log, args));
  }

  public CompletionStage<RingProbe> probeRing() {
    return async(() -> {
      lastProbeReceieved = new CompletableFuture<>();
      probeRingUntilResponse();
//      return lastProbeReceieved;
      return timeout.timeout(lastProbeReceieved, 1000);
    });
  }

  private void probeRingUntilResponse() {
    if (successor != null && successor != me) {
      log("sending ring probe");
      final RingProbe ringProbe = new RingProbe();
      ringProbe.nodes.add(localUUID);

      final CompletionStage<RingProbe> replyFuture = successor.send(ringProbe)
          .thenCompose(ignored -> lastProbeReceieved);
      timeout.timeout(replyFuture, 1000, () -> {
//        log("ex %s", throwable);
//        if (throwable instanceof Timeout.TimeoutException || throwable.getCause() instanceof Timeout.TimeoutException) {
        probeRingUntilResponse();
//        } else {
//          log("y?");
//          throw new RuntimeException(throwable);
//        }
//        return null;
      });
    }
  }

//  private <T> CompletionStage<T> forkJoin(Supplier<CompletionStage<T>> supplier) {
//    ForkJoinPool.commonPool().submit(new ForkJoinTask<Void>() {
//
////    ForkJoinTask.adapt(asd -> null);
//    final CompletableFuture<CompletionStage<T>> done = new CompletableFuture<>();
////    CompletableFuture.supplyAsync(() -> {
////
////    })
//    ForkJoinPool.commonPool().submit(() -> {
//      done.complete(supplier.get());
//    });
//
//    return done.get().g;
//  }

  public CompletionStage<Void> replicate() {
    return handleErrors(async(() -> {
      if (successor != null && successor != me) {
        return successor.send(new ReplicationRequest(store)).thenApply(ignored -> null);
      }

      return completedFuture(null);
    }), "replicate");
  }

  public CompletionStage<Void> stabilize() {
    return handleErrors(async(() -> {
      if (successor == me && predecessor == null && !isAlone.isDone()) {
        isAlone.complete(null);
      }

      if (successor == null) {
        return findSuccessor(localUUID).thenApplyAsync(peer -> {
          successor = peer;
          return null;
        });
      }

      if (successor == me && (predecessor == me || predecessor == null)) {
        return completedFuture(null);
      }

      if (successor == me) {
        return updateSuccessorAndSendNotification(predecessor);
      }

      if (predecessor != null && predecessor != me && !isStable.isDone()) {
        isStable.complete(null);
      }

//      log("Sending predecessor request to " + successor.getUUID());
//    return timeout.catchTimeout(successor.send(new PredecessorRequest())
      return successor.send(new PredecessorRequest())
          .thenApply(uuid -> {
//            log("Sent predecessor request");
            return uuid;
          })
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

  private CompletionStage<Void> updateSuccessorAndSendNotification(final Peer successorsPredecessor) {
    if (successorsPredecessor != null && isBetween(successorsPredecessor.getUUID(), localUUID + 1, successor.getUUID())) {
      successor = successorsPredecessor;
    }

    if (successor == me) {
      return completedFuture(null);
    }

//    log("Sending predecessor notification to " + successor.getUUID());
    return successor.send(new PredecessorNotification(localUUID, overlay.getListeningAddress()))
        .thenApplyAsync(ignored -> null);
  }

  public CompletionStage<Void> fixFingers() {
    return handleErrors(async(() -> {
      if (successor == null) {
        return completedFuture(null);
      }
      if (nextFingerToFix > fingers.size() - 1) {
        if (!fingersFixed.isDone()) {
          fingersFixed.complete(null);
        }
        nextFingerToFix = 0;
      }

      final int nextPosition = (localUUID + (int) Math.pow(K, nextFingerToFix)) % size;
      return findSuccessor(nextPosition).thenApply(successor -> {
//      System.out.println(localUUID + " set finger " + nextFingerToFix + " to " + successor.getUUID() + " at position " + nextPosition);
        fingers.set(nextFingerToFix, successor);
        nextFingerToFix = nextFingerToFix + 1;
        return null;
      });
    }), "fix fingers");
  }

  public void initiate() {
    running = true;
    predecessor = null;
    successor = me;
    while (fingers.size() < numFingers) {
      fingers.add(me);
    }

    stabilizeForever();
  }

  private Peer getClosestPreceedingFinger(final int uuid) {
    if (successor == null) {
      return me;
    }
//    return successor;

//    if (uuid == 244 || uuid == 201) {
//      printFingerTable();
//    }
    for (final Peer peer : Lists.reverse(fingers)) {
      if (peer == me) {
        continue;
      }
      if (isBetween(peer.getUUID(), localUUID, uuid - 1)) {
        return peer;
      }
    }
//
//    log("closest is me for %s", uuid);
////    printFingerTable();
//    if (successor != me) {
//      return successor;
//    }

    return me;
  }

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

  private void stabilizeForever() {
    // Announce known peers to all connected peers every second
    scheduledExecutorService.schedule(() -> {
      try {
        heartbeat().toCompletableFuture().get();
//        ForkJoinPool.commonPool().submit(ForkJoinTask.adapt())
        stabilize().toCompletableFuture().get(15000, TimeUnit.MILLISECONDS);
        replicate().toCompletableFuture().get();
        fixFingers().toCompletableFuture().get();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (lastLog.isBefore(Instant.now().minus(10, ChronoUnit.SECONDS))) {
          lastLog = Instant.now();
//          log("predecessor: " + (predecessor != null ? predecessor.getUUID() : "null"));
//          log("successor: " + (successor != null ? successor.getUUID() : "null"));
          printFingerTable();
        }

        if (running) {
//          log("stabilize forever");
          stabilizeForever();
        } else {
//          log("stabilize stop");
          if (!stopped.isDone()) {
            stopped.complete(null);
          }
        }
      }
    }, 1, TimeUnit.SECONDS);
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
