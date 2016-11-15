package org.deadlock.id2212;

import org.deadlock.id2212.overlay.Overlay;
import org.deadlock.id2212.overlay.Peer;
import org.deadlock.id2212.overlay.PeerExchangeOverlay;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OverlayTest {
  private PeerExchangeOverlay overlay1;
  private PeerExchangeOverlay overlay2;
  private PeerExchangeOverlay overlay3;
  private Overlay overlay4;

  @Before
  public void setUp() throws Exception {
    overlay1 = PeerExchangeOverlay.createDefault();
    overlay2 = PeerExchangeOverlay.createDefault();
    overlay3 = PeerExchangeOverlay.createDefault();
  }

  @After
  public void tearDown() throws Exception {
    overlay1.close();
    overlay2.close();
    overlay3.close();
  }

  @Test
  public void canConnectAndAccept() throws ExecutionException, InterruptedException, IOException {
    // Given
    overlay1.listen(0).toCompletableFuture().get();

    // When
    final CompletionStage<Peer> connectedPeerFuture = overlay2.connect(new InetSocketAddress(overlay1.getListeningPort()));
    final CompletionStage<Peer> acceptedPeerFuture = overlay1.accept();

    // Then
    final Peer connectedPeer = connectedPeerFuture.toCompletableFuture().get();
    final Peer acceptedPeer = acceptedPeerFuture.toCompletableFuture().get();
    assertNotNull(connectedPeer);
    assertNotNull(acceptedPeer);
  }

  @Test
  public void twoConnectedKnowsEachOther() throws ExecutionException, InterruptedException, IOException {
    // Given
    overlay1.listen(0).toCompletableFuture().get();
    overlay2.listen(0).toCompletableFuture().get();
    overlay3.listen(0).toCompletableFuture().get();
    System.out.println(overlay1.getUUID() + " " + overlay1.getListeningPort());
    System.out.println(overlay2.getUUID() + " " + overlay2.getListeningPort());
    System.out.println(overlay3.getUUID() + " " + overlay3.getListeningPort());

    // When
    // Connect peer 2 -> peer 1
    final CompletionStage<Peer> connectedPeer2Future = overlay2.connect(new InetSocketAddress(overlay1.getListeningPort()));
    final CompletionStage<Peer> acceptedPeer2Future = overlay1.accept();
    final Peer connectedPeer2 = connectedPeer2Future.toCompletableFuture().get();
    final Peer acceptedPeer2 = acceptedPeer2Future.toCompletableFuture().get();

    // Connect peer 3 -> peer 1
    final CompletionStage<Peer> connectedPeer3Future = overlay3.connect(new InetSocketAddress(overlay1.getListeningPort()));
    final CompletionStage<Peer> acceptedPeer3Future = overlay1.accept();
    final Peer acceptedPeer3 = acceptedPeer3Future.toCompletableFuture().get();

    // Connect new peers

    // Anticipate peer 2 or 3 to accept a connection from the other
    final CompletionStage<Peer> acceptedPeerX2Future = overlay2.accept();
    final CompletionStage<Peer> acceptedPeerX3Future = overlay3.accept();

    // Discover peers
    final CompletionStage<Void> peersAnnounced1 = overlay1.announceKnownPeers();
    final CompletionStage<Void> peersAnnounced2 = overlay2.announceKnownPeers();
    final CompletionStage<Void> peersAnnounced3 = overlay3.announceKnownPeers();
    peersAnnounced1.toCompletableFuture().get();
    peersAnnounced2.toCompletableFuture().get();
    peersAnnounced3.toCompletableFuture().get();

    overlay1.waitForConnectedPeers(2).toCompletableFuture().get();
    overlay2.waitForConnectedPeers(2).toCompletableFuture().get();
    overlay3.waitForConnectedPeers(2).toCompletableFuture().get();

    // Then
    // Peer 3 should be connected after exchange
    final Peer connectedPeer3 = connectedPeer3Future.toCompletableFuture().get();

    final Set<UUID> overlay1ExpectedKnownPeers = new HashSet<>();
    overlay1ExpectedKnownPeers.add(overlay2.getUUID());
    overlay1ExpectedKnownPeers.add(overlay3.getUUID());
    assertEquals(overlay1ExpectedKnownPeers, overlay1.getKnownPeers().peerInfos.stream().map(peerInfo -> peerInfo.uuid).collect(Collectors.toSet()));

    final Set<UUID> overlay2ExpectedKnownPeers = new HashSet<>();
    overlay2ExpectedKnownPeers.add(overlay1.getUUID());
    overlay2ExpectedKnownPeers.add(overlay3.getUUID());
    assertEquals(overlay2ExpectedKnownPeers, overlay2.getKnownPeers().peerInfos.stream().map(peerInfo -> peerInfo.uuid).collect(Collectors.toSet()));

    final Set<UUID> overlay3ExpectedKnownPeers = new HashSet<>();
    overlay3ExpectedKnownPeers.add(overlay1.getUUID());
    overlay3ExpectedKnownPeers.add(overlay2.getUUID());
    assertEquals(overlay3ExpectedKnownPeers, overlay3.getKnownPeers().peerInfos.stream().map(peerInfo -> peerInfo.uuid).collect(Collectors.toSet()));
  }

  @Test
  public void twoStartedConnectedKnowsEachOther() throws ExecutionException, InterruptedException, IOException {
    // Given
    overlay1.start(0);
    overlay2.start(0);
    overlay3.start(0);
    System.out.println(overlay1.getUUID() + " " + overlay1.getListeningPort());
    System.out.println(overlay2.getUUID() + " " + overlay2.getListeningPort());
    System.out.println(overlay3.getUUID() + " " + overlay3.getListeningPort());

    // When
    // Connect peer 2 -> peer 1
    overlay2.connect(new InetSocketAddress(overlay1.getListeningPort()));
    // Connect peer 3 -> peer 1
    overlay3.connect(new InetSocketAddress(overlay1.getListeningPort()));

    overlay1.waitForConnectedPeers(2).toCompletableFuture().get();
    overlay2.waitForConnectedPeers(2).toCompletableFuture().get();
    overlay3.waitForConnectedPeers(2).toCompletableFuture().get();

    // Then
    final Set<UUID> overlay1ExpectedKnownPeers = new HashSet<>();
    overlay1ExpectedKnownPeers.add(overlay2.getUUID());
    overlay1ExpectedKnownPeers.add(overlay3.getUUID());
    assertEquals(overlay1ExpectedKnownPeers, overlay1.getKnownPeers().peerInfos.stream().map(peerInfo -> peerInfo.uuid).collect(Collectors.toSet()));

    final Set<UUID> overlay2ExpectedKnownPeers = new HashSet<>();
    overlay2ExpectedKnownPeers.add(overlay1.getUUID());
    overlay2ExpectedKnownPeers.add(overlay3.getUUID());
    assertEquals(overlay2ExpectedKnownPeers, overlay2.getKnownPeers().peerInfos.stream().map(peerInfo -> peerInfo.uuid).collect(Collectors.toSet()));

    final Set<UUID> overlay3ExpectedKnownPeers = new HashSet<>();
    overlay3ExpectedKnownPeers.add(overlay1.getUUID());
    overlay3ExpectedKnownPeers.add(overlay2.getUUID());
    assertEquals(overlay3ExpectedKnownPeers, overlay3.getKnownPeers().peerInfos.stream().map(peerInfo -> peerInfo.uuid).collect(Collectors.toSet()));
  }
}