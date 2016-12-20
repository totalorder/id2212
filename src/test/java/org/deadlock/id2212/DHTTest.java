package org.deadlock.id2212;

import org.deadlock.id2212.messages.RingProbe;
import org.deadlock.id2212.overlay.Peer;
import org.deadlock.id2212.overlay.PeerExchangeOverlay;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class DHTTest {

  private DHT dht1;
  private DHT dht2;
  private DHT dht3;

  @Before
  public void setUp() throws Exception {
//    dht1 = DHT.createDefault(Integer.MAX_VALUE / 2 + 1);
//    dht2 = DHT.createDefault(Integer.MAX_VALUE);
//    dht3 = DHT.createDefault(-Integer.MAX_VALUE / 2);
//
    dht1 = DHT.createDefault(100);
    dht2 = DHT.createDefault(200);
    dht3 = DHT.createDefault(300);
  }

  @After
  public void tearDown() throws Exception {
    dht1.close();
    dht2.close();
    dht3.close();
  }

  @Test
  public void canJoin() throws Exception {
    dht1.start(0);
    dht2.start(0);

    dht1.initiate();
    dht2.join(dht1.getListeningAddress()).toCompletableFuture().get();
  }

  @Test
  public void canStabilize() throws Exception {
    dht1.start(0).toCompletableFuture().get();
    dht2.start(0).toCompletableFuture().get();
    dht3.start(0).toCompletableFuture().get();

    dht1.initiate();
    dht2.join(dht1.getListeningAddress()).toCompletableFuture().get();
    dht3.join(dht1.getListeningAddress()).toCompletableFuture().get();

    dht1.waitForStable().toCompletableFuture().get();
    dht2.waitForStable().toCompletableFuture().get();
    dht3.waitForStable().toCompletableFuture().get();

    System.out.println(dht3.probeRing().toCompletableFuture().get());
  }

  @Test
  public void canStabilizeFuzzy() throws Exception {
    final List<DHT> nodes = IntStream.range(0, 4)
        .mapToObj(idx -> DHT.createDefault())
        .collect(Collectors.toList());
    nodes.add(DHT.createDefault(0));
    final List<DHT> connected = new ArrayList<>();

    nodes.forEach(node -> node.start(0).toCompletableFuture().join());

    nodes.forEach(node -> {
      if (connected.size() == 0) {
        node.initiate();
      } else {
        Collections.shuffle(connected);
        node.join(connected.get(0).getListeningAddress()).toCompletableFuture().join();
      }
      connected.add(node);
    });

    nodes.forEach(node -> node.waitForStable().toCompletableFuture().join());

    final List<Integer> probedUUIDs = connected.get(0).probeRing().toCompletableFuture().get().nodes;

    final List<Integer> expectedUUIDs = nodes.stream().map(DHT::getUUID).sorted().collect(Collectors.toList());
    Collections.rotate(expectedUUIDs, -expectedUUIDs.indexOf(probedUUIDs.get(0)));
    assertEquals(expectedUUIDs, probedUUIDs);

//    final List<CompletionStage<Void>> fixes = nodes.stream().map(DHT::waitForFingersFixed).collect(Collectors.toList());
//    fixes.forEach(fix -> fix.toCompletableFuture().join());
    System.out.println(connected.get(0).probeRing().toCompletableFuture().get());
//    nodes.forEach(DHT::printFingerTable);
  }

  @Test
  public void canFixFingers() throws Exception {
    dht1.start(0);
    dht2.start(0);
    dht3.start(0);

    dht1.initiate();
    dht2.join(dht1.getListeningAddress()).toCompletableFuture().get();
    dht3.join(dht1.getListeningAddress()).toCompletableFuture().get();

    dht1.waitForStable().toCompletableFuture().get();
    dht2.waitForStable().toCompletableFuture().get();
    dht3.waitForStable().toCompletableFuture().get();

    CompletionStage<Void> fingers1 = dht1.waitForFingersFixed();
    CompletionStage<Void> fingers2 = dht2.waitForFingersFixed();
    CompletionStage<Void> fingers3 = dht3.waitForFingersFixed();
    fingers1.toCompletableFuture().get();
    fingers2.toCompletableFuture().get();
    fingers3.toCompletableFuture().get();

    System.out.println(dht1.probeRing().toCompletableFuture().get());
    dht1.printFingerTable();
    dht2.printFingerTable();
    dht3.printFingerTable();

    fingers1 = dht1.waitForFingersFixed();
    fingers2 = dht2.waitForFingersFixed();
    fingers3 = dht3.waitForFingersFixed();

    fingers1.toCompletableFuture().get();
    fingers2.toCompletableFuture().get();
    fingers3.toCompletableFuture().get();

    System.out.println(dht1.probeRing().toCompletableFuture().get());
    dht1.printFingerTable();
    dht2.printFingerTable();
    dht3.printFingerTable();
  }

  @Test
  public void canGetAndSet() throws Exception {
    dht1.start(0);
    dht2.start(0);
    dht3.start(0);

    dht1.initiate();
    dht2.join(dht1.getListeningAddress()).toCompletableFuture().get();
    dht3.join(dht1.getListeningAddress()).toCompletableFuture().get();

    dht1.waitForStable().toCompletableFuture().get();
    dht2.waitForStable().toCompletableFuture().get();
    dht3.waitForStable().toCompletableFuture().get();

    dht1.set("key", "val").toCompletableFuture().get();
    assertEquals(dht3.get("key").toCompletableFuture().get(), "val");
  }

  @Test
  public void canReplicate() throws Exception {
    dht1.start(0);
    dht2.start(0);
    dht3.start(0);

    dht1.initiate();
    dht2.join(dht1.getListeningAddress()).toCompletableFuture().get();
    dht3.join(dht1.getListeningAddress()).toCompletableFuture().get();

    dht1.waitForStable().toCompletableFuture().get();
    dht2.waitForStable().toCompletableFuture().get();
    dht3.waitForStable().toCompletableFuture().get();

    final Peer owner = dht1.getSuccessor("key").toCompletableFuture().get();
    dht1.set("key", "val").toCompletableFuture().get();
    assertEquals(dht3.get("key").toCompletableFuture().get(), "val");

    dht1.waitForStable().toCompletableFuture().get();
    dht2.waitForStable().toCompletableFuture().get();
    dht3.waitForStable().toCompletableFuture().get();
    Thread.sleep(1500);

    if (dht1.getUUID() == owner.getUUID()) {
      dht1.stop().toCompletableFuture().get();
      dht2.waitForStable().toCompletableFuture().get();
      dht3.waitForStable().toCompletableFuture().get();
      assertEquals(dht2.get("key").toCompletableFuture().get(), "val");
      assertEquals(dht3.get("key").toCompletableFuture().get(), "val");
    } else if (dht2.getUUID() == owner.getUUID()) {
      dht2.stop().toCompletableFuture().get();
      dht1.waitForStable().toCompletableFuture().get();
      dht3.waitForStable().toCompletableFuture().get();
      assertEquals(dht1.get("key").toCompletableFuture().get(), "val");
      assertEquals(dht3.get("key").toCompletableFuture().get(), "val");
    } else if (dht3.getUUID() == owner.getUUID()) {
      dht3.stop().toCompletableFuture().get();
      dht1.waitForStable().toCompletableFuture().get();
      dht2.waitForStable().toCompletableFuture().get();
      assertEquals(dht1.get("key").toCompletableFuture().get(), "val");
      assertEquals(dht2.get("key").toCompletableFuture().get(), "val");
    }
  }

  @Test
  public void canGetSuccessor() throws Exception {
    dht1.start(0);
    dht2.start(0);
    dht3.start(0);

    dht1.initiate();
    dht2.join(dht1.getListeningAddress()).toCompletableFuture().get();
    dht3.join(dht1.getListeningAddress()).toCompletableFuture().get();

    dht1.waitForStable().toCompletableFuture().get();
    dht2.waitForStable().toCompletableFuture().get();
    dht3.waitForStable().toCompletableFuture().get();

    CompletionStage<Void> fingers1 = dht1.waitForFingersFixed();
    CompletionStage<Void> fingers2 = dht2.waitForFingersFixed();
    CompletionStage<Void> fingers3 = dht3.waitForFingersFixed();
    fingers1.toCompletableFuture().get();
    fingers2.toCompletableFuture().get();
    fingers3.toCompletableFuture().get();
//
    dht1.printFingerTable();
    dht2.printFingerTable();
    dht3.printFingerTable();

    fingers1 = dht1.waitForFingersFixed();
    fingers2 = dht2.waitForFingersFixed();
    fingers3 = dht3.waitForFingersFixed();
    fingers1.toCompletableFuture().get();
    fingers2.toCompletableFuture().get();
    fingers3.toCompletableFuture().get();
//
    dht1.printFingerTable();
    dht2.printFingerTable();
    dht3.printFingerTable();

    boolean a99 = (100 == dht1.getSuccessor(99).toCompletableFuture().get().getUUID());
    boolean a100 = (100 == dht1.getSuccessor(100).toCompletableFuture().get().getUUID());
    boolean a101 = (200 == dht1.getSuccessor(101).toCompletableFuture().get().getUUID());
    boolean a199 = (200 == dht1.getSuccessor(199).toCompletableFuture().get().getUUID());
    boolean a200 = (200 == dht1.getSuccessor(200).toCompletableFuture().get().getUUID());
    boolean a201 = (300 == dht1.getSuccessor(201).toCompletableFuture().get().getUUID());
    boolean a299 = (300 == dht1.getSuccessor(299).toCompletableFuture().get().getUUID());
    boolean a300 = (300 == dht1.getSuccessor(300).toCompletableFuture().get().getUUID());
    boolean a301 = (100 == dht1.getSuccessor(301).toCompletableFuture().get().getUUID());
    boolean a0 = (100 == dht1.getSuccessor(0).toCompletableFuture().get().getUUID());

    System.out.println(dht1.probeRing().toCompletableFuture().get());
    System.out.println("99 " + a99);
    System.out.println("100 " + a100);
    System.out.println("101 " + a101);
    System.out.println("199 " + a199);
    System.out.println("200 " + a200);
    System.out.println("201 " + a201);
    System.out.println("299 " + a299);
    System.out.println("300 " + a300);
    System.out.println("301 " + a301);
    System.out.println("0 " + a0);
  }

  @Test
  public void canLeave() throws Exception {
    dht1.start(0);
    dht2.start(0);

    dht1.initiate();
    dht2.join(dht1.getListeningAddress()).toCompletableFuture().get();

    dht1.waitForStable().toCompletableFuture().get();
    dht2.waitForStable().toCompletableFuture().get();
    System.out.println(dht2.probeRing().toCompletableFuture().get());

    dht1.stop().toCompletableFuture().get();
    dht2.waitForAlone().toCompletableFuture().get();
  }

  @Test
  public void canLeaveAndRejoin() throws Exception {
    dht1.start(0);
    dht2.start(0);

    dht1.initiate();
    dht2.join(dht1.getListeningAddress()).toCompletableFuture().get();

    dht1.waitForStable().toCompletableFuture().get();
    dht2.waitForStable().toCompletableFuture().get();
    System.out.println(dht2.probeRing().toCompletableFuture().get());

    dht1.stop().toCompletableFuture().get();
    dht2.waitForAlone().toCompletableFuture().get();

    dht1.join(dht2.getListeningAddress()).toCompletableFuture().get();
    System.out.println("Joined!");

    dht1.waitForStable().toCompletableFuture().get();
    dht2.waitForStable().toCompletableFuture().get();
    System.out.println(dht2.probeRing().toCompletableFuture().get());
  }

  @Test
  public void canDisconnect() throws Exception {
    dht1.start(0);
    dht2.start(0);

    dht1.initiate();
    dht2.join(dht1.getListeningAddress()).toCompletableFuture().get();

    dht1.stop().toCompletableFuture().get();
    dht2.waitForAlone().toCompletableFuture().get();
  }
}
