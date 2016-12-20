package org.deadlock.id2212;

import org.deadlock.id2212.messages.RingProbe;
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
    dht1 = DHT.createDefault(Integer.MAX_VALUE / 2 + 1);
    dht2 = DHT.createDefault(Integer.MAX_VALUE);
    dht3 = DHT.createDefault(-Integer.MAX_VALUE / 2);
//
//    dht1 = DHT.createDefault();
//    dht2 = DHT.createDefault();
//    dht3 = DHT.createDefault();
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
    final List<DHT> nodes = IntStream.range(0, 5)
        .mapToObj(idx -> DHT.createDefault())
        .collect(Collectors.toList());
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
//    nodes.forEach(DHT::printFingerTable);
  }

  @Test
  public void canFixFingers() throws Exception {
    dht1.start(0);
    dht2.start(0);

    dht1.initiate();
    dht2.join(dht1.getListeningAddress()).toCompletableFuture().get();

    dht1.waitForFingersFixed().toCompletableFuture().get();
    dht2.waitForFingersFixed().toCompletableFuture().get();

    dht1.printFingerTable();
    dht2.printFingerTable();
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

//    System.out.println(dht2.probeRing().toCompletableFuture().get());

    System.out.println("STOPPING!");
    dht1.stop().toCompletableFuture().get();
    System.out.println("Stopped!");
    dht2.waitForAlone().toCompletableFuture().get();
  }
}
