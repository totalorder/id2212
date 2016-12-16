package org.deadlock.id2212;

import org.deadlock.id2212.overlay.PeerExchangeOverlay;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CompletionStage;

import static org.junit.Assert.*;

public class DHTTest {

  private DHT dht1;
  private DHT dht2;

  @Before
  public void setUp() throws Exception {
    dht1 = DHT.createDefault();
    dht2 = DHT.createDefault();
  }

  @After
  public void tearDown() throws Exception {
    dht1.close();
    dht2.close();
  }

  @Test
  public void canJoin() throws Exception {
    dht1.start(0);
    dht2.start(0);

    dht1.initiate();
    dht2.join(new InetSocketAddress(dht1.getListeningPort())).toCompletableFuture().get();
  }

  @Test
  public void canStabilize() throws Exception {
    dht1.start(0);
    dht2.start(0);

    dht1.initiate();
    dht2.join(new InetSocketAddress(dht1.getListeningPort())).toCompletableFuture().get();

    dht1.waitForStable().toCompletableFuture().get();
    dht2.waitForStable().toCompletableFuture().get();
  }

  @Test
  public void canFixFingers() throws Exception {
    dht1.start(0);
    dht2.start(0);

    dht1.initiate();
    dht2.join(new InetSocketAddress(dht1.getListeningPort())).toCompletableFuture().get();

    dht1.waitForFingersFixed().toCompletableFuture().get();
    dht2.waitForFingersFixed().toCompletableFuture().get();
  }
}