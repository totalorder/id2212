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

public class ScheduleManagerTest {

  private ScheduleManager scheduleManager1;
  private ScheduleManager scheduleManager2;
  private Instant time1;
  private Instant time2;
  private Instant time3;

  @Before
  public void setUp() throws Exception {
    time1 = LocalDateTime.of(2016, 11, 15, 16, 1).toInstant(ZoneOffset.UTC);
    time2 = LocalDateTime.of(2016, 11, 15, 16, 2).toInstant(ZoneOffset.UTC);
    time3 = LocalDateTime.of(2016, 11, 15, 16, 3).toInstant(ZoneOffset.UTC);

    scheduleManager1 = new ScheduleManager(PeerExchangeOverlay.createDefault());
    scheduleManager2 = new ScheduleManager(PeerExchangeOverlay.createDefault());
  }

  @After
  public void tearDown() throws Exception {
    scheduleManager1.close();
    scheduleManager2.close();
  }

  @Test
  public void findTime() throws Exception {
    scheduleManager1.addTime(time1);
    scheduleManager1.addTime(time2);

    scheduleManager2.addTime(time2);
    scheduleManager2.addTime(time3);

    scheduleManager1.start(0);
    scheduleManager2.start(0);

    scheduleManager1.connect(new InetSocketAddress(scheduleManager2.getListeningPort()));
    final CompletionStage<Instant> agreedTime1Future = scheduleManager1.findTime(1);
    assertEquals(time2, agreedTime1Future.toCompletableFuture().get());
  }
}