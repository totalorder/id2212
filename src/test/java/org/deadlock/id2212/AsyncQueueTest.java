package org.deadlock.id2212;

import org.deadlock.id2212.asyncio.AsyncQueue;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class AsyncQueueTest {

  private AsyncQueue<Integer> asyncQueue;

  @Before
  public void setUp() throws Exception {
    asyncQueue = new AsyncQueue<>();
  }

  @Test
  public void addsThenRemoves() throws ExecutionException, InterruptedException {
    asyncQueue.add(1);
    final int result = asyncQueue.remove().toCompletableFuture().get();
    assertEquals(1, result);
  }

  @Test
  public void removesThenAdds() throws ExecutionException, InterruptedException {
    final CompletionStage<Integer> removed = asyncQueue.remove();
    asyncQueue.add(1);
    final int result = removed.toCompletableFuture().get();
    assertEquals(1, result);
  }
}