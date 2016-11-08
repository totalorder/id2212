package org.deadlock.id2212.asyncio;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class AsyncQueue<T> {
  private final Queue<T> incoming = new LinkedList<>();
  private final Queue<CompletableFuture<T>> requested = new LinkedList<>();

  public AsyncQueue() {
  }

  private synchronized void update() {
    if (incoming.peek() != null && requested.peek() != null) {
      final T element = incoming.remove();
      final CompletableFuture<T> future = requested.remove();
      future.complete(element);
    }
  }

  public synchronized void add(T element) {
    incoming.add(element);
    update();
  }

  public synchronized CompletionStage<T> remove() {
    final CompletableFuture<T> future = new CompletableFuture<>();
    requested.add(future);
    update();
    return future;
  }
}
