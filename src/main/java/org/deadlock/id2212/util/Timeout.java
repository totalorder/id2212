package org.deadlock.id2212.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.deadlock.id2212.util.CompletableExtras.asyncApply;

public class Timeout {
  private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

//  public <T> CompletionStage<T> timeout(final CompletionStage<T> stage, final long delay) {
//    return timeout(stage, delay, () -> {
//      System.out.println("Boom!");
////      new Throwable().printStackTrace();
//      throw new RuntimeException("Timeoutt!");
//    });
//  }

  public <T> CompletionStage<T> timeout(final CompletionStage<T> stage, final long delay) { //, final Supplier<T> onTimeout) {
    final CompletableFuture<T> future = stage.toCompletableFuture();
//    System.out.println(Thread.currentThread().getName() + " Timeout?");
    scheduledExecutorService.schedule(() -> {


//        System.out.println("Die!");
      asyncApply(() -> {
        if (!future.isDone()) {
//          System.out.println(Thread.currentThread().getName() + " Timeout! " + future.isDone());
          future.completeExceptionally(new TimeoutException("Timeout!"));
        }
        return null;
      });

      return null;
    }, delay, TimeUnit.MILLISECONDS);

    return future;
  }

  public <T> CompletionStage<T> timeout(final CompletionStage<T> stage, final long delay, final Runnable onTimeout) {
    final CompletableFuture<T> future = stage.toCompletableFuture();
//    System.out.println("Timeout?");
    scheduledExecutorService.schedule(() -> {
//      System.out.println("Timeout! " + future.isDone());

//        System.out.println("Die!");
      asyncApply(() -> {
        if (!future.isDone()) {
          onTimeout.run();
        }
        return null;
      });

      return null;
    }, delay, TimeUnit.MILLISECONDS);

    return future;
//    return future.exceptionally(throwable -> {
//      if (throwable instanceof TimeoutException) {
//        System.out.println("Dead?");
//        return onTimeout.get();
//      }
//      System.out.println("Dead?!");
//      throw new RuntimeException(throwable);
//    });
  }

  public class TimeoutException extends RuntimeException {
    TimeoutException(String message) { super(message); }
  }


  public <T> CompletionStage<T> catchTimeout(final CompletionStage<T> stage, final Supplier<T> onTimeout) {
    return stage.toCompletableFuture().exceptionally(throwable -> {
      if (throwable.getCause() instanceof TimeoutException) {
        return onTimeout.get();
      }
      throw new RuntimeException(throwable);
    });
  }
}
