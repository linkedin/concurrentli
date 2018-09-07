package com.concurrentli;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;


public class VirtualExecutorServiceTest {
  @Test
  public void basicTest() throws InterruptedException, ExecutionException {
    ExecutorService baseExecutor = Executors.newFixedThreadPool(4);
    VirtualExecutorService virtualExecutor = new VirtualExecutorService(baseExecutor);

    Future<?> future = virtualExecutor.submit(Interrupted.unchecked(() -> {
      while (true) {
        Thread.sleep(0);
      }
    }));

    future.cancel(true);

    Future<Integer> future5 = virtualExecutor.submit(() -> 5);
    Future<Integer> future8 = virtualExecutor.submit(() -> 8);

    Assert.assertEquals((int) future5.get(), 5);
    Assert.assertEquals((int) future8.get(), 8);

    Assert.assertFalse(virtualExecutor.isTerminated());
    virtualExecutor.shutdown();
    virtualExecutor.awaitTermination(10, TimeUnit.SECONDS);
    Assert.assertTrue(virtualExecutor.isTerminated());
    Assert.assertFalse(baseExecutor.isTerminated());
  }

  @Test
  public void shutdownNowTest() throws InterruptedException, ExecutionException {
    ExecutorService baseExecutor = Executors.newFixedThreadPool(4);
    VirtualExecutorService virtualExecutor = new VirtualExecutorService(baseExecutor);

    for (int i = 0; i < 10; i++) {
      virtualExecutor.submit(Interrupted.unchecked(() -> {
        while (true) {
          Thread.sleep(Long.MAX_VALUE);
        }
      }));
    }

    Thread.sleep(100); // wait a while for the jobs to start

    List<Runnable> list = virtualExecutor.shutdownNow();
    Assert.assertEquals(6, list.size());
    virtualExecutor.awaitTermination(10, TimeUnit.SECONDS);
    Assert.assertTrue(virtualExecutor.isTerminated());
    Assert.assertFalse(baseExecutor.isTerminated());

    try {
      virtualExecutor.submit(() -> 5);
      Assert.fail();
    } catch (RejectedExecutionException e) {
    } catch (Exception e) {
      Assert.fail();
    }

    Assert.assertEquals((int) (baseExecutor.submit(() -> 8).get()), 8);
  }

  // same as above, but doesn't wait after scheduling and merely checks that there are at least 6 (not exactly 6)
  // un-run items remaining when shutdownNow() is called
  @Test
  public void shutdownNowTestWeak() throws InterruptedException, ExecutionException {
    ExecutorService baseExecutor = Executors.newFixedThreadPool(4);
    VirtualExecutorService virtualExecutor = new VirtualExecutorService(baseExecutor);

    for (int i = 0; i < 10; i++) {
      virtualExecutor.submit(Interrupted.unchecked(() -> {
        while (true) {
          Thread.sleep(Long.MAX_VALUE);
        }
      }));
    }

    List<Runnable> list = virtualExecutor.shutdownNow();
    Assert.assertTrue(list.size() >= 6);
    virtualExecutor.awaitTermination(10, TimeUnit.SECONDS);
    Assert.assertTrue(virtualExecutor.isTerminated());
    Assert.assertFalse(baseExecutor.isTerminated());
  }

  // Test if we can simply wait a second for the executor to shut down rather than using awaitShutdown
  @Test
  public void shutdownNowTimeAwaitTest() throws InterruptedException, ExecutionException {
    ExecutorService baseExecutor = Executors.newFixedThreadPool(4);
    VirtualExecutorService virtualExecutor = new VirtualExecutorService(baseExecutor);

    for (int i = 0; i < 10; i++) {
      virtualExecutor.submit(Interrupted.unchecked(() -> {
        while (true) {
          Thread.sleep(Long.MAX_VALUE);
        }
      }));
    }

    virtualExecutor.shutdownNow();
    Thread.sleep(1000);
    Assert.assertTrue(virtualExecutor.isTerminated());
    Assert.assertFalse(baseExecutor.isTerminated());
  }
}
