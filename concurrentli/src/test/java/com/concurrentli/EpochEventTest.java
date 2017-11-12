/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import com.concurrentli.FutureEpochEvent;
import com.concurrentli.ImminentEpochEvent;
import com.concurrentli.Interrupted;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;


public class EpochEventTest {
  @Test
  public void nonblockingTest() throws InterruptedException {
    FutureEpochEvent e = new FutureEpochEvent();
    e.get(-1);
  }

  @Test
  public void lockstepTest() throws InterruptedException {
    FutureEpochEvent e = new FutureEpochEvent();

    List<Integer> results = new ArrayList<>();

    ExecutorService threadPool = Executors.newFixedThreadPool(5);

    for (int i = 0; i < 100; i++) {
      int epoch = i;
      threadPool.submit(Interrupted.unchecked(() -> {
        e.get(epoch);
        results.add(epoch);
        e.set(epoch + 1);
      }));
    }

    e.get(100);

    for (int i = 0; i < 100; i++) {
      assertEquals((int) results.get(i), i);
    }

    threadPool.shutdownNow();
  }

  @Ignore
  @Test
  public void perfTest() throws InterruptedException {
    long startTime = System.currentTimeMillis();

    AtomicLong index = new AtomicLong(0);
    ImminentEpochEvent epochEvent = new ImminentEpochEvent(100, 0);

    final int threadCount = 8;
    final int itemsPerThread = 5000000;

    Thread[] writers = new Thread[threadCount];
    for (int i = 0; i < writers.length; i++) {
      writers[i] = new Thread(Interrupted.ignored(() -> {
        for (int j = 0; j < itemsPerThread; j++) {
          int val = 0;
          for (int k = 0; k < 1000; k++) {
            val += k;
          }
          epochEvent.get(index.getAndIncrement());
        }
      }));
      writers[i].start();
    }

    Thread reader = new Thread(Interrupted.ignored(() -> {
      for (int i = 0; i < (threadCount * itemsPerThread); i++) {
        epochEvent.set(i);
      }
    }));
    reader.setPriority(Thread.MAX_PRIORITY);
    reader.start();

    reader.join();
    System.out.println(System.currentTimeMillis() - startTime);
  }
}
