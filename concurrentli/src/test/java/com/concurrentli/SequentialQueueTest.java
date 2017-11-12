/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import com.concurrentli.Interrupted;
import com.concurrentli.SequentialQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;


public class SequentialQueueTest {
  @Test
  public void test() throws InterruptedException {
    ExecutorService threadPool = Executors.newFixedThreadPool(11);
    SequentialQueue<Integer> queue = new SequentialQueue<>(10);

    for (int i = 0; i < 10000; i++) {
      int toQueue = (20 * (i / 20)) + (19 - (i % 20));
      threadPool.submit(Interrupted.unchecked(() -> queue.enqueue(toQueue, toQueue)));
    }

    for (int i = 0; i < 1000; i++) {
      assertEquals((int) queue.dequeue(), i);
    }
  }

  @Test
  public void testInterruption() throws InterruptedException {
    ExecutorService threadPool = Executors.newFixedThreadPool(10);
    SequentialQueue<Integer> queue = new SequentialQueue<>(10);

    threadPool.submit(Interrupted.unchecked(() -> queue.dequeue()));

    threadPool.submit(Interrupted.unchecked(() -> {
      for (int i = 0; i < 200; i++) {
        queue.dequeue();
      }
    }));

    for (int i = 0; i < 100; i++) {
      final int index = i;
      threadPool.submit(Interrupted.unchecked(() -> {
        queue.enqueue(index, null);
      }));
    }

    queue.enqueueException(100, new ClassCastException());

    threadPool.shutdown();
    threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
  }

  @Test
  public void perfTest() throws InterruptedException {
    long startTime = System.currentTimeMillis();

    ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(100);

    final int threadCount = 8;
    final int itemsPerThread = 5000000;

    Thread[] writers = new Thread[threadCount];
    for (int i = 0; i < writers.length; i++) {
      writers[i] = new Thread(Interrupted.ignored(() -> {
        for (int j = 0; j < itemsPerThread; j++) {
          int val = 0;
          for (int k = 0; k < 10000; k++) {
            val += k;
          }
          queue.put(val);
        }
      }));
      writers[i].start();
    }

    Thread reader = new Thread(Interrupted.ignored(() -> {
      for (int i = 0; i < (threadCount * itemsPerThread); i++) {
        queue.take();
      }
    }));
    reader.start();

    reader.join();
    System.out.println(System.currentTimeMillis() - startTime);
  }

  @Ignore
  @Test
  public void perfTest2() throws InterruptedException {
    long startTime = System.currentTimeMillis();

    AtomicLong index = new AtomicLong(0);
    SequentialQueue<Integer> queue = new SequentialQueue<>(100);

    final int threadCount = 8;
    final int itemsPerThread = 5000000;

    Thread[] writers = new Thread[threadCount];
    for (int i = 0; i < writers.length; i++) {
      writers[i] = new Thread(Interrupted.ignored(() -> {
        for (int j = 0; j < itemsPerThread; j++) {
          int val = 0;
          for (int k = 0; k < 10000; k++) {
            val += k;
          }
          queue.enqueue(index.getAndIncrement(), val);
        }
      }));
      writers[i].start();
    }

    Thread reader = new Thread(Interrupted.ignored(() -> {
      for (int i = 0; i < (threadCount * itemsPerThread); i++) {
        queue.dequeue();
      }
    }));
    reader.setPriority(Thread.MAX_PRIORITY);
    reader.start();

    reader.join();
    System.out.println(System.currentTimeMillis() - startTime);
  }
}
