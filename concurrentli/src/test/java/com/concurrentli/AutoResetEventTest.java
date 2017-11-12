/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import com.concurrentli.Interrupted;
import com.concurrentli.ResettableEvent;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.*;


public class AutoResetEventTest {
  @Test
  public void test() throws InterruptedException {
    AtomicInteger incrementor = new AtomicInteger();
    ResettableEvent are = new ResettableEvent(true);
    ResettableEvent done = new ResettableEvent(false);

    ExecutorService threadPool = Executors.newFixedThreadPool(3);
    for (int i = 0; i < 10; i++) {
      threadPool.submit(Interrupted.unchecked(() -> {
        are.getAndReset();
        incrementor.incrementAndGet();
        done.set();
      }));
    }

    done.getAndReset();
    Thread.sleep(100); // give a little time to other threads to increment, if there's indeed a bug
    assertEquals(1, incrementor.get());
    threadPool.shutdownNow();
  }
}
