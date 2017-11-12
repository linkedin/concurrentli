/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import com.concurrentli.Interrupted;
import com.concurrentli.ResettableEvent;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.*;


public class ManualResetEventTest {
  @Test
  public void simpleAcquisitionTest() throws InterruptedException {
    ResettableEvent ev = new ResettableEvent(true);
    ev.getWithoutReset();
  }

  @Test
  public void test() throws InterruptedException {
    ResettableEvent start = new ResettableEvent(true);
    ResettableEvent finish = new ResettableEvent(true);

    // test reset()
    start.reset();
    finish.reset();

    AtomicInteger runCount = new AtomicInteger(0);

    java.util.concurrent.ExecutorService threadPool = Executors.newFixedThreadPool(5);

    for (int i = 0; i < 100; i++) {
      threadPool.submit(Interrupted.unchecked(() -> {
        start.getWithoutReset();
        int val = runCount.incrementAndGet();

        if (val == 100) {
          finish.set();
        }
      }));
    }

    start.set();
    finish.getWithoutReset();
    assertEquals(runCount.get(), 100);
  }
}
