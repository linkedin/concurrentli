/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import com.concurrentli.AtomicWriteOnceReference;
import com.concurrentli.Interrupted;
import org.junit.Test;

import static org.junit.Assert.*;


public class AtomicWriteOnceReferenceTest {
  @Test
  public void test() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      runTest(i);
    }
  }

  private void runTest(int index) throws InterruptedException {
    Thread[] threads = new Thread[20];
    AtomicWriteOnceReference<C> ref = new AtomicWriteOnceReference<>();
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(Interrupted.ignored(() -> {
        Thread.sleep(50);
        assertEquals(ref.get()._a, index);
        assertEquals(ref.get()._b, index + 1);
        assertEquals(ref.get()._c, index + 2);
      }));
      threads[i].start();
    }

    ref.set(new C(index));
    Thread.sleep(200);

    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
  }

  private static class C {
    public int _a;
    public int _b;
    public int _c;

    public C(int val) {
      _a = val;
      _b = val + 1;
      _c = val + 2;
    }
  }
}
