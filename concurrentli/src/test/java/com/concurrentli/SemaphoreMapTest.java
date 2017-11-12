/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import com.concurrentli.SemaphoreMap;
import org.junit.Test;

import static org.junit.Assert.*;


public class SemaphoreMapTest {
  @Test
  public void cleanup() throws InterruptedException {
    SemaphoreMap<Integer> ts = new SemaphoreMap<>();
    ts.release(1);
    ts.release(2);
    ts.release(3);
    ts.release(1);

    ts.acquire(1);
    ts.acquire(1);
    ts.acquire(2);
    ts.acquire(3);

    assertEquals(ts.keyCount(), 0);
  }
}
