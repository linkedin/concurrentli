/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import com.concurrentli.UnsafeCircularIntegerBuffer;
import org.junit.Test;

import static org.junit.Assert.*;


public class CircularIntegerBufferTest {
  @Test
  public void basic() {
    UnsafeCircularIntegerBuffer buff = new UnsafeCircularIntegerBuffer(10, 42);
    buff.set(9, 10);
    assertEquals(42, buff.getAndAdd(8, -42));
    assertTrue(buff.compareAndSet(0, 42, 10));

    assertEquals(0, buff.advanceIfEqual(10));
    assertEquals(-1, buff.advanceIfEqual(10));

    assertTrue(buff.compareAndAdvance(1));
    assertEquals(buff.getFirstElementIndex(), 2);
    assertEquals(42, buff.get(10));
    assertEquals(0, buff.get(8));
    assertEquals(10, buff.get(9));
  }
}
