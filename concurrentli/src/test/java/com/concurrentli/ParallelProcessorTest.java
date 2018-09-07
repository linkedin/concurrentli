/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import com.concurrentli.ParallelProcessor;
import org.junit.Test;

import static org.junit.Assert.*;


public class ParallelProcessorTest {

  class DummyProcessor extends ParallelProcessor<Integer, Integer> {
    private static final int iterationLimit = 10000;
    private final boolean _throwException;

    public DummyProcessor(boolean throwException) {
      super (new Config().setProcessorThreads(10).setBatchSize(3).setInputBufferBatchCount(5).setOutputBufferBatchCount(3));
      _throwException = throwException;
    }

    private int _nextInput = 0;

    @Override
    protected Integer nextInput() {
      if (_nextInput == iterationLimit) {
        if (_throwException) {
          throw new ClassCastException(); // the type of exception doesn't really matter
        } else {
          return null;
        }
      }
      return _nextInput++;
    }

    @Override
    protected Integer processInput(Integer input) {
      return input;
    }
  }

  @Test
  public void test() throws InterruptedException {
    DummyProcessor dp = new DummyProcessor(true);

    try {
      for (int i = 0; i < 10001; i++) {
        Integer res = dp.nextInterruptable();
        assertEquals(i, (int) res);
      }

      fail("Did not throw expected exception");
    } catch (ParallelProcessor.NextInputException e) {
      assertTrue(e.getCause() instanceof ClassCastException);
    }

    dp.close();
  }

  @Test
  public void fillupTest() throws InterruptedException {
    DummyProcessor dp = new DummyProcessor(false);
    dp.start();
    dp.nextInterruptable();
    Thread.sleep(300);
    for (int i = 1; i < 10000; i++) {
      Integer res = dp.nextInterruptable();
      assertEquals(i, (int) res);
    }
  }

  @Test
  public void skipTest() throws InterruptedException {
    DummyProcessor dp = new DummyProcessor(false);

    for (int i = 0; i < 10000; i += 5) {
      Integer res = dp.nextInterruptable();
      assertEquals(i, (int) res);
      dp.skip(4);
    }
  }

  @Test
  public void skipTest2() throws InterruptedException {
    DummyProcessor dp = new DummyProcessor(false);

    for (int i = 0; i < 10000; i += 5000) {
      Integer res = dp.nextInterruptable();
      assertEquals(i, (int) res);
      dp.skip(4999);
    }
  }
}
