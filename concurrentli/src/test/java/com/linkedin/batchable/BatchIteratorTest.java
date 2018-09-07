package com.linkedin.batchable;

import org.junit.Assert;
import org.junit.Test;


public class BatchIteratorTest {
  @Test
  public void skipTest() {
    BatchIterator<Integer> bi = new BatchIterator<Integer>() {
      private int _nextValue = 0;
      private final int _maxValue = 100;

      @Override
      public boolean hasNext() {
        return _nextValue <= _maxValue;
      }

      @Override
      public Integer next() {
        return _nextValue++;
      }

      @Override
      public long available() {
        return _maxValue - _nextValue + 1;
      }

      @Override
      public void close() {
        // noop
      }
    };

    for (int i = 0; i < 95; i += 5) {
      Assert.assertEquals(5, bi.skip(5));
    }

    Assert.assertEquals(95, (int) bi.next());
    Assert.assertEquals(5, bi.skip(8));
    Assert.assertFalse(bi.hasNext());
  }
}
