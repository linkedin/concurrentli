package com.concurrentli;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests the ObjectPool class.
 */
public class ObjectPoolTest {
  @Test
  public void test() {
    ForkJoinPool threadPool = ForkJoinPool.commonPool();
    ObjectPool<String> pool = new ObjectPool<>(0, 2, () -> "abc");

    ObjectPool<java.lang.String>.Entry entry1 = pool.tryGet();
    ObjectPool<java.lang.String>.Entry entry2 = pool.tryGet();

    Assert.assertNotNull(entry1);
    Assert.assertNotNull(entry2);

    ForkJoinTask<?> task1 = threadPool.submit(() -> {
      Assert.assertEquals("abc", entry1.get());
      entry1.close();
    });

    ForkJoinTask<?> task2 = threadPool.submit(() -> {
      entry2.close("def");
    });

    // note that these joins create a memory barrier, so the pool will certainly have the returned items as seen by
    // this main thread
    task1.join();
    task2.join();

    ObjectPool<java.lang.String>.Entry entry3 = pool.tryGet();
    ObjectPool<java.lang.String>.Entry entry4 = pool.tryGet();

    Assert.assertNotNull(entry3);
    Assert.assertNotNull(entry4);

    Assert.assertTrue(entry3.get().equals("def") || entry4.get().equals("def"));
    Assert.assertEquals(2, pool.getItemCreationCount());

    Assert.assertNull(pool.tryGet());
    Assert.assertEquals("abc", pool.get().get());
  }
}
