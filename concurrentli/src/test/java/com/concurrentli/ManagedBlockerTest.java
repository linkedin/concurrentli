package com.concurrentli;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.Assert;
import org.junit.Test;


/**
 * Simple tests for managed blockers.
 */
public class ManagedBlockerTest {
  @Test
  public void test() throws InterruptedException {
    ArrayBlockingQueue<String> arrayBlockingQueue = new ArrayBlockingQueue<>(10, false, Arrays.asList("a", "b", "c"));
    Assert.assertEquals("a", ManagedDequeueBlocker.dequeue(arrayBlockingQueue));
    Assert.assertEquals("b", ManagedDequeueBlocker.dequeue(arrayBlockingQueue));

    ReentrantLock lock = new ReentrantLock();
    ManagedLockBlocker.lock(lock);
    Assert.assertTrue(lock.isHeldByCurrentThread());
    lock.unlock();

    byte[] buffer = new byte[100];
    for (byte b = 0; b < buffer.length; b++) {
      buffer[b] = b;
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
    byte[] readBuffer = new byte[10];
    Assert.assertEquals(10, ManagedStreamReadBlocker.read(bais, readBuffer, 10));
    for (byte b = 0; b < readBuffer.length; b++) {
      Assert.assertEquals(b, readBuffer[b]);
    }

    ManagedStreamReadBlocker streamReadBlocker = new ManagedStreamReadBlocker(bais, readBuffer, 0, 10);
    ForkJoinPool.managedBlock(streamReadBlocker);
    Assert.assertEquals(10, streamReadBlocker.getReadCount());
    for (byte b = 0; b < readBuffer.length; b++) {
      Assert.assertEquals(b + 10, readBuffer[b]);
    }

    streamReadBlocker.reinitialize(0, 10);
    ForkJoinPool.managedBlock(streamReadBlocker);
    Assert.assertEquals(10, streamReadBlocker.getReadCount());
    for (byte b = 0; b < readBuffer.length; b++) {
      Assert.assertEquals(b + 20, readBuffer[b]);
    }

    ManagedDequeueBlocker<String> dequeueBlocker = new ManagedDequeueBlocker<>(arrayBlockingQueue);
    ManagedLockBlocker lockBlocker = new ManagedLockBlocker(lock);
    streamReadBlocker.reinitialize(0, 10);

    ManagedMultiBlocker.block(dequeueBlocker, lockBlocker, streamReadBlocker);
    Assert.assertEquals("c", dequeueBlocker.getItem());
    Assert.assertTrue(lock.isHeldByCurrentThread());
    Assert.assertEquals(10, streamReadBlocker.getReadCount());
    for (byte b = 0; b < readBuffer.length; b++) {
      Assert.assertEquals(b + 30, readBuffer[b]);
    }

    // attempt to read past EOS
    Assert.assertEquals(60, ManagedStreamReadBlocker.read(bais, new byte[100], 100));
  }
}
