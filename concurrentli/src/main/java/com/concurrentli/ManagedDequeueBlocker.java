package com.concurrentli;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;


/**
 * Implements a {@link java.util.concurrent.ForkJoinPool.ManagedBlocker} for dequeuing items from a blocking queue.
 *
 * @author Jeff Pasternack
 * @param <T> the type of item to dequeue
 */
public class ManagedDequeueBlocker<T> implements ForkJoinPool.ManagedBlocker {
  private final BlockingQueue<T> _queue;
  private T _item; // will be retrieved by the worker thread

  /**
   * Convenience method that creates and executes the blocker on the current thread, returning the result.
   *
   * @param queue the queue whose next item should be retrieved
   * @param <T> the type of item in the queue
   * @return the dequeued item
   * @throws InterruptedException if the thread is interrupted while blocking
   */
  public static <T> T dequeue(BlockingQueue<T> queue) throws InterruptedException {
    ManagedDequeueBlocker<T> blocker = new ManagedDequeueBlocker<>(queue);
    ForkJoinPool.managedBlock(blocker);
    return blocker.getItem();
  }

  /**
   * Creates a new blocker that will retrieve the next item from a blocking queue
   * @param queue the queue from which the item will be retrieved
   */
  public ManagedDequeueBlocker(BlockingQueue<T> queue) {
    _queue = queue;
  }

  /**
   * @return the dequeued item, if this blocker has been executed, or null otherwise
   */
  public T getItem() {
    return _item;
  }

  @Override
  public boolean block() throws InterruptedException {
    if (_item == null) {
      _item = _queue.take();
    }
    return true; // we don't need to block again
  }

  @Override
  public boolean isReleasable() {
    return (_item != null || ((_item = _queue.poll()) != null));
  }
}
