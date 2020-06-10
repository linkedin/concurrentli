package com.concurrentli;

import java.util.concurrent.ForkJoinPool;


/**
 * Implements a {@link ForkJoinPool.ManagedBlocker} for dequeuing items from a {@link SequentialQueue}.
 *
 * @author Jeff Pasternack
 * @param <T> the type of item to dequeue
 */
public class ManagedSequentialDequeueBlocker<T> implements ForkJoinPool.ManagedBlocker {
  private final SequentialQueue<T> _queue;
  private T _item; // will be retrieved by the worker thread; dequeued item may be null
  private boolean _hasItem = false; // distinguish when we've successfully dequeued an item

  /**
   * Convenience method that creates and executes the blocker on the current thread, returning the result.
   *
   * @param queue the queue whose next item should be retrieved
   * @param <T> the type of item in the queue
   * @return the dequeued item
   * @throws InterruptedException if the thread is interrupted while blocking
   */
  public static <T> T dequeue(SequentialQueue<T> queue) throws InterruptedException {
    ManagedSequentialDequeueBlocker<T> blocker = new ManagedSequentialDequeueBlocker<>(queue);
    ForkJoinPool.managedBlock(blocker);
    return blocker.getItem();
  }

  /**
   * Creates a new blocker that will retrieve the next item from a blocking queue
   * @param queue the queue from which the item will be retrieved
   */
  public ManagedSequentialDequeueBlocker(SequentialQueue<T> queue) {
    _queue = queue;
  }

  /**
   * @return the dequeued item, if this blocker has been executed, or null otherwise
   */
  public T getItem() {
    return _item;
  }

  /**
   * @return true if the blocker has executed and an item retrieved from the queue, false otherwise
   */
  public boolean hasItem() {
    return _hasItem;
  }

  @Override
  public boolean block() throws InterruptedException {
    if (!_hasItem) {
      _item = _queue.dequeue();
      _hasItem = true;
    }
    return true;
  }

  @Override
  public boolean isReleasable() {
    if (!_hasItem) {
      if (_queue.isNextAvailable()) {
        try {
          _item = _queue.dequeue();
          _hasItem = true;
        } catch (InterruptedException e) {
          throw new UncheckedInterruptedException(e); // should never happen: dequeue() won't block
        }
      }
    }
    return _hasItem;
  }
}
