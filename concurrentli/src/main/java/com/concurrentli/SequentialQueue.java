/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

/**
 * Queue where items are dequeued in a fixed, sequential order, starting with item 0, then 1, then 2, etc.
 * Items are enqueued together with their index; the queue has a bounded capacity for future items, so enqueueing items
 * will not block so long as they are not too far in the future (beyond the capacity of the queue).
 *
 * For example, if we create the queue with a capacity of 10, the next item is initially 0.  If we try to insert an
 * item with index 10 at this point, the insertion will block until the first item (at index 0) is dequeued, because
 * only then will the queue have a slot to accommodate the inserted value.  Conversely, inserting a value at index 9
 * would not block, because there's already a slot available for it.
 *
 * This class is an efficient way to obtain the results of parallel computation in a predetermined order.  For example,
 * a parallel compression utility that compressed blocks of input in parallel threads could put each compressed
 * block into a {@link SequentialQueue} while another thread wrote them to an underlying stream in their proper order.
 *
 * @param <T> the type of item stored in this queue
 */
public class SequentialQueue<T> {
  private volatile long _nextIndex = 0;

  private static class QueueItem<T> {
    public RuntimeException _exception = null;
    public T _item = null;
  }

  private final QueueItem<T>[] _queue;
  private final ResettableEvent[] _filledSignals;
  private final FutureEpochEvent _freeSignal = new FutureEpochEvent(-1);

  /**
   * Creates a new SequentialQueue with the specified capacity.  Any index from nextIndex (the next item to be read,
   * initially 0) to (nextIndex + capacity - 1) may be enqueued without blocking.
   *
   * @param capacity the capacity of this sequential queue.
   */
  public SequentialQueue(int capacity) {
    _queue = new QueueItem[capacity];
    _filledSignals = new ResettableEvent[capacity];

    for (int i = 0; i < capacity; i++) {
      _queue[i] = new QueueItem<>();
      _filledSignals[i] = new ResettableEvent(false);
    }
  }

  /**
   * Checks whether the next result is immediately dequeueable.
   *
   * @return true if the next result is available and can be dequeued without blocking.
   */
  public boolean isNextAvailable() {
    int arrayIndex = (int) (_nextIndex % _queue.length);
    return _filledSignals[arrayIndex].peek();
  }

  /**
   * Removes and returns the next object from the queue, immediately if it is already available, blocking until it is
   * available otherwise.
   *
   * Note that SequentialQueue has no intrinsic notion of "end of results".  Clients can use their own signalling
   * method if needed to indicate this by, e.g. enqueueing a null value.
   *
   * Only one thread should call dequeue(...) and peek(...) at a time.  Since there is usually one "consumer" thread
   * this is typically trivial to verify, but you should otherwise provide external synchronization to avoid this.
   *
   * @return the next item in the queue, in order of index (starting with 0).
   * @throws InterruptedException if interrupted while waiting for the next item to become available
   */
  public T dequeue() throws InterruptedException {
    return dequeue(false);
  }

  private T dequeue(boolean peek) throws InterruptedException {
    long nextIndex = _nextIndex; // local copy of volatile long

    int arrayIndex = (int) (nextIndex % _queue.length);

    // wait for a value to be available
    if (peek) {
      _filledSignals[arrayIndex].getWithoutReset();
    } else {
      _filledSignals[arrayIndex].getAndReset();
    }

    QueueItem<T> queueItem = _queue[arrayIndex];
    try {
      if (queueItem._exception != null) {
        throw queueItem._exception;
      } else {
        return queueItem._item;
      }
    } finally {
      if (!peek) {
        queueItem._item = null;
        queueItem._exception = null;
        _freeSignal.set(nextIndex);
        // nextIndex needs to be updated last, because enqueue will not wait on freeSignal if it's high enough:
        _nextIndex = nextIndex + 1;
      }
    }
  }

  /**
   * Returns the next object from the queue, immediately if it is already available, blocking until it is
   * available otherwise.  The item is not removed and the queue is unchanged.
   *
   * peek() will still throw any enqueued exception, just like dequeue().
   *
   * Only one thread should call dequeue(...) and peek(...) at a time.  Since there is usually one "consumer" thread
   * this is typically trivial to verify, but you should otherwise provide external synchronization to avoid this.
   *
   * @return the next item in the queue, in order of index (starting with 0).
   * @throws InterruptedException if interrupted while waiting for the next item to become available
   */
  public T peek() throws InterruptedException {
    return dequeue(true);
  }

  /**
   * Dequeueing the item specified by index will cause dequeue() to throw the given exception.
   * This exception is only thrown when this particular item is dequeued; the SequentialQueue may continue to be used
   * after this point, if appropriate.
   *
   * If the index is more than (nextIndex + capacity - 1), this method blocks until there is space available.  Do not
   * enqueue an item at an index you have also enqueued an exception at!
   *
   * @param index the index of the item that, when dequeued, will result in an exception
   * @param exception the exception that will be thrown
   *
   * @throws InterruptedException if the enqueing operation is interrupted
   */
  public void enqueueException(long index, RuntimeException exception) throws InterruptedException {
    enqueue(index, null, exception);
  }

  /**
   * Enqueues an object at a specific index.  Objects are dequeued in sequential order of their index, starting with
   * 0.  The first item dequeued will be 0, then 1, then 2, etc.
   *
   * Returns immediately if there is capacity to enqueue the item, blocks until there is otherwise.
   *
   * Enqueue should only be called for a particular index once.  Enqueueing at a particular index more than once
   * may result in logic errors.
   *
   * @param index the non-negative index of the item being enqueued
   * @param obj the object to be enqueued; may be null
   * @throws InterruptedException if interrupted while waiting for capacity to become available
   * @throws IllegalArgumentException if the index is {@literal < 0}, or is an object that has already been dequeued
   */
  public void enqueue(long index, T obj) throws InterruptedException {
    enqueue(index, obj, null);
  }

  private void enqueue(long index, T obj, RuntimeException exception) throws InterruptedException {
    long nextIndex = _nextIndex; // get a local non-volatile copy of the volatile long

    if (index < nextIndex) {
      throw new IllegalArgumentException("Attempted to enqueue an object at an index that has already been dequeued");
    }

    int arrayIndex = (int) (index % _queue.length);

    if (index >= nextIndex + _queue.length) {
      // we're trying to write too far into the future--need to wait
      _freeSignal.get(index - _queue.length);
    }

    // write to queue
    _queue[arrayIndex]._item = obj;
    _queue[arrayIndex]._exception = exception;
    _filledSignals[arrayIndex].set();
  }
}
