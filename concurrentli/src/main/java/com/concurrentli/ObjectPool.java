package com.concurrentli;

import java.util.Objects;
import java.util.function.Supplier;


/**
 * An {@link ObjectPool} is a means to reuse a pool of objects without the use of locks or other memory barriers (such
 * as atomics or volatiles) by essentially hoping for advantageous data races (although "losing" this bet carries with
 * it only a performance penalty so long as the class is properly used).  The overall performance of an ObjectPool--and
 * whether it's worthwhile at all versus, say, a {@link java.util.concurrent.LinkedBlockingQueue}--depends on:
 * (1) The search cost within the pool array to find an available item.  The search strategy is "examine the
 *     least-recently checked-out pool entry first", and then all subsequent items in the pool.  Larger pools with
 *     higher utilization rates (fewer non-checked-out items at any given time) will have higher search costs.
 * (2) How often we "win" the data race in the form of updates propagating between threads.  This may happen "naturally"
 *     on certain hardware, occur as a result of the OS scheduler invalidating a thread's cache, or as a result of
 *     a happens-before relationship occurring between the threads for some external reason (e.g. a shared semaphore).
 * (3) How expensive items are to create.  ObjectPools are best used when the items are expensive enough that you'd
 *     prefer to reuse them, but not so expensive that it's worth using memory barriers to avoid creating new ones when
 *     the pool is "unlucky" and loses a data race.
 *
 * Use of the ObjectPool class and its methods are not thread-safe, although an {@link Entry} may be passed to another
 * thread, and its methods called, without memory barriers or other synchronization.  The typical usage is a "main"
 * thread retrieving an {@link Entry} containing an item from the pool via {@link #get()} or {@link #tryGet()} and then
 * passing it to a "worker" thread, which then eventually calls {@link Entry#close()} to return the item.
 *
 * It is possible for modification to mutable pool items in one thread to incompletely carry over to another thread
 * later using the same item.  If the item has a {@code reset()} method or similar capable of restoring it to a valid
 * state, this may be used before each use of the pool item in the worker thread; otherwise, call
 * {@link Entry#close(Object)} on the modified item before returning its entry to the pool (this ensures all changes
 * will be fully visible to other threads).
 *
 * @param <T> the type of element in the pool
 */
public class ObjectPool<T> {
  /**
   * An Entry from an {@link ObjectPool} that will return to the pool when the {@link #close()} method is called.
   */
  public class Entry implements AutoCloseable {
    private final T _item;
    private final int _index; // it is possible (and harmless) for multiple entries with the same index to coexist

    private Entry(int index, T item) {
      _item = item;
      _index = index;
    }

    /**
     * @return the item from the pool associated with this entry
     */
    public T get() {
      return _item;
    }

    /**
     * Replaces this entry's item in the pool with something different.  Should also be used it the item has been
     * mutated while checked out, unless it is either impossible or irrelevant if another views it in a
     * partially-updated state.
     *
     * The replacement is <strong>not</strong> guaranteed to be retrievable by the pool in the future, and may simply
     * be discarded.
     *
     * @param replacement the replacement to place in the pool
     */
    public void close(T replacement) {
      _pool[_index] = new Entry(_index, replacement);
    }

    /**
     * Puts back this entry in the pool from whence it came.
     */
    @Override
    public void close() {
      _pool[_index] = this;
    }
  }

  private int _nextIndex = 0; // the next index to check
  private int _size; // current size of the pool
  private long _extraneousCreationCount = 0; // [how many times we've called _objectSupplier] - [_size]
  private final Entry[] _pool; // entry pool
  private final Supplier<? extends T> _objectSupplier; // provides new pool items on demand

  /**
   * Creates a new {@link ObjectPool}.
   *
   * @param initialSize the initial size of the pool (the number of items that will be immediately created)
   * @param maxSize the maximum size of the pool
   * @param objectSupplier supplies new objects to fill the pool as needed
   */
  @SuppressWarnings("unchecked") // generic array creation is safe as array never escapes this instance
  public ObjectPool(int initialSize, int maxSize, Supplier<? extends T> objectSupplier) {
    if (maxSize < 1) {
      throw new IllegalArgumentException("Maximum size must be at least 1");
    } else if (initialSize > maxSize) {
      throw new IllegalArgumentException("Initial size cannot exceed max size");
    }

    _pool = (Entry[]) new ObjectPool.Entry[maxSize];
    _objectSupplier = Objects.requireNonNull(objectSupplier);
    _size = initialSize;
    for (int i = 0; i < initialSize; i++) {
      _pool[i] = new Entry(i, objectSupplier.get());
    }
  }

  private Entry getAndClear(int index) {
    assert _pool[index] != null;

    Entry result = _pool[index];
    _pool[index] = null;
    return result;
  }

  /**
   * Gets an entry from the pool if available or, if not, returns null.
   *
   * If the pool is below its maximum size and an entry is not otherwise available, a new entry will be added to the
   * pool and returned.
   *
   * Due to the pool's reliance on data races, this method may fail to return an entry even if all previously
   * checked-out entries have been returned on other threads.
   *
   * @return an {@link Entry} containing a pooled item
   */
  public Entry tryGet() {
    int startIndex = _nextIndex;

    for (; _nextIndex < _size; _nextIndex++) {
      if (_pool[_nextIndex] != null) {
        return getAndClear(_nextIndex);
      }
    }
    for (_nextIndex = 0; _nextIndex < startIndex; _nextIndex++) {
      if (_pool[_nextIndex] != null) {
        return getAndClear(_nextIndex);
      }
    }

    // item is not among currently available items; is there enough space to add a new entry?
    if (_size < _pool.length) {
      return new Entry(_size++, _objectSupplier.get());
    }

    return null;
  }

  /**
   * Gets an entry from the pool if available or, if not, creates and returns a new one.
   *
   * @return an {@link Entry} containing a pooled item
   */
  public Entry get() {
    Entry result = tryGet();
    if (result != null) {
      return result;
    }

    // just create a new entry
    _extraneousCreationCount++;
    int entryIndex = _nextIndex;
    _nextIndex = (_nextIndex + 1) % _size;
    return new Entry(entryIndex, _objectSupplier.get());
  }

  /**
   * @return the total number of times the object supplier has been invoked to create new item instances; this is mostly
   *         useful for debugging and performance tuning
   */
  public long getItemCreationCount() {
    return _size + _extraneousCreationCount;
  }
}
