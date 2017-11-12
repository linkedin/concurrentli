/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * There are cases where a block of code must execute exclusively, but additional (exclusive) executions are harmless
 * and the thread running it is immaterial.  {@link ExclusiveIdempotentMethod} handles this situation by wrapping an
 * idempotent (extra executions are harmless) method and protecting its exclusivity in a lock-less, no-waiting
 * fashion.
 *
 * (1) If the current thread wants to execute the method and it is not currently running, that thread starts running the
 *     method.
 * (2) If another thread is already executing the method, that thread will run it at least one more time, and the
 *     current thread will immediately return.
 *
 * {@link ExclusiveIdempotentMethod} is particularly useful in lock-free concurrent programming when a method must run
 * exclusively to update shared data structures (e.g. in response to some event).  For example, if multiple threads are
 * writing elements to a circular buffer, after each write we can check to see if the first element is non-null and, if
 * so, perform work on the elements in sequential index order, using a method like:
 *
 * while((elem = circBuff.get(circBuff.getFirstElementIndex()) != null) {
 *   circBuff.advance();
 *   consumeSequentialElement(elem);
 * }
 *
 * If this method is wrapped by this class and tryRun() is invoked after every write to the circular buffer, correct
 * behavior (calling consumeSequentialElement on elements in sequentially increasing order of index) is guaranteed.
 * However, calling the method directly would result in incorrect behavior, since multiple threads could be executing
 * the loop at once.
 */
public class ExclusiveIdempotentMethod {
  private final AtomicInteger _pending = new AtomicInteger(0);
  private final Runnable _runnable;

  /**
   * Wraps a idempotent method for exclusive execution
   *
   * @param runnable the method that will be called exclusively
   */
  public ExclusiveIdempotentMethod(Runnable runnable) {
    _runnable = runnable;
  }

  /**
   * Attempts to run the wrapped method.  One of two things will happen:
   * (1) No other thread is currently running the wrapped method, in which case the current thread will run the
   *     method at least once, then return true.
   * (2) Another thread is already running the wrapped method, in which case that thread will run it at least once more.
   *     The current thread immediately returns false;
   *
   * @return true if the current thread executed the wrapped method, false otherwise
   */
  public boolean tryRun() {
    if (_pending.getAndIncrement() == 0) { // increment by 1; if previously 0, we now have the "lock"
      while (true) {
        _runnable.run();
        int pending = _pending.decrementAndGet();
        if (pending == 0) {
          // if we decrement and see 0, nobody else has called tryRun() since we started, and we can quit
          return true;
        } else if (pending > 1) {
          // set _pending to 1 because multiple tryRun() calls do not "stack"
          _pending.set(1);
        }
      }
    }

    // another thread is running; we've incremented _pending, so they'll execute the method on our behalf;
    // we can simply return
    return false;
  }
}
