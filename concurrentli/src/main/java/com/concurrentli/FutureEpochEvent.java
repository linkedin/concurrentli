/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import java.util.PriorityQueue;


/**
 * Allows threads to wait for an "epoch" to occur.  Once an epoch is set(), all threads
 * waiting for that epoch or a lower-valued ("earlier") epoch will be awoken.  A thread attempting to await an epoch
 * that has already occurred will return immediately.
 *
 * Epochs are monotonically increasing; they never go down and start at 0 by default; this rule avoids
 * race conditions where two threads try to set different epochs, as the ultimate result will be the same as if only
 * the set(...) call with the higher epoch value was made.
 *
 * {@link FutureEpochEvent} is intended to be used when the epochs being awaited are sparse and far in the future.
 * If you typically wait for epochs that will occur in the near future, {@link ImminentEpochEvent} may be more
 * efficient.
 */
public class FutureEpochEvent implements EpochEvent {
  private final Object _synchronizer = new Object();
  private long _epoch = 0;
  private final PriorityQueue<Long> _queue = new PriorityQueue<>();

  /**
   * Creates a new instance, which will begin at epoch 0.
   */
  public FutureEpochEvent() {
    this(0);
  }

  /**
   * Creates a new instance, which will begin at the specified epoch.
   *
   * @param epoch the initial epoch
   */
  public FutureEpochEvent(long epoch) {
    _epoch = epoch;
  }

  /**
   * Sets the epoch, if higher than the current epoch.  Otherwise, this is a no-op.
   * Any threads waiting for an epoch equal or lower to this one will be awoken.
   *
   * If multiple calls to set(...) are made in parallel, the effect is the same as if only the highest-epoch-value call
   * had been made.
   */
  public void set(long epoch) {
    synchronized (_synchronizer) {
      if (epoch > _epoch) {
        _epoch = epoch;
        Long i;
        // CHECKSTYLE:OFF
        while ((i = _queue.peek()) != null && i <= epoch) {
        // CHECKSTYLE:ON
          _queue.poll();
          synchronized (i) {
            i.notifyAll();
          }
        }
      }
    }
  }

  /**
   * If the epoch is less than or equal to the requested epoch, returns immediately.
   *
   * Otherwise, blocks until this epoch occurs.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public void get(long epoch) throws InterruptedException {
    Long epochObj;
    synchronized (_synchronizer) {
      if (epoch <= _epoch) {
        return;
      }

      epochObj = new Long(epoch);
      _queue.add(epochObj);
    }

    synchronized (epochObj) {
      while (epoch > _epoch) {
        epochObj.wait();
      }
    }
  }
}
