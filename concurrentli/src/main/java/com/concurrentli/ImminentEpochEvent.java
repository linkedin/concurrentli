/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Allows threads to wait for an "epoch" to occur.  Once an epoch is set(), all threads
 * waiting for that epoch or a lower-valued ("earlier") epoch will be awoken.  A thread attempting to await an epoch
 * that has already occurred will return immediately.
 *
 * Epochs are monotonically increasing; they never go down and start at 0 by default; this rule avoids
 * race conditions where two threads try to set different epochs, as the ultimate result will be the same as if only
 * the set(...) call with the higher epoch value was made.
 *
 * {@link ImminentEpochEvent} is intended to be used when the epochs being awaited are in the near future, within a
 * known, fixed horizon, and fairly contiguous distribution (e.g. waiting for 3, 4, 5, 6, not 4, 19, 30).
 * {@link FutureEpochEvent} may be more efficient if you regularly await epochs farther in the future.
 */
public class ImminentEpochEvent implements EpochEvent {
  private final ReentrantLock[] _locks;
  private final Condition[] _conditions;
  private final AtomicLong _epoch;

  /**
   * Creates a new instance, which will begin at epoch 0 and efficiently accommodate waiting for epochs up to
   * (current epoch) + (horizon).
   *
   * @param horizon how far in the future an epoch can be waited for without sacrificing efficiency.
   */
  public ImminentEpochEvent(int horizon) {
    this(horizon, 0);
  }

  /**
   * Creates a new instance, which will begin at the specified epoch and efficiently accommodate waiting for epochs up
   * to (current epoch) + (horizon).
   *
   * @param horizon how far in the future an epoch can be waited for without sacrificing efficiency.
   * @param epoch the initial epoch; must be at least -1
   */
  public ImminentEpochEvent(int horizon, long epoch) {
    if (epoch < -1) {
      throw new IndexOutOfBoundsException("Initial epoch cannot be less than -1");
    }

    _locks = new ReentrantLock[horizon];
    _conditions = new Condition[horizon];
    _epoch = new AtomicLong(epoch);

    for (int i = 0; i < _locks.length; i++) {
      _locks[i] = new ReentrantLock();
      _conditions[i] = _locks[i].newCondition();
    }
  }

  /**
   * Sets the epoch, if higher than the current epoch.  Otherwise, this is a no-op.
   * Any threads waiting for an epoch equal or lower to this one will be awoken.
   *
   * If multiple calls to set(...) are made in parallel, the effect is the same as if only the highest-epoch-value call
   * had been made.
   */
  public void set(long epoch) {
    long currentEpoch = _epoch.get();

    if (epoch <= currentEpoch) {
      return;
    }

    while (!_epoch.compareAndSet(currentEpoch, epoch)) {
      currentEpoch = _epoch.get();

      if (epoch <= currentEpoch) {
        return;
      }
    }

    int count = (int) Math.min(epoch - currentEpoch, _locks.length);

    for (int i = 1; i <= count; i++) {
      int index = (int) ((currentEpoch + i) % _locks.length);
      _locks[index].lock();
      _conditions[index].signalAll();
      _locks[index].unlock();
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
    if (epoch <= _epoch.get()) {
      return;
    }

    int index = (int) (epoch % _locks.length);
    ReentrantLock sync = _locks[index];
    sync.lock();
    while (epoch > _epoch.get()) {
      _conditions[index].await();
    }
    sync.unlock();
  }
}
