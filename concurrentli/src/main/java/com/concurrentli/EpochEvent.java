/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

/**
 * Allows threads to wait for an "epoch" to occur.  Once an epoch is set(), all threads
 * waiting for that epoch or a lower-valued ("earlier") epoch will be awoken.  A thread attempting to await an epoch
 * that has already occurred will return immediately.
 *
 * Epochs are monotonically increasing; they never go down and start at 0 by default; this rule avoids
 * race conditions where two threads try to set different epochs, as the ultimate result will be the same as if only
 * the set(...) call with the higher epoch value was made.
 */
public interface EpochEvent {
  /**
   * Sets the epoch, if higher than the current epoch.  Otherwise, this is a no-op.
   * Any threads waiting for an epoch equal or lower to this one will be awoken.
   *
   * If multiple calls to set(...) are made in parallel, the effect is the same as if only the highest-epoch-value call
   * had been made.
   *
   * @param epoch the epoch to set
   */
  void set(long epoch);

  /**
   * If the epoch is less than or equal to the requested epoch, returns immediately.
   *
   * Otherwise, blocks until this epoch occurs.
   *
   * @param epoch the epoch to wait for
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  void get(long epoch) throws InterruptedException;
}
