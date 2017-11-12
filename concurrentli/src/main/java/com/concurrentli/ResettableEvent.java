/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

/**
 * Inspired by C#'s AutoResetEvent and ManualResetEvent, this class is an easier-to-use variant of the kind of
 * inter-thread signalling achieved with object.wait() and object.notifyAll() or Conditions.
 *
 * ResettableEvent has two states: set and unset.  If "set" before any threads are waiting via "get", the next "get"
 * call immediately returns.  Otherwise, all threads currently waiting are activated when "set".
 */
public class ResettableEvent {
  private final Object _synchronizer = new Object();
  private int _setEpoch;
  private int _getEpoch = 0;

  /**
   * Creates a new ResettableEvent with the specified initial state, set or unset.
   * If set, the first call to get() will immediately succeed.
   *
   * @param initiallySet whether or not the object will initially be in the "set" state
   */
  public ResettableEvent(boolean initiallySet) {
    _setEpoch = initiallySet ? 1 : 0;
  }

  /**
   * Sets the state of this ResettableEvent.  If any threads are currently waiting on get(), they will be awoken.
   * Otherwise, the next call to get() will immediately succeed.
   *
   * ResettableEvents are only reset once the first waiting thread is awoken (or when the next call to get() occurs, if
   * no threads are waiting when set() is called).  Before this happens, subsequent calls to set() have no effect.
   */
  public void set() {
    synchronized (_synchronizer) {
      _setEpoch = _getEpoch + 1;
      _synchronizer.notifyAll();
    }
  }

  /**
   * Makes this instance "unset" until set() is next called.  While unset, calls to get() will block.
   */
  public void reset() {
    synchronized (_synchronizer) {
      _getEpoch = _setEpoch;
    }
  }

  /**
   * Returns true if a call to get() would return immediately (the state is "set");
   * false otherwise (state is "unset").
   *
   * Note that a peek() == true followed by a get() is NOT guaranteed not to block, as the state may change between
   * the two calls.
   *
   * @return the state of object, true if "set", false if "unset"
   */
  public boolean peek() {
    synchronized (_synchronizer) {
      return _getEpoch != _setEpoch;
    }
  }

  /**
   * If this ResettableEvent is currently "set", returns immediately.
   * Otherwise, waits until set() is called on this instance.
   *
   * The ResettableEvent will be "reset" before returning.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public void getAndReset() throws InterruptedException {
    get(true);
  }

  /**
   * If this ResettableEvent is currently "set", returns immediately.
   * Otherwise, waits until set() is called on this instance.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public void getWithoutReset() throws InterruptedException {
    get(false);
  }

  /**
   * If this ResettableEvent is currently "set", returns immediately.
   * Otherwise, waits until set() is called on this instance.
   *
   * Before returning, the ResettableEvent is "reset" if the reset flag is true; it is unaffected otherwise.
   *
   * @param reset true if the ResettableEvent should be reset before returning; false otherwise.  Passing false is a way
   *              to wait until the instance is set without resetting it.
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  private void get(boolean reset) throws InterruptedException {
    synchronized (_synchronizer) {
      int myEpoch = _getEpoch;
      while (myEpoch == _setEpoch) {
        _synchronizer.wait();
      }

      // we can, at most, increment the _getEpoch; if it's already advanced further by the time we woke up no update
      // should be performed
      if (reset && _getEpoch == myEpoch) {
        _getEpoch++;
      }
    }
  }
}
