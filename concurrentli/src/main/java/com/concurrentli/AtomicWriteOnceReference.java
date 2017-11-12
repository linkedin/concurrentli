/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;


/**
 * An atomic reference, initially null, that is written only once.
 *
 * The advantage over an AtomicReference or volatile is that, once the reference is set, reads become
 * synchronization-free.
 *
 * @param <T> the type of object being referenced.
 */
public class AtomicWriteOnceReference<T> implements Serializable {
  private transient T _cachedObj = null;
  private volatile T _obj = null;

  private static final AtomicReferenceFieldUpdater<AtomicWriteOnceReference, Object> UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(AtomicWriteOnceReference.class, Object.class, "_obj");

  /**
   * Gets the referenced value, or null if it has not yet been set.
   * If the value has been set, the second read (or earlier) in any given thread is synchronization free.
   *
   * @return the referenced value
   */
  public T get() {
    if (_cachedObj != null) {
      return _cachedObj;
    }

    _cachedObj = _obj;
    return _cachedObj;
  }

  /**
   * Sets the value.  Throws an IllegalStateException if a value has already been set.
   *
   * @param value the value to set.  Setting a null will result in a thrown exception.
   */
  public void set(T value) {
    if (!trySet(value)) {
      throw new IllegalStateException("Value has already been set");
    }
  }

  /**
   * Set the value if no value has previously been set.
   *
   * @param value the value to set.  Setting a null is effectively a no-op.
   * @return true if the set succeeded, false otherwise.
   */
  public boolean trySet(T value) {
    if (get() != null) {
      return false;
    }

    // we use getFullyConstructed to avoid the case where:
    // (1) Thread A sets the value
    // (2) Thread B reads the value and sets _cachedObj (A's set happens-before B due to the volatile)
    // (3) Thread C reads _cachedObj, but the value has not been fully written to memory from its POV (there's no
    //     happens-before with A or B in this case)
    // By using getFullyConstructed, we ensure that the value being fully written to memory happens-before the reference
    // in _obj is written, and hence before any possible read of _cacheObj.  _cacheObj will thus either be null or have
    // a reference to a fully-constructed value.
    if (!UPDATER.compareAndSet(this, null, Singleton.getFullyConstructed(value))) {
      return false;
    }

    _cachedObj = value;
    return true;
  }
}
