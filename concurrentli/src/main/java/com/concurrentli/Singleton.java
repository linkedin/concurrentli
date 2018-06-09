/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import java.io.Serializable;
import java.util.function.Supplier;


/**
 * Provides a singleton using efficient, thread-safe double-checked locking.
 *
 * Use of Singleton can be much faster than a traditional double-checked locking singleton implementation using
 * a volatile, although it's worth noting that a volatile read on x86 is no more expensive than a normal read from
 * memory except that many optimizations are disallowed; the performance advantage of Singleton is thus dependent
 * on Java being able to take advantage of optimizations like "lifting" the read of the singleton out of a loop.
 *
 * Nonetheless, the best way to implement a singleton is through the use of a final field or an enum.  Use this class
 * only when neither of these is an option.
 *
 * To use, create a derived class and implement the getValue() method that returns a new singleton instance
 * when needed.
 *
 * To avoid the cost of the Singleton object, you may also call the static getFullyConstructed(...) method
 * directly.
 *
 * Note that, in practice, naive (and incorrect) use of double-checked locking to create singletons is likely
 * to still work, particularly on x86 machines (which have strong memory ordering guarantees).  This makes
 * it hard, as a pratical matter, to empirically test an implementation for correctness.
 *
 * Adapted from https://en.wikipedia.org/wiki/Double-checked_locking#Usage_in_Java
 *
 * @param <T> the type of object provided as a singleton.
 */
public abstract class Singleton<T> implements Supplier<T>, Serializable {
  private static final long serialVersionUID = 1;

  /**
   * Uses a constructor with a final field to ensure that (in effect) a StoreStore memory barrier is enforced
   * before returning a reference to the passed argument.  So long as the passed object has not leaked previously,
   * any subsequent reads of the returned reference will see the "full" object and not, e.g. something partially
   * constructed.  This is effectively a no-op on x86 due to its already strong memory ordering guarantees.
   *
   * Note that writes and reads of references are always atomic in Java.
   *
   * A double-checked lock singleton pattern can thus look like:
   * public ClassName get() {
   *   ClassName s = _singleton; // need temp variable because reads can be reordered
   *   if (s != null) {
   *     return s; // guarantee to point to complete object, if not null
   *   }
   *
   *   synchronized (this) {
   *     if (_singleton == null) {
   *       _singleton = Singleton.getFullyConstructed(new ClassName());
   *     }
   *     return _singleton;
   *   }
   * }
   *
   * @param obj the object you wish to be completely visible to all threads
   * @param <T> the type of the object
   * @return a reference to the object that will appear as a correct and complete instance to other threads
   */
  public static <T> T getFullyConstructed(T obj) {
    return new FinalWrapper<>(obj).value;
  }

  private static final class FinalWrapper<T> {
    final T value;
    FinalWrapper(T value) {
      this.value = value;
    }
  }

  private transient T _obj = null;

  protected abstract T getValue();

  public final T get() {
    // read from memory
    T temp = _obj;
    if (temp != null) {
      return temp;
    }

    synchronized (this) { // synchronize to avoid calling supplier twice
      if (_obj == null) { // is _obj still null?  Maybe someone calculated it for us already.
        _obj = getFullyConstructed(getValue()); // create _obj, write to memory
      }
      return _obj;
    }
  }

  /**
   * Clears the value stored in the singleton.  Other threads may continue seeing the old value until they
   * synchronize with the calling thread (i.e. establish a happens-before relationship).
   */
  public void clear() {
    synchronized (this) {
      _obj = null;
    }
  }
}
