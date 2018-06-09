/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Provides semaphores associated with a particular key; i.e. a thread subscribes to a key and will acquire the
 * semaphore when that key is ready/released.  Multiple threads may wait on a single key.
 *
 * Semaphores with no permits and no threads waiting on them are automatically removed from the map.
 *
 * @param <K> the type of the key
 */
public class SemaphoreMap<K> {
  private static class QualifiedSemaphore extends Semaphore {
    AtomicInteger pending = new AtomicInteger(0);

    QualifiedSemaphore(int permits) {
      super(permits);
    }
  }

  private ConcurrentHashMap<K, QualifiedSemaphore> _semaphoreMap = new ConcurrentHashMap<>();

  /**
   * Acquires the semaphore for a given key.
   *
   * @param key the key whose semaphore will be acquired
   * @throws InterruptedException if interrupted while waiting to acquire the semaphore
   */
  public void acquire(K key) throws InterruptedException {
    QualifiedSemaphore waitObj = _semaphoreMap.compute(key, (t, v) -> {
      if (v == null) {
        v = new QualifiedSemaphore(0);
      }
      v.pending.incrementAndGet();
      return v;
    });

    waitObj.acquire();
    int pending = waitObj.pending.decrementAndGet();

    if (pending == 0 && waitObj.availablePermits() == 0) {
      _semaphoreMap.computeIfPresent(key, (t, s) -> {
        if (s.pending.intValue() == 0 && waitObj.availablePermits() == 0) {
          return null;
        }
        return s;
      });
    }
  }

  /**
   * Releases one permit for the given key.
   *
   * @param key the key to release a permit for
   */
  public void release(K key) {
    _semaphoreMap.compute(key, (t, s) -> {
      if (s == null) {
        s = new QualifiedSemaphore(1);
      } else {
        s.release();
      }
      return s;
    });
  }

  /**
   * Returns the number of keys either being waited for or with permits available.  This is *not* the total number of
   * permits available.
   *
   * This value may, of course, be stale and no longer accurate by the time it is read due to other concurrent usage
   * of the class.
   *
   * @return the number of keys
   */
  public int keyCount() {
    return _semaphoreMap.size();
  }
}
