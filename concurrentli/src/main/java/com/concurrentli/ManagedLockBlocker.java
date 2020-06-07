package com.concurrentli;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.Lock;


/**
 * A {@link java.util.concurrent.ForkJoinPool.ManagedBlocker} for obtaining a lock.
 *
 * @author Jeff Pasternack
 */
public class ManagedLockBlocker implements ForkJoinPool.ManagedBlocker {
  private final Lock _lock;
  private boolean _hasLock;

  /**
   * Convenience method that acquires the lock on the current thread.
   *
   * @param lock the lock to acquire
   * @throws InterruptedException if the thread is interrupted while blocking
   */
  public static void lock(Lock lock) throws InterruptedException {
    ForkJoinPool.managedBlock(new ManagedLockBlocker(lock));
  }

  /**
   * Creates a new blocker that will block to acquire the provided lock.
   *
   * @param lock the lock on which to block
   */
  public ManagedLockBlocker(Lock lock) {
    _lock = lock;
  }

  @Override
  public boolean block() throws InterruptedException {
    if (!_hasLock) {
      _lock.lock();
      _hasLock = true;
    }
    return true; // we have the lock and don't need to block again
  }

  @Override
  public boolean isReleasable() {
    return _hasLock || (_hasLock = _lock.tryLock());
  }
}
