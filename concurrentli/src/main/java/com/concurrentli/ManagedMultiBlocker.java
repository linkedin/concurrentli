package com.concurrentli;

import java.util.concurrent.ForkJoinPool;


/**
 * A {@link java.util.concurrent.ForkJoinPool.ManagedBlocker} that bundles multiple other
 * {@link java.util.concurrent.ForkJoinPool.ManagedBlocker} into a single blocker; this may reduce the overhead relative
 * to blocking on them individually.
 *
 * @author Jeff Pasternack
 */
public class ManagedMultiBlocker implements ForkJoinPool.ManagedBlocker {
  private final ForkJoinPool.ManagedBlocker[] _blockers;

  /**
   * Convenience method that blocks on multiple {@link java.util.concurrent.ForkJoinPool.ManagedBlocker}s
   * simultaneously.   Blockers are executed in the order in which they are provided.
   *
   * @param blockers the blockers on which to block en masse
   * @throws InterruptedException if the thread is interrupted while blocking
   */
  public static void block(ForkJoinPool.ManagedBlocker... blockers)
      throws InterruptedException {
    ForkJoinPool.managedBlock(new ManagedMultiBlocker(blockers));
  }

  /**
   * Creates a new blocker that will block on multiple blocker instances in one go.  Blockers are executed in the order
   * in which they are provided.
   *
   * @param blockers the blockers on which to block en masse
   */
  public ManagedMultiBlocker(ForkJoinPool.ManagedBlocker... blockers) {
    _blockers = blockers;
  }

  @Override
  public boolean block() throws InterruptedException {
    for (ForkJoinPool.ManagedBlocker blocker : _blockers) {
      if (!blocker.block()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isReleasable() {
    for (ForkJoinPool.ManagedBlocker blocker : _blockers) {
      if (!blocker.isReleasable()) {
        return false;
      }
    }
    return true;
  }
}
