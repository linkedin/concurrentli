/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

/**
 * Utility methods for creating unchecked Runnables from lambdas throwing InterruptedException
 */
public final class Interrupted {
  @FunctionalInterface
  public interface InterruptableRunnable<T, R> {
    void run() throws InterruptedException;
  }

  private Interrupted() { }

  /**
   * Given an interruptable method, returns an unchecked Runnable.
   * If an interruption does occur, the thread is re-interrupted and an UncheckedInterruptedException is thrown.
   *
   * @param r the InterruptableRunnable throwing a checked InterruptedException
   * @return a Runnable that does not throw a checked exception
   */
  public static Runnable unchecked(InterruptableRunnable r) {
    return () -> {
      try {
        r.run();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UncheckedInterruptedException(e);
      }
    };
  }

  /**
   * Given an interruptable method, returns an unchecked Runnable.
   * If an interruption does occur, no exception is thrown and the Runnable immediately returns.
   * The thread's state is not (re-)set to interrupted.
   *
   * @param r the InterruptableRunnable throwing a checked InterruptedException
   * @return a Runnable that does not throw a checked exception, instead returning if one is thrown
   */
  public static Runnable ignored(InterruptableRunnable r) {
    return () -> {
      try {
        r.run();
      } catch (InterruptedException e) {
        // do nothing
      }
    };
  }
}
