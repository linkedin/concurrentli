/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * Provides a "virtual" {@link ExecutorService} implementation that is backed by another executor.  The virtual
 * executor service can be shut down without affecting the backing executor.  The backing executor is never shut down
 * by {@link VirtualExecutorService} and the client is responsible for doing so themselves where desired.
 *
 * At a high level, this class is chiefly useful when you wish to use a shared thread pool while still being able to
 * terminate (and/or await termination) of groups of threads separately.  {@link VirtualExecutorService} can also
 * replace many "real" thread pool {@link ExecutorService}s that can then be backed by a single, shared thread pool,
 * saving resources.
 */
public class VirtualExecutorService extends AbstractExecutorService {
  private final Executor _executor;

  private static final int STAGE_NORMAL = 0;
  private static final int STAGE_SHUTDOWN = 1;
  private static final int STAGE_SHUTDOWN_NOW = 2;

  private int _stage = STAGE_NORMAL;

  private final Object _pendingLock = new Object();
  private final LinkedNode<Runnable> _pendingRunnables = new LinkedNode<>();
  private final LinkedNode<Thread> _pendingThreads = new LinkedNode<>();

  // Simple implementation of linked list with constant-time deletion
  private static final class LinkedNode<T> {
    private LinkedNode<T> _prev;
    private LinkedNode<T> _next;
    private final T _value;

    // Given a head node, collect all the values in the list
    static <T> List<T> values(LinkedNode<T> head) {
      ArrayList<T> res = new ArrayList<>();
      LinkedNode<T> cur = head._next;
      while (cur != null) {
        res.add(cur._value);
        cur = cur._next;
      }
      return res;
    }

    // Create a head node
    LinkedNode() {
      _value = null;
      _next = null;
      _prev = null;
    }

    // Create a linked node
    LinkedNode(LinkedNode<T> head, T value) {
      _value = value;

      _next = head._next;
      _prev = head;

      if (head._next != null) {
        head._next._prev = this;
      }

      head._next = this;
    }

    void delete() {
      _prev._next = _next;
      if (_next != null) {
        _next._prev = _prev;
      }
    }

    T getValue() {
      return _value;
    }
  }

  public VirtualExecutorService(Executor executor) {
    _executor = executor;
  }

  @Override
  public void shutdown() {
    synchronized (_pendingLock) {
      if (_stage == STAGE_NORMAL) {
        _stage = STAGE_SHUTDOWN;
      }
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    synchronized (_pendingLock) {
      _stage = STAGE_SHUTDOWN_NOW;

      for (Thread pending : LinkedNode.values(_pendingThreads)) {
        pending.interrupt();
      }

      return LinkedNode.values(_pendingRunnables);
    }
  }

  @Override
  public boolean isShutdown() {
    synchronized (_pendingLock) {
      return _stage >= STAGE_SHUTDOWN;
    }
  }

  @Override
  public boolean isTerminated() {
    synchronized (_pendingLock) {
      return _stage >= STAGE_SHUTDOWN // are we shutting down?
          && _pendingThreads._next == null // no actively running tasks?
          // and are future tasks done or set to be discarded?
          && (_stage == STAGE_SHUTDOWN_NOW || _pendingRunnables._next == null);
    }
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long endTime = System.currentTimeMillis() + unit.toMillis(timeout);

    synchronized (_pendingLock) {
      long timeRemaining = endTime - System.currentTimeMillis();
      while (timeRemaining >= 0 && !isTerminated()) {
        _pendingLock.wait(timeRemaining + 1);
        timeRemaining = endTime - System.currentTimeMillis();
      }
    }

    return isTerminated();
  }

  @Override
  public void execute(Runnable command) {
    final LinkedNode<Runnable> runnableNode;

    synchronized (_pendingLock) {
      if (_stage >= STAGE_SHUTDOWN) {
        throw new RejectedExecutionException();
      }
      runnableNode = new LinkedNode<>(_pendingRunnables, command); // register our runnable in the pending list
    }

    try {
      _executor.execute(() -> {
        LinkedNode<Thread> threadNode = null;
        try {
          synchronized (_pendingLock) {
            runnableNode.delete(); // we're now "running", so remove ourself from the pending runnable list

            if (_stage >= STAGE_SHUTDOWN_NOW) { // are we shutting down now?
              return;
            }

            // register our thread on the pending threads list
            threadNode = new LinkedNode<>(_pendingThreads, Thread.currentThread());
          }

          // run the runnable
          command.run();
        } finally {
          // if we registered our thread, delete the registration node and notify any blocked awaitTerminate() calls
          if (threadNode != null) {
            synchronized (_pendingLock) {
              threadNode.delete();
              _pendingLock.notifyAll();
            }
          }
        }
      });
    } catch (RejectedExecutionException e) {
      // if something goes wrong while scheduling, delete our runnable node
      synchronized (_pendingLock) {
        runnableNode.delete();
      }
      throw e;
    }
  }
}
