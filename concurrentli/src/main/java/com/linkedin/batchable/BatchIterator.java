/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.batchable;

import java.util.Iterator;

/**
 * An interface for an iterator that supports batched operations, potentially retrieving many items in a single call
 * rather than getting them one at a time.
 *
 * @param <T> the type of object iterated over
 */
public interface BatchIterator<T> extends Iterator<T>, AutoCloseable {
  @Override
  boolean hasNext();

  /*
   * Reference, but inefficient implementation for next() if you choose to implement the
   * next(T[], int, int) method below:
   *
   * T next() {
   *   T[] arr = (T[]) new Object[1];
   *   if (next(arr, 0, 1) == 0) {
   *     throw new NoSuchElementException();
   *   }
   *   return arr[0];
   * }
   */


  /**
   * Obtains the next sequence of items.
   *
   * If fewer than count items can be returned without blocking, only that number should be obtained.  However, if
   * the iterator is not exhausted, at least one item should be copied, even if blocking is required.
   *
   * The default implementation calls next() and hasNext() and can thus be relatively inefficient in many applications.
   * Classes are encouraged to override this method with an efficient implementation instead.
   *
   * @param destination the array to which the next items will be copied
   * @param offset the offset in the array at which copying begins
   * @param count the maximum number of items to copy
   *
   * @return the actual number of items copied, or 0 if the iterator is exhausted (hasNext() == false)
   */
  default int next(T[] destination, int offset, int count) {
    int copied = 0;
    while (hasNext() && copied < count) {
      destination[offset + copied] = next();
      copied++;
    }

    return copied;
  }

  /**
   * Obtains the next sequence of items.
   *
   * If fewer than destination.length items can be returned without blocking, only that number should be obtained.
   * However, if the iterator is not exhausted, at least one item should be copied, even if blocking is required.
   *
   * @param destination the array to which the next items will be copied
   *
   * @return the actual number of items copied, or 0 if the iterator is exhausted (hasNext() == false)
   */
  default int next(T[] destination) {
    return next(destination, 0, destination.length);
  }

  /**
   * Skips the next element.
   *
   * @return true if an item was skipped, false if the iterator was already at its end (hasNext() == false).
   */
  default boolean skip() {
    return skip(1) > 0;
  }

  /**
   * Skips the given number of elements.
   *
   * If the requested number of elements exceeds the number remaining, skips to the end of the iterator (such that
   * hasNext() == false).
   *
   * Skipping elements may be faster than reading them with next().  The default implementation simply calls next()
   * repeatedly.
   *
   * @param toSkip the number of elements to skip
   * @return the number of elements skipped; if fewer than toSkip items remain in the iterator, the number of items
   *         remaining in the iterator is returned.
   */
  default long skip(long toSkip) {
    for (long i = 0; i < toSkip; i++) {
      if (!hasNext()) {
        return i;
      }
      next();
    }
    return toSkip;
  }

  /**
   * At least this many items can be read without blocking with high probability (e.g. acquiring a rarely-contested lock
   * would not qualify as "blocking" because the thread would only very rarely need to park).
   *
   * Note that this is a more strict definition than that for the similar InputStream method of the same name,
   * as the return value is strictly a lower-bound, not simply an "estimate".
   *
   * If any size read could or would block, 0 should be returned.  0 will also be returned if the end of stream has been
   * reached.
   *
   * @return the number of items that can (with certainty) be returned without blocking.
   */
  long available();

  /**
   * "Closes" this iterator and releases its resources.  Further operations on the iterator after being
   * closed are undefined.
   */
  @Override
  void close();
}
