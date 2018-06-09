/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;


/**
 * A circular buffer intended to store and retrieve, with random access, items with sequentially-increasing
 * indices, starting at a particular offset (initially 0).  The buffer can be advance()'ed to increase the offset
 * as reported by getFirstElementIndex(); at any given time the buffer can only store items with indices between
 * [getFirstElementIndex()] and [getFirstElementIndex() + capacity - 1], inclusive.
 *
 * E.g. a buffer of size 3 might store items with indices 0, 1, and 2, and, after advancing, contain only items
 * with indices 1 and 2.
 *
 * This class is "unsafe" because it is partly dependent upon the caller to ensure correct behavior.  Asserts are used
 * to check against out-of-bounds access, but when asserts are disabled there are no such checks.  Additionally, it is
 * not safe to advance a {@link UnsafeCircularIntegerBuffer} while simultaneously accessing its entries, unless it is
 * otherwise externally ensured that, while advancing from index i {@literal ->} i + 1, no access to element i or i + 1
 * occurs.
 *
 * Unlike a SequentialQueue, unsafe circular buffers offer random access to a particular horizon from the current
 * starting offset, never block, and require more care to use safely.
 */
public class UnsafeCircularIntegerBuffer implements Serializable {
  private static final long serialVersionUID = 20170901;

  private final AtomicLongArray _data;
  private final AtomicLong _firstElementIndex = new AtomicLong(0);
  private final int _initialValue;

  private static long getElement(long index, int value) {
    return (index << 32) | value;
  }

  private static int getIndex32(long element) {
    return (int) (element >>> 32);
  }

  private static int getValue(long element) {
    return (int) element;
  }

  private int getDataIndex(long index) {
    return (int) (index % _data.length());
  }

  /**
   * Creates a new circular buffer with the specified capacity and initial value.
   * Any element whose value has not been explicitly set will have the initial value.
   *
   * @param capacity the length of the circular buffer
   * @param initialValue the initial value assumed by all elements
   */
  public UnsafeCircularIntegerBuffer(int capacity, int initialValue) {
    long[] initialValues = new long[capacity];

    for (int i = 0; i < capacity; i++) {
      initialValues[i] = getElement(i, initialValue);
    }

    _data = new AtomicLongArray(initialValues);
    _initialValue = initialValue;
  }

  /**
   * Sets the element corresponding to the given index.  To be valid, the index must be between
   * [getFirstElementIndex()] and [getFirstElementIndex() + length() - 1], inclusive.  Violations of this
   * are checked by assertions.
   *
   * @param index the index of the element to set
   * @param value the element to set
   */
  public void set(long index, int value) {
    assert index >= _firstElementIndex.get();
    assert index < _firstElementIndex.get() + _data.length();

    assert getIndex32(_data.get(getDataIndex(index))) == (int) index;

    _data.set(getDataIndex(index), getElement(index, value));
  }

  /**
   * Sets the element corresponding to the given index if it currently has the expected value.  To be valid, the index
   * must be between [getFirstElementIndex()] and [getFirstElementIndex() + length() - 1], inclusive.  Violations of
   * this result in the update failing (and returning false)
   *
   * @param index the index of the element to set
   * @param expected the value that is expected to be present; if it is not, the update is not performed
   * @param update the new value
   * @return true if the update was performed, false otherwise
   */
  public boolean compareAndSet(long index, int expected, int update) {
    assert getIndex32(_data.get(getDataIndex(index))) == (int) index;

    return _data.compareAndSet(getDataIndex(index), getElement(index, expected), getElement(index, update));
  }

  /**
   * Gets the element corresponding to the given index.  To be valid, the index must be between
   * [getFirstElementIndex()] and [getFirstElementIndex() + length() - 1], inclusive.  Violations of this
   * are checked by assertions.
   *
   * @param index the index of the element to retrieve
   * @return the element at the specified index
   */
  public int get(long index) {
    assert index >= _firstElementIndex.get();
    assert index < _firstElementIndex.get() + _data.length();

    long val = _data.get(getDataIndex(index));
    assert getIndex32(val) == (int) index : "Trying to access " + index + " but recorded index is " + getIndex32(val);

    return getValue(val);
  }

  /**
   * Adds to an element at the given index and returns the old value.  To be valid, the index must be between
   * [getFirstElementIndex()] and [getFirstElementIndex() + length() - 1], inclusive.  Violations of this
   * are checked by assertions.
   *
   * @param index the index of the element to retrieve
   * @param delta the amount to add
   * @return the previous value at the specified index
   */
  public int getAndAdd(long index, int delta) {
    assert index >= _firstElementIndex.get();
    assert index < _firstElementIndex.get() + _data.length();

    long val;
    do {
      val = _data.get(getDataIndex(index));
      assert getIndex32(val) == (int) index : "Trying to access " + index + " but recorded index is " + getIndex32(val);
    } while (!_data.compareAndSet(getDataIndex(index), val, getElement(index, getValue(val) + delta)));

    return getValue(val);
  }

  /**
   * Gets the index of the element that is currently first in the buffer.
   *
   * @return the current index
   */
  public long getFirstElementIndex() {
    return _firstElementIndex.get();
  }

  /**
   * Gets the length of the buffer
   *
   * @return the length of the buffer
   */
  public int length() {
    return _data.length();
  }

  private boolean resetIfOutdated(long elementIndex) {
    assert elementIndex >= 0;

    long val;
    do {
      val = _data.get(getDataIndex(elementIndex));
      // if current first index is no more than Integer.MAX_VALUE ahead of the element's index, we conclude
      // that this element is outdated and should be reset
      if (Integer.compareUnsigned(((int) elementIndex) - getIndex32(val), Integer.MAX_VALUE) >= 0) {
        // the current value is not outdated
        return false;
      }
    } while (!_data.compareAndSet(getDataIndex(elementIndex), val, getElement(elementIndex, _initialValue)));

    return true;
  }

  /**
   * Advances the first element, forgetting the current first element and incrementing the value returned by
   * getFirstElementIndex().
   *
   * Note that attempting to write to either the old first element or the new last element (at index
   * getFirstElementIndex() + length() - 1) before this method returns is an error but may not be caught by
   * the relevant assertions.
   *
   * @return the previous first element's index
   */
  public long advance() {
    long index = _firstElementIndex.getAndIncrement();
    resetIfOutdated(index + _data.length());
    return index;
  }

  /**
   * Advances the first element, forgetting the current first element and incrementing the value returned by
   * getFirstElementIndex(), only if the first element's index is currently at the specified value.
   *
   * Note that attempting to write to either the old first element or the new last element (at index
   * getFirstElementIndex() + length() - 1) before this method returns is an error but may not be caught by
   * the relevant assertions.
   *
   * @param index if the first element's index is this value, the buffer advances
   * @return true if the buffer advanced, false otherwise
   */
  public boolean compareAndAdvance(long index) {
    if (_firstElementIndex.compareAndSet(index, index + 1)) {
      resetIfOutdated(index + _data.length());
      return true;
    }
    return false;
  }

  /**
   * Advances the first element, forgetting the current first element and incrementing the value returned by
   * getFirstElementIndex(), only if the current first element has the specified value.
   *
   * Note that attempting to write to either the old first element or the new last element (at index
   * getFirstElementIndex() + length() - 1) before this method returns is an error but may not be caught by
   * the relevant assertions.
   *
   * @param value if the first element has this value, the buffer advances
   * @return the previous first element's index if an advance() occurred, or -1 otherwise
   */
  public long advanceIfEqual(int value) {
    while (true) {
      long index = _firstElementIndex.get();
      long val = _data.get(getDataIndex(index));
      if (getIndex32(val) != (int) index) {
        // the stored element is not in sync with what its index is supposed to be!
        continue; // refresh our values and try again
      }

      if (getValue(val) != value) {
        // the values don't match, so we're done
        return -1;
      }

      if (_firstElementIndex.compareAndSet(index, index + 1)) {
        resetIfOutdated(index + _data.length());
        return index;
      }
    }
  }
}
