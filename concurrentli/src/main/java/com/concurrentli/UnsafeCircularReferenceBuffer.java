/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;


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
 * not safe to advance a {@link UnsafeCircularReferenceBuffer} while simultaneously accessing its entries, unless it is
 * otherwise externally ensured that, while advancing from index i {@literal ->} i + 1, no access to element i or i + 1
 * occurs.
 *
 * Unlike a SequentialQueue, unsafe circular buffers offer random access to a particular horizon from the current
 * starting offset, never block, and require more care to use safely.
 */
public class UnsafeCircularReferenceBuffer<T> implements Serializable {
  private static final long serialVersionUID = 20170901;

  private final AtomicReferenceArray<T> _data;
  private final AtomicLong _firstElementIndex = new AtomicLong(0);
  private final Supplier<T> _initialValueSupplier;

  private static class ConstantSupplier<T> implements Supplier<T>, Serializable {
    private final T _obj;

    ConstantSupplier(T obj) {
      _obj = obj;
    }

    @Override
    public T get() {
      return _obj;
    }
  }

  /**
   * Creates a new circular buffer with the specified capacity and initial value.
   * Any element whose value has not been explicitly set will have the initial value.
   *
   * @param capacity the length of the circular buffer
   * @param initialValue the initial value assumed by all elements
   */
  public UnsafeCircularReferenceBuffer(int capacity, T initialValue) {
    this(capacity, new ConstantSupplier<>(initialValue));
  }

  /**
   * Creates a new circular buffer with the specified capacity and initial value supplier.
   * Note that initial values returned *must* be logically equivalent and indistinguishable; e.g. it
   * should not matter whether an element is initialized with the value returned by the first call or
   * the fiftieth.
   *
   * Any element whose value has not been explicitly set will have an initial value by this supplier.
   *
   * @param capacity the length of the circular buffer
   * @param initialValueSupplier supplier providing an initial value for all elements
   */
  public UnsafeCircularReferenceBuffer(int capacity, Supplier<T> initialValueSupplier) {
    T[] initialValues = (T[]) new Object[capacity];
    for (int i = 0; i < initialValues.length; i++) {
      initialValues[i] = initialValueSupplier.get();
    }
    _data = new AtomicReferenceArray<T>(initialValues);
    _initialValueSupplier = initialValueSupplier;
  }

  private boolean isValid(long index) {
    assert index >= _firstElementIndex.get();
    assert index < _firstElementIndex.get() + _data.length();
    return true;
  }

  private int getDataIndex(long index) {
    return (int) (index % _data.length());
  }

  /**
   * Sets the element corresponding to the given index.  To be valid, the index must be between
   * [getFirstElementIndex()] and [getFirstElementIndex() + length() - 1], inclusive.  Violations of this
   * are checked by assertions.
   *
   * @param index the index of the element to set
   * @param value the element to set
   */
  public void set(long index, T value) {
    assert isValid(index);
    _data.set(getDataIndex(index), value);
  }

  /**
   * Gets the element corresponding to the given index and then sets a new value.  To be valid, the index must be
   * between [getFirstElementIndex()] and [getFirstElementIndex() + length() - 1], inclusive.  Violations of this
   * are checked by assertions.
   *
   * @param index the index of the element to set
   * @param value the element to set
   *
   * @return the previous value for the specified element
   */
  public T getAndSet(long index, T value) {
    assert isValid(index);
    return _data.getAndSet(getDataIndex(index), value);
  }

  /**
   * Checks the current value at a given index and, if it matches the value expected, sets a new value.
   * To be valid, the index must be between [getFirstElementIndex()] and [getFirstElementIndex() + length() - 1],
   * inclusive.  Violations of this are checked by assertions.
   *
   * @param index the index of the element to set
   * @param expected what the current value is expected to be; the update will not take place if it is
   *                 not the same object (reference-wise, not equals()).
   * @param update the element to set
   *
   * @return true if the value was set successfully, false otherwise
   */
  public boolean compareAndSet(long index, T expected, T update) {
    assert isValid(index);
    return _data.compareAndSet(getDataIndex(index), expected, update);
  }

  /**
   * Gets the element corresponding to the given index.  To be valid, the index must be between
   * [getFirstElementIndex()] and [getFirstElementIndex() + length() - 1], inclusive.  Violations of this
   * are checked by assertions.
   *
   * @param index the index of the element to retrieve
   * @return the element at the specified index
   */
  public T get(long index) {
    assert isValid(index);
    return _data.get(getDataIndex(index));
  }

  /**
   * Gets the index of the element that is current first in the buffer.
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

  /**
   * Advances the first element, forgetting the current first element and incrementing the value returned by
   * getFirstElementIndex().
   *
   * Note that attempting to write to either the old first element or the new last element (at index
   * getFirstElementIndex() + length() - 1) before this method returns is an error but may not be caught by
   * the relevant assertions.
   */
  public void advance() {
    long index = _firstElementIndex.getAndIncrement();
    _data.set((int) (index % _data.length()), _initialValueSupplier.get());
  }

  /**
   * Advances the first element, forgetting the current first element and incrementing the value returned by
   * getFirstElementIndex(), only if the current first element has the specified value.
   *
   * Note that the comparison is done with by testing if the references are equal, not equals().
   *
   * Note that attempting to write to either the old first element or the new last element (at index
   * getFirstElementIndex() + length() - 1) before this method returns is an error but may not be caught by
   * the relevant assertions.
   *
   * @param value if the first element has this value, the buffer advances
   * @return the previous first element's index if an advance() occurred, or -1 otherwise
   */
  public long advanceIfReferenceEqual(T value) {
    while (true) {
      long index = _firstElementIndex.get();
      if (!_data.compareAndSet(getDataIndex(index), value, _initialValueSupplier.get())) {
        // the values don't match, so we're done
        return -1;
      }
      // at this point, we've deleted the last-known first element's value
      // but someone else may have advanced the first element index;
      // need to check for this
      if (_firstElementIndex.compareAndSet(index, index + 1)) {
        return index;
      }
    }
  }
}
