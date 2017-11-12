/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

/**
 * Represents an {@link InterruptedException} that has been caught and rethrown as a runtime (unchecked) exception.
 */
public class UncheckedInterruptedException extends RuntimeException {
  /**
   * Creates a new instance, wrapped the provided {@link InterruptedException}
   *
   * @param e the {@link InterruptedException} being wrapped
   */
  public UncheckedInterruptedException(InterruptedException e) {
    super(e);
  }

  @Override
  public InterruptedException getCause() {
    return (InterruptedException) super.getCause();
  }
}
