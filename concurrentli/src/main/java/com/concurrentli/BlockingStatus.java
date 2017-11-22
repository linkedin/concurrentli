/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

/**
 * Represents whether an operation will "block" or not.
 *
 * Block, as used here, is a subjective term and used as a performance hint as to whether it may be preferable to invoke
 * the operation now or wait until later.
 *
 * Quickly acquiring a low-contention mutex or synchronized data structure would not be considering blocking (despite a
 * small chance of the thread being parked), but a potentially expensive read from an underlying stream would be.
 */
public enum BlockingStatus {
  /**
   * The operation will not to block.
   */
  NOT_BLOCKING(0),

  /**
   * The operation will block, and waiting will not change this: blocking is inevitable.
   */
  ALWAYS_BLOCKING(2),

  /**
   * The operation would block if invoked now, but will become NOT_BLOCKING eventually.
   */
  TEMPORARILY_BLOCKING(1);

  private final int _level;
  BlockingStatus(int level) {
    _level = level;
  }

  /**
   * Given zero or more statuses, determines the "maximum" blocking status, that is, the one that is most
   * severe, according to the ordering {@literal NOT_BLOCKING < TEMPORARILY_BLOCKING < ALWAYS_BLOCKING}.
   *
   * @param statii the statuses whose maximum will be found
   * @return the "maximum" blocking status
   */
  public static BlockingStatus max(BlockingStatus... statii) {
    if (statii.length == 0) {
      return NOT_BLOCKING;
    }

    BlockingStatus res = statii[0];
    for (BlockingStatus status : statii) {
      if (status._level > res._level) {
        res = status;
      }
    }

    return res;
  }
}
