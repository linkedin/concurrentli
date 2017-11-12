/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import com.concurrentli.Singleton;
import org.junit.Test;


public class SingletonTester {
  private static long[] _arr = new long[1000];

  public interface I {
    public String get();

    public void setZ(int z);
    public int getZ();
  }

  public static class A implements I {
    private int _z;

    public void setZ(int z) {
      _z = z;
    }

    public int getZ() { return _z; }

    public A(int z) {
      _z = z;
    }

    Singleton<String> _s = new Singleton<String>() {
      @Override
      protected String getValue() {
        return Integer.toString(_z);
      }
    };

    public String get() {
      return _s.get();
    }
  }

  public static class B implements I {
    private int _z;

    public B(int z) {
      _z = z;
    }

    public void setZ(int z) {
      _z = z;
    }

    public int getZ() { return _z; }

    volatile String _s;

    public String get() {
      String t = _s;
      if (t != null) {
        return t;
      }

      synchronized (this) {
        t = _s;
        if (t == null) {
          _s = t = Integer.toString(_z);
        }
      }

      return t;
    }
  }

  private void doBasic(I obj) {
    for (long i = 0; i < 1000000000; i++) {
      obj.setZ(obj.get().length());
    }
  }

  private void testBasic(I obj) throws InterruptedException {
    long start = System.nanoTime();

    Thread a = new Thread(() -> doBasic(obj));
    Thread b = new Thread(() -> doBasic(obj));
    Thread c = new Thread(() -> doBasic(obj));
    Thread d = new Thread(() -> doBasic(obj));
    a.start();
    b.start();
    c.start();
    d.start();

    a.join();
    b.join();
    c.join();
    d.join();

    System.out.println((System.nanoTime() - start)/1000000000.0);
  }

  @Test
  public void test1() throws InterruptedException {
    testBasic(new A(4));
  }

  @Test
  public void test2() throws InterruptedException {
    testBasic(new B(4));
  }
}
