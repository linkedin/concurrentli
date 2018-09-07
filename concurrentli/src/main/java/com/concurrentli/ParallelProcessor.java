/*
 * Copyright 2017 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.concurrentli;

import com.linkedin.batchable.BatchIterator;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;


/**
 * Represents a common multithreaded processing scenario wherein there is one, single-threaded "reader" that provides
 * tasks, and multiple processors that run the tasks.  The results are then placed into a SequentialQueue for access
 * in the order they were read.  For efficiency, tasks may be optionally batched, reducing lock contention on the queues
 * and reducing thread thrashing (since a thread may work on many tasks before having to access a guarded resource).
 */
public abstract class ParallelProcessor<T, R> implements AutoCloseable, BatchIterator<R> {
  /**
   * Gets the next input.  nextInput() is guaranteed to be called by only a single thread, and does not need to be
   * thread-safe.  Returns null when no more inputs are available.
   *
   * The order of inputs returned by nextInput() is the same order that results will be provided.
   *
   * @return the next input to be processed, or null if there are no more to be processed.
   */
  protected abstract T nextInput();

  /**
   * Fills an array with the next batch of inputs (starting at offset 0) and returns the number of inputs that were
   * stored.  Obtains at least one input unless there are no more, in which case 0 is returned.
   *
   * Subclasses may optionally override this method for the sake of efficiency (e.g. to do a System.arrayCopy); the
   * default implementation simply calls nextInput() repeatedly.
   *
   * @param inputs the target array that will be filled with inputs
   * @return the number of inputs read, or 0 if there are no more inputs.
   */
  protected int readInputs(T[] inputs) {
    for (int i = 0; i < inputs.length; i++) {
      T next = nextInput();
      if (next == null) {
        return i;
      }
      inputs[i] = next;
    }
    return inputs.length;
  }

  /**
   * Process an input and obtain a result.  This method will be called in parallel and must be thread-safe.
   *
   * @param input the input to be processed
   * @return the result of processing the input
   */
  protected abstract R processInput(T input);

  /**
   * Processes an array of inputs in-place, replacing each input with its result.
   *
   * This method is protected to allow for subclasses to override it for efficient batch processing (e.g. when
   * serializing objects, serializing lists of objects as larger "subgraphs" reduces the serialization of redundant
   * shared data vs. serializing them one-at-a-time).
   *
   * However, it is extremely important to verify that, unless this method throws an exception, the first (length)
   * elements in the data array are really of type T when it returns.  There are no compile or run-time checks for this,
   * and heap pollution will result otherwise.  This is why this method is "unsafe".
   *
   * The reason for this relative lack of safety is improved efficiency: input values immediately become un-referenced
   * as they are replaced by results, and only one array is required rather than two.
   *
   * @param length the number of inputs, starting at offset 0, in the data[] array.
   * @param data stored the inputs that are then overwritten with the results
   */
  protected void processInputsUnsafe(int length, Object[] data) {
    for (int i = 0; i < length; i++) {
      data[i] = processInput((T) data[i]);
    }
  }

  /**
   * Used by subclasses to configure the ParallelProcessor superclass; the configured {@link Config} object
   * is passed to ParallelProcessor's constructor.
   */
  protected static class Config {
    /**
     * Create a new Config instance.
     */
    public Config() { }

    public static final int DEFAUT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_BATCH_SIZE = 1000;

    private int _readerThreadPriority = Thread.NORM_PRIORITY;
    private int _processorThreads = DEFAUT_THREAD_COUNT;
    private int _processorThreadPriority = Thread.NORM_PRIORITY;
    private int _batchSize = DEFAULT_BATCH_SIZE;
    private int _inputBufferBatchCount = DEFAUT_THREAD_COUNT * 2;
    private int _outputBufferBatchCount = DEFAUT_THREAD_COUNT * 2;

    /**
     * Sets the priority of the reader thread that obtains inputs.  A higher-than-normal reader thread priority
     * or a lower-than-normal processor thread priority may help avoid "input starvation" where processor threads
     * block while waiting for further inputs.
     *
     * @param readerThreadPriority the priority to assign, e.g. Thread.NORM_PRIORITY
     * @return this instance
     */
    public Config setReaderThreadPriority(int readerThreadPriority) {
      this._readerThreadPriority = readerThreadPriority;
      return this;
    }

    /**
     * Sets the number of threads that will be used to process inputs.  The default is the number of logical processors
     * available to the JVM (usually the number on the physical machine).
     *
     * @param processorThreads the number of threads that will be used
     * @return this instance
     */
    public Config setProcessorThreads(int processorThreads) {
      this._processorThreads = processorThreads;
      return this;
    }

    /**
     * Sets the priority of the processor thread that transforms inputs.  A higher-than-normal reader thread priority
     * or a lower-than-normal processor thread priority may help avoid "input starvation" where processor threads
     * block while waiting for further inputs.
     *
     * @param processorThreadPriority the priority to assign, e.g. Thread.NORM_PRIORITY
     * @return this instance
     */
    public Config setProcessorThreadPriority(int processorThreadPriority) {
      this._processorThreadPriority = processorThreadPriority;
      return this;
    }

    /**
     * Sets the number of inputs that will be read (and then processed) in a single batch.
     * Synchronization costs are essentially fixed per batch, so larger batches reduce the amortized synchronization
     * cost per item at the cost of increased memory consumption.  For larger, more expensive-to-process inputs, a
     * smaller batch size should be preferred, as the synchronization cost is likely immaterial.
     *
     * The default value is 1000.
     *
     * @param batchSize the new batch size
     * @return this instance
     */
    public Config setBatchSize(int batchSize) {
      this._batchSize = batchSize;
      return this;
    }

    /**
     * Sets the input buffer batch count.  This is the maximum number of batches of inputs that can be
     * held in memory while awaiting processing.
     *
     * The default value is twice the number of logical processors available to the JVM.
     *
     * @param inputBufferBatchCount the number of batches of input to buffer
     * @return this instance
     */
    public Config setInputBufferBatchCount(int inputBufferBatchCount) {
      this._inputBufferBatchCount = inputBufferBatchCount;
      return this;
    }

    /**
     * Sets the output buffer batch count.  This is the maximum number of batches of outputs that can be held in memory
     * while awaiting consumption via the next() method.
     *
     * The default value is twice the number of logical processors available to the JVM.
     *
     * @param outputBufferBatchCount the number of batches of output to buffer
     * @return this instance
     */
    public Config setOutputBufferBatchCount(int outputBufferBatchCount) {
      this._outputBufferBatchCount = outputBufferBatchCount;
      return this;
    }
  }

  /**
   * Wraps an exception that occurred while calling nextInput().
   */
  public static class NextInputException extends RuntimeException {
    public NextInputException(String message, RuntimeException cause) {
      super(message, cause);
    }
  }

  /**
   * Wraps an exception that occured while calling processInput().
   */
  public static class ProcessInputException extends RuntimeException {
    public ProcessInputException(String message, RuntimeException cause) {
      super(message, cause);
    }
  }

  private static class InputResultBatch<T, R> {
    // index of this batch of results, relative to other batches
    long _batchIndex = -1; // represents invalid index

    int _length = 0;
    private final Object[] _data;

    InputResultBatch(int capacity) {
      _data = new Object[capacity];
    }
  }

  private final ArrayBlockingQueue<InputResultBatch<T, R>> _inputQueue;
  private final SequentialQueue<InputResultBatch<T, R>> _resultQueue;

  private InputResultBatch<T, R> _currentResult = null;
  private int _currentResultIndex = 0;

  private final Thread _readerThread;
  private final Thread[] _processorThreads;
  private NextInputException _nextInputException = null;
  private boolean _started = false;
  private boolean _endOfResults = false;

  private final InputResultBatch<T, R>[] _batchPool;

  /**
   * Creates a new ParallelProcessor with the specified Config
   *
   * @param config the configuration to use
   */
  public ParallelProcessor(Config config) {
    this(config._readerThreadPriority, config._processorThreads, config._processorThreadPriority, config._batchSize,
        config._inputBufferBatchCount, config._outputBufferBatchCount);
  }

  /**
   * Creates a new ParallelProcessor.
   *
   * Please note that the maximum number of items that may be in-memory at once is:
   * (_inputBufferBatchCount + _outputBufferBatchCount + _processorThreads + 2) * _batchSize
   *
   * @param readerThreadPriority the priority given to the reader thread
   * @param processorThreads the number of threads that will be used for processing
   * @param processorThreadPriority thread given to processor threads
   * @param batchSize how many inputs will be read and processed at a time (i.e. without accessing blocking queues)
   * @param inputBufferSize the maximum number of input batches that can be queued for processing
   * @param outputBufferSize the maximum number of output batches that can be queued for reading
   */
  private ParallelProcessor(int readerThreadPriority, int processorThreads, int processorThreadPriority,
      int batchSize, int inputBufferSize, int outputBufferSize) {

    _inputQueue = new ArrayBlockingQueue<>(inputBufferSize);
    _resultQueue = new SequentialQueue<>(outputBufferSize);

    _readerThread = new Thread(Interrupted.ignored(this::readerThread));
    _readerThread.setDaemon(true);
    _readerThread.setPriority(readerThreadPriority);

    // allocate enough buffers for "worst case":
    // (1) input queue is full, with one batch read and pending entry into queue
    // (2) output queue is full, with (_processorThreads) batches processed and pending entry into queue
    // (3) current batch being returned to client via nextInterruptable() et al.
    _batchPool = new InputResultBatch[inputBufferSize + outputBufferSize + processorThreads + 2];
    for (int i = 0; i < _batchPool.length; i++) {
      _batchPool[i] = new InputResultBatch<>(batchSize);
    }

    _processorThreads = new Thread[processorThreads];
    for (int i = 0; i < processorThreads; i++) {
      _processorThreads[i] = new Thread(Interrupted.ignored(this::processorThread));
      _processorThreads[i].setDaemon(true);
      _processorThreads[i].setPriority(processorThreadPriority);
    }
  }

  private boolean ensureBuffer() throws InterruptedException {
    start();

    if (_nextInputException != null) {
      throw _nextInputException;
    }

    if (_endOfResults) {
      return false;
    }

    if (_currentResult == null || _currentResult._length == _currentResultIndex) {
      try {
        // possibly throws if an error occurred reading input for or processing this batch:
        _currentResult = _resultQueue.dequeue();
        _currentResultIndex = 0;
      } catch (NextInputException e) {
        _nextInputException = e;
        throw e;
      }
    }

    if (_currentResult == null) {
      _endOfResults = true;
      return false;
    }

    return true;
  }

  /**
   * Gets the next result, blocking until it is ready if necessary.  Null will be returned when there are no more
   * results.
   *
   * A {@link NextInputException} will be thrown if an exception was thrown while trying to get the inputs for
   * the next batch of results.  Subsequent calls to next() will throw the same exception.
   *
   * A {@link ProcessInputException} will be thrown if an exception was thrown while trying to process the next batch
   * of results.  Subsequent calls to next() may still be made to continue getting further results.
   *
   * @return the next InputOutput pair, or null if there are no more results.
   * @throws NextInputException if an asynchronous exception occurred in nextInput()
   * @throws ProcessInputException if an asynchronous exception occurred in processInput(...)
   * @throws InterruptedException if the operation is interrupted
   */
  public R nextInterruptable() throws InterruptedException {
    if (!ensureBuffer()) {
      return null; // end of results
    }

    R result = (R) _currentResult._data[_currentResultIndex];
    _currentResult._data[_currentResultIndex] = null; // cleanup as soon as we can
    _currentResultIndex++;

    return result;
  }

  @Override
  public boolean hasNext() {
    try {
      return ensureBuffer();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e);
    }
  }

  /**
   * Gets the next result, blocking until it is ready if necessary.  Null will be returned when there are no more
   * results rather than throwing an exception.
   *
   * A {@link NextInputException} will be thrown if an exception was thrown while trying to get the inputs for
   * the next batch of results.  Subsequent calls to next() will throw the same exception.
   *
   * A {@link ProcessInputException} will be thrown if an exception was thrown while trying to process the next batch
   * of results.  Subsequent calls to next() may still be made to continue getting further results.
   *
   * @return the next InputOutput pair, or null if there are no more results.
   * @throws NextInputException if an asynchronous exception occurred in nextInput()
   * @throws ProcessInputException if an asynchronous exception occurred in processInput(...)
   */
  @Override
  public R next() {
    try {
      return nextInterruptable();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e);
    }
  }

  @Override
  public int next(R[] destination, int offset, int count) {
    try {
      if (!ensureBuffer()) {
        return 0; // end of results
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e);
    }

    int toCopy = Math.min(count, _currentResult._length - _currentResultIndex);
    System.arraycopy(_currentResult._data, _currentResultIndex, destination, offset, toCopy);
    Arrays.fill(_currentResult._data, _currentResultIndex, _currentResultIndex + toCopy, null);

    _currentResultIndex += toCopy;

    return toCopy;
  }

  @Override
  public long skip(long count) {
    long skipped = 0;
    while (skipped < count) {
      try {
        if (!ensureBuffer()) {
          return skipped; // end of results
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UncheckedInterruptedException(e);
      }

      int toSkip = (int) Math.min(count - skipped, _currentResult._length - _currentResultIndex);
      Arrays.fill(_currentResult._data, _currentResultIndex, _currentResultIndex + toSkip, null);

      _currentResultIndex += toSkip;
      skipped += toSkip;
    }

    assert skipped == count;
    return skipped;
  }

  /**
   * Checks the {@link BlockingStatus} of this instance.
   *
   * This value will be:
   * NOT_BLOCKING if the next result is available (or the end of results has been reached)
   * TEMPORARILY_BLOCKING otherwise
   *
   * @return a BlockingStatus indicating whether the next call to next() will block.
   */
  public BlockingStatus getBlockingStatus() {
    if (_endOfResults) {
      return BlockingStatus.NOT_BLOCKING;
    }

    if (_currentResult == null || _currentResult._length == _currentResultIndex) {
      return _resultQueue.isNextAvailable() ? BlockingStatus.NOT_BLOCKING : BlockingStatus.TEMPORARILY_BLOCKING;
    }

    return BlockingStatus.NOT_BLOCKING;
  }

  @Override
  public long available() {
    if (_endOfResults) {
      return 0;
    }

    try {
      if (_currentResult == null || _currentResult._length == _currentResultIndex) {
        return _resultQueue.isNextAvailable() ? _resultQueue.peek()._length : 0;
      }
    } catch (Exception e) {
      return 0;
    }

    return _currentResult._length - _currentResultIndex;
  }

  @Override
  public void close() {
    _endOfResults = true;
    stopNow();
  }

  /**
   * Starts asynchronous processing.  It may be desirable to call this as early as possible; otherwise, start() is
   * automatically invoked the first time next() et al. is called.
   */
  public void start() {
    if (!_started) {
      _started = true;

      _readerThread.start();
      for (Thread thread : _processorThreads) {
        thread.start();
      }
    }
  }

  private void stopNow() {
    for (Thread thread : _processorThreads) {
      thread.interrupt();
    }
    _readerThread.interrupt();
  }

  private void enqueueException(long index, RuntimeException e) throws InterruptedException {
    _resultQueue.enqueueException(index, e);
  }

  private void enqueueInputTerminators(long index) throws InterruptedException {
    for (int i = 0; i < _processorThreads.length; i++) {
      InputResultBatch<T, R> batch = _batchPool[(int) (index % _batchPool.length)];
      batch._batchIndex = -1; // indicate the input is terminating
      _inputQueue.put(batch);
    }
  }

  private void readerThread() throws InterruptedException {
    long index = 0;
    try {
      while (true) {
        InputResultBatch<T, R> batch = _batchPool[(int) (index % _batchPool.length)];
        batch._batchIndex = index;
        batch._length = readInputs((T[]) batch._data);

        if (batch._length == 0) {
          break;
        }

        _inputQueue.put(batch);
        index++;
      }

      _resultQueue.enqueue(index, null);
      enqueueInputTerminators(index);
    } catch (RuntimeException e) {
      enqueueException(index, new NextInputException("An exception occurred while getting input batch " + index, e));
      enqueueInputTerminators(index);
    }
  }

  private void processorThread() throws InterruptedException {
    while (true) {
      InputResultBatch<T, R> batch = _inputQueue.take();
      try {
        if (batch._batchIndex < 0) {
          return;
        }

        processInputsUnsafe(batch._length, batch._data);

        _resultQueue.enqueue(batch._batchIndex, batch);
      } catch (RuntimeException e) {
        enqueueException(batch._batchIndex,
            new ProcessInputException("An exception occurred while processing input batch " + batch._batchIndex, e));
      }
    }
  }
}
