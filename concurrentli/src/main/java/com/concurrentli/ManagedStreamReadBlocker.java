package com.concurrentli;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.ForkJoinPool;


/**
 * A {@link ForkJoinPool.ManagedBlocker} for reading a specified number of bytes from an input stream into a byte array.
 *
 * The blocker uses {@link InputStream#available()} to determine if bytes can be read without blocking.  If all the
 * requested bytes can be read without blocking, it will do so.  Otherwise, the thread will block to wait for the
 * required bytes to become available.
 *
 * @author Jeff Pasternack
 */
public class ManagedStreamReadBlocker implements ForkJoinPool.ManagedBlocker {
  private int _offset;
  private int _endOffset;
  private int _bytesToRead; // used for bookkeeping; not modified during reading
  private boolean _finished; // e.g. EOF

  private final byte[] _buffer;
  private final InputStream _inputStream;

  /**
   * Convenience method that creates the blocker, executes it against the current thread's ForkJoinPool, and returns
   * the number of bytes read.
   *
   * @param inputStream the stream from which to read
   * @param buffer the buffer that will store the read bytes
   * @param bufferOffset the offset at which bytes should be stored in the buffer
   * @param bytesToRead the number of bytes to read; fewer bytes will be written if the end of the stream is reached
   * @return the number of bytes actually read (may be less than the number requested if end-of-stream is reached)
   * @throws InterruptedException if the thread is interrupted while blocking
   */
  public static int read(InputStream inputStream, byte[] buffer, int bufferOffset, int bytesToRead)
      throws InterruptedException {
    ManagedStreamReadBlocker blocker = new ManagedStreamReadBlocker(inputStream, buffer, bufferOffset, bytesToRead);
    ForkJoinPool.managedBlock(blocker);
    return blocker.getReadCount();
  }

  /**
   * Convenience method that creates the blocker, executes it against the current thread's ForkJoinPool, and returns
   * the number of bytes read.  Bytes are copied into the buffer starting at offset 0.
   *
   * @param inputStream the stream from which to read
   * @param buffer the buffer that will store the read bytes
   * @param bytesToRead the number of bytes to read; fewer bytes will be written if the end of the stream is reached
   * @return the number of bytes actually read (may be less than the number requested if end-of-stream is reached)
   * @throws InterruptedException if the thread is interrupted while blocking
   */
  public static int read(InputStream inputStream, byte[] buffer, int bytesToRead)
      throws InterruptedException {
    return read(inputStream, buffer, 0, bytesToRead);
  }

  /**
   * @return the buffer in which the read bytes are stored
   */
  public byte[] getBuffer() {
    return _buffer;
  }

  /**
   * @return the number of bytes actually read (may be less than that requested if the end of stream is reached or this
   *         blocker has not yet run)
   */
  public int getReadCount() {
    return _offset - (_endOffset - _bytesToRead);
  }

  /**
   * Creates a new blocker that will read from the specified stream into the given buffer.
   *
   * @param inputStream the stream from which to read
   * @param buffer the buffer that will store the read bytes
   * @param bufferOffset the offset in the buffer at which storage of the read bytes will begin
   * @param bytesToRead the number of bytes to read; fewer bytes will be written if the end of the stream is reached
   */
  public ManagedStreamReadBlocker(InputStream inputStream, byte[] buffer, int bufferOffset, int bytesToRead) {
    _inputStream = inputStream;
    _buffer = buffer;
    reinitialize(bufferOffset, bytesToRead);
  }

  /**
   * Reinitializes this instance so that it may be reused if desired.  This should only be done after its previous
   * blocking has completed; the blocker <strong>must not</strong> be reused by a thread while it is in the middle of
   * blocking another thread.
   *
   * @param bufferOffset the offset in the buffer at which storage of the read bytes will begin
   * @param bytesToRead the number of bytes to read; fewer bytes will be written if the end of the stream is reached
   */
  public void reinitialize(int bufferOffset, int bytesToRead) {
    _offset = bufferOffset;
    _endOffset = bufferOffset + bytesToRead;
    _bytesToRead = bytesToRead;
    _finished = false;
  }

  @Override
  public boolean block() {
    if (!_finished) {
      try {
        int read;
        while ((read = _inputStream.read(_buffer, _offset, _endOffset - _offset)) > 0) {
          _offset += read;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      _finished = true;
    }
    return true;
  }

  @Override
  public boolean isReleasable() {
    // try to read without blocking
    try {
      int available;
      while (!_finished && (available = _inputStream.available()) > 0) {
        int read = _inputStream.read(_buffer, _offset, Math.min(available, _endOffset - _offset));
        if (read < 0) {
          _finished = true;
        } else {
          _offset += read;
          _finished = (_offset == _endOffset);
        }
      }
      return _finished;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
