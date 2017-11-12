# Concurrentli

Concurrentli (com.concurrentli) is a collection of classes for multithreading that have proven useful at LinkedIn that expand
on java.util.concurrent, adding convenience, efficiency and new tools to multithreaded Java programs.

## New Locks, Semaphores and Signals
- FutureEpochEvent and ImminentEpochEvent allow the caller to wait for a particular epoch (a long) or higher to be set.
- ResettableEvent is inspired by C#'s AutoResetEvent and ManualResetEvent classes, providing a simple way to wait for
an event to be "set" with or without automatic resetting of the event.
- SemaphoreMap associates semaphores with keys, storing only those semaphores that currently have available permits or
waiting threads.

## New Queues and Buffers
- SequentialQueue assigns queued items a sequentially-increasing index the corresponds with the order they will be
dequeued while allowing for random-access enqueing of future items.
- UnsafeCircularIntegerBuffer and UnsafeCircularReferenceBuffer are high-performance, thread-safe circular (ring)
buffers.

## ParallelProcessor
ParallelProcessor simplifies a common concurrency scenario where a single-threaded reader thread produces inputs (e.g.
by reading a stream) which are then processed by many threads.  ParallelProcessor provides an abstract class that
handles the multithreading, synchronization, and returning of processed elements in the order in which they were read;
all client code needs to do is implement the nextInput() and processInput() methods.

## Efficiency
- AtomicWriteOnceReference provides cheap, lock- and synchronization-free (but still thread-safe) access to a value that
is set only once (the naive alternative, reading directly from a volatile, requires a memory fence on each read).
- Singleton similarly provides cheap, lock- and synchronization-free (but still thread-safe) access to a value that is
generated only once using a provided getValue() method.  This is a cheaper alternative to standard double-checked
locking (correct implementations of which will read a volatile on each access).
- ExclusiveIdempotentMethod provides a nonblocking, lock-free way to ensure that exactly one thread is exclusively
running a particular method when tryRun() is called.

## Convenience
- Interrupted is a utility class that wraps methods throwing InterruptedException, either ignoring the error
(appropriate for the top-level method in a thread, which will thus terminate when the method returns) or re-throwing it
as an unchecked UncheckedInterruptedException.

## BSD 2-CLAUSE LICENSE
Copyright 2017 LinkedIn Corporation
All Rights Reserved.
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.