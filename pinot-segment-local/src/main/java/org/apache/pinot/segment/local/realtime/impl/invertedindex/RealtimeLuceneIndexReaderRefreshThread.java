/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import org.apache.lucene.search.SearcherManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Background thread to refresh the realtime lucene index readers for supporting
 * near-realtime text search. The task maintains a queue of realtime segments.
 * This queue is global (across all realtime segments of all realtime/hybrid tables).
 *
 * Each element in the queue is of type {@link RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders}.
 * It encapsulates a lock and all the realtime lucene readers for the particular realtime segment.
 * Since text index is also create on a per column basis, there will be as many realtime lucene
 * readers as the number of columns with text search enabled.
 *
 * Between each successive execution of the task, there is a fixed delay (regardless of how long
 * each execution took). When the task wakes up, it pick the RealtimeLuceneReadersForRealtimeSegment
 * from the head of queue, refresh it's readers and adds this at the tail of queue.
 */
public class RealtimeLuceneIndexReaderRefreshThread implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeLuceneIndexReaderRefreshThread.class);
  // TODO: make this configurable and choose a higher default value
  private static final int DELAY_BETWEEN_SUCCESSIVE_EXECUTION_MS_DEFAULT = 10;

  private final ConcurrentLinkedQueue<RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders> _luceneRealtimeReaders;
  private final Lock _mutex;
  private final Condition _conditionVariable;

  private volatile boolean _stopped = false;

  RealtimeLuceneIndexReaderRefreshThread(
      ConcurrentLinkedQueue<RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders> luceneRealtimeReaders, Lock mutex,
      Condition conditionVariable) {
    _luceneRealtimeReaders = luceneRealtimeReaders;
    _mutex = mutex;
    _conditionVariable = conditionVariable;
  }

  void setStopped() {
    _stopped = true;
  }

  @Override
  public void run() {
        while (!_stopped) {
          waitForSegmentReaders();
          if (_stopped) {
            break;
          }
          refreshSegmentReaders();
          sleepBetweenCycles();
        }
      }
    
      private void waitForSegmentReaders() {
        _mutex.lock();
        try {
          while (_luceneRealtimeReaders.isEmpty()) {
            _conditionVariable.await();
          }
        } catch (InterruptedException e) {
          LOGGER.warn("Realtime lucene reader refresh thread got interrupted while waiting on condition variable: ", e);
          Thread.currentThread().interrupt();
        } finally {
          _mutex.unlock();
        }
      }
    
      private void refreshSegmentReaders() {
        RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders realtimeReadersForSegment = _luceneRealtimeReaders.poll();
        if (realtimeReadersForSegment != null) {
          String segmentName = realtimeReadersForSegment.getSegmentName();
          realtimeReadersForSegment.getLock().lock();
          try {
            if (!realtimeReadersForSegment.isSegmentDestroyed()) {
              refreshReadersForSegment(realtimeReadersForSegment, segmentName);
            }
          } finally {
            if (!realtimeReadersForSegment.isSegmentDestroyed()) {
              _luceneRealtimeReaders.offer(realtimeReadersForSegment);
            }
            realtimeReadersForSegment.getLock().unlock();
          }
        }
      }
    
      private void refreshReadersForSegment(
          RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders realtimeReadersForSegment, String segmentName) {
        List<RealtimeLuceneTextIndex> realtimeLuceneReaders =
            realtimeReadersForSegment.getRealtimeLuceneReaders();
        for (RealtimeLuceneTextIndex realtimeReader : realtimeLuceneReaders) {
          if (_stopped) {
            break;
          }
          SearcherManager searcherManager = realtimeReader.getSearcherManager();
          try {
            searcherManager.maybeRefresh();
          } catch (Exception e) {
            LOGGER.warn("Caught exception {} while refreshing realtime lucene reader for segment: {}", e,
                segmentName);
          }
        }
      }
    
      private void sleepBetweenCycles() {
        try {
          Thread.sleep(DELAY_BETWEEN_SUCCESSIVE_EXECUTION_MS_DEFAULT);
        } catch (Exception e) {
          LOGGER.warn("Realtime lucene reader refresh thread got interrupted while sleeping: ", e);
          Thread.currentThread().interrupt();
        }
      }
//Refactoring end
}
