/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"github.com/nlnwa/veidemann-frontier-workers/database"
	zlog "github.com/rs/zerolog/log"
	"sync"
	"time"
)

// start workers and wait for them to complete.
func start(done <-chan struct{}, dbAdapter database.DbAdapter, redisAdapter database.QueueAdapter) {
	wg := new(sync.WaitGroup)
	defer wg.Wait()

	go schedule(done, wg, time.Second, updateJobExecutions(dbAdapter, redisAdapter))
	go schedule(done, wg, time.Second, chgQueueWorker())
}

// schedule to run fn in a loop with given delay between calls.
func schedule(done <-chan struct{}, wg *sync.WaitGroup, delay time.Duration, fn func()) {
	wg.Add(1)
	defer wg.Done()
	for {
		fn()
		select {
		case <-done:
			return
		case <-time.After(delay):
		}
	}
}

func updateJobExecutions(adapter database.DbAdapter, queueAdapter database.QueueAdapter) func() {
	log := zlog.Logger.With().Logger()
	return func() {
		jess, err := queueAdapter.GetJobExecutionStatuses()
		if err != nil {
			log.Error().Err(err).Msg("")
			return
		}
		for _, _ = range jess {
			// adapter.Save(jes)
		}
	}
}

func chgQueueWorker() func() {
	return func() {

	}
}

func waitQueueWorker() func() {
	return func() {

	}
}

func busyQueueWorker() func() {
	return func() {

	}
}

func executionDurationWorker() func() {
	return func() {

	}
}

func crawlExecutionTimeoutWorker() func() {
	return func() {

	}
}
