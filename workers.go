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
	"context"
	"fmt"

	"github.com/nlnwa/veidemann-frontier-queue-workers/database"
	"github.com/rs/zerolog/log"
)

// worker is a function that may return an error.
type worker func() error

// chgWaitQueueWorker returns a worker that moves crawl host groups from wait to ready queue.
func chgWaitQueueWorker(db database.Database) worker {
	return func() error {
		if moved, err := db.MoveWaitToReady(); err != nil {
			return fmt.Errorf("error moving crawl host groups from wait queue to ready queue: %w", err)
		} else if moved > 0 {
			log.Debug().Msgf("%d crawl host group(s) is ready", moved)
		}
		return nil
	}
}

// chgBusyQueueWorker returns a worker that moves crawl host groups from busy to timeout queue.
func chgBusyQueueWorker(db database.Database) worker {
	return func() error {
		if moved, err := db.MoveBusyToTimeout(); err != nil {
			return fmt.Errorf("error moving crawl host groups from busy queue to timeout queue: %w", err)
		} else if moved > 0 {
			log.Debug().Msgf("%d crawl host group(s) timed out", moved)
		}
		return nil
	}
}

// removeUriQueueWorker returns a worker that removes queued URIs.
func removeUriQueueWorker(db database.Database) worker {
	return func() error {
		if removed, err := db.RemoveFromUriQueue(context.Background()); err != nil {
			return err
		} else if removed > 0 {
			log.Debug().Msgf("Removed %d queued uris", removed)
		}
		return nil
	}
}

// crawlExecutionRunningQueueWorker returns a worker that moves crawl executions from running to timeout queue.
func crawlExecutionRunningQueueWorker(db database.Database) worker {
	return func() error {
		if moved, err := db.MoveRunningToTimeout(); err != nil {
			return fmt.Errorf("error moving crawl executions from running to timeout queue: %w", err)
		} else if moved > 0 {
			log.Debug().Msgf("%d crawl execution(s) timed out", moved)
		}
		return nil
	}
}

// crawlExecutionTimeoutQueueWorker returns a worker that sets desired state to ABORTED_TIMOUT on crawl executions in timeout queue.
func crawlExecutionTimeoutQueueWorker(db database.Database) worker {
	return func() error {
		if timeouts, err := db.TimeoutCrawlExecutions(context.Background()); err != nil {
			return fmt.Errorf("time out crawl executions: %w", err)
		} else if timeouts > 0 {
			log.Debug().Msgf("%d crawl execution(s) timed out", timeouts)
		}
		return nil
	}
}

// updateJobExecutions returns a worker that updates stats on job executions.
func updateJobExecutions(db database.Database) worker {
	return func() error {
		if count, err := db.UpdateJobExecutions(context.Background()); err != nil {
			return fmt.Errorf("failed to update job executions: %w", err)
		} else if count > 0 {
			log.Debug().Msgf("Updated %d job execution(s)", count)
		}
		return nil
	}
}
