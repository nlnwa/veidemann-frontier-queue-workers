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

package database

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	frontierV1 "github.com/nlnwa/veidemann-api/go/frontier/v1"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

// Database is an abstraction layer between the business layer and database implementation details
type Database interface {
	UpdateJobExecutions(ctx context.Context) (int, error)
	RemoveFromUriQueue(ctx context.Context) (int, error)
	MoveWaitToReady() (int, error)
	MoveBusyToTimeout() (int, error)
	MoveRunningToTimeout() (int, error)
	TimeoutCrawlExecutions(ctx context.Context) (int, error)
}

// rethinkdb constants
const (
	rethinkDbTableUriQueue        = "uri_queue"
	rethinkDbTableCrawlExecutions = "executions"
	rethinkDbTableJobExecutions   = "job_executions"
)

// redis constants
const (
	redisChgDelayedQueueScriptName = "chg_delayed_queue.lua"

	redisRemoveUriQueue     = "REMURI"
	redisJobExecutionPrefix = "JEID:"

	redisWaitQueue    = "chg_wait{chg}"
	redisReadyQueue   = "chg_ready{chg}"
	redisBusyQueue    = "chg_busy{chg}"
	redisTimeoutQueue = "chg_timeout{chg}"

	redisCrawlExecutionRunningQueue = "ceid_running"
	redisCrawlExecutionTimeoutQueue = "ceid_timeout"
)

type database struct {
	// rethinkdb
	rethinkDB *RethinkDbConnection
	// redis
	redis      *redis.Client
	moveScript *redis.Script
}

func NewDatabase(redisClient *redis.Client, conn *RethinkDbConnection, scriptPath string) (Database, error) {
	moveScript, err := loadRedisScript(redisClient, filepath.Join(scriptPath, redisChgDelayedQueueScriptName))
	if err != nil {
		return nil, err
	}

	return &database{
		redis:      redisClient,
		rethinkDB:  conn,
		moveScript: moveScript,
	}, nil
}

func (d *database) moveChg(fromQueue string, toQueue string) (int, error) {
	return d.moveScript.Run(d.redis, []string{fromQueue, toQueue}, time.Now().UTC().UnixNano()/int64(time.Millisecond)).Int()
}

func (d *database) MoveWaitToReady() (int, error) {
	return d.moveChg(redisWaitQueue, redisReadyQueue)
}

func (d *database) MoveBusyToTimeout() (int, error) {
	return d.moveChg(redisBusyQueue, redisTimeoutQueue)
}

func (d *database) MoveRunningToTimeout() (int, error) {
	return d.moveChg(redisCrawlExecutionRunningQueue, redisCrawlExecutionTimeoutQueue)
}

func (d *database) RemoveFromUriQueue(ctx context.Context) (int, error) {
	// Get up to 10000 uriIds from redis REMURI queue
	uriIds, err := d.redis.LRange(redisRemoveUriQueue, 0, 9999).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get list of uriIds to be removed: %w", err)
	}
	if len(uriIds) == 0 {
		return 0, nil
	}

	// Delete from rethinkdb table uri_queue
	removed, err := removeQueuedUris(d.rethinkDB, ctx, uriIds)
	if err != nil {
		return removed, fmt.Errorf("removed %d of %d queued uris: %w", removed, len(uriIds), err)
	}

	if err := deleteFromRemoveQueue(d.redis, uriIds); err != nil {
		return removed, fmt.Errorf("failed to remove some queued uri ids from REMURI: %w", err)
	}
	return removed, nil
}

func removeQueuedUris(rethinkDB *RethinkDbConnection, ctx context.Context, uriIds []string) (int, error) {
	term := r.Table(rethinkDbTableUriQueue).GetAll(r.Args(uriIds)).Delete(
		r.DeleteOpts{
			Durability: "soft",
		})
	wr, err := rethinkDB.execWrite(ctx, "delete-queued-uris", &term)
	return wr.Deleted, err
}

func deleteFromRemoveQueue(redis *redis.Client, uriIds []string) error {
	pipe := redis.Pipeline()
	for _, uriId := range uriIds {
		pipe.LRem(redisRemoveUriQueue, 1, uriId)
	}
	_, err := pipe.Exec()
	return err
}

func (d *database) UpdateJobExecutions(ctx context.Context) (int, error) {
	jess, err := getJobExecutionStatuses(d.redis)
	if err != nil {
		return 0, fmt.Errorf("failed to get job executions: %w", err)
	}
	count := 0
	for _, jes := range jess {
		replaced, err := updateJobExecution(d.rethinkDB, ctx, jes)
		if err != nil {
			return replaced, fmt.Errorf("failed to update job execution status: %w", err)
		}
		count += replaced
	}
	return count, nil
}

func getJobExecutionStatuses(redis *redis.Client) ([]map[string]interface{}, error) {
	// Get all keys prefixed with "JEID:"
	var jobExecutionKeys []string
	err := redis.Keys(redisJobExecutionPrefix + "*").ScanSlice(&jobExecutionKeys)
	if err != nil {
		return nil, err
	}

	var jobExecutionStatuses []map[string]interface{}
	for _, key := range jobExecutionKeys {
		if exists, err := redis.Exists(key).Result(); err != nil {
			return nil, err
		} else if exists == 0 {
			continue
		}
		jeMap, err := redis.HGetAll(key).Result()
		if err != nil {
			return nil, err
		}

		m := make(map[string]interface{})

		m["id"] = strings.TrimPrefix(key, redisJobExecutionPrefix)

		var executionsState []map[string]int64
		for k, v := range jeMap {
			_, ok := frontierV1.CrawlExecutionStatus_State_value[k]
			c, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				continue
			}
			if !ok {
				m[k] = c
			} else {
				executionsState = append(executionsState, map[string]int64{k: c})
			}
		}
		m["executionsState"] = executionsState
		jobExecutionStatuses = append(jobExecutionStatuses, m)
	}

	return jobExecutionStatuses, nil
}

func updateJobExecution(rethinkDB *RethinkDbConnection, ctx context.Context, jes map[string]interface{}) (int, error) {
	term := r.Table(rethinkDbTableJobExecutions).
		Get(jes["id"]).
		Update(func(doc r.Term) interface{} {
			// only update if jes is active
			return r.Branch(r.Expr([]string{
				"FINISHED",
				"ABORTED_TIMEOUT",
				"ABORTED_SIZE",
				"ABORTED_MANUAL",
				"FAILED",
				"DIED",
			}).Contains(doc.Field("state")),
				nil,
				jes,
			)
		})
	wr, err := rethinkDB.execWrite(ctx, "update-job-execution-status", &term)
	return wr.Replaced, err
}

func (d *database) TimeoutCrawlExecutions(ctx context.Context) (int, error) {
	count := 0
	for {
		ceid, err := d.redis.LPop(redisCrawlExecutionTimeoutQueue).Result()
		if err == redis.Nil {
			break
		} else if err != nil {
			return count, fmt.Errorf("get timed out crawl execution: %w", err)
		}

		replaced, err := setCrawlExecutionStateAbortedTimeout(d.rethinkDB, ctx, ceid)
		if err != nil {
			// put ceid back in timout queue to recover
			_, rollbackErr := d.redis.RPush(redisCrawlExecutionTimeoutQueue, ceid).Result()
			if rollbackErr != nil {
				return count, fmt.Errorf("%v:  %w: failed to recover ceid %s (must be inserted into timeout queue manually):", err, rollbackErr, ceid)
			}
			break
		}
		count += replaced
	}
	return count, nil
}

func setCrawlExecutionStateAbortedTimeout(rethinkDB *RethinkDbConnection, ctx context.Context, crawlExecutionId string) (int, error) {
	term := r.Table(rethinkDbTableCrawlExecutions).Get(crawlExecutionId).Update(
		func(doc r.Term) interface{} {
			return r.Branch(
				doc.HasFields("endTime"),
				nil,
				map[string]string{
					"desiredState": frontierV1.CrawlExecutionStatus_ABORTED_TIMEOUT.String(),
				})
		})
	wr, err := rethinkDB.execWrite(ctx, "set-crawl-execution-state-aborted-timeout", &term)
	return wr.Replaced, err
}
