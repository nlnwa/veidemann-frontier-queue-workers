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
	"fmt"
	"github.com/go-redis/redis"
	frontierV1 "github.com/nlnwa/veidemann-api/go/frontier/v1"
	"strconv"
	"strings"
)

const (
	RemoveUriQueue     = "REMURI"
	JobExecutionPrefix = "JEID:"
)

type RedisClient struct {
	*redis.Client
}

func NewRedisClient(host string, port int) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", host, port),
		//Dialer:    nil,
		//OnConnect: nil,
		//MaxRetries:         0,
		//MinRetryBackoff:    0,
		//MaxRetryBackoff:    0,
		//DialTimeout:        0,
		//ReadTimeout:        0,
		//WriteTimeout:       0,
		//PoolSize:           0,
		//MinIdleConns:       0,
		//MaxConnAge:         0,
		//PoolTimeout:        0,
		//IdleTimeout:        0,
		//IdleCheckFrequency: 0,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &RedisClient{Client: client}, nil
}

func (r *RedisClient) GetJobExecutionStatuses() ([]*frontierV1.JobExecutionStatus, error) {
	// Get all keys prefixed with "JEID:"
	var jobExecutionKeys []string
	err := r.Keys(JobExecutionPrefix + "*").ScanSlice(&jobExecutionKeys)
	if err != nil {
		return nil, err
	}

	var jobExecutionStatuses []*frontierV1.JobExecutionStatus
	for _, key := range jobExecutionKeys {
		exists, err := r.Exists(key).Result()
		if err != nil {
			return nil, err
		}
		if exists == 0 {
			continue
		}
		jeMap, err := r.HGetAll(key).Result()
		if err != nil {
			return nil, err
		}

		jes := new(frontierV1.JobExecutionStatus)
		jes.Id = strings.TrimPrefix(key, JobExecutionPrefix)
		jes.DocumentsCrawled, _ = strconv.ParseInt(jeMap["documentsCrawled"], 10, 64)
		jes.DocumentsDenied, _ = strconv.ParseInt(jeMap["documentsDenied"], 10, 64)
		jes.DocumentsFailed, _ = strconv.ParseInt(jeMap["documentsFailed"], 10, 64)
		jes.DocumentsOutOfScope, _ = strconv.ParseInt(jeMap["documentsOutOfScope"], 10, 64)
		jes.DocumentsRetried, _ = strconv.ParseInt(jeMap["documentsRetried"], 10, 64)
		jes.UrisCrawled, _ = strconv.ParseInt(jeMap["urisCrawled"], 10, 64)
		jes.BytesCrawled, _ = strconv.ParseInt(jeMap["bytesCrawled"], 10, 64)
		jes.ExecutionsState = make(map[string]int32, len(frontierV1.CrawlExecutionStatus_State_value))
		for key, _ := range frontierV1.CrawlExecutionStatus_State_value {
			v, _ := strconv.ParseInt(jeMap[key], 10, 32)
			jes.ExecutionsState[key] = int32(v)
		}
		jobExecutionStatuses = append(jobExecutionStatuses, jes)
	}

	return jobExecutionStatuses, nil
}
