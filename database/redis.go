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
	"os"

	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
)

func NewRedisClient(host string, port int) (*redis.Client, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	client := redis.NewClient(&redis.Options{
		Addr:       addr,
		MaxRetries: 3,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	log.Info().Str("component", "redis").Msgf("Connected to Redis at %s", addr)

	return client, err
}

func loadRedisScript(client *redis.Client, path string) (*redis.Script, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// create script
	script := redis.NewScript(string(bytes))

	// load script if it doesn't exist in redis
	boolSlice, err := script.Exists(client).Result()
	if err != nil {
		return nil, err
	}
	for _, exists := range boolSlice {
		if !exists {
			if err := script.Load(client).Err(); err != nil {
				return nil, err
			}
		}
	}

	return script, nil
}
