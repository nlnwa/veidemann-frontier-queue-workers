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
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nlnwa/veidemann-frontier-queue-workers/database"
	"github.com/nlnwa/veidemann-frontier-queue-workers/logger"
	"github.com/nlnwa/veidemann-frontier-queue-workers/telemetry"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func main() {
	pflag.String("db-host", "rethinkdb-proxy", "RethinkDB host")
	pflag.Int("db-port", 28015, "RethinkDB port")
	pflag.String("db-name", "veidemann", "RethinkDB database name")
	pflag.String("db-user", "", "RethinkDB username")
	pflag.String("db-password", "", "RethinkDB password")
	pflag.Duration("db-query-timeout", 10*time.Second, "RethinkDB query timeout")
	pflag.Int("db-max-retries", 3, "Max retries when query fails")
	pflag.Int("db-max-open-conn", 10, "Max open connections")
	pflag.Bool("db-use-opentracing", false, "Use opentracing for queries")

	pflag.String("redis-host", "redis-veidemann-frontier-master", "Redis host")
	pflag.Int("redis-port", 6379, "Redis port")
	pflag.String("redis-script-path", "./lua", "Path to redis lua scripts")

	pflag.String("log-level", "info", "log level, available levels are panic, fatal, error, warn, info, debug and trace")
	pflag.String("log-formatter", "logfmt", "log formatter, available values are logfmt and json")
	pflag.Bool("log-method", false, "log method names")

	pflag.Parse()

	// setup viper
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		panic(err)
	}

	// setup logging
	logger.InitLog(viper.GetString("log-level"), viper.GetString("log-formatter"), viper.GetBool("log-method"))

	defer func() {
		if err := recover(); err != nil {
			log.Fatal().Msgf("%v", err)
		}
	}()

	// setup telemetry
	if tracer, closer := telemetry.InitTracer("Scope checker", logger.NewJaegerLogger()); tracer != nil {
		opentracing.SetGlobalTracer(tracer)
		defer func() {
			_ = closer.Close()
		}()
	}

	// setup rethinkdb connection
	rethinkDbConnection := database.NewRethinkDbConnection(
		database.RethinkDbOptions{
			Address:            fmt.Sprintf("%s:%d", viper.GetString("db-host"), viper.GetInt("db-port")),
			Username:           viper.GetString("db-user"),
			Password:           viper.GetString("db-password"),
			Database:           viper.GetString("db-name"),
			QueryTimeout:       viper.GetDuration("db-query-timeout"),
			MaxOpenConnections: viper.GetInt("db-max-open-conn"),
			MaxRetries:         viper.GetInt("db-max-retries"),
			UseOpenTracing:     viper.GetBool("db-use-opentracing"),
		},
	)
	if err := rethinkDbConnection.Connect(); err != nil {
		panic(err)
	}
	defer func() {
		_ = rethinkDbConnection.Close()
	}()

	redisClient, err := database.NewRedisClient(viper.GetString("redis-host"), viper.GetInt("redis-port"))
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = redisClient.Close()
	}()

	db, err := database.NewDatabase(redisClient, rethinkDbConnection, viper.GetString("redis-script-path"))
	if err != nil {
		panic(err)
	}

	ctx, stop := context.WithCancel(context.Background())

	go func() {
		signals := make(chan os.Signal, 1)
		defer signal.Stop(signals)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		sig := <-signals
		log.Info().Str("signal", sig.String()).Msg("Shutting down")
		stop()
	}()

	wg := new(errgroup.Group)

	for _, v := range []struct {
		name  string
		delay time.Duration
		fn    worker
	}{
		{"update-job-executions", 5 * time.Second, updateJobExecutions(db)},
		{"ceid-timeout-queue", 1100 * time.Millisecond, crawlExecutionTimeoutQueueWorker(db)},
		{"remuri-queue", 200 * time.Millisecond, removeUriQueueWorker(db)},
		{"busy-queue", 50 * time.Millisecond, chgBusyQueueWorker(db)},
		{"wait-queue", 50 * time.Millisecond, chgWaitQueueWorker(db)},
		{"ceid-running-queue", 50 * time.Millisecond, crawlExecutionRunningQueueWorker(db)},
	} {
		t := v
		log.Info().Dur("delayMs", t.delay).Msgf("Starting worker: %s", t.name)

		wg.Go(func() error {
			defer stop()
			for {
				// io.EOF can be returned by the go-redis driver but
				// is to be seen as transient
				if err := t.fn(); err != nil && !errors.Is(err, io.EOF) {
					return fmt.Errorf("%s: %w", t.name, err)
				}
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(t.delay):
				}
			}
		})
	}

	if err := wg.Wait(); err != nil {
		panic(err)
	}
}
