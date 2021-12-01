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
	"fmt"
	"github.com/nlnwa/veidemann-frontier-workers/database"
	"github.com/nlnwa/veidemann-frontier-workers/logger"
	"github.com/nlnwa/veidemann-frontier-workers/telemetry"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"strings"
)

func main() {
	pflag.String("interface", "", "interface the browser controller api listens to. No value means all interfaces.")
	pflag.Int("port", 8080, "port the browser controller api listens to.")
	pflag.String("host-name", "", "")
	pflag.String("warc-dir", "", "")
	pflag.String("warc-version", "1.1", "which WARC version to use for generated records. Allowed values: 1.0, 1.1")
	pflag.Int("warc-writer-pool-size", 1, "")
	pflag.Bool("flush-record", false, "if true, flush WARC-file to disk after each record.")
	pflag.String("work-dir", "", "")
	pflag.Int("termination-grace-period-seconds", 0, "")
	pflag.Bool("strict-validation", false, "if true, use strict record validation")

	pflag.String("db-host", "rethinkdb-proxy", "DB host")
	pflag.Int("db-port", 28015, "DB port")
	pflag.String("db-name", "veidemann", "DB name")
	pflag.String("db-user", "", "Database username")
	pflag.String("db-password", "", "Database password")
	pflag.Duration("db-query-timeout", 1*time.Minute, "Database query timeout")
	pflag.Int("db-max-retries", 5, "Max retries when database query fails")
	pflag.Int("db-max-open-conn", 10, "Max open database connections")
	pflag.Bool("db-use-opentracing", false, "Use opentracing for database queries")

	pflag.String("redis-host", "redis-veidemann-frontier-master", "Redis host")
	pflag.Int("redis-port", 6379, "Redis port")



	pflag.String("log-level", "info", "log level, available levels are panic, fatal, error, warn, info, debug and trace")
	pflag.String("log-formatter", "logfmt", "log formatter, available values are logfmt and json")
	pflag.Bool("log-method", false, "log method names")

	pflag.Parse()

	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatal().Err(err).Msg("Could not parse flags")
	}

	// setup logging
	logger.InitLog(viper.GetString("log-level"), viper.GetString("log-formatter"), viper.GetBool("log-method"))

	// setup telemetry
	tracer, closer := telemetry.InitTracer("Scope checker")
	if tracer != nil {
		opentracing.SetGlobalTracer(tracer)
		defer func() {
			_ = closer.Close()
		}()
	}

	// setup rethinkdb connection
	db := database.NewRethinkDbConnection(
		database.Options{
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
	if err := db.Connect(); err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	dbAdapter := database.NewDbAdapter(db)

	redi, err := database.NewRedisClient(viper.GetString("redis-host"), viper.GetInt("redis-port"))
	if err != nil {
		log.Error().Err(err).Msg("Failed to communicate with redis")
	}

	queueAdapter := database.NewQueueAdapter(redi)

	done := make(chan struct{})
	go func() {
		signals := make(chan os.Signal, 1)
		defer signal.Stop(signals)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		sig := <-signals
		log.Debug().Msgf("Received signal: %s", sig)

		close(done)
	}()

	start(done, dbAdapter, queueAdapter)
}
