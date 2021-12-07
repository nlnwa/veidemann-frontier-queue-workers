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
	"time"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

// RethinkDbConnection holds the database connection
type RethinkDbConnection struct {
	connectOpts  r.ConnectOpts
	session      r.QueryExecutor
	maxRetries   int
	waitTimeout  time.Duration
	queryTimeout time.Duration
	batchSize    int
	logger       zerolog.Logger
}

type RethinkDbOptions struct {
	Username           string
	Password           string
	Database           string
	UseOpenTracing     bool
	Address            string
	QueryTimeout       time.Duration
	MaxRetries         int
	MaxOpenConnections int
}

// NewRethinkDbConnection creates a new RethinkDbConnection object
func NewRethinkDbConnection(opts RethinkDbOptions) *RethinkDbConnection {
	return &RethinkDbConnection{
		connectOpts: r.ConnectOpts{
			Address:        opts.Address,
			Username:       opts.Username,
			Password:       opts.Password,
			Database:       opts.Database,
			InitialCap:     2,
			MaxOpen:        opts.MaxOpenConnections,
			UseOpentracing: opts.UseOpenTracing,
			NumRetries:     10,
			Timeout:        10 * time.Second,
		},
		maxRetries:   opts.MaxRetries,
		waitTimeout:  60 * time.Second,
		queryTimeout: opts.QueryTimeout,
		batchSize:    200,
		logger:       zlog.With().Str("component", "rethinkdb").Logger(),
	}
}

// Connect establishes connections
func (c *RethinkDbConnection) Connect() error {
	log := c.logger
	var err error
	// Set up database RethinkDbConnection
	c.session, err = r.Connect(c.connectOpts)
	if err != nil {
		return fmt.Errorf("failed to connect to RethinkDB at %s: %w", c.connectOpts.Address, err)
	}
	log.Info().Msgf("Connected to RethinkDB at %s", c.connectOpts.Address)
	return nil
}

// Close closes the RethinkDbConnection
func (c *RethinkDbConnection) Close() error {
	log := c.logger
	log.Info().Msgf("Closing connection to RethinkDB")
	return c.session.(*r.Session).Close()
}

// execRead executes the given read term with a timeout
func (c *RethinkDbConnection) execRead(ctx context.Context, name string, term *r.Term) (*r.Cursor, error) {
	q := func(ctx context.Context) (*r.Cursor, error) {
		runOpts := r.RunOpts{
			Context: ctx,
		}
		return term.Run(c.session, runOpts)
	}
	return c.execWithRetry(ctx, name, q)
}

// execWrite executes the given write term with a timeout
func (c *RethinkDbConnection) execWrite(ctx context.Context, name string, term *r.Term) (writeResponse r.WriteResponse, err error) {
	q := func(ctx context.Context) (*r.Cursor, error) {
		runOpts := r.RunOpts{
			Context:    ctx,
			Durability: "soft",
		}
		writeResponse, err = (*term).RunWrite(c.session, runOpts)
		return nil, err
	}
	_, err = c.execWithRetry(ctx, name, q)
	return
}

// execWithRetry executes given query function repeatedly until successful or max retry limit is reached
func (c *RethinkDbConnection) execWithRetry(ctx context.Context, name string, q func(ctx context.Context) (*r.Cursor, error)) (cursor *r.Cursor, err error) {
	attempts := 0
	log := c.logger.With().Str("operation", name).Logger()
out:
	for {
		attempts++
		cursor, err = c.exec(ctx, q)
		if err == nil {
			return
		}
		log.Warn().Err(err).Int("retries", attempts-1).Msg("Failed to execute query")
		switch err {
		case r.ErrQueryTimeout:
			err := c.wait()
			if err != nil {
				log.Warn().Err(err).Msg("Timed out waiting for database to be ready")
			}
		case r.ErrConnectionClosed:
			err := c.Connect()
			if err != nil {
				log.Warn().Err(err).Msg("Failed to reconnect database")
			}
		default:
			break out
		}
		if attempts > c.maxRetries {
			break
		}
	}
	return nil, fmt.Errorf("failed to %s after %d of %d attempts: %w", name, attempts, c.maxRetries+1, err)
}

// exec the given query with a timeout
func (c *RethinkDbConnection) exec(ctx context.Context, q func(ctx context.Context) (*r.Cursor, error)) (*r.Cursor, error) {
	ctx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()
	return q(ctx)
}

// wait for database to be fully up-to-date and ready for read/write
func (c *RethinkDbConnection) wait() error {
	waitOpts := r.WaitOpts{
		Timeout: c.waitTimeout,
	}
	_, err := r.DB(c.connectOpts.Database).Wait(waitOpts).Run(c.session)
	return err
}
