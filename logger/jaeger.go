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

package logger

import (
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/uber/jaeger-client-go"
)

// jaegerLogger implements the jaeger.Logger interface using zerolog
type jaegerLogger struct {
	zerolog.Logger
}

func NewJaegerLogger() jaeger.Logger {
	return &jaegerLogger{
		Logger: zlog.With().Str("component", "jaeger").Logger(),
	}
}

func (j jaegerLogger) Error(msg string) {
	j.Logger.Error().Msg(msg)
}

func (j *jaegerLogger) Infof(msg string, args ...interface{}) {
	j.Logger.Info().Msgf(msg, args...)
}
