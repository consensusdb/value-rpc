/*
 *
 * Copyright 2020-present Arpabet Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package server

import "github.com/consensusdb/value"

/**
@author Alex Shvid
*/

type FunctionCallback func(args ...value.Value) (value.Value, error)

type Server interface {
	AddFunction(name string, numArgs int, cb FunctionCallback) error

	// GET for client
	AddOutcomingStream(name string) error

	// PUT for client
	AddIncomingStream(name string) error

	AddChat(name string) error

	Run() error

	Close() error
}
