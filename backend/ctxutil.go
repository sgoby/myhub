/*
Copyright 2018 Sgoby.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backend

import (
	"context"
	"errors"
	"github.com/sgoby/myhub/backend/driver"
)

var errLevelNotSupported = errors.New("sql: selected isolation level is not supported")


//
func ctxDriverBegin(ctx context.Context, opts *TxOptions, ci driver.Conn) (driver.Tx, error) {
	if ctx.Done() == context.Background().Done() {
		return ci.Begin()
	}

	if opts != nil {
		// Check the transaction level. If the transaction level is non-default
		// then return an error here as the BeginTx driver value is not supported.
		if opts.Isolation != LevelDefault {
			return nil, errors.New("sql: driver does not support non-default isolation level")
		}

		// If a read-only transaction is requested return an error as the
		// BeginTx driver value is not supported.
		if opts.ReadOnly {
			return nil, errors.New("sql: driver does not support read-only transactions")
		}
	}

	txi, err := ci.Begin()
	if err == nil {
		select {
		default:
		case <-ctx.Done():
			txi.Rollback()
			return nil, ctx.Err()
		}
	}
	return txi, err
}
