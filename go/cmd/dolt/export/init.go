// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"sync"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

var exportInitOnce = sync.Once{}

func exportInit() int {
	ret := 0
	exportInitOnce.Do(func() {
		// Seed the rand
		bs := make([]byte, 8)
		_, err := crand.Read(bs)
		if err != nil {
			cli.PrintErrln(color.RedString("failed to initialize rand %v`", err))
			ret = 1
			return
		}
		rand.Seed(int64(binary.LittleEndian.Uint64(bs)))

		warnIfMaxFilesTooLow()

		ctx := context.Background()
		dEnv = env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "0.0.0")
		if dEnv.CfgLoadErr != nil {
			cli.PrintErrln(color.RedString("Failed to load the global config. %v", dEnv.CfgLoadErr))
			ret = 1
			return
		}
		err = reconfigIfTempFileMoveFails(dEnv)
		if err != nil {
			cli.PrintErrln(color.RedString("Failed to setup the temporary directory. %v`", err))
			ret = 1
			return
		}

		defer tempfiles.MovableTempFileProvider.Clean()
		err = dsess.InitPersistedSystemVars(dEnv)
		if err != nil {
			cli.Printf("error: failed to load persisted global variables: %s\n", err.Error())
			ret = 1
			return
		}
	})
	return ret
}

func main() {}
