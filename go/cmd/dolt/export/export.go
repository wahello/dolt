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
	"C"
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/tblcmds"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

var dEnv *env.DoltEnv

//export Exec
func Exec(command int, args []string) int {
	ret := exportInit()
	if ret != 0 {
		return ret
	}
	var cmd cli.Command
	switch command {
	case 0:
		cmd = commands.InitCmd{}
	case 1:
		cmd = commands.StatusCmd{}
	case 2:
		cmd = commands.AddCmd{}
	case 3:
		cmd = commands.DiffCmd{}
	case 4:
		cmd = commands.ResetCmd{}
	case 5:
		cmd = commands.CleanCmd{}
	case 6:
		cmd = commands.CommitCmd{}
	case 7:
		cmd = commands.SqlCmd{VersionStr: "0.0.0"}
	case 8:
		cmd = sqlserver.SqlServerCmd{VersionStr: "0.0.0"}
	case 9:
		cmd = sqlserver.SqlClientCmd{}
	case 10:
		cmd = commands.LogCmd{}
	case 11:
		cmd = commands.BranchCmd{}
	case 12:
		cmd = commands.CheckoutCmd{}
	case 13:
		cmd = commands.MergeCmd{}
	case 14:
		cmd = commands.RevertCmd{}
	case 15:
		cmd = commands.CloneCmd{}
	case 16:
		cmd = commands.FetchCmd{}
	case 17:
		cmd = commands.PullCmd{}
	case 18:
		cmd = commands.PushCmd{}
	case 19:
		cmd = commands.ConfigCmd{}
	case 20:
		cmd = commands.RemoteCmd{}
	case 21:
		cmd = commands.BackupCmd{}
	case 22:
		cmd = commands.LoginCmd{}
	case 23:
		cmd = commands.LsCmd{}
	case 24:
		cmd = commands.TagCmd{}
	case 25:
		cmd = commands.BlameCmd{}
	case 26:
		cmd = commands.SendMetricsCmd{}
	case 27:
		cmd = commands.MigrateCmd{}
	case 28:
		cmd = commands.ReadTablesCmd{}
	case 29:
		cmd = commands.GarbageCollectionCmd{}
	case 30:
		cmd = commands.FilterBranchCmd{}
	case 31:
		cmd = commands.MergeBaseCmd{}
	case 32:
		cmd = commands.RootsCmd{}
	case 33:
		cmd = commands.VersionCmd{VersionStr: "0.0.0"}
	case 34:
		cmd = commands.DumpCmd{}
	case 35:
		cmd = commands.InspectCmd{}
	case 36:
		cmd = commands.CherryPickCmd{}
	case 37:
		cmd = tblcmds.ImportCmd{}
	default:
		cli.PrintErrln(color.RedString("unknown command with number %v`", command))
	}
	res := exec(cmd, args)
	if res == 0 && command == 0 {
		exportInitOnce = sync.Once{}
	}
	return res
}

//export IsInitialized
func IsInitialized() bool {
	ret := exportInit()
	if ret != 0 {
		return false
	}
	return dEnv.HasDoltDir()
}

func exec(cmd cli.Command, args []string) int {
	ctx := context.Background()
	var wg sync.WaitGroup
	ctx, stop := context.WithCancel(ctx)

	//res := doltCommand.Exec(ctx, "dolt", args, dEnv)
	if rnrCmd, ok := cmd.(cli.RepoNotRequiredCommand); ok {
		if rnrCmd.RequiresRepo() {
			isValid := checkEnvIsValid(dEnv)
			if !isValid {
				stop()
				return 2
			}
		}
	}

	// Certain commands cannot tolerate a top-level signal handler (which cancels the root context) but need to manage
	// their own interrupt semantics.
	if signalCmd, ok := cmd.(cli.SignalCommand); !ok || !signalCmd.InstallsSignalHandlers() {
		var stop2 context.CancelFunc
		ctx, stop2 = signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer stop2()
	}
	ret := cmd.Exec(ctx, "", args, dEnv)

	stop()
	wg.Wait()
	return ret
}
