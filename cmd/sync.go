package cmd

import (
	"time"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"github.com/zhangguanzhang/google_containers/core"
)

func NewSyncComamnd(Options *core.SyncOption) *cobra.Command {
	if Options == nil {
		Options = &core.SyncOption{}
	}

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync docker images",
		Long: `
Sync docker images.`,
		//Args:   cobra.ExactArgs(1),
		// 1. 检验用户名密码是否存在,并尝试登录 registry.
		// 2. 打开键值对数据库文件
		PreRunE: Options.PreRun,
		Run: func(cmd *cobra.Command, args []string) {
			core.Run(Options)
		},
	}

	AddSyncAuthFlags(cmd.Flags(), Options)
	AddSyncLimitFlags(cmd.Flags(), Options)

	return cmd
}

func AddSyncAuthFlags(flagSet *flag.FlagSet, op *core.SyncOption) {
	flagSet.StringVarP(
		&op.Auth.User, "user", "u", "",
		"The username to push.",
	)
	flagSet.StringVarP(
		&op.Auth.Pass, "password", "p", "",
		"The password to push.",
	)
	flagSet.StringVar(
		&op.PushRepo, "push-to", "docker.io",
		"the repo push to",
	)
	flagSet.StringVar(
		&op.PushNS, "push-ns", "",
		"the ns push to",
	)
	flagSet.Uint8Var(
		&op.LoginRetry, "login-retry", 2,
		"login retry when timeout.",
	)
}

func AddSyncLimitFlags(flagSet *flag.FlagSet, op *core.SyncOption) {
	flagSet.StringVar(
		&op.DbFile, "db", "bolt.db",
		"the boltdb file",
	)
	flagSet.IntVar(
		&op.QueryLimit, "query-limit", 10,
		"http query limit.",
	)
	flagSet.IntVar(
		&op.Limit, "process-limit", 2,
		"sync process limit.",
	)
	flagSet.DurationVar(
		&op.CmdTimeout, "command-timeout", 0,
		"timeout for the command execution.",
	)
	flagSet.DurationVar(
		&op.SingleTimeout, "img-timeout", 15*time.Minute,
		"sync single image timeout.",
	)
	flagSet.DurationVar(
		&op.LiveInterval, "live-interval", 0,
		"live output in ci-runner.",
	)
	flagSet.IntVar(
		&op.Retry, "retry", 4,
		"retry count while err.",
	)
	flagSet.DurationVar(
		&op.RetryInterval, "retry-interval", 4*time.Second,
		"retry interval while err.",
	)
	flagSet.StringArrayVar(
		&op.AdditionNS, "addition-ns", nil,
		"addition ns to sync")
}
