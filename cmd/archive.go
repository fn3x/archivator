/*
Copyright © 2024 Art P fn3x@proton.me
*/
package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive tables",
	Long:  `Archive tables from source database to destination database specified in config using pt-archiver`,
	Args:  cobra.MinimumNArgs(1),
	RunE: func(command *cobra.Command, args []string) error {
		if err := viper.ReadInConfig(); err != nil {
			return err
		}

		tables := strings.Join(args, ",")

		allArgs := []string{
			"--progress",
			viper.GetString("progress"),
			"--socket",
			viper.GetString("socket"),
			"--source",
			fmt.Sprintf("h=%s,D=%s,P=%s,u=%s,p=%s,t=%s",
				viper.GetString("source.host"),
				viper.GetString("source.db"),
				viper.GetString("source.port"),
				viper.GetString("source.user"),
				viper.GetString("source.password"),
				tables),
			"--dest",
			fmt.Sprintf("h=%s,D=%s,P=%s,u=%s,p=%s,t=%s",
				viper.GetString("destination.host"),
				viper.GetString("destination.db"),
				viper.GetString("destination.port"),
				viper.GetString("destination.user"),
				viper.GetString("destination.password"),
				tables),
			"--where",
			`'1=1'`,
		}

		cmd := exec.Command("pt-archiver", allArgs...)
		var stdout, stderr bytes.Buffer

		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			return errors.New(fmt.Sprintf("%s %s", err.Error(), stderr.String()))
		}

		fmt.Fprintln(os.Stderr, stderr.String())
		fmt.Fprintln(os.Stdout, stdout.String())
		return nil
	},
}

func init() {
  initConfig()
	rootCmd.AddCommand(archiveCmd)
}
