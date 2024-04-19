/*
Copyright © 2024 Art P fn3x@proton.me
*/
package cmd

import (
	"bytes"
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
	Long:  `archives tables from source database to destination database specified in config using pt-archiver`,
	Args:  cobra.MinimumNArgs(1),
	Run: func(command *cobra.Command, args []string) {
		if err := viper.ReadInConfig(); err != nil {
			fmt.Fprintf(
				os.Stderr,
				"Couldn't read from config file: %+v\n",
				err,
			)
			os.Exit(1)
		}

		tables := strings.Join(args, ",")

		allArgs := []string{
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
			fmt.Fprintf(os.Stderr, "Error: %s\nMessage: %s\n", err, stderr.String())
			os.Exit(1)
		}

		fmt.Fprintln(os.Stderr, stderr.String())
		fmt.Fprintln(os.Stdout, stdout.String())
	},
}

func init() {
	rootCmd.AddCommand(archiveCmd)
	initConfig()
}
