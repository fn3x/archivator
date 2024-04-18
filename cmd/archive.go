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
      "--source",
      fmt.Sprintf(`-h="%s"`,viper.GetString("source.host")),
      fmt.Sprintf(`-D="%s"`,viper.GetString("source.db")),
      fmt.Sprintf(`-P=%s`,viper.GetString("source.port")),
      fmt.Sprintf(`-u="%s"`,viper.GetString("source.user")),
      fmt.Sprintf(`-p="%s"`,viper.GetString("source.password")),
			fmt.Sprintf("-t=%s", tables),
      "--dest",
      fmt.Sprintf(`--host="%s"`,viper.GetString("destination.host")),
      fmt.Sprintf(`--database="%s"`,viper.GetString("destination.db")),
      fmt.Sprintf(`--port=%s`,viper.GetString("destination.port")),
      fmt.Sprintf(`--user="%s"`,viper.GetString("destination.user")),
      fmt.Sprintf(`--password="%s"`,viper.GetString("destination.password")),
			fmt.Sprintf("-t=%s", tables),
      "--where",
      `"1=1"`,
    }

		cmd := exec.Command("pt-archiver", allArgs...)
		var stdout, stderr bytes.Buffer

		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stdout, "exec args: %+v\n", allArgs)
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		fmt.Fprint(os.Stderr, stderr, "\n")
		fmt.Fprintf(os.Stdout, "Result of the command:\n\n%s\n", stdout.String())
	},
}

func init() {
	rootCmd.AddCommand(archiveCmd)
	initConfig()
}
