/*
Copyright © 2024 Art P fn3x@proton.me
*/
package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/term"
)

var (
	Verbose bool
	Source  string
)

// initCmd represents the init command
var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Create config file",
	Long: `creates config file archive.config.yml in the current directory
containing database connections, ports and users`,
	Args: cobra.MaximumNArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		scanner := bufio.NewScanner(os.Stdin)
		if err := viper.ReadInConfig(); err == nil {
			fmt.Printf("Found config file: %s\n\nDo you want to continue and override existing config file? (y/n) ", viper.ConfigFileUsed())

			scanner.Scan()
			answer := scanner.Text()
			if scanner.Err() != nil {
				fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
				os.Exit(1)
			}

			if answer != "y" {
				fmt.Print("Aborted")
				os.Exit(0)
			}
		}

		fmt.Print("\n\n--- Source connection ---\n\n")
		fmt.Print("host: ")
		scanner.Scan()
		mainHost := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Print("port: ")
		scanner.Scan()
		mainPortRead := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		mainPort, err := strconv.Atoi(mainPortRead)
		if err != nil {
			fmt.Fprint(os.Stderr, "Wrong value provided for port")
			os.Exit(1)
		}

		fmt.Print("user: ")
		scanner.Scan()
		mainUser := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Print("password: ")
		byteMainPassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}
		mainPassword := string(byteMainPassword)

		fmt.Print("\ndatabase name: ")
		scanner.Scan()
		mainDb := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Print("\n\n--- Destination connection ---\n\n")
		fmt.Print("host: ")
		scanner.Scan()
		archiveHost := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Print("port: ")
		scanner.Scan()
		archivePortRead := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		archivePort, err := strconv.Atoi(archivePortRead)
		if err != nil {
			fmt.Println("Wrong value provided for port")
			os.Exit(1)
		}

		fmt.Print("user: ")
		scanner.Scan()
		archiveUser := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Print("password: ")
		byteArchivePassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read password from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		archivePassword := string(byteArchivePassword)

		fmt.Print("\ndatabase name: ")
		scanner.Scan()
		archiveDb := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		viper.Set("source.host", mainHost)
		viper.Set("source.port", mainPort)
		viper.Set("source.db", mainDb)
		viper.Set("source.user", mainUser)
		viper.Set("source.password", mainPassword)

		viper.Set("destination.host", archiveHost)
		viper.Set("destination.port", archivePort)
		viper.Set("destination.db", archiveDb)
		viper.Set("destination.user", archiveUser)
		viper.Set("destination.password", archivePassword)

		err = viper.WriteConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't write config to file: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Println("\nConfiguration has been successfully saved.")
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
	initConfig()
}

func initConfig() {
	viper.SetDefault("main.host", "localhost")
	viper.SetDefault("main.port", "3306")
	viper.SetDefault("main.db", "database")
	viper.SetDefault("main.user", "user")
	viper.SetDefault("main.password", "")

	viper.SetDefault("archive.host", "localhost")
	viper.SetDefault("archive.port", "3306")
	viper.SetDefault("archive.db", "archive_database")
	viper.SetDefault("archive.user", "archive_user")
	viper.SetDefault("archive.password", "")

	viper.AddConfigPath("./")
	viper.SetConfigType("yaml")
	viper.SetConfigName("archive.config")
}
