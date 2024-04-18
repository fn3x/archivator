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
				fmt.Println("Aborted")
				os.Exit(0)
			}
		}

		fmt.Print("\n--- Source connection ---\n\n")
		fmt.Print("host (localhost): ")
		scanner.Scan()
		sourceHost := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Print("port (3306): ")
		scanner.Scan()
		mainPortRead := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		if mainPortRead == "" {
			mainPortRead = viper.GetString("source.port")
		}

		sourcePort, err := strconv.Atoi(mainPortRead)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Wrong value provided for port")
			os.Exit(1)
		}

		fmt.Print("user: ")
		scanner.Scan()
		sourceUser := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Print("password: ")
		byteMainPassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", err)
			os.Exit(1)
		}
		sourcePassword := string(byteMainPassword)

		fmt.Print("\ndatabase name: ")
		scanner.Scan()
		sourceDb := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Print("\n\n--- Destination connection ---\n\n")
		fmt.Print("host (localhost): ")
		scanner.Scan()
		destHost := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Print("port (3306): ")
		scanner.Scan()
		archivePortRead := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		if archivePortRead == "" {
			archivePortRead = viper.GetString("destination.port")
		}

		destPort, err := strconv.Atoi(archivePortRead)
		if err != nil {
			fmt.Println("Wrong value provided for port")
			os.Exit(1)
		}

		fmt.Print("user: ")
		scanner.Scan()
		destUser := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		fmt.Print("password: ")
		byteArchivePassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read password from stdin: %+v\n", err)
			os.Exit(1)
		}

		destPassword := string(byteArchivePassword)

		fmt.Print("\ndatabase name: ")
		scanner.Scan()
		destDb := scanner.Text()
		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Couldn't read from stdin: %+v\n", scanner.Err())
			os.Exit(1)
		}

		if sourceHost != "" {
			viper.Set("source.host", sourceHost)
		}

		if sourcePort != 0 {
			viper.Set("source.port", sourcePort)
		}

		viper.Set("source.db", sourceDb)
		viper.Set("source.user", sourceUser)
		viper.Set("source.password", sourcePassword)

		if destHost != "" {
			viper.Set("destination.host", destHost)
		}

		if destPort != 0 {
			viper.Set("destination.port", destPort)
		}

		viper.Set("destination.db", destDb)
		viper.Set("destination.user", destUser)
		viper.Set("destination.password", destPassword)

		err = viper.WriteConfigAs(".archivator.config.yaml")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't write config to file: %+v\n", err)
			os.Exit(1)
		}

		fmt.Printf("\nConfiguration has been successfully saved.\n")
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
	initConfig()
}

func initConfig() {
	viper.SetDefault("source.host", "localhost")
	viper.SetDefault("source.port", "3306")
	viper.SetDefault("source.db", "")
	viper.SetDefault("source.user", "")
	viper.SetDefault("source.password", "")

	viper.SetDefault("destination.host", "localhost")
	viper.SetDefault("destination.port", "3306")
	viper.SetDefault("destination.db", "")
	viper.SetDefault("destination.user", "")
	viper.SetDefault("destination.password", "")

	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")
	viper.SetConfigName(".archivator.config")
}
