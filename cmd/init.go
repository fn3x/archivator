/*
Copyright © 2024 Art P fn3x@proton.me
*/
package cmd

import (
	"bufio"
	"errors"
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
	Long: `
Create config file .archi.config.yml in the current directory with database connections, ports and users`,
	Args: cobra.MaximumNArgs(0),
	RunE: func(cmd *cobra.Command, args []string) error {
		scanner := bufio.NewScanner(os.Stdin)
		if err := viper.ReadInConfig(); err == nil {
			fmt.Printf("Found config file: %s\n\nDo you want to continue and override existing config file? (y/n) ", viper.ConfigFileUsed())

			scanner.Scan()
			answer := scanner.Text()
			if scanner.Err() != nil {
				return scanner.Err()
			}

			if answer != "y" {
				fmt.Println("Aborted")
				return nil
			}
		}

		fmt.Print("Client (/var/run/mysqld/mysqld.sock): ")
		scanner.Scan()
		socket := scanner.Text()
		if scanner.Err() != nil {
			return scanner.Err()
		}

		fmt.Print("\n--- Source connection ---\n\n")
		fmt.Print("host (127.0.0.1): ")
		scanner.Scan()
		sourceHost := scanner.Text()
		if scanner.Err() != nil {
			return scanner.Err()
		}

		fmt.Print("port (3306): ")
		scanner.Scan()
		mainPortRead := scanner.Text()
		if scanner.Err() != nil {
			return scanner.Err()
		}

		if mainPortRead == "" {
			mainPortRead = viper.GetString("source.port")
		}

		sourcePort, err := strconv.Atoi(mainPortRead)
		if err != nil {
			return errors.New(fmt.Sprintf("Wrong value provided for port"))
		}

		fmt.Print("user: ")
		scanner.Scan()
		sourceUser := scanner.Text()
		if scanner.Err() != nil {
			return scanner.Err()
		}

		fmt.Print("password: ")
		byteMainPassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return scanner.Err()
		}
		sourcePassword := string(byteMainPassword)

		fmt.Print("\ndatabase name: ")
		scanner.Scan()
		sourceDB := scanner.Text()
		if scanner.Err() != nil {
			return scanner.Err()
		}

		fmt.Print("\n\n--- Destination connection ---\n\n")
		fmt.Print("host (127.0.0.1): ")
		scanner.Scan()
		destHost := scanner.Text()
		if scanner.Err() != nil {
			return scanner.Err()
		}

		fmt.Print("port (3306): ")
		scanner.Scan()
		archivePortRead := scanner.Text()
		if scanner.Err() != nil {
			return scanner.Err()
		}

		if archivePortRead == "" {
			archivePortRead = viper.GetString("destination.port")
		}

		destPort, err := strconv.Atoi(archivePortRead)
		if err != nil {
			return errors.New(fmt.Sprintf("Wrong value provided for port"))
		}

		fmt.Print("user: ")
		scanner.Scan()
		destUser := scanner.Text()
		if scanner.Err() != nil {
			return scanner.Err()
		}

		fmt.Print("password: ")
		byteArchivePassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return errors.New(fmt.Sprintf("Couldn't read password from stdin: %+v\n", err))
		}

		destPassword := string(byteArchivePassword)

		fmt.Print("\ndatabase name: ")
		scanner.Scan()
		destDB := scanner.Text()
		if scanner.Err() != nil {
			return scanner.Err()
		}

		if sourceHost != "" {
			viper.Set("source.host", sourceHost)
		}

		if sourcePort != 0 {
			viper.Set("source.port", sourcePort)
		}

		viper.Set("source.db", sourceDB)
		viper.Set("source.user", sourceUser)
		viper.Set("source.password", sourcePassword)

		if destHost != "" {
			viper.Set("destination.host", destHost)
		}

		if destPort != 0 {
			viper.Set("destination.port", destPort)
		}

		viper.Set("destination.db", destDB)
		viper.Set("destination.user", destUser)
		viper.Set("destination.password", destPassword)

		viper.Set("socket", socket)
		viper.Set("progress", "100")

		err = viper.WriteConfigAs(".archivator.config.yaml")
		if err != nil {
			return errors.New(fmt.Sprintf("Couldn't write config to file: %+v\n", err))
		}

		fmt.Printf("\nConfiguration has been successfully saved.\n")

		return nil
	},
}

func init() {
	initConfig()
	rootCmd.AddCommand(initCmd)
}

func initConfig() {
	viper.SetDefault("socket", "/var/run/mysqld/mysqld.sock")
	viper.SetDefault("source.host", "127.0.0.1")
	viper.SetDefault("source.port", "3306")
	viper.SetDefault("source.db", "")
	viper.SetDefault("source.user", "")
	viper.SetDefault("source.password", "")

	viper.SetDefault("destination.host", "127.0.0.1")
	viper.SetDefault("destination.port", "3306")
	viper.SetDefault("destination.db", "")
	viper.SetDefault("destination.user", "")
	viper.SetDefault("destination.password", "")

	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")
	viper.SetConfigName(".archi.config")
}
