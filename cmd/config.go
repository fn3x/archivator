/*
Copyright © 2025 fn3x <fn3x@proton.me>
*/
package cmd

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/term"
)

var cfgCmd = &cobra.Command{
	Use:   "config",
	Short: "Create config file",
	Long: `
Create config file archi.json in the current directory with database connections, ports and users`,
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

		switch runtime.GOOS {
		case "linux":
			fmt.Print("MySQL socket location (/var/run/mysqld/mysqld.sock): ")
			scanner.Scan()
			socket := scanner.Text()
			if scanner.Err() != nil {
				return scanner.Err()
			}

			if socket != "" {
				viper.Set("socket", "/var/run/mysqld/mysqld.sock")
			}
		case "darwin":
			fmt.Print("MySQL socket location (/tmp/mysql.sock): ")
			scanner.Scan()
			socket := scanner.Text()
			if scanner.Err() != nil {
				return scanner.Err()
			}

			if socket != "" {
				viper.Set("socket", "/tmp/mysql.sock")
			}
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
			return fmt.Errorf("wrong value provided for port")
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
			return fmt.Errorf("wrong value provided for port")
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
			return fmt.Errorf("couldn't read password from stdin: %+v", err)
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

		if destPort > 0 {
			viper.Set("destination.port", destPort)
		}

		viper.Set("destination.db", destDB)
		viper.Set("destination.user", destUser)
		viper.Set("destination.password", destPassword)

		err = viper.WriteConfigAs("archi.json")
		if err != nil {
			return fmt.Errorf("couldn't write config to file: %+v", err)
		}

		fmt.Printf("\nConfiguration has been successfully saved.\n")

		return nil
	},
}

func init() {
	initConfig()
	rootCmd.AddCommand(cfgCmd)
}

func initConfig() {
	viper.SetDefault("socket", "")
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
	viper.SetConfigType("json")
	viper.SetConfigName("archi")
}
