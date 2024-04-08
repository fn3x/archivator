/*
Copyright © 2024 Art P <fn3x@proton.me>
*/
package cmd

import (
	"bufio"
	"fmt"
	"log"
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
				log.Fatal(scanner.Err())
			}

			if answer != "y" {
				fmt.Print("Abort")
				os.Exit(0)
			}
		}

		fmt.Print("\n\n--- Main connection ---\n\n")
		fmt.Print("host: ")
		scanner.Scan()
		mainHost := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		fmt.Print("port: ")
		scanner.Scan()
		mainPortRead := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		mainPort, err := strconv.Atoi(mainPortRead)
		if err != nil {
			fmt.Println("Wrong value provided for port")
      os.Exit(1)
		}

		fmt.Print("user: ")
		scanner.Scan()
		mainUser := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		fmt.Print("password: ")
		byteMainPassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			log.Fatal(scanner.Err())
		}
		mainPassword := string(byteMainPassword)

		fmt.Print("\ndatabase name: ")
		scanner.Scan()
		mainDb := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		fmt.Print("\n\n--- Archive connection ---\n\n")
		fmt.Print("host: ")
		scanner.Scan()
		archiveHost := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		fmt.Print("port: ")
		scanner.Scan()
		archivePortRead := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
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
			log.Fatal(scanner.Err())
		}

		fmt.Print("password: ")
		byteArchivePassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			log.Fatal(err)
		}

		archivePassword := string(byteArchivePassword)

		fmt.Print("\ndatabase name: ")
		scanner.Scan()
		archiveDb := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		viper.Set("main.host", mainHost)
		viper.Set("main.port", mainPort)
		viper.Set("main.db", mainDb)
		viper.Set("main.user", mainUser)
		viper.Set("main.password", mainPassword)

		viper.Set("archive.host", archiveHost)
		viper.Set("archive.port", archivePort)
		viper.Set("archive.db", archiveDb)
		viper.Set("archive.user", archiveUser)
		viper.Set("archive.password", archivePassword)

		err = viper.WriteConfig()
		if err != nil {
			log.Fatal(err)
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
