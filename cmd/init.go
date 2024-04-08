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
			fmt.Printf("Found config file: %s\nDo you want to continue override existing config file (./archive.config.yaml)? (y/n) ", viper.ConfigFileUsed())
			var answer string

			for {
        scanner.Scan()
				answer = scanner.Text()
				if scanner.Err() != nil {
					log.Fatal(scanner.Err())
				}

				if answer != "y" && answer != "n" {
					fmt.Println("Wrong option")
				} else {
					break
				}
			}

			if answer == "n" {
				fmt.Println("Exiting init command")
				return
			}
		}

		fmt.Print("Main connection host: ")
    scanner.Scan()
		mainHost := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		fmt.Print("Main database port: ")
    scanner.Scan()
		mainPortRead := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		mainPort, err := strconv.Atoi(mainPortRead)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Print("Main connection user: ")
    scanner.Scan()
		mainUser := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		fmt.Print("Main connection password: ")
    byteMainPassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			log.Fatal(scanner.Err())
		}
    mainPassword := string(byteMainPassword)

		fmt.Print("\nMain database name: ")
    scanner.Scan()
		mainDb := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		fmt.Print("Archive connection host: ")
    scanner.Scan()
		archiveHost := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		fmt.Print("Archive connection port: ")
    scanner.Scan()
		archivePortRead := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		archivePort, err := strconv.Atoi(archivePortRead)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Print("Archive connection user: ")
    scanner.Scan()
		archiveUser := scanner.Text()
		if scanner.Err() != nil {
			log.Fatal(scanner.Err())
		}

		fmt.Print("Archive connection password: ")
    byteArchivePassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			log.Fatal(scanner.Err())
		}
    archivePassword := string(byteArchivePassword)

		fmt.Print("\nArchive database name: ")
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

		fmt.Println("Configuration has been successfully saved.")
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
