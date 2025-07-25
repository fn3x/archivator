package helpers

import (
	"fmt"
	"log"
)

func Assert(expected bool, message string) {
	if !expected {
		log.Panic(message)
	}
}

func AssertError(expected bool, message string) error {
	if !expected {
		return fmt.Errorf("ASSERTION ERROR: %s\n", message)
	}

	return nil
}
