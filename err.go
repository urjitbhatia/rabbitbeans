package rabbitbeans

import (
	"fmt"
	"log"
)

// Prints formatted error message and panics
func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", err, msg))
	}
}

// Prints formatted error message and but DOES NOT panic
// Use this to log recoverable errors and continue
func LogOnError(err error, msg string) {
	if err != nil {
		log.Printf("Recovering from error: %s: %s", err, msg)
	}
}
