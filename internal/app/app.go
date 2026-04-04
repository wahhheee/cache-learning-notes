/*
Package app contains the core logic of the application.
*/
package app

import (
	"fmt"
)

// Run executes the main application logic.
func Run() error {
	fmt.Println("Hello from internal app logic (cache)!")
	return nil
}
