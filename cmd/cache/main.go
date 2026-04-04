/*
Package main is the entry point for the application.
*/
package main

import (
	"log"

	"cache/internal/app"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
