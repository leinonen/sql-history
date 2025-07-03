package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/leinonen/sql-history/pkg/parser"
)

func main() {
	var trackUser bool
	var userSource string
	
	flag.BoolVar(&trackUser, "track-user", false, "Add user tracking to history tables")
	flag.StringVar(&userSource, "user-source", "current_user", "Source for user info: 'current_user' or 'session'")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Usage: sql-history [flags] <input.sql> [output.sql]")
		fmt.Println("  input.sql  - SQL file containing CREATE TABLE statements")
		fmt.Println("  output.sql - Output file for history tables and triggers (optional)")
		fmt.Println("\nFlags:")
		fmt.Println("  --track-user        Add user tracking to history tables")
		fmt.Println("  --user-source       Source for user info: 'current_user' or 'session' (default: current_user)")
		os.Exit(1)
	}

	if userSource != "current_user" && userSource != "session" {
		fmt.Println("Error: --user-source must be either 'current_user' or 'session'")
		os.Exit(1)
	}

	inputFile := args[0]
	outputFile := ""

	if len(args) >= 2 {
		outputFile = args[1]
	} else {
		ext := filepath.Ext(inputFile)
		base := strings.TrimSuffix(inputFile, ext)
		outputFile = base + "_history" + ext
	}

	content, err := readFile(inputFile)
	if err != nil {
		fmt.Printf("Error reading input file: %v\n", err)
		os.Exit(1)
	}

	tables, err := parser.ParseCreateTables(content)
	if err != nil {
		fmt.Printf("Error parsing SQL: %v\n", err)
		os.Exit(1)
	}

	if len(tables) == 0 {
		fmt.Println("No CREATE TABLE statements found in the input file")
		os.Exit(1)
	}

	config := parser.Config{
		TrackUser:  trackUser,
		UserSource: userSource,
	}

	output, err := parser.GenerateHistorySQL(tables, config)
	if err != nil {
		fmt.Printf("Error generating history SQL: %v\n", err)
		os.Exit(1)
	}

	err = writeFile(outputFile, output)
	if err != nil {
		fmt.Printf("Error writing output file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully processed %d table(s)\n", len(tables))
	fmt.Printf("Generated history tables and triggers in: %s\n", outputFile)

	for _, table := range tables {
		originalName := parser.GetOriginalTableName(table)
		historyName := parser.GetHistoryTableName(table)
		fmt.Printf("  - %s -> %s\n", originalName, historyName)
	}
}

func readFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

func writeFile(filename, content string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(content)
	return err
}
