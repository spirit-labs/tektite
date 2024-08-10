//go:build mage

package main

import (
	"bufio"
	"fmt"
	"github.com/spirit-labs/tektite/kafkagen"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

const (
	GotestsumUrl    = "gotest.tools/gotestsum"
	GolangciLintUrl = "github.com/golangci/golangci-lint/cmd/golangci-lint"
	AddLicenseUrl   = "github.com/google/addlicense"
)

var (
	goexec = mg.GoCmd()
	g0     = sh.RunCmd(goexec)
)

// Build builds the binary
func Build() error {
	fmt.Println("Building the binary...")
	return g0("build", "-o", "bin", "./...")
}

func mustRun(cmd string, args ...string) {
	out := lipgloss.NewStyle().Bold(true).Render(
		fmt.Sprintf("\n> %s %s\n", cmd, strings.Join(args, " ")),
	)

	fmt.Println(out)
	if err := sh.RunV(cmd, args...); err != nil {
		panic(err)
	}
}

func checkTools() error {
	if _, err := exec.LookPath("gotestsum"); err != nil {
		fmt.Println("gotestsum is not installed. Installing...")
		fmt.Printf("Installing gotestsum from %s\n", GotestsumUrl)
		mustRun(goexec, "install", GotestsumUrl)
	}

	if _, err := exec.LookPath("golangci-lint"); err != nil {
		fmt.Println("golangci-lint not found, installing...")
		fmt.Printf("Installing golangci-lint from %s\n", GolangciLintUrl)
		mustRun(goexec, "install", GolangciLintUrl)
	}

	if _, err := exec.LookPath("addlicense"); err != nil {
		fmt.Println("addlicense not found, installing...")
		fmt.Printf("Installing addlicense from %s\n", AddLicenseUrl)
		mustRun(goexec, "install", AddLicenseUrl)
	}
	return nil
}

// Lint runs the linter
func Lint() error {
	mg.Deps(checkTools)
	fmt.Println("Running golanci-lint linter...")
	return sh.RunV("golangci-lint", "run")
}

// Test runs only the unit tests
func Test() error {
	mg.Deps(checkTools)
	fmt.Println("Running unit tests...")
	return sh.RunV("gotestsum", "-f", "standard-verbose", "--", "-race", "-failfast", "-count", "1", "-timeout", "10m", "./...")
}

// Test runs only the integration tests
func Integration() error {
	mg.Deps(checkTools)
	fmt.Println("Running integration tests...")
	return sh.RunV("gotestsum", "-f", "standard-verbose", "--", "-tags", "integration", "-race", "-failfast", "-count", "1", "-timeout", "10m", "./integration/...")
}

// LicenseCheck fixes any missing license header in the source code
func LicenseCheck() error {
	mg.Deps(checkTools)
	fmt.Println("Running license check...")
	return sh.RunV("addlicense", "-c", "The Tektite Authors", "-ignore", "**/*.yml", "-ignore", "**/*.xml", ".")
}

// Presubmit is intended to be run by contributors before pushing the code and creating a PR.
// It depends on LicenseCheck, Build, Lint, Test and Integration in order
func Presubmit() error {
	mg.Deps(LicenseCheck, Build, Lint, Test)
	return Integration()
}

// Run tests in a continuous loop
func Loop() error {
	iteration := 0
	logFile, err := os.OpenFile("test-results.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer logFile.Close()

	logWriter := bufio.NewWriter(logFile)
	defer logWriter.Flush()

	// Write the current date and time to the log file
	date := time.Now().Format(time.RFC1123)
	logWriter.WriteString(date + "\n")

	for {
		iteration++
		iter := fmt.Sprintf("Running loop iteration %d", iteration)
		fmt.Println(iter)
		logWriter.WriteString(iter + "\n")

		ran, err := sh.Exec(nil, logWriter, logWriter, "gotestsum", "-f", "standard-verbose", "--", "-tags", "integration", "-race", "-failfast", "-count", "1", "-timeout", "7m", "./integration/...")

		if !ran {
			errString := fmt.Sprintf("Go test failed. Exiting loop. Error: %v\n", err)
			fmt.Println(errString)
			logWriter.WriteString(errString)
			break
		}
	}
	return nil
}

// Run tektited in a standalone setup
func Run() error {
	fmt.Println("Running tektited in a standalone setup...")
	return g0("run", "cmd/tektited/main.go", "--config", "cfg/standalone.conf")
}

// GenKafkaProtocol generates the Kafka protocol code from the protocol JSON descriptors
func GenKafkaProtocol() error {
	return kafkagen.Generate("asl/kafka/spec", "kafkaserver/protocol")
}
