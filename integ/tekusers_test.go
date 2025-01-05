package integ

import (
	"bytes"
	"fmt"
	"github.com/spirit-labs/tektite/compress"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestTekUsers(t *testing.T) {
	err := buildTekUsersBinary()
	require.NoError(t, err)

	numAgents := 3
	agents, tearDown := startAgents(t, numAgents, false, false, compress.CompressionTypeNone,
		compress.CompressionTypeLz4)
	defer tearDown(t)

	var addresses strings.Builder
	for i, agent := range agents {
		addresses.WriteString(agent.kafkaListenAddress)
		if i != numAgents-1 {
			addresses.WriteString(",")
		}
	}

	commandLine := fmt.Sprintf("user put --username some-user1 --password some-password1 --addresses %s",
		addresses.String())
	runTekUsersExpectOutput(t, commandLine, "OK", agents[0].kafkaListenAddress)

	commandLine = fmt.Sprintf("user put --username some-user2 --password some-password2 --addresses %s",
		agents[1].kafkaListenAddress)
	runTekUsersExpectOutput(t, commandLine, "OK", agents[1].kafkaListenAddress)

	commandLine = fmt.Sprintf("user put --username some-user1 --password changed-password --addresses %s",
		addresses.String())
	runTekUsersExpectOutput(t, commandLine, "OK", agents[0].kafkaListenAddress)

	commandLine = fmt.Sprintf("user delete --username some-user1 --addresses %s",
		addresses.String())
	runTekUsersExpectOutput(t, commandLine, "OK", agents[0].kafkaListenAddress)

	commandLine = fmt.Sprintf("user delete --username unknown --addresses %s",
		addresses.String())
	runTekUsersExpectOutput(t, commandLine, "failed to delete user - user does not exist", agents[0].kafkaListenAddress)
}

func TestTekUsersHelp(t *testing.T) {
	err := buildTekUsersBinary()
	require.NoError(t, err)

	numAgents := 3
	agents, tearDown := startAgents(t, numAgents, false, false, compress.CompressionTypeNone,
		compress.CompressionTypeLz4)
	defer tearDown(t)

	var addresses strings.Builder
	for i, agent := range agents {
		addresses.WriteString(agent.kafkaListenAddress)
		if i != numAgents-1 {
			addresses.WriteString(",")
		}
	}

	expected := `Usage: tekusers <command>

Flags:
  -h, --help    Show context-sensitive help.

Commands:
  user put --addresses="localhost:9092" --username=STRING --password=STRING
    create/update a user

  user delete --addresses="localhost:9092" --username=STRING
    delete a user

Run "tekusers <command> --help" for more information on a command.
`

	runTekUsersExpectOutputRaw(t, "--help", expected)
}

func runTekUsersExpectOutput(t *testing.T, commandLine string, expectedOut string, listenAddress string) {
	expected := fmt.Sprintf(`connected to agent: %s
%s
`, listenAddress, expectedOut)
	runTekUsersExpectOutputRaw(t, commandLine, expected)
}

func runTekUsersExpectOutputRaw(t *testing.T, commandLine string, expectedOut string) {
	args := strings.Split(commandLine, " ")

	start := time.Now()
	for {
		cmd := exec.Command("../bin/tekusers", args...)
		cmd.Env = append(os.Environ(), "COLUMNS=160")

		out, err := cmd.Output()
		require.NoError(t, err)
		sout := string(out)

		if strings.Contains(sout, "controller has not received cluster membership") {
			// After first starting the agents there is a small amount of time before the controller is chosen, so we
			// retry
			if time.Now().Sub(start) >= 5*time.Second {
				require.Fail(t, "timedout waiting for controller")
			}
			time.Sleep(200 * time.Millisecond)
			log.Warnf("retrying as congroller not available")
			continue
		}
		require.Equal(t, expectedOut, string(out))
		break
	}
}

func buildTekUsersBinary() error {
	log.Infof("building tekusers binary")
	cmd := exec.Command("go", "build", "-o", "../bin", "../tekusers")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		log.Errorf("failed to build tekusers binary:\n%s", out.String())
		return err
	}
	return nil
}
