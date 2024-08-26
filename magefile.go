//go:build mage

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/spirit-labs/tektite/kafkagen"
	"io/ioutil"
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

type Ca struct {
	Bits      int      `json:"bits"`
	Crt       string   `json:"crt"`
	Days      int      `json:"days"`
	Key       string   `json:"key"`
	Paths     []string `json:"paths"`
	Pubkey    string   `json:"pubkey"`
	SignedCrt string   `json:"signedCrt"`
	Subject   string   `json:"subject"`
}
type CaSigned struct {
	Crt     string   `json:"crt"`
	Days    int      `json:"days"`
	ExtFile string   `json:"extFile"`
	Key     string   `json:"key"`
	Paths   []string `json:"paths"`
	PkeyOpt string   `json:"pkeyOpt"`
	Req     string   `json:"req"`
	Subject string   `json:"subject"`
}
type SelfSigned struct {
	ConfigFile string   `json:"configFile"`
	Crt        string   `json:"crt"`
	Days       int      `json:"days"`
	Key        string   `json:"key"`
	Paths      []string `json:"paths"`
	PkeyOpt    string   `json:"pkeyOpt"`
	Subject    string   `json:"subject"`
}
type CertsConfig struct {
	Ca                *Ca         `json:"ca"`
	CaSignedServer    *CaSigned   `json:"caSignedServer"`
	CaSignedClient    *CaSigned   `json:"caSignedClient"`
	SelfSignedServer  *SelfSigned `json:"selfSignedServer"`
	SelfSignedClient  *SelfSigned `json:"selfSignedClient"`
	SelfSignedClient2 *SelfSigned `json:"selfSignedClient2"`
	ServerUtils       *SelfSigned `json:"serverUtils"`
}

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

// Check internal certificates
func CheckCerts() error {
	fmt.Println("Checking internal certificates expiration dates")
	certsConfigPath := "asl/cli/testdata/certsConfig.json"
	// Open the JSON file
	file, err := os.Open(certsConfigPath)
	if err != nil {
		return fmt.Errorf("could not open file: %w", err)
	}
	defer file.Close()
	// Read the file's content
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("could not read file: %w", err)
	}

	var config CertsConfig
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return fmt.Errorf("could not unmarshal certsConfig.json: %s\n", err)
	}
	caEnv := strings.Join([]string{config.Ca.Paths[0], config.Ca.Crt}, "/")
	caSignedServerEnv := strings.Join([]string{config.CaSignedServer.Paths[0], config.CaSignedServer.Crt}, "/")
	caSignedClientEnv := strings.Join([]string{config.CaSignedClient.Paths[0], config.CaSignedClient.Crt}, "/")
	selfSignedServerEnv := strings.Join([]string{config.SelfSignedServer.Paths[0], config.SelfSignedServer.Crt}, "/")
	selfSignedClientEnv := strings.Join([]string{config.SelfSignedClient.Paths[0], config.SelfSignedClient.Crt}, "/")
	selfSignedClient2Env := strings.Join([]string{config.SelfSignedClient2.Paths[0], config.SelfSignedClient2.Crt}, "/")
	adminEnv := strings.Join([]string{config.ServerUtils.Paths[0], config.ServerUtils.Crt}, "/")
	apiEnv := strings.Join([]string{config.ServerUtils.Paths[1], config.ServerUtils.Crt}, "/")
	integrationEnv := strings.Join([]string{config.ServerUtils.Paths[2], config.ServerUtils.Crt}, "/")
	remotingEnv := strings.Join([]string{config.ServerUtils.Paths[3], config.ServerUtils.Crt}, "/")
	shutdownEnv := strings.Join([]string{config.ServerUtils.Paths[4], config.ServerUtils.Crt}, "/")
	tektclientEnv := strings.Join([]string{config.ServerUtils.Paths[5], config.ServerUtils.Crt}, "/")

	Envs := []string{caEnv, caSignedServerEnv, caSignedClientEnv, selfSignedServerEnv, selfSignedClientEnv, selfSignedClient2Env, adminEnv, apiEnv, integrationEnv, remotingEnv, shutdownEnv, tektclientEnv}

	for e := range Envs {
		env := Envs[e]
		opensslCmd := fmt.Sprintf(`openssl x509 -enddate -noout -in "%s"|cut -d= -f 2`, env)
		output, err := sh.Output("sh", "-c", opensslCmd)
		if err != nil {
			return fmt.Errorf("could not execute command openssl cert check command on %s", env)
		}
		fmt.Println(output, ": ", env)
	}
	return nil
}

// Renew internal certificates
func RenewCerts() error {
	fmt.Println("Renewing internal certificates")

	certsConfigPath := "asl/cli/testdata/certsConfig.json"
	// Open the JSON file
	file, err := os.Open(certsConfigPath)
	if err != nil {
		return fmt.Errorf("could not open file: %w", err)
	}
	defer file.Close()
	// Read the file's content
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("could not read file: %w", err)
	}

	var config CertsConfig
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return fmt.Errorf("could not unmarshal certsConfig.json: %s\n", err)
	}

	tmpDir := "tmp"
	fmt.Println("Creating", tmpDir, "directory")
	err = sh.Run("mkdir", "-p", tmpDir)
	if err != nil {
		return fmt.Errorf("failed to create tmp directory: %v", err)
	}
	err = os.Chdir(tmpDir)
	defer os.Chdir("..")
	if err != nil {
		return fmt.Errorf("failed to change directory to %s: %v", tmpDir, err)
	}

	fmt.Println("Generating CA keys and certificates")
	caKey := config.Ca.Key
	caBits := config.Ca.Bits
	opensslCmd := fmt.Sprintf(`openssl genrsa -out %s %d`, caKey, caBits)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not execute openssl command to create CA private key: %v", err)
	}

	caDays := config.Ca.Days
	caSignedCrt := config.Ca.SignedCrt
	caSubject := config.Ca.Subject
	opensslCmd = fmt.Sprintf(`openssl req -x509 -new -nodes -key %s -sha256 -days %d -out %s -subj "%s"`, caKey, caDays, caSignedCrt, caSubject)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate CA Signed certificate: %v", err)
	}

	caCrt := config.Ca.Crt
	catCmd := fmt.Sprintf(`cat %s %s > %s`, caSignedCrt, caKey, caCrt)
	err = sh.Run("sh", "-c", catCmd)
	if err != nil {
		return fmt.Errorf("could not generate CA PEM certificate: %v", err)
	}

	fmt.Println("Generating server and clients keys, self-signed certificates")
	selfSignedServerKey := config.SelfSignedServer.Key
	selfSignedClientKey := config.SelfSignedClient.Key
	selfSignedClient2Key := config.SelfSignedClient2.Key
	serverPkeyOpt := config.SelfSignedServer.PkeyOpt
	clientPkeyOpt := config.SelfSignedClient.PkeyOpt
	client2PkeyOpt := config.SelfSignedClient.PkeyOpt
	opensslCmd = fmt.Sprintf(`openssl genpkey -algorithm RSA -out %s -pkeyopt %s`, selfSignedServerKey, serverPkeyOpt)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Server key: %v", err)
	}
	opensslCmd = fmt.Sprintf(`openssl genpkey -algorithm RSA -out %s -pkeyopt %s`, selfSignedClientKey, clientPkeyOpt)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Client key: %v", err)
	}
	opensslCmd = fmt.Sprintf(`openssl genpkey -algorithm RSA -out %s -pkeyopt %s`, selfSignedClient2Key, client2PkeyOpt)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Client2 key: %v", err)
	}
	serverSelfSignedCrt := config.SelfSignedServer.Crt
	clientSelfSignedCrt := config.SelfSignedClient.Crt
	client2SelfSignedCrt := config.SelfSignedClient2.Crt
	serverSelfSignedConfigFile := config.SelfSignedServer.ConfigFile
	clientSelfSignedConfigFile := config.SelfSignedClient.ConfigFile
	client2SelfSignedConfigFile := config.SelfSignedClient.ConfigFile
	serverSelfSignedDays := config.SelfSignedServer.Days
	clientSelfSignedDays := config.SelfSignedClient.Days
	client2SelfSignedDays := config.SelfSignedClient2.Days
	serverSelfSignedSubject := config.SelfSignedServer.Subject
	clientSelfSignedSubject := config.SelfSignedClient.Subject
	client2SelfSignedSubject := config.SelfSignedClient2.Subject
	opensslCmd = fmt.Sprintf(`openssl req -new -x509 -key %s -out %s -config %s -days %d -subj "%s"`, selfSignedServerKey, serverSelfSignedCrt, serverSelfSignedConfigFile, serverSelfSignedDays, serverSelfSignedSubject)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Server self signed certificate: %v", err)
	}
	opensslCmd = fmt.Sprintf(`openssl req -new -x509 -key %s -out %s -config %s -days %d -subj "%s"`, selfSignedClientKey, clientSelfSignedCrt, clientSelfSignedConfigFile, clientSelfSignedDays, clientSelfSignedSubject)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Client self signed certificate: %v", err)
	}
	opensslCmd = fmt.Sprintf(`openssl req -new -x509 -key %s -out %s -config %s -days %d -subj "%s"`, selfSignedClient2Key, client2SelfSignedCrt, client2SelfSignedConfigFile, client2SelfSignedDays, client2SelfSignedSubject)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Client2 self signed certificate: %v", err)
	}

	fmt.Println("Generating server and client keys, requests, signed certificates")

	caSignedServerKey := config.CaSignedServer.Key
	caSignedClientKey := config.CaSignedClient.Key
	serverPkeyOpt = config.CaSignedServer.PkeyOpt
	clientPkeyOpt = config.CaSignedClient.PkeyOpt
	opensslCmd = fmt.Sprintf(`openssl genpkey -algorithm RSA -out %s -pkeyopt %s`, caSignedServerKey, serverPkeyOpt)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Server key: %v", err)
	}
	opensslCmd = fmt.Sprintf(`openssl genpkey -algorithm RSA -out %s -pkeyopt %s`, caSignedClientKey, clientPkeyOpt)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Client key: %v", err)
	}

	serverReq := config.CaSignedServer.Req
	clientReq := config.CaSignedClient.Req
	serverCaSignedSubject := config.CaSignedServer.Subject
	clientCaSignedSubject := config.CaSignedClient.Subject
	opensslCmd = fmt.Sprintf(`openssl req -new -key %s -out %s -subj "%s"`, caSignedServerKey, serverReq, serverCaSignedSubject)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Server request: %v", err)
	}
	opensslCmd = fmt.Sprintf(`openssl req -new -key %s -out %s -subj "%s"`, caSignedClientKey, clientReq, clientCaSignedSubject)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Client request: %v", err)
	}
	serverCaSignedCrt := config.CaSignedServer.Crt
	clientCaSignedCrt := config.CaSignedClient.Crt
	serverCaSignedDays := config.CaSignedServer.Days
	clientCaSignedDays := config.CaSignedClient.Days
	serverCaSignedExtFile := config.CaSignedServer.ExtFile
	clientCaSignedExtFile := config.CaSignedClient.ExtFile
	opensslCmd = fmt.Sprintf(`openssl x509 -req -in %s -CA %s -out %s -days %d -sha256 -extfile %s`, serverReq, caCrt, serverCaSignedCrt, serverCaSignedDays, serverCaSignedExtFile)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not sign Server certificate: %v", err)
	}
	opensslCmd = fmt.Sprintf(`openssl x509 -req -in %s -CA %s -out %s -days %d -sha256 -extfile %s`, clientReq, caCrt, clientCaSignedCrt, clientCaSignedDays, clientCaSignedExtFile)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not sign Client certificate: %v", err)
	}

	fmt.Println("Generating server utils self-signed certificate from server ca-signed key")
	newServerCert := config.ServerUtils.Crt
	serverUtilsConfigFile := config.ServerUtils.ConfigFile
	serverUtilsDays := config.ServerUtils.Days
	serverUtilsSubject := config.ServerUtils.Subject
	opensslCmd = fmt.Sprintf(`openssl req -new -x509 -key %s -out %s -config %s -days %d -subj "%s"`, caSignedServerKey, newServerCert, serverUtilsConfigFile, serverUtilsDays, serverUtilsSubject)
	err = sh.Run("sh", "-c", opensslCmd)
	if err != nil {
		return fmt.Errorf("could not generate Server self signed certificate: %v", err)
	}

	fmt.Println("Copying newly generated files in place")
	adminPath := strings.Join([]string{"..", config.ServerUtils.Paths[0], newServerCert}, "/")
	apiPath := strings.Join([]string{"..", config.ServerUtils.Paths[1], newServerCert}, "/")
	integrationPath := strings.Join([]string{"..", config.ServerUtils.Paths[2], newServerCert}, "/")
	remotingPath := strings.Join([]string{"..", config.ServerUtils.Paths[3], newServerCert}, "/")
	shutdownPath := strings.Join([]string{"..", config.ServerUtils.Paths[4], newServerCert}, "/")
	tektclientPath := strings.Join([]string{"..", config.ServerUtils.Paths[5], newServerCert}, "/")

	serverUtilsPaths := []string{adminPath, apiPath, integrationPath, remotingPath, shutdownPath, tektclientPath}
	for p := range serverUtilsPaths {
		path := serverUtilsPaths[p]
		cpCmd := fmt.Sprintf(`cp -v %s %s`, newServerCert, path)
		err = sh.RunV("sh", "-c", cpCmd)
		if err != nil {
			return fmt.Errorf("could not copy newly self-signed Server certificate on %s: %v", path, err)
		}
	}
	newServerKey := "serverkey.pem"
	cpCmd := fmt.Sprintf(`cp -v %s %s`, caSignedServerKey, newServerKey)
	err = sh.RunV("sh", "-c", cpCmd)
	if err != nil {
		return fmt.Errorf("could not rename newly generated Server key: %v", err)
	}

	adminPath = strings.Join([]string{"..", config.ServerUtils.Paths[0], newServerKey}, "/")
	apiPath = strings.Join([]string{"..", config.ServerUtils.Paths[1], newServerKey}, "/")
	integrationPath = strings.Join([]string{"..", config.ServerUtils.Paths[2], newServerKey}, "/")
	remotingPath = strings.Join([]string{"..", config.ServerUtils.Paths[3], newServerKey}, "/")
	shutdownPath = strings.Join([]string{"..", config.ServerUtils.Paths[4], newServerKey}, "/")
	tektclientPath = strings.Join([]string{"..", config.ServerUtils.Paths[5], newServerKey}, "/")

	serverUtilsPaths = []string{adminPath, apiPath, integrationPath, remotingPath, shutdownPath, tektclientPath}
	for p := range serverUtilsPaths {
		path := serverUtilsPaths[p]
		cpCmd = fmt.Sprintf(`cp -v %s %s`, newServerKey, path)
		err = sh.RunV("sh", "-c", cpCmd)
		if err != nil {
			return fmt.Errorf("could not copy generated Server key on %s: %v", path, err)
		}
	}

	fmt.Println("Storing newly generated files in place")
	cliPath := strings.Join([]string{"..", config.Ca.Paths[0]}, "/")
	mvCmd := fmt.Sprintf(`mv -v *.* %s`, cliPath)
	err = sh.RunV("sh", "-c", mvCmd)
	if err != nil {
		return fmt.Errorf("could not move newly generated files in %s: %v", cliPath, err)
	}

	fmt.Printf("Removing %s directory\n", tmpDir)
	tmpPath := strings.Join([]string{"..", tmpDir}, "/")
	os.RemoveAll(tmpPath)
	return nil
}

// GenKafkaProtocol generates the Kafka protocol code from the protocol JSON descriptors
func GenKafkaProtocol() error {
	return kafkagen.Generate("asl/kafka/spec", "kafkaserver/protocol")
}
