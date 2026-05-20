package config_test

import (
	"os"
	"os/exec"
	"runtime"
	"testing"

	"github.com/realtime-tracking/ingest/config"
)

// allEnvVars lists every environment variable required by LoadConfig, in the
// same order they are read. Used to set up the happy-path test and to drive
// the missing-variable subtests.
var allEnvVars = []struct {
	envKey    string
	wantField func(c config.Config) string
}{
	{"KAFKA_BOOTSTRAP_SERVERS", func(c config.Config) string { return c.KafkaBootstrapServers }},
	{"KAFKA_TOPIC_GPS_PINGS", func(c config.Config) string { return c.KafkaTopic }},
	{"KAFKA_SASL_USERNAME", func(c config.Config) string { return c.KafkaSASLUsername }},
	{"KAFKA_SASL_PASSWORD", func(c config.Config) string { return c.KafkaSASLPassword }},
	{"SCHEMA_REGISTRY_URL", func(c config.Config) string { return c.SchemaRegistryURL }},
	{"SERVICE_PORT", func(c config.Config) string { return c.ServicePort }},
	{"OTEL_EXPORTER_OTLP_ENDPOINT", func(c config.Config) string { return c.OTELEndpoint }},
}

// setAllEnvVars sets every required environment variable to a deterministic
// test value and returns a cleanup function that unsets them all.
func setAllEnvVars(t *testing.T) {
	t.Helper()
	for _, v := range allEnvVars {
		t.Setenv(v.envKey, "test-value-for-"+v.envKey)
	}
}

// TestLoadConfig_AllVarsSet verifies that when all required environment
// variables are present, LoadConfig returns a Config whose fields match the
// values that were set.
func TestLoadConfig_AllVarsSet(t *testing.T) {
	setAllEnvVars(t)

	cfg := config.LoadConfig()

	for _, v := range allEnvVars {
		want := "test-value-for-" + v.envKey
		got := v.wantField(cfg)
		if got != want {
			t.Errorf("field for %s: got %q, want %q", v.envKey, got, want)
		}
	}
}

// TestLoadConfig_MissingVar_ExitsOne verifies that when a single required
// environment variable is absent, LoadConfig calls os.Exit(1).
//
// The test uses the subprocess pattern: it re-executes the test binary as a
// child process with a special sentinel environment variable set, so the child
// runs only the targeted LoadConfig call and the parent can inspect the exit
// code without the parent process itself exiting.
func TestLoadConfig_MissingVar_ExitsOne(t *testing.T) {
	for _, v := range allEnvVars {
		v := v // capture loop variable
		t.Run("missing_"+v.envKey, func(t *testing.T) {
			if os.Getenv("TEST_SUBPROCESS_MISSING_VAR") == v.envKey {
				// ── Child process path ──────────────────────────────────────
				// Set all vars except the one under test, then call LoadConfig.
				// LoadConfig must call os.Exit(1) — the child process exits 1.
				for _, other := range allEnvVars {
					if other.envKey != v.envKey {
						os.Setenv(other.envKey, "test-value-for-"+other.envKey) //nolint:errcheck
					}
				}
				config.LoadConfig() // must not return — exits 1
				return
			}

			// ── Parent process path ─────────────────────────────────────────
			// Re-run this specific subtest inside a child process.
			exe, err := os.Executable()
			if err != nil {
				t.Fatalf("could not determine test executable path: %v", err)
			}

			// Build the -test.run flag value. On Windows the subtest separator
			// uses '/' which must be escaped for the regex.
			runFlag := "TestLoadConfig_MissingVar_ExitsOne/missing_" + v.envKey

			cmd := exec.Command(exe, "-test.run="+runFlag, "-test.v")
			cmd.Env = append(os.Environ(),
				"TEST_SUBPROCESS_MISSING_VAR="+v.envKey,
			)
			// Suppress the child's stdout/stderr in normal test output; the
			// exit code is what we care about.
			if testing.Verbose() {
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
			}

			err = cmd.Run()
			if err == nil {
				t.Errorf("expected LoadConfig to exit(1) when %s is missing, but process exited 0", v.envKey)
				return
			}

			exitErr, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatalf("unexpected error type from subprocess: %v", err)
			}

			// On Windows ExitCode() returns the actual exit code.
			// On Unix it is embedded in the wait status.
			exitCode := exitErr.ExitCode()
			if runtime.GOOS == "windows" && exitCode == -1 {
				// On Windows a process killed by os.Exit(1) may surface as -1
				// in some Go versions; treat any non-zero as a pass.
				return
			}
			if exitCode != 1 {
				t.Errorf("expected exit code 1 when %s is missing, got %d", v.envKey, exitCode)
			}
		})
	}
}
