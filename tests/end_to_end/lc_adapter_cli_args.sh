#!/usr/bin/env bats

ROOT_DIR=$(realpath "$(dirname "${BATS_TEST_FILENAME}")/../../")
FIXTURES_DIR="${ROOT_DIR}/tests/fixtures/configs"

# Set up functions (run after and before every test function)
setup() {
  # Create a secure temp file for test output
  export TEST_LOG_FILE=$(mktemp -t lc_adapter_test.XXXXXXXXXX)
}

teardown() {
  # Clean up temp file after test
  echo "Cleaning up temp file: ${TEST_LOG_FILE}"
  #rm -f "${TEST_LOG_FILE}"
}

# Edge cases
@test "no args provided, should print usage with available configs" {
  run ./lc-adapter
  [ "$status" -eq 1 ]
  [[ "$output" == *"error: not enough arguments"* ]]
  [[ "$output" == *"Usage: lc-adapter [-help] [-version] adapter_type [config_file.yaml | <param>...]"* ]]
  [[ "$output" == *"Available configs:"* ]]
}

@test "not enough args provided, should print usage with available configs" {
  run ./lc-adapter file
  [ "$status" -eq 1 ]
  [[ "$output" == *"error: not enough arguments"* ]]
  [[ "$output" == *"Usage: lc-adapter [-help] [-version] adapter_type [config_file.yaml | <param>...]"* ]]
  [[ "$output" == *"Available configs:"* ]]
}

# Service mode
@test "service mode (arbitrary args)" {
  run ./lc-adapter -service
  [ "$status" -eq 0 ]
  [[ "$output" == *"starting"* ]]
  [[ "$output" == *"service mode"* ]]
}

# Invalid configs
@test "invalid config file provided, doesn't exist, prints error and usage without available configs" {
  run ./lc-adapter file doesnt_exist.yaml
  [ "$status" -eq 1 ]
  [[ "$output" == *"loading config from file: doesnt_exist.yaml"* ]]
  [[ "$output" == *"error: os.Open(): open doesnt_exist.yaml: no such file or directory"* ]]
  [[ "$output" == *"Usage: lc-adapter [-help] [-version] adapter_type [config_file.yaml | <param>...]"* ]]
  [[ "$output" == *"For a list of all the available configs, run: lc-adapter -help or run the program without any arguments"* ]]
  [[ "$output" != *"Available configs:"* ]]
}

@test "invalid config file provided, invalid yaml, prints error and usage without available configs" {
  run ./lc-adapter file "${FIXTURES_DIR}/invalid_yaml.yaml"
  [ "$status" -eq 1 ]
  [[ "$output" == *"loading config from file: ${FIXTURES_DIR}/invalid_yaml.yaml"* ]]
  [[ "$output" == *"error: decoding error: json=invalid character '-' in numeric literal yaml=yaml: line 4: could not find expected ':'"* ]]
  [[ "$output" == *"Usage: lc-adapter [-help] [-version] adapter_type [config_file.yaml | <param>...]"* ]]
  [[ "$output" == *"For a list of all the available configs, run: lc-adapter -help or run the program without any arguments"* ]]
  [[ "$output" != *"Available configs:"* ]]
}

@test "invalid config file provided, multi doc invalid yaml, prints error and usage without available configs" {
  run ./lc-adapter file "${FIXTURES_DIR}/invalid_yaml_multi_doc.yaml"
  [ "$status" -eq 1 ]
  [[ "$output" == *"loading config from file: ${FIXTURES_DIR}/invalid_yaml_multi_doc.yaml"* ]]
  [[ "$output" == *"error: decoding error: json=invalid character '-' in numeric literal yaml=yaml: line 8: could not find expected ':'"* ]]
  [[ "$output" == *"Usage: lc-adapter [-help] [-version] adapter_type [config_file.yaml | <param>...]"* ]]
  [[ "$output" == *"For a list of all the available configs, run: lc-adapter -help or run the program without any arguments"* ]]
  [[ "$output" != *"Available configs:"* ]]
}

# Valid configs
@test "valid config (syslog) - invalid oid" {
  run ./lc-adapter syslog "${FIXTURES_DIR}/syslog.yaml"
  [ "$status" -eq 1 ]
  [[ "$output" == *"starting"* ]]
  [[ "$output" == *"loading config from file: ${FIXTURES_DIR}/syslog.yaml"* ]]
  [[ "$output" == *"found 1 configs to run"* ]]
  [[ "$output" == *"starting adapter: syslog"* ]]
  [[ "$output" == *"Configs in use (syslog):"* ]]
  [[ "$output" == *"client_options:"* ]]
  [[ "$output" == *"error instantiating client: invalid client options: invalid OID: invalid client options: invalid UUID length: 5"* ]]  
}

@test "valid config (file) - single auth.log" {
  ./lc-adapter file "${FIXTURES_DIR}/authlog.yaml" > $TEST_LOG_FILE 2>&1 &
  pid=$!
  sleep 2
  kill ${pid}
  wait ${pid} 2>/dev/null || true
  run grep "starting" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "loading config from file: ${FIXTURES_DIR}/authlog.yaml" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "found 1 configs to run" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "starting adapter: file" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "Configs in use (file):" $TEST_LOG_FILE && [ "$status" -eq 0 ]
}

@test "valid config (file) - multi auth.log" {
  ./lc-adapter file "${FIXTURES_DIR}/authlog_multi.yaml" > $TEST_LOG_FILE 2>&1 &
  pid=$!
  sleep 2
  kill ${pid}
  wait ${pid} 2>/dev/null || true
  run grep "starting" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "loading config from file: ${FIXTURES_DIR}/authlog_multi.yaml" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "found 2 configs to run" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "starting adapter: file" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "Configs in use (file):" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  # NOTE: To able to test end to end that two clients are running, we need to either use
  # valid installation keys or expose test_sink_mode in the UPSClient and use this in
  # the config.
}

# Config options are specified as CLI args
@test "valid config (file) - options via cli args" {
  ./lc-adapter file client_options.identity.installation_key=test \
    client_options.identity.oid=62a0a22f-f30c-446e-816b-18c379b766d5 \
    client_options.platform=json \
    client_options.sensor_seed_key=file-1-cli \
    file_path=/var/auth.log.2 > $TEST_LOG_FILE 2>&1 &
  pid=$!
  sleep 2
  kill ${pid}
  wait ${pid} 2>/dev/null || true
  echo $TEST_LOG_FILE
  run grep "starting" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep -v "loading config from file" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "loading config from CLI arguments" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "starting adapter: file" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "Configs in use (file):" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "sensor_seed_key: file-1-cli" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "file_path: /var/auth.log.2" $TEST_LOG_FILE && [ "$status" -eq 0 ]
}

# Config options are specified as environment variables args
@test "valid config (file) - options via env variables" {
  # TODO: This is currently not working
  return
  export client_options.identity.installation_key=test
  export client_options.identity.oid=62a0a22f-f30c-446e-816b-18c379b766d7
  export client_options.platform=json
  export client_options.sensor_seed_key=file-2-env
  export file_path=/var/auth.log.3
  ./lc-adapter file > $TEST_LOG_FILE 2>&1 &
  pid=$!
  sleep 2
  kill ${pid}
  wait ${pid} 2>/dev/null || true
  run grep "starting" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep -v "loading config from file" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "loading config from environment variables" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "starting adapter: file" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "Configs in use (file):" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "sensor_seed_key: file-2-cli" $TEST_LOG_FILE && [ "$status" -eq 0 ]
  run grep "file_path: /var/auth.log.3" $TEST_LOG_FILE && [ "$status" -eq 0 ]
}

# Common CLI flags
@test "-help arg" {
  run ./lc-adapter -help
  [ "$status" -eq 2 ]
  [[ "$output" == *"Usage: lc-adapter [-help] [-version] adapter_type [config_file.yaml | <param>...]"* ]]
  [[ "$output" == *"Available configs:"* ]]
}

@test "-version arg" {
  run ./lc-adapter -version
  [ "$status" -eq 2 ]
  [[ "$output" == *"Version: (devel)"* ]]
  [[ "$output" != *"Available configs:"* ]]
}