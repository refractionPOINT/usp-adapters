#!/usr/bin/env bats

ROOT_DIR=$(realpath "$(dirname "${BATS_TEST_FILENAME}")/../../")
FIXTURES_DIR="${ROOT_DIR}/tests/fixtures/configs"

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

@test "service mode (arbitrary args)" {
  run ./lc-adapter -service
  [ "$status" -eq 0 ]
  [[ "$output" == *"starting"* ]]
  [[ "$output" == *"service mode"* ]]
}

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

@test "valid config (syslog), invalid oid" {
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

@test "valid config (file) - auth.log" {
  ./lc-adapter file "${FIXTURES_DIR}/authlog.yaml" > output.log 2>&1 &
  pid=$!
  sleep 2
  kill ${pid}
  wait ${pid} 2>/dev/null || true
  run grep "starting" output.log
  run grep "loading config from file: ${FIXTURES_DIR}/syslog.yaml" output.log
  run grep "found 1 configs to run" output.log
  run grep "starting adapter: file" output.log
  run grep "Configs in use (file):" output.log
}

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