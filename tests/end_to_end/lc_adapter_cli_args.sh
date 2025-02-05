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