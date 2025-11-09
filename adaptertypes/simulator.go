package adaptertypes

// SimulatorConfig defines the configuration for the simulator adapter (testing/replay)
type SimulatorConfig struct {
	ClientOptions  ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Reader         ReadCloser    `json:"-" yaml:"-"`
	FilePath       string        `json:"file_path" yaml:"file_path" description:"Path to file containing events to replay" category:"source"`
	IsReplayTiming bool          `json:"is_replay_timing" yaml:"is_replay_timing" description:"Replay events with original timing" category:"behavior" default:"false" llmguidance:"If true, respects timestamp gaps between events. If false, sends as fast as possible"`
}
