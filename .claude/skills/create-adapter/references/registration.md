# Registering a new adapter

After the `myadapter/` package compiles, wire it into the general container so
`./general myadapter ...` runs it. Three files, plus the root README.

## 1. `containers/conf/all.go`

Add the import (keep the alphabetical-ish grouping with the others):

```go
import (
    // ...
    usp_myadapter "github.com/refractionPOINT/usp-adapters/myadapter"
    // ...
)
```

Add a field to the `GeneralConfigs` struct. The `json`/`yaml` tag is the
adapter's CLI/config name:

```go
type GeneralConfigs struct {
    // ...
    ThreatLocker usp_threatlocker.ThreatLockerConfig `json:"threatlocker" yaml:"threatlocker"`
    MyAdapter    usp_myadapter.MyAdapterConfig       `json:"myadapter" yaml:"myadapter"`
    // ...
}
```

## 2. `containers/general/tool.go`

Add the import:

```go
"github.com/refractionPOINT/usp-adapters/myadapter"
```

Add a dispatch branch in `runAdapter` (the `else if` chain ending in `unknown
adapter_type`). Match the existing branches exactly:

```go
} else if method == "myadapter" {
    configs.MyAdapter.ClientOptions = applyLogging(configs.MyAdapter.ClientOptions)
    configs.MyAdapter.ClientOptions.Architecture = "usp_adapter"
    configToShow = configs.MyAdapter
    client, chRunning, err = usp_myadapter.NewMyAdapter(ctx, configs.MyAdapter)
}
```

(Use whatever package alias the import resolves to — `threatlocker`'s package is
`usp_threatlocker`, so its branch calls `usp_threatlocker.NewThreatLockerAdapter`.
Name your package `usp_myadapter` and the call is `usp_myadapter.NewMyAdapter`.)

## 3. Root `README.md`

Add a short section under "## Examples" describing the adapter and a run line,
mirroring the ThreatLocker/Gmail entries. Link to `myadapter/README.md` for the
full config reference.

## 4. Build to verify

```
go fmt ./myadapter/...
go build ./containers/general
```

The config field shows up automatically in `./general` (no args) usage output
because `printUsage` reflects over `Configuration` / `GeneralConfigs`.
