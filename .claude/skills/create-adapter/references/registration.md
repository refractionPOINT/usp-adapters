# Registering a new adapter

After the `myadapter/` package compiles, wire it into the general container so
`./general myadapter ...` runs it. The cleanest way is to **copy how an existing
adapter is wired** — grep for `threatlocker` (or `gmail`) across these files and
replicate every hit for your adapter. Four touch points:

## 1. `containers/conf/all.go`

- Add the package import alongside the others (`usp_myadapter "…/myadapter"` —
  match the alias style the file already uses).
- Add a field to the `GeneralConfigs` struct. The `json`/`yaml` tag **is** the
  adapter's CLI/config name. Find the `ThreatLocker` field there and add an
  analogous `MyAdapter usp_myadapter.MyAdapterConfig` line.

## 2. `containers/general/tool.go`

- Add the package import.
- In `runAdapter` (the `else if method == …` chain that ends in
  `unknown adapter_type`), copy the `else if method == "threatlocker"` branch
  and adapt it: `applyLogging`, set `Architecture = "usp_adapter"`, set
  `configToShow`, then call your `usp_myadapter.NewMyAdapter(ctx, …)`.

Read the real `threatlocker` branch in `tool.go` and the real `ThreatLocker`
field in `all.go` rather than working from memory — that way you match the
current conventions exactly (package alias, field ordering, tags).

## 3. Root `README.md`

Add a short section under "## Examples" describing the adapter and a run line,
mirroring the existing **ThreatLocker** / **Gmail** entries. Link to
`myadapter/README.md` for the full config reference.

## 4. Build to verify

```
go fmt ./myadapter/...
go build ./containers/general
```

The config field shows up automatically in `./general` (no args) usage output —
`printUsage` reflects over `Configuration` / `GeneralConfigs`.
