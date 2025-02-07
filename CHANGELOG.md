# Changelog

## v1.26.4 - Upcoming Release

### Enhancements

- **New Flags:** Introduced `-help` and `-version` flags. The `-help` flag displays usage information,  
  while the `-version` flag prints the tool's version.

- **Improved Messaging:** Refined standard output (stdout) and error output (stderr) messages for better  
  clarity in both normal and error scenarios.

  - Previously, error messages were often duplicated, making it difficult to identify critical issues as  
    they were buried in unnecessary output (e.g., the list of available adapters). Now, the list of  
    adapters is displayed only when the tool is run without any CLI arguments or when the `-help` flag  
    is used.

  - **Potential Breaking Change:** If you rely on parsing the tool's stdout or stderr in scripts or  
    automation, adjustments may be necessary. Note that the tool’s output is not considered a stable API  
    and may change at any time. Instead, it's recommended to rely on exit status codes, as the tool  
    returns a non-zero status on errors.