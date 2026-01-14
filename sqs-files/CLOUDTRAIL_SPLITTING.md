# CloudTrail Record Splitting in sqs-files Adapter

## Overview
The sqs-files adapter now supports automatic splitting of CloudTrail events that contain multiple records.

## Configuration

Add this option to your config:

```yaml
split_cloudtrail_records: true
```

## How It Works

1. **Downloads file from S3** - As before, the adapter downloads the file referenced in the SQS message

2. **Detects CloudTrail format** - Checks if the file contains:
   - `event.Records[]` structure (with routing wrapper)
   - `Records[]` structure (direct CloudTrail format)

3. **Splits records** - Each record in the array is extracted and sent as an individual message

4. **Preserves metadata** - Each split record maintains:
   - Original `routing` information
   - Original `ts` timestamp
   - Individual `event` data

5. **Backward compatible** - If the file is not a CloudTrail event or splitting is disabled, it processes the entire file as before

## Example

### Input file (1 file with 6 records):
```json
{
  "event": {
    "Records": [
      { "eventName": "DescribeTargetHealth", ... },
      { "eventName": "DescribeTargetHealth", ... },
      { "eventName": "DescribeTargetHealth", ... },
      { "eventName": "AssumeRole", ... },
      { "eventName": "DescribeTargetHealth", ... },
      { "eventName": "DescribeTargetHealth", ... }
    ]
  },
  "routing": { ... },
  "ts": "2026-01-14 17:30:10"
}
```

### Output (6 separate messages sent to USP):
```json
// Message 1
{
  "event": { "eventName": "DescribeTargetHealth", ... },
  "routing": { ... },
  "ts": "2026-01-14 17:30:10"
}

// Message 2
{
  "event": { "eventName": "DescribeTargetHealth", ... },
  "routing": { ... },
  "ts": "2026-01-14 17:30:10"
}

// ... and so on for all 6 records
```

## Benefits

- **Better granularity** - Each CloudTrail event is processed individually
- **Improved querying** - Easier to search and filter individual events
- **Session tracking** - Can track individual API calls per session
- **Flexible** - Works with both wrapped and unwrapped CloudTrail formats
- **Safe fallback** - Non-CloudTrail files are processed normally

## Config Example

```yaml
client_options:
  hostname: "cloudtrail-adapter"
  oid: "your-org-id"
  installation_key: "your-key"

access_key: "AKIA..."
secret_key: "secret..."
queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/cloudtrail-queue"
region: "us-east-1"

# Optional: for cross-account access
role_arn: "arn:aws:iam::999888777666:role/CloudTrailRole"
external_id: "secure-id"

# Enable CloudTrail splitting
split_cloudtrail_records: true

bucket_path: "bucket"
file_path: "s3/objectKey"
```
