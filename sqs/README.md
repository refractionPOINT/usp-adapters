# SQS delivery

## Optional visibility renewal

Existing IAM policies do not need `sqs:ChangeMessageVisibility` for collection to work. Renewal remains preferred for long processing and acknowledgement waits when that permission is available.

A denial of the visibility-renewal operation disables further renewal attempts for this adapter instance and emits one counted warning (`renewal_disabled_count=1`). It does not invoke the fatal error callback or create an OrgError. Without renewal, the adapter uses only the conservative original visibility deadline; it never assumes the denied request extended ownership.

Receipts arriving within that original deadline allow normal cleanup. Work, receipt waits, or cleanup that exceed it retain the notification for natural source replay, release the leased-batch slot, and let the collector continue later work. Already-buffered SDK records remain buffered and may still deliver, so duplicates are possible. Very slow work may need renewal permission to make progress; queue retention and redrive settings still apply.

Shutdown/abort and deadline cancellation remain active. Permission or other errors on Receive, Delete, or source-object access retain their existing fatal policy; this fallback applies only to visibility renewal. There are no additional IAM checks, source API operations, or new configuration settings.
