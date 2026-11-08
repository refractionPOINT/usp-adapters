# SQS file delivery and retained notifications

A notification is deleted from SQS only after all of its readable referenced files have been downloaded, decoded, and acknowledged by USP. Delivery is at least once: interrupted processing, uncertain visibility renewal, or incomplete cleanup can replay records that were already delivered.

A definitively missing object in an existing bucket (404 / NoSuchKey / NotFound from the object read) produces a warning and counts as complete: there are no bytes left to deliver. If the notification also refers to existing files, those files still require USP delivery acknowledgement before deletion. An all-missing-object notification needs no delivery dependency, but deletion still requires known SQS lease ownership.

Bucket-level failures, including missing buckets, access denied, region lookup errors and a bucket disappearing after lookup, retain the notification and invoke the error callback. They may indicate a wrong bucket setting or mapping and must not silently consume the queue. Permission errors, server failures, transport failures, and cancellation never count as missing-object completion.

A file that was successfully downloaded but cannot be decoded is skipped with an existing warning callback. Each warning reports `skipped_undecodable_files=1`, the bucket, and the actual object key used for the download (after configured key decoding). The original object is left untouched in its bucket. The notification can delete after every readable file is acknowledged by USP; a decode-only notification needs no delivery dependency. Decode failures do not require a quarantine or dead-letter queue.

Invalid notification envelopes/configuration remain distinct and retained. Malformed JSON, missing body/bucket/file fields, and invalid object-key escaping produce warnings and leave the notification in SQS. They do not invoke the fatal error callback or stop other valid notifications.

The S3 `s3:TestEvent` control notice contains no telemetry. It is recognized, warned about, and retained rather than deleted.

A source redrive policy and dead-letter queue are required to quarantine retained invalid envelopes/configuration and control notices. The adapter does not create or change that policy. Without redrive, these notifications can be received repeatedly until the source queue's retention period expires.

Transient download failures (network errors, throttling, or server failures) retain the entire notification for retry; already delivered readable files may replay on redelivery. Permission errors such as 403 remain fatal. Transport, source, admission, acknowledgement-observation, and visibility-ownership failures remain fatal: they use the error callback and stop collection conservatively. Graceful shutdown first stops intake and allows delivered notifications to be acknowledged and deleted within its shutdown budget; uncertain work remains available for source replay.

## Optional visibility renewal

Existing IAM policies do not need `sqs:ChangeMessageVisibility`. Renewal remains preferred when allowed because it reduces concurrent redelivery during long processing and acknowledgement waits.

A denial of visibility renewal disables further renewal attempts for this adapter instance and emits one counted warning (`renewal_disabled_count=1`). It does not invoke the fatal error callback or create an OrgError. The adapter retains the original known visibility deadline as ownership evidence; it never assumes the denied request extended it.

In this native no-renew mode, downloads, admission, and receipt waiting continue beyond that original visibility window under caller/shutdown cancellation. Only after every readable record has its delivery receipt does the adapter attempt one bounded `DeleteMessageBatch` using the received handles. AWS documents that an old receipt handle may succeed without deleting the message, so source replay and duplicates remain possible even after a successful response. See [AWS DeleteMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html).

Definitive expired/invalid receipt-handle responses warn with `delete_retained_count=N`, where N is the number of failed message entries, and retain those messages for natural source replay. These results release the batch slot and allow later collection without a fatal callback. The adapter does not retry that cleanup operation itself or discard SDK-buffered records. Partial successful cleanup is allowed only after every readable record in the batch has its receipt. Queue retention and redrive settings remain source-controlled.

Shutdown cancellation and bounded RPCs remain active. Receive, Delete permission, server, transport, and source-object failures retain their existing fatal policy. Normal renewal continues to enforce conservative known-ownership deadlines and uncertainty handling. No IAM checks, additional API operations, or new configuration settings are introduced. Renewal remains disabled until the adapter restarts.
