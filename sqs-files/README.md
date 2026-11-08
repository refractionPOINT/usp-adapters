# SQS file delivery and retained notifications

A notification is deleted from SQS only after all of its referenced files have been downloaded, decoded, and acknowledged by USP. Delivery is at least once: interrupted processing, uncertain visibility renewal, or incomplete cleanup can replay records that were already delivered.

Permanently unprocessable notifications produce the existing warning callback and remain in the source queue. These include malformed JSON, missing bucket or file fields, invalid object-key escaping, and file decode failures. They do not invoke the fatal error callback or stop other valid notifications from being processed. A notification with several files is retained in full if any file is permanently unprocessable.

The S3 `s3:TestEvent` control notice contains no telemetry. It is recognized, warned about, and retained rather than deleted.

A source redrive policy and dead-letter queue are required to quarantine retained failures. The adapter does not create or change that policy. Without redrive, these notifications can be received repeatedly until the source queue's retention period expires.

Transport, source, admission, acknowledgement-observation, and visibility-ownership failures remain fatal: they use the error callback and stop collection conservatively. Graceful shutdown first stops intake and allows delivered notifications to be acknowledged and deleted within its shutdown budget; uncertain work remains available for source replay.
