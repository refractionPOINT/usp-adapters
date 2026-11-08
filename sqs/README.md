# SQS delivery

## Optional visibility renewal

Existing IAM policies do not need `sqs:ChangeMessageVisibility`. Renewal remains preferred when allowed because it reduces concurrent redelivery during long processing and acknowledgement waits.

A denial of visibility renewal disables further renewal attempts for this adapter instance and emits one counted warning (`renewal_disabled_count=1`). It does not invoke the fatal error callback or create an OrgError. The adapter retains the original known visibility deadline as ownership evidence; it never assumes the denied request extended it.

In this native no-renew mode, downloads, admission, and receipt waiting continue beyond that original visibility window under caller/shutdown cancellation. Only after every readable record has its delivery receipt does the adapter attempt one bounded `DeleteMessageBatch` using the received handles. AWS documents that an old receipt handle may succeed without deleting the message, so source replay and duplicates remain possible even after a successful response. See [AWS DeleteMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html).

Definitive expired/invalid receipt-handle responses warn with `delete_retained_count=N`, where N is the number of failed message entries, and retain those messages for natural source replay. These results release the batch slot and allow later collection without a fatal callback. The adapter does not retry that cleanup operation itself or discard SDK-buffered records. Partial successful cleanup is allowed only after every readable record in the batch has its receipt. Queue retention and redrive settings remain source-controlled.

Shutdown cancellation and bounded RPCs remain active. Receive, Delete permission, server, transport, and source-object failures retain their existing fatal policy. Normal renewal continues to enforce conservative known-ownership deadlines and uncertainty handling. No IAM checks, additional API operations, or new configuration settings are introduced. Renewal remains disabled until the adapter restarts.
