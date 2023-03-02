# Amazon Key Management Service

You can activate encryption on SQS using [Amazon's KMS](https://aws.amazon.com/kms/) by setting the `KmsMasterKeyId` on your DSN:

    sqs://AWS_ID:AWS_KEY@?KmsMasterKeyId=alias/aws/sqs
    
You can also use [your own key](https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html) by using the key ID, which would be a value like: `XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXXX` as the value for `KmsMasterKeyId`.


## Search

* sqs encryption tutorial - [Tutorial: Creating an Amazon SQS Queue with Server-Side Encryption (SSE)](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-create-queue-sse.html)


## Links

* [Why you need to know about AWS Server-Side Encryption for SQS?](https://blog.cloudconformity.com/aws-server-side-encryption-for-sqs-c3c84267823e)
* [Protecting Amazon SQS Data Using Server-Side Encryption (SSE) and AWS KMS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html)
* [Tutorial: Configuring Server-Side Encryption (SSE) for an Existing Amazon SQS Queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-configure-sse-existing-queue.html)
