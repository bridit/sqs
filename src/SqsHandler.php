<?php

namespace Brid\Sqs;

use Brid\Core\Handlers\Handler;
use Throwable;

abstract class SqsHandler extends Handler
{

  /**
   * @param mixed $event
   * @param null $context
   * @throws Throwable
   */
  public function handleSqs(mixed $event, $context = null): void
  {
    foreach ($event->getRecords() as $sqsRecord)
    {
      $recordData = $sqsRecord->toArray();

      $arnParts = explode(':', $recordData['eventSourceARN']);
      $queue = end($arnParts);

      $jobData = [
        'MessageId' => $recordData['messageId'],
        'ReceiptHandle' => $recordData['receiptHandle'],
        'Attributes' => $recordData['attributes'],
        'Arn' => $recordData['eventSourceARN'],
        'Queue' => $queue,
        'Body' => $recordData['body'], //$this->getRecordBody($recordData),
      ];

      $job = new SqsJob($this, $jobData);

      $this->process($job);

    }
  }

  /**
   * @param SqsJob $job
   * @throws Throwable
   */
  protected function process(SqsJob $job): void
  {
    try {
      $job->fire();
    } catch (Throwable $e) {
      // Report exception to defined log channel
//      $this->exceptions->report($e);

      // Rethrow the exception to let SQS handle it
      throw $e;
    }
  }

}