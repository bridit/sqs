<?php

namespace Brid\Sqs;

use AsyncAws\Sqs\Input\DeleteMessageRequest;
use AsyncAws\Sqs\SqsClient;
use Brid\Core\Contracts\Foundation\Container;
use Brid\Core\Contracts\Queue\Job as JobContract;
use Brid\Core\Queue\Job;

class SqsJob extends Job implements JobContract
{

  /**
   * The Amazon SQS client instance.
   *
   * @var SqsClient
   */
  protected SqsClient $sqs;

  /**
   * The Amazon SQS job instance.
   *
   * @var array
   */
  protected array $jobData;

  /**
   * Create a new job instance.
   *
   * @param Container $container
   * @param array $jobData
   * @param string|null $queue
   * @param string|null $connectionName
   */
  public function __construct(Container $container, array $jobData, string $queue = null, string $connectionName = null)
  {
    $this->container = $container;
    $this->sqs = $this->container->get(SqsClient::class);
    $this->jobData = $jobData;
    $this->queue = $queue ?? $this->jobData['Queue'] ?? null;
    $this->connectionName = $connectionName ?? 'sqs';
  }

  /**
   * Release the job back into the queue.
   *
   * @param int $delay
   * @return void
   */
  public function release(int $delay = 0)
  {
    parent::release($delay);

    $this->sqs->changeMessageVisibility([
      'QueueUrl' => $this->queue,
      'ReceiptHandle' => $this->jobData['ReceiptHandle'],
      'VisibilityTimeout' => $delay,
    ]);
  }

  /**
   * Delete the job from the queue.
   *
   * @return void
   */
  public function delete(): void
  {
    parent::delete();

    $this->sqs->deleteMessage(new DeleteMessageRequest([
      'QueueUrl' => config('queue.connections.sqs.prefix') . '/' . $this->queue,
      'ReceiptHandle' => $this->jobData['ReceiptHandle'],
    ]));
  }

  /**
   * Get the number of times the job has been attempted.
   *
   * @return int
   */
  public function attempts(): int
  {
    return (int) $this->jobData['Attributes']['ApproximateReceiveCount'];
  }

  /**
   * Get the job identifier.
   *
   * @return string
   */
  public function getJobId(): string
  {
    return $this->jobData['MessageId'];
  }

  /**
   * Get the raw body string for the job.
   *
   * @return string
   */
  public function getRawBody(): string
  {
    return $this->jobData['Body'];
  }

}
