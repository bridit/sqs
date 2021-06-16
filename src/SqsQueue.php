<?php

namespace Brid\Sqs;

use AsyncAws\Sqs\Input\GetQueueAttributesRequest;
use AsyncAws\Sqs\Input\GetQueueUrlRequest;
use AsyncAws\Sqs\Input\PurgeQueueRequest;
use AsyncAws\Sqs\Input\ReceiveMessageRequest;
use AsyncAws\Sqs\Input\SendMessageRequest;
use AsyncAws\Sqs\Result\SendMessageResult;
use AsyncAws\Sqs\SqsClient;
use Brid\Core\Contracts\Foundation\Container;
use Brid\Core\Contracts\Queue\Job;
use Brid\Core\Contracts\Queue\Queue as QueueContract;
use Brid\Core\Queue\Queue;
use DateInterval;
use DateTimeInterface;
use Illuminate\Support\InteractsWithTime;
use Illuminate\Support\Str;
use Throwable;

class SqsQueue extends Queue implements QueueContract
{

  use InteractsWithTime;

  /**
   * @var Container
   */
  private Container $container;

  /**
   * The Amazon SQS instance.
   *
   * @var SqsClient
   */
  protected SqsClient $sqs;

  /**
   * The name of the default queue.
   *
   * @var string|null
   */
  protected ?string $queue;

  /**
   * @var array
   */
  protected array $config;

  /**
   * The name of the default queue.
   *
   * @var string
   */
  protected string $default;

  /**
   * The queue URL prefix.
   *
   * @var string
   */
  protected string $prefix;

  public function __construct(Container $container, SqsClient $sqs, string $queue = null)
  {
    $this->container = $container;
    $this->sqs = $sqs;
    $this->queue = $queue;
    $this->config = config('queue.connections.sqs', []);
  }

  protected function getQueueUrl(string $queue = null): string
  {
    if (!blank($this->config['prefix'] ?? null)) {
      return Str::finish($this->config['prefix'], '/') . ($queue ?? $this->config['queue']);
    }

    return $this->sqs->getQueueUrl(new GetQueueUrlRequest([
      'QueueName' => $this->getQueue($queue ?? $this->config['queue']),
    ]))->getQueueUrl();
  }

  /**
   * Push a new job onto the queue.
   *
   * @param mixed $job
   * @param string|null $queue
   * @return void
   */
  public function push(mixed $job, string $queue = null): void
  {
    $this->pushRaw(json_encode($this->getPayloadArray($job)), $queue);
  }

  /**
   * Push a raw payload onto the queue.
   *
   * @param  string  $payload
   * @param  string|null  $queue
   * @param  array  $options
   * @return SendMessageResult
   */
  public function pushRaw(string $payload, string $queue = null, array $options = []): SendMessageResult
  {
    return $this->sqs->sendMessage(new SendMessageRequest(array_merge([
      'QueueUrl' => $this->getQueueUrl($queue),
      'MessageBody' => $payload,
    ], $options)));
  }

  /**
   * Push a new job onto the queue after a delay.
   *
   * @param DateInterval|DateTimeInterface|int $delay
   * @param string  $job
   * @param string|null $queue
   * @return void
   */
  public function later(DateInterval|DateTimeInterface|int $delay, mixed $job, string $queue = null): void
  {
    $this->pushRaw(json_encode($this->getPayloadArray($job)), $queue, [
      'DelaySeconds' => $this->secondsUntil($delay),
    ]);
  }

  /**
   * Pop the next job off of the queue.
   *
   * @param string|null $queue
   * @return Job|null
   */
  public function pop(string $queue = null): ?Job
  {
    $response = $this->sqs->receiveMessage(new ReceiveMessageRequest([
      'QueueUrl' => $queue = $this->getQueue($queue),
      'AttributeNames' => ['ApproximateReceiveCount'],
    ]));

    if (! is_null($response['Messages']) && count($response['Messages']) > 0) {
      return new SqsJob($this->container, $response['Messages'][0]);
    }

    return null;
  }

  /**
   * Delete all of the jobs from the queue.
   *
   * @param string $queue
   * @return int
   */
  public function clear(string $queue): int
  {
    return tap($this->size($queue), function () use ($queue) {
      $this->sqs->purgeQueue(new PurgeQueueRequest([
        'QueueUrl' => $this->getQueue($queue),
      ]));
    });
  }

  /**
   * Get the queue or return the default.
   *
   * @param string|null $queue
   * @return string
   */
  public function getQueue(?string $queue): string
  {
    $queue = $queue ?: $this->default;

    return filter_var($queue, FILTER_VALIDATE_URL) === false
      ? rtrim($this->prefix, '/') . '/' . $queue
      : $queue;
  }

  /**
   * Get the size of the queue.
   *
   * @param string|null $queue
   * @return int
   */
  public function size(string $queue = null): int
  {
    $response = $this->sqs->getQueueAttributes(new GetQueueAttributesRequest([
      'QueueUrl' => $this->getQueue($queue),
      'AttributeNames' => ['ApproximateNumberOfMessages'],
    ]));

    return (int) $response->getAttributes()['ApproximateNumberOfMessages'];
  }

  /**
   * Handle an exception that occurred while processing a job.
   *
   * @param  Job  $queueJob
   * @param  Throwable  $e
   * @return void
   *
   * @throws Throwable
   */
  protected function handleException(Job $queueJob, Throwable $e): void
  {
    $queueJob->fail($e);

    throw $e;
  }

}