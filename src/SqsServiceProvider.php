<?php

namespace Brid\Sqs;

use AsyncAws\Sqs\SqsClient;
use Brid\Core\Foundation\Providers\ServiceProvider;
use Illuminate\Support\Arr;

class SqsServiceProvider extends ServiceProvider
{

  public function boot(): void
  {
    $this->container->set(SqsClient::class, function() {
      $sqsConfig = config('queue.connections.sqs', []);
      $awsConfig = config('aws', []);

      return new SqsClient([
        'accessKeyId' => $sqsConfig['key'] ?? Arr::get($awsConfig, 'credentials.key'),
        'accessKeySecret' => $sqsConfig['secret'] ?? Arr::get($awsConfig, 'credentials.secret'),
        'region' => $sqsConfig['region'] ?? $awsConfig['region'] ?? 'us-east-1',
        'endpoint' => $sqsConfig['endpoint'] ?? 'https://%service%.%region%.amazonaws.com',
      ]);
    });
  }

}
