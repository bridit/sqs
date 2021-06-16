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
      $awsConfig = config('aws', []);

      return new SqsClient([
        'accessKeyId' => Arr::get($awsConfig, 'credentials.key'),
        'accessKeySecret' => Arr::get($awsConfig, 'credentials.secret'),
        'region' => Arr::get($awsConfig, 'region', 'sa-east-1'),
      ]);
    });
  }

}
