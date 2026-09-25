<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="SnerdMQ PHP Logo" />
  <h1>🐘 SnerdMQ PHP SDK v0.3.4</h1>
  <p>A zero-config, C-speed background job queue for modern PHP. Ditch Redis and heavy queue workers for a simple, embedded Rust daemon.</p>

  [![Packagist Version](https://img.shields.io/packagist/v/speed-nerd/snerdmq)](https://packagist.org/packages/speed-nerd/snerdmq)
  [![Docs](https://img.shields.io/badge/docs-speed--nerd.github.io-blue)](https://speed-nerd.github.io/docs/)
</div>

This is the official PHP SDK wrapper for **SnerdMQ**. It handles all JSON-RPC communication and `proc_open` orchestration so you can write lightning-fast background jobs in Laravel, Symfony, or vanilla PHP without managing any external databases like Redis, Beanstalkd, or RabbitMQ.

## ✨ v0.3.4 AI Features
- **Smart API Rate-Limiting**: Natively tracks `rate_limit_group` execution velocity to prevent 429 "Too Many Requests" API errors.
- **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
- **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
- **Job Chaining (DAGs)**: Define complex workflow dependencies natively. Tasks wait in a blocked state until their parent tasks succeed.
- **Progress Streaming & Live Dashboard**: Handlers can stream progress updates to a built-in React UI dashboard served by the SDK.
- **Ditch Redis**: Gives your PHP apps persistent state, automatic retries, and dead-letter queues right out of the box with zero external infrastructure.
- **Zero Rust Required**: Our Composer installation script automatically downloads the pre-compiled C-speed Rust binary for your OS.
- **Non-Blocking**: Uses native PHP `stream_select` to listen to the daemon's output efficiently without pegging your CPU or requiring heavy C-extensions like Swoole.

### ⚙️ Advanced Task Configuration (v0.3.4)
To power complex AI workflows, tasks can now be configured with advanced orchestration parameters:

* **`auto_dedupe` (`bool`)**: If set to `true`, the daemon computes a cryptographic hash of the `task_type` and `data`. If an identical payload is currently sitting in the queue pending execution, this new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users!
* **`urgency_score` (`float`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a true Binary Max-Heap to continually float tasks with the highest urgency score to the very front of the execution line. Standard tasks default to `0.0`.
* **`rate_limit_group` (`string`)**: A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control.
* **`max_per_minute` (`int`)**: Used in conjunction with `rate_limit_group`. If the queue processes more tasks in this group than the allowed limit within a 60-second rolling window, further tasks in this group are temporarily paused. This natively prevents 429 "Too Many Requests" errors when bursting third-party APIs.
* **`execute_at` (`string` | `DateTimeInterface`)**: A timestamp of when the job should be executed in the future.
* **`retry_after_hours` (`float`)**: Backoff in **hours** before a failed job is retried (default `0.0`). See *Cron Jobs vs. Retryable Jobs* below.
* **`cron` (`string`)**: A cron expression (e.g. `"0 * * * *"`) for recurring jobs. Shorthands like `"2h"` or `"10m"` are also supported.
* **`webhook_url` (`string`)**: Optional URL to receive the task payload via POST request instead of local execution.
* **`max_execution_seconds` (`int`)**: Optional hard timeout in seconds. If execution takes longer, it's marked as failed. Note: Requires the `pcntl` extension, which is not supported on Windows. On Windows, the timeout is ignored locally but still enforced by the Rust daemon.
* **`trigger_after_ids` (`array`)**: A list of parent task IDs that must complete successfully before this task is allowed to dispatch. Enables complex DAG workflows natively within the queue.

### Note on Hard Timeouts (`max_execution_seconds`)
The PHP SDK uses the `pcntl_alarm` extension to enforce hard timeouts locally for runaway handlers. 
- **Requirements**: Your PHP environment must have the `pcntl` extension enabled.
- **Windows**: `pcntl` is not supported on Windows. On Windows, the SDK will skip local timeout enforcement, but the background Rust daemon will still forcefully time out the IPC channel if it takes too long.
- **Side effects**: Since `pcntl_alarm` uses process-level alarms, avoid using other `pcntl_alarm` calls within your handlers to prevent conflicts.

### 🌐 HTTP Webhooks (Serverless Execution)
You can configure a task to execute externally via an HTTP POST request. By setting a `webhook_url`, the internal background processor will skip any registered handlers (`$queue->registerHandler`) and directly invoke the HTTP endpoint.

If the HTTP endpoint returns a non-200 status code, it triggers a retry. If it permanently fails (reaches `max_retries`), the Dead Letter Queue event is automatically fired via a final HTTP POST to the same `webhook_url` but with the header `X-SnerdMQ-Event: MaxRetriesReached`.

### 🕒 Cron Jobs vs. Retryable Jobs
When using the new scheduling features, it is important to understand the difference between Cron and Retry behaviors:
> - **A Cron Job** is a *Repeatable Job* that executes again **only after a success**, on a fixed schedule.
> - **A Retryable Job** is a *Recovery Job* that executes again **only after a failure**, attempting to recover using the `retry_after_hours` backoff.
> - **Combined:** If a Cron Job fails, it temporarily uses `retry_after_hours` to retry until it recovers. Once it succeeds, it goes back to ticking on its standard cron schedule!

## 📦 Installation

Installing the SDK is a simple one-step process via Composer:

```bash
composer require speed-nerd/snerdmq
```

*Note: The moment you run this command, Composer will automatically execute our post-install hook to download the correct highly-optimized SnerdMQ binary for your operating system (macOS/Linux/Windows) directly into the `bin/` directory!*

---

## ⚡ Quickstart

Using the SDK is incredibly simple. Initialize the queue, register your handler closures, and start listening!

```php
<?php

require 'vendor/autoload.php';
use Snerdmq\SnerdQueue;

// 1. Initialize the daemon in the background
$queue = new SnerdQueue();

// 2. Register your background job logic using a standard closure
$queue->registerHandler("send_email", function($data) {
    $to = $data["to"];
    $subject = $data["subject"];
    echo "Sending email to {$to} with subject: {$subject}...\n";
    
    // Throw an Exception here to automatically trigger SnerdMQ's retry logic!
});

// 3. Start the process
$queue->startListening();
echo "SnerdMQ PHP SDK is listening for jobs...\n";

// 4. Enqueue a job from anywhere in your codebase
$queue->enqueue(
    "email-123",
    "send_email",
    ["to" => "rasmus@php.net", "subject" => "SnerdMQ Update"],
    3,              // max retries
    0.5,            // retry after hours (wait 30 minutes before retrying)
    "email_api",    // rate_limit_group
    100,            // max_per_minute
    null,           // auto_dedupe
    null,           // urgency_score
    null,           // execute_at
    null,           // cron
    null,           // webhook_url
    null            // max_execution_seconds
);

// 5. Need scheduling, deduplication, or serverless execution? All
// orchestration options are opt-in — combine only what you need:
$queue->enqueue(
    "email-digest-1",
    "send_email",
    ["to" => "rasmus@php.net", "subject" => "Daily Digest"],
    3,
    0.0,
    null,           // no rate limit group
    null,           // no max_per_minute cap
    true,           // auto_dedupe: drop identical pending payloads
    0.99,           // urgency_score: float to the front of the queue
    null,
    "0 8 * * *",    // cron: run every day at 08:00
    "https://api.example.com/webhook", // Execute via HTTP instead of local closures
    300             // max_execution_seconds: hard timeout
);

// 6. Run the event loop (usually done in a dedicated worker script)
$queue->listenLoop();
```

### ☠️ Dead Letter Queue (Handling Permanent Failures)

When a task fails repeatedly and exhausts its `max_retries`, the SnerdMQ daemon permanently moves it to the Dead Letter Queue. You can hook into this event to alert your team, update your database, or send a Slack message by registering a Max Retry Handler.

> **Delivery semantics:** SnerdMQ provides **at-least-once** delivery. In rare cases — e.g. if the daemon is killed while a task is executing — a task may be executed again after restart. Make your handlers idempotent.

```php
// 5. Catch tasks that have permanently failed (Dead Letter Queue)
$queue->registerMaxRetryHandler('send_email', function($data) {
    echo "Email task failed after all retries! Data: " . json_encode($data) . "\n";
});
```

---

## 📊 Live Dashboard

SnerdMQ ships with a built-in **React UI dashboard** served directly by the SDK (via PHP's built-in web server) — no extra services to manage in your infrastructure. It gives you a real-time window into your queue:

- **Live stats**: total enqueued, processed, and failed jobs
- **Recent Jobs table**: per-task status (`queued`, `active`, `completed`, `failed`, `dead_letter`), retry counts, and badges showing which features a task uses (cron / webhook / timeout)
- **Real-time Progress Stream**: live output from `yieldProgress` calls in your handlers

```php
$queue = new SnerdQueue();

// Start the built-in dashboard on http://localhost:9090
$queue->startDashboard(9090);

// ... register handlers, start listening, enqueue jobs ...
```

Then open **http://localhost:9090** in your browser. The dashboard UI automatically uses HTTP polling to stay up to date (progress events included), and the SDK also exposes a small JSON API (`/api/stats`, `/api/tasks`, `/api/progress`) if you want to build your own tooling on top. The dashboard assets ship inside the Composer package — nothing extra to deploy.

> **Note:** `startDashboard` only serves the UI — your jobs keep running whether or not the dashboard is open.

---

## 📡 Progress Reporting

Long-running handlers can stream live updates to the Dashboard's Progress Stream (ideal for streaming LLM tokens or multi-step ETL work):

```php
$queue->registerHandler("generate_report", function($data) use ($queue) {
    for ($step = 1; $step <= 10; $step++) {
        doWork($step);
        $queue->yieldProgress("Step {$step}/10 complete");
    }
});
```

> `yieldProgress` must be called **inside a task handler** — the SDK tracks which task is currently executing so each update lands on the right job in the dashboard.

---

## 🧩 Queue Topology: One Queue or Many?

### ✅ Recommended: one queue, all job types (singleton)

Each `SnerdQueue` client spawns its own Rust daemon and **exclusively owns** its storage directory (`.snerdata` by default). The recommended pattern is **one client per application process**: register every job type on it and serve a single shared dashboard:

```php
<?php

require 'vendor/autoload.php';
use Snerdmq\SnerdQueue;

// ONE queue client for the whole app
$queue = new SnerdQueue();

// Job type #1: image processing
$queue->registerHandler("process_image", function($data) {
    echo "Processing image: {$data['image_id']}\n";
});

// Job type #2: OTP emails — same queue, same daemon
$queue->registerHandler("send_otp_email", function($data) {
    echo "Sending OTP to: {$data['to']}\n";
});

$queue->startListening();

// Both job types flow through the exact same queue
$queue->enqueue("img-1", "process_image", ["image_id" => "abc123"], 3, 0.5);
$queue->enqueue("otp-1", "send_otp_email", ["to" => "john@wick.com"], 3, 0.5);

// ONE dashboard shows every job type
$queue->startDashboard(8080);
```

All job types share everything: the same persistent job log, retry/DLQ pipeline, rate-limit state, stats — and one dashboard at `http://localhost:8080` showing all of them.

### 🚫 Same storage twice = fails fast

The daemon takes an **exclusive OS-level lock** on its storage directory at startup. A second client on the same storage fails instead of silently double-executing your jobs:

```php
$first = new SnerdQueue();   // ✅ owns .snerdata
$second = new SnerdQueue();  // ❌ daemon refuses to start:
// "Another daemon is already running on storage '.snerdata'"
```

This applies across processes too — multiple long-running PHP CLI workers on the same machine each spawn their own daemon, so each worker needs its own `$storage_path`.

### 🔀 Need multiple queues? Give each one its own storage

```php
$images = new SnerdQueue(null, ".snerdata-images");
$emails = new SnerdQueue(null, ".snerdata-emails");

$images->startDashboard(8080); // separate dashboards, so separate ports
$emails->startDashboard(8081);
```

Now you have two fully independent engines: separate job logs, separate rate-limit state, separate dashboards. Only split when you actually need isolation (different teams, different retention, independent monitoring) — otherwise the singleton is simpler and recommended.

---

## 🌍 Advanced: Distributed Scaling

Because the daemon exclusively locks its storage directory, scaling horizontally means **one queue per server**, each with its own storage. Your load balancer routes requests across servers, and every server processes the jobs it enqueued:

```php
// Each server runs its own daemon on its own storage dir (local disk works fine)
$queue = new SnerdQueue(null, "/var/data/snerd"); // per-server storage
```

A shared network drive (AWS EFS or NFS) is still a good home for that storage when a single instance needs durable state — e.g. a container that restarts but must keep its queue. Native OS file locking (`flock`) keeps writes safe — no Redis required.

*Built with ❤️ for John Wick tier engineering.*


## Architecture Best Practices

When building production applications with SnerdMQ, it is recommended to initialize the queue as a Singleton, isolate your domain workers into separate files/functions, use Dead Letter Queues (DLQ) for failed tasks via `RegisterMaxRetryHandler`, and ensure manual graceful shutdown. The embedded Dashboard UI can also be easily served from the same instance.

```php
<?php
require 'vendor/autoload.php';

use Snerd\SnerdQueue;

$queue = new SnerdQueue(['storage_path' => './.snerdata']);

$queue->registerHandler('send_email', function($data) {
    echo "Sending email to {$data['email']}...
";
});

$queue->registerMaxRetryHandler('send_email', function($data) {
    echo "Email to {$data['email']} failed permanently. Dead letter processing...
";
});

$queue->registerHandler('process_image', function($data) {
    echo "Processing image {$data['imageId']}...
";
});

$queue->startDashboard(8080);

// Graceful shutdown
if (function_exists('pcntl_signal')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGINT, function() use ($queue) {
        $queue->shutdown();
        exit;
    });
    pcntl_signal(SIGTERM, function() use ($queue) {
        $queue->shutdown();
        exit;
    });
}

$queue->listenLoop();
```
