<?php

namespace Snerdmq;

class SnerdQueue
{
    private $binary_path;
    private $storage_path;
    private $shards;
    private $max_local_shards;
    private $max_workers;
    private $handlers = [];
    private $maxRetryHandlers = [];

    private $process;
    private $pipes;
    private $is_shutting_down = false;
    private $pending_acks = [];
    private $tick_activity = false;
    private $owned_shards = [];
    public static $current_task_id = null;

    public function __construct(?string $binary_path = null, ?string $storage_path = null, ?int $shards = null, ?int $maxLocalShards = null, ?int $maxWorkers = null)
    {
        $this->binary_path = $binary_path;
        $this->storage_path = $storage_path;
        $this->shards = $shards;
        $this->max_local_shards = $maxLocalShards;
        $this->max_workers = $maxWorkers;

        if ($this->binary_path === null) {
            $ext = (PHP_OS_FAMILY === 'Windows') ? '.exe' : '';
            $this->binary_path = realpath(__DIR__ . "/../bin/snerdmq{$ext}");
        }

        if (!file_exists($this->binary_path)) {
            throw new \RuntimeException("[Snerd] Binary not found at {$this->binary_path}. Ensure it is compiled or run 'php bin/snerdmq-install.php'.");
        }
    }

    public function registerHandler(string $task_type, callable $callback): void
    {
        $this->handlers[$task_type] = $callback;
        
        if (is_resource($this->process) && !$this->is_shutting_down) {
            $this->sendMessage([
                'action' => 'register',
                'task_type' => $task_type
            ]);
        }
    }

    public function registerMaxRetryHandler(string $task_type, callable $callback): void
    {
        $this->maxRetryHandlers[$task_type] = $callback;
    }

    public function startListening(): void
    {
        $cmd = escapeshellarg($this->binary_path);
        if ($this->storage_path) {
            $cmd .= " " . escapeshellarg($this->storage_path);
        }

        // Build env array with sharding options merged over the current environment.
        $env = getenv();
        if ($this->shards !== null)           $env['SNERD_SHARDS']      = (string)$this->shards;
        if ($this->max_local_shards !== null) $env['SNERD_MAX_SHARDS']  = (string)$this->max_local_shards;
        if ($this->max_workers !== null)      $env['SNERD_MAX_WORKERS'] = (string)$this->max_workers;

        $descriptorspec = [
            0 => ["pipe", "r"],  // stdin
            1 => ["pipe", "w"],  // stdout
            2 => ["pipe", "w"]   // stderr
        ];

        $this->process = proc_open($cmd, $descriptorspec, $this->pipes, null, $env);

        if (!is_resource($this->process)) {
            throw new \RuntimeException("Failed to spawn SnerdMQ process.");
        }

        // Make stdout non-blocking for stream_select
        stream_set_blocking($this->pipes[1], false);

        // Register existing handlers
        foreach (array_keys($this->handlers) as $task_type) {
            $this->sendMessage([
                'action' => 'register',
                'task_type' => $task_type
            ]);
        }
    }

    public function enqueue(string $task_id, string $task_type, array $data, int $max_retries = 3, float $retry_after_hours = 0.0, ?string $rate_limit_group = null, ?int $max_per_minute = null, ?bool $auto_dedupe = null, ?float $urgency_score = null, $execute_at = null, ?string $cron = null, ?string $webhook_url = null, ?int $max_execution_seconds = null, ?string $pool = null): void
    {
        if (!is_resource($this->process) || $this->is_shutting_down) {
            throw new \RuntimeException("[Snerd] Cannot enqueue task: Queue is not running. Call startListening first.");
        }

        $payload = [
            'action' => 'enqueue',
            'task_id' => $task_id,
            'task_type' => $task_type,
            'task_data' => json_encode($data),
            'max_retries' => $max_retries,
            'retry_after_hours' => $retry_after_hours
        ];

        if ($rate_limit_group !== null) {
            $payload['rate_limit_group'] = $rate_limit_group;
        }
        if ($max_per_minute !== null) {
            $payload['max_per_minute'] = $max_per_minute;
        }
        if ($auto_dedupe !== null) {
            $payload['auto_dedupe'] = $auto_dedupe;
        }
        if ($urgency_score !== null) {
            $payload['urgency_score'] = $urgency_score;
        }
        if ($execute_at !== null) {
            if ($execute_at instanceof \DateTimeInterface) {
                // Ensure format matches what backend expects
                $payload['execute_at'] = $execute_at->format(\DateTime::RFC3339);
            } else {
                $payload['execute_at'] = (string)$execute_at;
            }
        }
        if ($cron !== null) {
            $payload['cron'] = $cron;
        }
        if ($webhook_url !== null) {
            $payload['webhook_url'] = $webhook_url;
        }
        if ($max_execution_seconds !== null) {
            $payload['max_execution_seconds'] = $max_execution_seconds;
        }
        if ($pool !== null) {
            $payload['pool'] = $pool;
        }

        $this->pending_acks[$task_id] = false;
        $this->sendMessage($payload);
        $this->waitForAck($task_id, 5);
    }

    public function tick(int $timeout_seconds = 1): void
    {
        if (!is_resource($this->process) || $this->is_shutting_down) {
            return;
        }

        $read = [$this->pipes[1]];
        $write = null;
        $except = null;

        // Block until there is data on stdout, or timeout is reached
        $num_changed_streams = stream_select($read, $write, $except, $timeout_seconds, 0);

        if ($num_changed_streams === false) {
            return; // interrupted or error
        }

        if ($num_changed_streams > 0) {
            while ($line = fgets($this->pipes[1])) {
                $line = trim($line);
                if (empty($line)) continue;
                
                $msg = json_decode($line, true);
                if ($msg && isset($msg['action'])) {
                    $this->tick_activity = true;
                    if ($msg['action'] === 'execute') {
                        $this->handleExecute($msg);
                    } elseif ($msg['action'] === 'ack') {
                        if (isset($msg['task_id'])) {
                            $this->pending_acks[$msg['task_id']] = true;
                        }
                    } elseif ($msg['action'] === 'error') {
                        if (isset($msg['task_id'])) {
                            $this->pending_acks[$msg['task_id']] = new \RuntimeException($msg['message']);
                        } else {
                            echo "[Snerd] Error from engine: {$msg['message']}\n";
                        }
                    } elseif ($msg['action'] === 'membership') {
                        // Informational only — daemon owns all routing.
                        $this->owned_shards = $msg['owned'] ?? [];
                        fwrite(STDERR, "[Snerd] Cluster: queue={$msg['queue']} shards={$msg['shards']} owned=" . json_encode($this->owned_shards) . " version={$msg['version']}\n");
                    } elseif ($msg['action'] === 'progress') {
                        // Persist progress events so the dashboard (which polls
                        // via HTTP in PHP) can display them in the Progress Stream.
                        $this->appendProgressEvent($msg);
                    } elseif ($msg['action'] === 'max_retries_reached') {
                        if (isset($this->maxRetryHandlers[$msg['task_type']])) {
                            try {
                                $raw_data = $msg['task_data'];
                                $task_data = is_string($raw_data) ? json_decode($raw_data, true) : $raw_data;
                                call_user_func($this->maxRetryHandlers[$msg['task_type']], $task_data);
                            } catch (\Exception $e) {
                                echo "[Snerd] Error in max retry handler for task {$msg['task_id']}: {$e->getMessage()}\n";
                            }
                        } else {
                            echo "[Snerd] Dead Letter Queue: Task {$msg['task_id']} ({$msg['task_type']}) permanently failed.\n";
                        }
                    }
                }
            }
        }
    }

    public function listenLoop(): void
    {
        while (!$this->is_shutting_down && is_resource($this->process)) {
            $this->tick();
        }
    }

    public function shutdown(): void
    {
        $this->is_shutting_down = true;

        if (is_resource($this->process)) {
            $status = proc_get_status($this->process);
            if ($status['running']) {
                proc_terminate($this->process, 15); // SIGTERM — triggers daemon drain
            }

            // Keep reading from stdout during drain so execute responses reach handlers.
            $deadline = time() + 35;
            while (time() < $deadline) {
                $status = proc_get_status($this->process);
                if (!$status['running']) break;
                $this->tick(1);
            }

            fclose($this->pipes[0]);
            fclose($this->pipes[1]);
            fclose($this->pipes[2]);

            proc_close($this->process);
        }
    }

    /** Returns the shard keys owned by this instance (last membership event). */
    public function getOwnedShards(): array
    {
        return $this->owned_shards;
    }

        private function waitForAck(string $task_id, int $timeout_seconds = 5): void
    {
        $start = time();
        while (time() - $start < $timeout_seconds) {
            if (isset($this->pending_acks[$task_id])) {
                if ($this->pending_acks[$task_id] === true) {
                    unset($this->pending_acks[$task_id]);
                    return;
                }
                if ($this->pending_acks[$task_id] instanceof \Exception) {
                    $e = $this->pending_acks[$task_id];
                    unset($this->pending_acks[$task_id]);
                    throw $e;
                }
            }
            $this->tick_activity = false;
            $this->tick(1);
            if ($this->tick_activity) {
                // The queue was busy executing real work (e.g. other task handlers),
                // so don't count that time against the ack timeout.
                $start = time();
            }
        }
        throw new \RuntimeException("[Snerd] Timeout waiting for daemon Ack on task $task_id");
    }

    private function sendMessage(array $msg): void
    {
        if ($this->is_shutting_down || !is_resource($this->process)) return;

        // Check if daemon process is still alive
        $status = proc_get_status($this->process);
        if (!$status['running']) return;

        $json = json_encode($msg) . "\n";
        @fwrite($this->pipes[0], $json);
        @fflush($this->pipes[0]);
    }

    private function appendProgressEvent(array $msg): void
    {
        $dir = $this->storage_path ?: './.snerdata';
        if (!is_dir($dir)) return;
        $path = $dir . '/progress_events.log';

        $event = json_encode([
            'ts' => microtime(true),
            'task_id' => $msg['task_id'] ?? null,
            'data' => $msg['data'] ?? ''
        ]) . "\n";
        file_put_contents($path, $event, FILE_APPEND | LOCK_EX);

        // Keep the file bounded: retain only the most recent events
        if (filesize($path) > 512 * 1024) {
            $lines = file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
            $tail = array_slice($lines, -200);
            file_put_contents($path, implode("\n", $tail) . "\n", LOCK_EX);
        }
    }

    private function handleExecute(array $msg): void
    {
        $task_id = $msg['task_id'];
        $task_type = $msg['task_type'];
        $max_execution_seconds = $msg['max_execution_seconds'] ?? null;
        
        $raw_data = $msg['task_data'];
        $task_data = is_string($raw_data) ? json_decode($raw_data, true) : $raw_data;

        if (!isset($this->handlers[$task_type])) {
            $this->sendMessage([
                'action' => 'result',
                'task_id' => $task_id,
                'status' => 'error',
                'error_msg' => 'No handler registered.'
            ]);
            return;
        }

        try {
            self::$current_task_id = $task_id;
            
            if ($max_execution_seconds !== null && function_exists('pcntl_alarm')) {
                pcntl_async_signals(true);
                pcntl_signal(SIGALRM, function() use ($max_execution_seconds) {
                    throw new \RuntimeException("Task execution timed out after {$max_execution_seconds} seconds");
                });
                pcntl_alarm($max_execution_seconds);
            }
            
            call_user_func($this->handlers[$task_type], $task_data);
            
            if ($max_execution_seconds !== null && function_exists('pcntl_alarm')) {
                pcntl_alarm(0);
                pcntl_signal(SIGALRM, SIG_DFL);
            }
            
            self::$current_task_id = null;
            $this->sendMessage([
                'action' => 'result',
                'task_id' => $task_id,
                'status' => 'success'
            ]);
            self::$current_task_id = null;
        } catch (\Exception $e) {
            $this->sendMessage([
                'action' => 'result',
                'task_id' => $task_id,
                'status' => 'error',
                'error_msg' => $e->getMessage()
            ]);
        }
    }

    public function yieldProgress($data): void
    {
        if (self::$current_task_id === null) {
            throw new \RuntimeException("[Snerd] yieldProgress must be called within a task handler context.");
        }
        $this->sendMessage([
            'action' => 'progress',
            'task_id' => self::$current_task_id,
            'data' => is_string($data) ? $data : json_encode($data)
        ]);
    }

    public function startDashboard(int $port = 8080): void
    {
        $routerPath = __DIR__ . '/router.php';
        $staticPath = __DIR__ . '/../static';
        if (!file_exists($staticPath)) {
            $staticPath = __DIR__ . '/../../../../static';
        }
        $storage = $this->storage_path ?: './.snerdata';
        $cmd = "SNERD_STORAGE=" . escapeshellarg($storage) . " php -S 0.0.0.0:$port -t " . escapeshellarg($staticPath) . " " . escapeshellarg($routerPath);
        
        echo "[Snerd] Dashboard running on http://localhost:$port\n";
        if (strtoupper(substr(PHP_OS, 0, 3)) === 'WIN') {
            pclose(popen("start /B " . $cmd, "r"));
        } else {
            exec($cmd . " > /dev/null 2>&1 &");
        }
    }

}
