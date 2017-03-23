<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue;

use DateTime;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
//use PhpAmqpLib\Channel\AMQPChannel;
//use PhpAmqpLib\Connection\AMQPConnection;
//use PhpAmqpLib\Message\AMQPMessage;
//use PhpAmqpLib\Wire\AMQPTable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;

class RabbitMQQueue extends Queue implements QueueContract
{

	protected $connection;
	protected $channel;
    protected $exchange;
    protected $queue;
    protected $messageCount;

	protected $declareExchange;
	protected $declareBindQueue;

	protected $defaultQueue;
	protected $configQueue;
	protected $configExchange;
    protected $persistent;

	/**
	 * @param AMQPConnection $amqpConnection
	 * @param array          $config
	 */
	public function __construct(\AMQPConnection $amqpConnection, $config)
	{
        // default set persistent for connection type 
        if (isset($config['persistent'])) {
            $this->persistent = $config['persistent'];
        } else {
            $this->persistent = true;
        }
        if ($this->persistent) {
            $amqpConnection->pconnect();
        } else {
            $amqpConnection->connect();
        }

		$this->connection = $amqpConnection;
		$this->defaultQueue = $config['queue'];
		$this->configQueue = $config['queue_params'];
		$this->configExchange = $config['exchange_params'];
		$this->declareExchange = $config['exchange_declare'];
		$this->declareBindQueue = $config['queue_declare_bind'];

		$this->channel = $this->getChannel();
	}

	/**
	 * Push a new job onto the queue.
	 *
	 * @param  string $job
	 * @param  mixed  $data
	 * @param  string $queue
	 *
	 * @return bool
	 */
	public function push($job, $data = '', $queue = null)
	{
		return $this->pushRaw($this->createPayload($job, $data), $queue, []);
	}

	/**
	 * Push a raw payload onto the queue.
	 *
	 * @param  string $payload
	 * @param  string $queue
	 * @param  array  $options
	 *
	 * @return mixed
	 */
	public function pushRaw($payload, $queue = null, array $options = [])
	{
		$queue = $this->getQueueName($queue);
		$this->declareQueue($queue);
		if (isset($options['delay'])) {
			$queue = $this->declareDelayedQueue($queue, $options['delay']);
		}

		// push job to a queue
		$attributes = [
			'content_type'  => 'application/json',
			'delivery_mode' => AMQP_DURABLE,
		];

		// push task to a queue
		$this->exchange->publish($payload, $queue, AMQP_NOPARAM, $attributes);

		return true;
	}

	/**
	 * Push a new job onto the queue after a delay.
	 *
	 * @param  \DateTime|int $delay
	 * @param  string        $job
	 * @param  mixed         $data
	 * @param  string        $queue
	 *
	 * @return mixed
	 */
	public function later($delay, $job, $data = '', $queue = null)
	{
		return $this->pushRaw($this->createPayload($job, $data), $queue, ['delay' => $delay]);
	}

	/**
	 * Pop the next job off of the queue.
	 *
	 * @param string|null $queue
	 *
	 * @return \Illuminate\Queue\Jobs\Job|null
	 */
	public function pop($queue = null)
	{
		$queue = $this->getQueueName($queue);

		// declare queue if not exists
		$this->declareQueue($queue);

		// get envelope
		$message = $this->queue->get();

		if ($message instanceof \AMQPEnvelope) {
			return new RabbitMQJob($this->container, $this, $this->channel, $queue, $message);
		}

		return null;
	}

    /**
     * @return boolean
     */
    public function isConnected()
    {
        return $this->connection->isConnected();
    }

    /**
     * @return AMQPChannel
     */
    public function reconnect()
    {
        if ($this->persistent) {
            $this->connection->preconnect();
        } else {
            $this->connection->reconnect();
        }
        $this->channel = $this->getChannel();
    }

	/**
	 * @param string $queue
	 *
	 * @return string
	 */
	private function getQueueName($queue)
	{
		return $queue ?: $this->defaultQueue;
	}

	/**
	 * @return AMQPChannel
	 */
	private function getChannel()
	{
		return new \AMQPChannel($this->connection);
	}

	/**
	 * @param string $name
	 */
	private function declareQueue($name)
	{
		$name = $this->getQueueName($name);

		if ($this->declareExchange) {
			// declare exchange
			$exFlags = $this->configExchange['durable'] ? AMQP_DURABLE : AMQP_NOPARAM;
            $this->exchange = new \AMQPExchange($this->channel);
            $this->exchange->setName($name); 
            $this->exchange->setType($this->configExchange['type']);
            $this->exchange->setFlags($exFlags);
			$this->exchange->setArgument('passive', $this->configExchange['passive']);
			$this->exchange->setArgument('auto_delete', $this->configExchange['auto_delete']);
            $this->exchange->declareExchange();
		}

		if ($this->declareBindQueue) {
			// declare queue
            $quFlags = $this->configQueue['durable'] ? AMQP_DURABLE : AMQP_NOPARAM;
            $this->queue = new \AMQPQueue($this->channel);
            $this->queue->setName($name);
            $this->queue->setFlags($quFlags);
            $this->queue->setArgument('passive', $this->configQueue['passive']);
            $this->queue->setArgument('auto_delete', $this->configQueue['auto_delete']);
            $this->queue->setArgument('exclusive', $this->configQueue['exclusive']);
            $this->messageCount = $this->queue->declareQueue();

			// bind queue to the exchange
            $this->queue->bind($name, $name);
		}
	}

	/**
	 * @param string       $destination
	 * @param DateTime|int $delay
	 *
	 * @return string
	 */
	private function declareDelayedQueue($destination, $delay)
	{
		$delay = $this->getSeconds($delay);
		$destination = $this->getQueueName($destination);
		$name = $this->getQueueName($destination) . '_deferred_' . $delay;

        // declare exchange
        $exFlags = $this->configExchange['durable'] ? AMQP_DURABLE : AMQP_NOPARAM;
        $this->exchange = new \AMQPExchange($this->channel);
        $this->exchange->setName($name); 
        $this->exchange->setType($this->configExchange['type']);
        $this->exchange->setFlags($exFlags);
        $this->exchange->setArgument('passive', $this->configExchange['passive']);
        $this->exchange->setArgument('auto_delete', $this->configExchange['auto_delete']);
        $this->exchange->declareExchange();

        // declare queue
        $quFlags = $this->configQueue['durable'] ? AMQP_DURABLE : AMQP_NOPARAM;
        $this->queue = new \AMQPQueue($this->channel);
        $this->queue->setName($name);
        $this->queue->setFlags($quFlags);
        $quArguments = array(
            'passive'                   => $this->configQueue['passive'],
            'auto_delete'               => $this->configQueue['auto_delete'],
            'exclusive'                 => $this->configQueue['exclusive'],
            'x-dead-letter-exchange'    => $destination,
            'x-dead-letter-routing-key' => $destination,
            'x-message-ttl'             => $delay * 1000,
        );
        $this->queue->setArguments($quArguments);
        $this->messageCount = $this->queue->declareQueue();

        // bind queue to the exchange
        $this->queue->bind($name, $name);

		return $name;
	}

}
