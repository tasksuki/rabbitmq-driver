<?php

namespace Tasksuki\Component\RabbitmqDriver\Test;

use Integrati;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPProtocolException;
use PHPUnit\Framework\TestCase;
use Tasksuki\Component\RabbitmqDriver\RabbitmqDriver;

class IntegrationTest extends TestCase
{
    const TOPIC = 'tasksuki_rabbitmq_driver_test';
    
    /**
     * @var AbstractConnection
     */
    private $connection;

    /**
     * @var AMQPChannel
     */
    private $channel;

    public function __construct($name = null, array $data = [], $dataName = '')
    {
        $this->connection = new AMQPStreamConnection('localhost', 5672, 'guest', 'guest');
        $this->channel = $this->connection->channel();

        parent::__construct($name, $data, $dataName);
    }

    public function __destruct()
    {
        $this->channel->close();
        $this->connection->close();
    }

    public function tearDown()
    {
        $this->channel->queue_delete(static::TOPIC);
    }

    public function testRun()
    {
        $driver = new RabbitmqDriver($this->channel);

        $message = 'foo_bar';

        $driver->send(static::TOPIC, $message);

        $called = 0;
        $handler = function ($data) use ($message, &$called) {
            $this->assertEquals($message, $data);
            
            $called++;
        };
        
        $driver->receive(static::TOPIC, $handler);
        
        $this->assertEquals(1, $called);
    }

    public function testRunMultipleMessages()
    {
        $driver = new RabbitmqDriver($this->channel);

        $message = 'foo_bar';

        for ($i = 0; $i < 10; $i++) {
            $driver->send(static::TOPIC, $message);
        }

        $called = 0;
        $handler = function ($data) use ($message, &$called) {
            $this->assertEquals($message, $data);

            $called++;
        };

        $driver->receive(static::TOPIC, $handler);

        $this->assertEquals(1, $called);
    }

    public function testRunMultipleMessagesFetchAll()
    {
        $driver = new RabbitmqDriver($this->channel);

        $message = 'foo_bar';

        for ($i = 0; $i < 10; $i++) {
            $driver->send(static::TOPIC, $message);
        }

        $called = 0;
        $handler = function ($data) use ($message, &$called) {
            $this->assertEquals($message, $data);

            $called++;
        };

        $lastCalled = null;
        while ($called < 9) {
            $driver->receive(static::TOPIC, $handler);
        }

        $this->assertEquals(9, $called);
    }

    public function testMultipleQueues()
    {
        $driver = new RabbitmqDriver($this->channel);

        $message = 'foo_bar';

        for ($i = 0; $i < 10; $i++) {
            $driver->send(static::TOPIC . $i, $message);
        }

        $called = 0;
        $handler = function ($data) use ($message, &$called) {
            $this->assertEquals($message, $data);

            $called++;
        };

        $lastCalled = null;
        while ($called < 10) {
            for ($i = 0; $i < 10; $i++) {
                $driver->receive(static::TOPIC . $i, $handler);
            }
        }

        $this->assertEquals(10, $called);

        for ($i = 0; $i < 10; $i++) {
            $this->channel->queue_delete(static::TOPIC . $i);
        }
    }
}
