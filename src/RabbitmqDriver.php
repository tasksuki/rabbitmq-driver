<?php

namespace Tasksuki\Component\RabbitmqDriver;

use Throwable;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Channel\AbstractChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Tasksuki\Component\Driver\DriverInterface;

/**
 * Class RabbitmqDriver
 *
 * @package Tasksuki\Component\RabbitmqDriver
 * @author  Aurimas Niekis <aurimas@niekis.lt>
 */
class RabbitmqDriver implements DriverInterface
{
    /**
     * @var AMQPChannel
     */
    private $channel;

    /**
     * @var array
     */
    private $queuesDeclared;

    /**
     * @var int
     */
    private $timeout;

    public function __construct(AbstractChannel $channel, int $timeout = 0)
    {
        $this->channel        = $channel;
        $this->queuesDeclared = [];
        $this->timeout        = $timeout;
    }

    /**
     * @return AMQPChannel
     */
    public function getChannel(): AMQPChannel
    {
        return $this->channel;
    }

    /**
     * @return int
     */
    public function getTimeout(): int
    {
        return $this->timeout;
    }

    /**
     * @inheritdoc
     */
    public function send(string $name, string $message): bool
    {
        $this->declareQueue($name);

        $msg = new AMQPMessage(
            $message,
            ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]
        );

        $this->getChannel()->basic_publish($msg, '', $name);

        return true;
    }

    /**
     * @inheritdoc
     */
    public function receive(string $name, callable $handler)
    {
        $this->declareQueue($name);

        $callback = function (AMQPMessage $message) use ($handler) {
            try {
                $data = $message->body;

                $handler($data);

                $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
            } catch (Throwable $e) {
                $message->delivery_info['channel']->basic_reject($message->delivery_info['delivery_tag'], true);

                throw $e;
            }
        };

        $this->getChannel()->basic_qos(null, 1, null);
        $this->getChannel()->basic_consume($name, '', false, false, false, false, $callback);

        if (count($this->getChannel()->callbacks) > 0) {
            $this->getChannel()->wait(null, true, $this->getTimeout());
        }
    }

    private function declareQueue(string $name)
    {
        if (isset($this->queuesDeclared[$name])) {
            return;
        }

        $this->getChannel()->queue_declare($name, false, true, false, false);

        $this->queuesDeclared[$name] = true;
    }
}