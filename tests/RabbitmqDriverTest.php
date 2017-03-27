<?php

namespace Tasksuki\Component\RabbitmqDriver\Test;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Tasksuki\Component\RabbitmqDriver\RabbitmqDriver;
use PHPUnit\Framework\TestCase;

class RabbitmqDriverTest extends TestCase
{
    public function testSend()
    {
        $message = 'foo_bar';

        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->setMethods(['queue_declare', 'basic_publish'])
            ->getMock();

        $channel->expects($this->once())
            ->method('queue_declare')
            ->with('foo', false, true, false, false);

        $channel->expects($this->exactly(2))
            ->method('basic_publish')
            ->will(
                $this->returnCallback(
                    function (AMQPMessage $msg, $exchange, $routingKey) use ($message) {
                        $this->assertEquals('', $exchange);
                        $this->assertEquals('foo', $routingKey);

                        $this->assertEquals($message, $msg->body);
                    }
                )
            );

        $driver = new RabbitmqDriver($channel);
        $driver->send('foo', $message);
        $driver->send('foo', $message);
    }

    public function testReceive()
    {
        $message = 'foo_bar';

        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->setMethods(['queue_declare', 'basic_ack', 'basic_reject', 'basic_qos', 'basic_consume', 'wait'])
            ->getMock();

        $channel->expects($this->once())
            ->method('queue_declare')
            ->with('foo', false, true, false, false);

        $channel->expects($this->once())
            ->method('basic_qos')
            ->with(null, 1, null);

        $channel->expects($this->once())
            ->method('basic_ack')
            ->with('123456');

        $channel->expects($this->once())
            ->method('basic_consume')
            ->will(
                $this->returnCallback(
                    function ($name, $tag, $noLocal, $noAck, $exclusive, $noWait, $callback) use ($message, $channel) {
                        $this->assertEquals('foo', $name);
                        $this->assertEquals('', $tag);
                        $this->assertFalse($noLocal);
                        $this->assertFalse($noAck);
                        $this->assertFalse($exclusive);
                        $this->assertFalse($noWait);

                        $msg = new AMQPMessage($message);
                        $msg->delivery_info['delivery_tag'] = '123456';
                        $msg->delivery_info['channel'] = $channel;

                        $callback($msg);
                    }
                )
            );

        $channel->callbacks = 1;

        $channel->expects($this->once())
            ->method('wait')
            ->with(null, true, 0);

        $handler = $this->getMockBuilder(\stdClass::class)
            ->setMethods(['call'])
            ->getMock();

        $handler->expects($this->once())
            ->method('call')
            ->with($message);

        $driver = new RabbitmqDriver($channel);

        $driver->receive('foo', [$handler, 'call']);
    }

    /**
     * @expectedException \Exception
     * @expectedExceptionMessage From Handler
     */
    public function testReceiveException()
    {
        $message = 'foo_bar';

        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->setMethods(['queue_declare', 'basic_ack', 'basic_reject', 'basic_qos', 'basic_consume', 'wait'])
            ->getMock();

        $channel->expects($this->once())
            ->method('queue_declare')
            ->with('foo', false, true, false, false);

        $channel->expects($this->once())
            ->method('basic_qos')
            ->with(null, 1, null);

        $channel->expects($this->once())
            ->method('basic_reject')
            ->with('123456', true);

        $channel->expects($this->once())
            ->method('basic_consume')
            ->will(
                $this->returnCallback(
                    function ($name, $tag, $noLocal, $noAck, $exclusive, $noWait, $callback) use ($message, $channel) {
                        $this->assertEquals('foo', $name);
                        $this->assertEquals('', $tag);
                        $this->assertFalse($noLocal);
                        $this->assertFalse($noAck);
                        $this->assertFalse($exclusive);
                        $this->assertFalse($noWait);

                        $msg = new AMQPMessage($message);
                        $msg->delivery_info['delivery_tag'] = '123456';
                        $msg->delivery_info['channel'] = $channel;

                        $callback($msg);
                    }
                )
            );

        $handler = function () {
            throw new \Exception('From Handler');
        };

        $driver = new RabbitmqDriver($channel);

        $driver->receive('foo', $handler);
    }
}
