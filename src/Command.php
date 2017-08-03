<?php

namespace Zan\Framework\Components\Nsq;

/**
 * Class Command
 * @package Zan\Framework\Components\Nsq
 *
 * 命令注释来自 http://nsq.io/clients/tcp_protocol_spec.html#commands
 */
class Command
{
    /**
     * Identify creates a new Command to provide information about the client.  After connecting,
     * it is generally the first message sent.
     * See http://nsq.io/clients/tcp_protocol_spec.html#identify for information
     *
     * @return string
     *
     * NOTE: if feature_negotiation was sent by the client
     * (and the server supports it) the response will be a JSON payload as described above.
     *
     * Success Response:
     *  OK
     * Error Responses:
     *  E_INVALID
     *  E_BAD_BODY
     */
    public static function identify($params = [])
    {
        $params = array_merge(NsqConfig::getIdentify(), $params);
        $payload = json_encode($params, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        return static::cmdWithBody("IDENTIFY", $payload);
    }

    /**
     * Auth sends credentials for authentication
     * If the IDENTIFY response indicates auth_required=true the client must send AUTH before any SUB, PUB or MPUB commands.
     * After `Identify`, this is usually the first message sent, if auth is used.
     *
     * @param string $secret
     * @return string
     *
     * Success Response:
     *  A JSON payload describing the authorized client’s ,
     *  an optional URL and a count of permissions which were authorized.
     *  {"identity":"...", "identity_url":"...", "permission_count":1}
     * Error Responses:
     *  E_AUTH_FAILED - An error occurred contacting an auth server
     *  E_UNAUTHORIZED - No permissions found
     */
    public static function auth($secret)
    {
        return static::cmdWithBody("AUTH", $secret);
    }

    /**
     * add a topic/channel for the connected nsqd
     *
     * @param string $topic
     * @param string|null $channel
     * @return string
     * @deprecated
     */
    public static function register($topic, $channel = null)
    {
        if ($channel) {
            return static::cmd("REGISTER", [$topic, $channel]);
        } else {
            return static::cmd("REGISTER", [$topic]);
        }
    }

    /**
     * remove a topic/channel for the connected nsqd
     *
     * @param string $topic
     * @param string|null $channel
     * @return string
     * @deprecated
     */
    public static function unRegister($topic, $channel = null)
    {
        if ($channel) {
            return static::cmd("UNREGISTER", [$topic, $channel]);
        } else {
            return static::cmd("UNREGISTER", [$topic]);
        }
    }

    /**
     * Ping creates a new Command to keep-alive the state of all the
     * announced topic/channels for a given client
     *
     * @return string
     * @deprecated
     */
    public static function ping()
    {
        return static::cmd("PING");
    }

    /**
     * Subscribe to a topic/channel
     *
     * @param string $topic
     * @param string $channel
     * @return string
     *
     * Success Response:
     *  OK
     * Error Responses:
     *  E_INVALID
     *  E_BAD_TOPIC
     *  E_BAD_CHANNEL
     */
    public static function subscribe($topic, $channel)
    {
        return static::cmd("SUB", [$topic, $channel]);
    }

    /**
     * Publish a message to a topic
     *
     * @param string $topic
     * @param string $body
     * @return string
     *
     * Success Response:
     *  OK
     * Error Responses:
     *  E_INVALID
     *  E_BAD_TOPIC
     *  E_BAD_MESSAGE
     *  E_MPUB_FAILED
     */
    public static function publish($topic, $body, $partition = -1)
    {
        $args = [$topic];
        if ($partition >= 0) {
            $args[]= $partition;
        }
        return static::cmdWithBody("PUB", $body, $args);
    }
    
    public static function publishWithExtends($topic, $body, $partition = -1, $params = [])
    {
        $args = [$topic];
        if ($partition >= 0) {
            $args[]= $partition;
        }
        //[2-byte header length][json header data]
        $extStr = json_encode($params);
        $body = pack('n', strlen($extStr)) . $extStr . $body;
        return static::cmdWithBody("PUB_EXT", $body, $args);
    }

    /**
     * DeferredPublish creates a new Command to write a message to a given topic
     * where the message will queue at the channel level until the timeout expires
     *
     * @param $topic string
     * @param int $delay Millisecond
     * @param string $body
     * @return string
     * @deprecated
     */
    public static function deferredPublish($topic, $delay, $body)
    {
        return static::cmdWithBody("DPUB", $body, array_merge([$topic, $delay], $params));
    }

    /**
     * Publish multiple messages to a topic (atomically)
     * (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
     *
     * @param string $topic
     * @param array $messages
     * @return string
     *
     * Success Response:
     *  OK
     * Error Responses:
     *  E_INVALID
     *  E_BAD_TOPIC
     *  E_BAD_BODY
     *  E_BAD_MESSAGE
     *  E_MPUB_FAILED
     */
    public static function multiPublish($topic, array $messages, $params = [])
    {
        $body = "";
        foreach ($messages as $message) {
            $body .= pack('N', strlen($message)) . $message;
        }
        $msgNum = pack('N', count($messages));
        $body = $msgNum . $body;
        return static::cmdWithBody("MPUB", $body, array_merge([$topic], $params));
    }


    /**
     * Update RDY state (indicate you are ready to receive N messages)
     *
     * @param int $count
     * @return string
     *
     * NOTE: there is no success response
     * Error Responses:
     *  E_INVALID
     */
    public static function ready($count)
    {
        return static::cmd("RDY", [$count]);
    }

    /**
     * Finish a message (indicate successful processing)
     *
     * @param Message $msg
     * @return string
     *
     * NOTE: there is no success response
     * Error Responses:
     *  E_INVALID
     *  E_FIN_FAILED
     */
    public static function finish(Message $msg)
    {
        return static::cmd("FIN", [$msg->getId()]);
    }

    /**
     * Re-queue a message (indicate failure to process)
     * NOTE: a delay of 0 indicates immediate requeue
     *
     * @param Message $msg
     * @param int $delay  N <= configured max timeout (Millisecond)
     *                      0 is a special case that will not defer re-queueing
     * @return string
     *
     * NOTE: there is no success response
     * Error Responses:
     *  E_INVALID
     *  E_REQ_FAILED
     */
    public static function requeue(Message $msg, $delay = 0)
    {
        return static::cmd("REQ", [$msg->getId(), $delay]);
    }

    /**
     * Reset the timeout for an in-flight message
     *
     * @param Message $msg
     * @return string
     *
     * NOTE: there is no success response
     * Error Responses:
     *  E_INVALID
     *  E_TOUCH_FAILED
     */
    public static function touch(Message $msg)
    {
        return static::cmd("TOUCH", [$msg->getId()]);
    }

    /**
     * Cleanly close your connection (no more messages are sent)
     *
     * @return string
     *
     * StartClose creates a new Command to indicate that the
     * client would like to start a close cycle.  nsqd will no longer
     * send messages to a client in this state and the client is expected
     * finish pending messages and close the connection
     *
     * Success Responses:
     *  CLOSE_WAIT
     * Error Responses:
     *  E_INVALID
     */
    public static function startClose()
    {
        return static::cmd("CLS");
    }

    /**
     * Nop creates a new Command that has no effect server side.
     * Commonly used to respond to heartbeats
     *
     * @return string
     *
     * NOTE: there is no response
     */
    public static function nop()
    {
        return static::cmd("NOP");
    }

    /**
     * @param string $name
     * @return string
     * @throws NsqException
     */
    public static function checkTopicChannelName($name)
    {
        $len = strlen($name);
        $isValid = $len > 1 && $len <=64 && preg_match('/^[\.a-zA-Z0-9_-]+?$/', $name, $matches);
        if (!$isValid) {
            throw new NsqException("Invalid topic or channel name $name");
        }
        return $name;
    }
    
    public static function cmd($cmd, $args = [])
    {
        $ret = $cmd;
        if (!empty($args)) {
            $ret .= ' ' . implode(' ', $args);
        }
        return $ret . "\n";
    }
    
    public static function cmdWithBody($cmd, $body, $args = [])
    {
        $ret = static::cmd($cmd, $args);
        $ret .= pack('N', strlen($body)) . $body;
        return $ret;
    }
}
