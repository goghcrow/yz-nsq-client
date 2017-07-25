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
    public static function identify()
    {
        $payload = json_encode(NsqConfig::getIdentity(), JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        return "IDENTIFY \n" . pack('N', strlen($payload)) . $payload;
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
     *  A JSON payload describing the authorized client’s identity,
     *  an optional URL and a count of permissions which were authorized.
     *  {"identity":"...", "identity_url":"...", "permission_count":1}
     * Error Responses:
     *  E_AUTH_FAILED - An error occurred contacting an auth server
     *  E_UNAUTHORIZED - No permissions found
     */
    public static function auth($secret)
    {
        return "AUTH\n" . pack('N', strlen($secret)) . $secret;
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
            return "REGISTER $topic $channel\n";
        } else {
            return "REGISTER $topic\n";
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
            return "UNREGISTER $topic $channel\n";
        } else {
            return "UNREGISTER $topic\n";
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
        return "PING\n";
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
        return "SUB $topic $channel\n";
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
    public static function publish($topic, $body)
    {
        return "PUB $topic\n" . pack('N', strlen($body)) . $body;
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
        return "DPUB $topic $delay\n" . pack('N', strlen($body)) . $body;
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
    public static function multiPublish($topic, array $messages)
    {
        $body = "";
        foreach ($messages as $message) {
            $body .= pack('N', strlen($message)) . $message;
        }
        $msgNum = pack('N', count($messages));
        $bodySize = pack('N', strlen($msgNum . $body));

        return "MPUB $topic\n" . $bodySize . $msgNum . $body;
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
        return "RDY $count\n";
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
        return "FIN {$msg->getId()}\n";
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
        return "REQ {$msg->getId()} $delay\n";
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
        return "TOUCH {$msg->getId()}\n";
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
        return "CLS\n";
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
        return "NOP\n";
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
}