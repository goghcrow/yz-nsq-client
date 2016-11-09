<?php

namespace Zan\Framework\Components\Nsq;


interface ConnDelegate
{
    /**
     * OnResponse is called when the connection
     * receives a FrameTypeResponse from nsqd
     * @param Connection $conn
     * @param string $bytes
     * @return mixed
     */
    public function onResponse(Connection $conn, $bytes);

    /**
     * OnError is called when the connection
     * receives a FrameTypeError from nsqd
     * @param Connection $conn
     * @param $bytes
     * @return mixed
     */
    public function onError(Connection $conn, $bytes);

    /**
     * OnMessage is called when the connection
     * receives a FrameTypeMessage from nsqd
     * @param Connection $conn
     * @param Message $msg
     * @return mixed
     */
    public function onMessage(Connection $conn, Message $msg);

    /**
     * OnMessageFinished is called when the connection
     * handles a FIN command from a message handler
     * @param Connection $conn
     * @param Message $msg
     * @return mixed
     */
    public function onMessageFinished(Connection $conn, Message $msg);

    /**
     * OnMessageRequeued is called when the connection
     * handles a REQ command from a message handler
     * @param Connection $conn
     * @param Message $msg
     * @return mixed
     */
    public function onMessageRequeued(Connection $conn, Message $msg);

    /**
     * OnBackoff is called when the connection triggers a backoff state
     * @param Connection $conn
     * @return mixed
     */
    public function onBackoff(Connection $conn);

    /**
     * OnContinue is called when the connection f
     * inishes a message without adjusting backoff state
     * @param Connection $conn
     * @return mixed
     */
    public function onContinue(Connection $conn);

    /**
     * OnResume is called when the connection
     * triggers a resume state
     * @param Connection $conn
     * @return mixed
     */
    public function onResume(Connection $conn);

    /**
     * OnIOError is called when the connection experiences
     * a low-level TCP transport error
     * @param Connection $conn
     * @param \Exception $ex
     * @return mixed
     */
    public function onIOError(Connection $conn, \Exception $ex);

    /**
     * OnHeartbeat is called when the connection
     * receives a heartbeat from nsqd
     * @param Connection $conn
     * @return mixed
     */
    public function onHeartbeat(Connection $conn);

    /**
     * OnClose is called when the connection
     * closes, after all cleanup
     * @param Connection $conn
     * @return mixed
     */
    public function onClose(Connection $conn);

    /**
     * Debug Hook
     * @param Connection $conn
     * @param string $bytes
     * @return mixed
     */
    public function onReceive(Connection $conn, $bytes);

    /**
     * Debug Hook
     * @param Connection $conn
     * @param string $bytes
     * @return mixed
     */
    public function onSend(Connection $conn, $bytes);
}