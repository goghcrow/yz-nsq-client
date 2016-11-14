<?php

namespace Zan\Framework\Components\Nsq\Utils;


/**
 * Class SpinLock
 * @package Utils
 *
 *  yield SpinLock::lock(__CLASS__);
 *  try {
 *      // do something
 *  } finally {
 *     yield SpinLock::unlock(__CLASS__);
 *  }
 */
final class SpinLock
{
    private $locked = false;

    /**
     * TODO memory leak, unset unused lock
     * @var static[] map<string, SpinLock>
     */
    private static $lockIns;

    private function __construct() { }

    private function lock_($ms = 1)
    {
        while (true) {
            if ($this->locked === true) {
                yield taskSleep($ms);
                continue;
            }
            $this->locked = true;
            break;
        }
    }

    private function unlock_()
    {
        $this->locked = false;
    }

    /**
     * @param $lockName
     * @return static
     */
    private static function get($lockName)
    {
        if (!isset(static::$lockIns[$lockName])) {
            static::$lockIns[$lockName] = new static;
        }
        return static::$lockIns[$lockName];
    }

    public static function lock($lockName, $ms = 1)
    {
        yield static::get($lockName)->lock_($ms);
    }

    public static function unlock($lockName)
    {
        static::get($lockName)->unlock_();
    }
}