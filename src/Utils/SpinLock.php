<?php

namespace Utils;


/**
 * Class SpinLock
 * @package Utils
 *
 *  SpinLock::lock(__CLASS__);
 *  try {
 *      // do something
 *  } finally {
 *     SpinLock::unlock(__CLASS__);
 *  }
 */
final class SpinLock
{
    private $locked = false;

    /**
     * @var static[] map<string, SpinLock>
     */
    private static $lock;

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
        if (!isset(static::$lock[$lockName])) {
            static::$lock[$lockName] = new static;
        }
        return static::$lock[$lockName];
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