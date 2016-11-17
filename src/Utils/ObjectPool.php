<?php

namespace Zan\Framework\Components\Nsq\Utils;


/**
 * Class ObjectPool
 * fix size object pool
 */
class ObjectPool
{
    const POOL_SIZE = 100;

    /**
     * @var \SplObjectStorage[]
     * k object
     * v objectTemple
     */
    private static $pool;

    /**
     * @var \SplObjectStorage[]
     */
    private static $inUse;

    public static function create($object, $size)
    {
        if (!is_object($object)) {
            throw new \InvalidArgumentException("must be object");
        }

        $class = get_class($object);
        if (isset(static::$pool[$class])) {
            return;
        }

        static::$pool[$class] = [];
        static::$inUse[$class] = [];

        static::$pool[$class][0] = [$object, $size];

        for ($i = 1; $i < $size + 1; $i++) {
            static::$pool[$class][] = clone $object;
        }
    }

    public static function release($object)
    {
        if (!is_object($object)) {
            throw new \InvalidArgumentException("must be object");
        }

        $class = get_class($object);
        if (!isset(static::$pool[$class])) {
            throw new \InvalidArgumentException("$class not in pool");
        }

        $hash = spl_object_hash($object);
        if (!isset(static::$inUse[$class][$hash])) {
            return;
        }
        unset(static::$inUse[$class][$hash]);

        $pool = static::$pool[$class];
        $maxSize = $pool[0][1];
        if (count(static::$pool[$class]) - 1 < $maxSize) {
            static::$pool[$class][$hash] = $object;
        }
        return;
    }

    public static function get($class)
    {
        if (!isset(static::$pool[$class]) || !isset(static::$pool[$class][0])) {
            throw new \InvalidArgumentException("$class not in pool");
        }

        $inUse = static::$inUse[$class];
        /* @var array $pool */
        $pool = static::$pool[$class];

        if (count($pool) > 1) {
            $object = array_pop($pool);
        } else {
            $tpl = static::$pool[$class][0][0];
            $object = clone $tpl;
        }

        $inUse[spl_object_hash($object)] = $object;
        return $object;
    }
}