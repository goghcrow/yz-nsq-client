<?php

namespace Zan\Framework\Components\Nsq\Test;

use Zan\Framework\Components\Nsq\Utils\Lock;
use Zan\Framework\Foundation\Coroutine\Task;
use Zan\Framework\Network\Server\Timer\Timer;

require_once __DIR__ . "/boot.php";

// 调度器并发任务产生的数据静态
// asyncSetOnce方法为临界区


class OnceTest1
{
    // 多个task共享数据
    private static $raceData = [];

    public static function get($key)
    {
        return static::$raceData[$key];
    }

    public static function task($key, $id)
    {
        // 临界区
        if (!isset(static::$raceData[$key])) {
            yield static::asyncSetOnce($key, $id);
        }
        // echo static::$raceData[$key], "\n";
    }

    private static function asyncSetOnce($key, $value)
    {
        // echo __FUNCTION__ . "($key, $value)", "\n";
        yield taskSleep(10 * rand(1, 10)); // 模拟异步操作, 并发的task的挂起时间不一样
        static::$raceData[$key] = $value;
    }
}


// 并发10各task set同一个值, 导致每个task看到get到的值都不一样
// asyncSetOnce 被调用多次~
//for ($i = 0; $i < 10; $i++) {
//    Task::execute(OnceTest1::task("hello", $i));
//}
//swoole_timer_after(200, function() {
//    echo "final result:" . OnceTest1::get("hello"), "\n";
//});


// final result 每次运行的结果都可能不一样
$task = function() {
    $tasks = [];
    for ($i = 0; $i < 10; $i++) {
        $tasks[] = OnceTest1::task("hello", $i);
    }
    yield parallel($tasks);
    echo "final result: " . OnceTest1::get("hello"), "\n";
};
//Task::execute($task());



class OnceTest2
{
    const NIL = -1;
    private static $raceData = [];

    public static function get($key)
    {
        return static::$raceData[$key];
    }

    public static function task($key, $id)
    {
        // 临界区
        if (!isset(static::$raceData[$key])) {
            yield static::asyncSetOnce($key, $id);
        }

        $value = static::$raceData[$key];

        if ($value === static::NIL) {
            $method = __METHOD__;
            $args = func_get_args();
            Timer::after(1, function() use($method, $args) {
                Task::execute(call_user_func_array($method, $args));
            });
        } else {
            // echo $value, "\n";
        }
    }

    // synchronized
    private static function asyncSetOnce($key, $value)
    {
        // echo __FUNCTION__ . "($key, $value)", "\n";
        static::$raceData[$key] = static::NIL;
        yield taskSleep(10 * rand(1, 10)); // 模拟异步操作
        static::$raceData[$key] = $value;
    }
}

// asyncSetOnce 只会被调用一次
// 10个task最终都会获取到 static::$raceData[$key] 唯一被set的值
//for ($i = 0; $i < 10; $i++) {
//    Task::execute(OnceTest2::task("hello", "t$i"));
//}
//swoole_timer_after(200, function() {
//    echo "final result:" . OnceTest2::get("hello"), "\n";
//});


// final result 结果恒等于asyncSetOnce方法第一次被task执行时设置的value
// 调度器的并发产生于异步任务, 第一次执行asyncSetOnce方法的为task0, set value = 0
//$task = function() {
//    $tasks = [];
//    for ($i = 0; $i < 10; $i++) {
//        $tasks[] = OnceTest2::task("hello", $i);
//    }
//    yield parallel($tasks);
//    echo "final result: " . OnceTest2::get("hello"), "\n";
//};
//Task::execute($task());




class OnceTest3
{
    private static $locked = false;

    // 自旋锁
    private static function lock()
    {
        while (true) {
            if (static::$locked === true) {
                yield taskSleep(1);
                continue;
            }
            static::$locked = true;
            break;
        }
    }

    private static function unlock()
    {
        static::$locked = false;
    }




    private static $raceData = [];

    public static function get($key)
    {
        return static::$raceData[$key];
    }

    public static function task($key, $id)
    {
        yield static::lock();
        if (!isset(static::$raceData[$key])) {
            yield static::asyncSetOnce($key, $id);
        }
        static::unlock();
    }

    private static function asyncSetOnce($key, $value)
    {
        echo __FUNCTION__ . "($key, $value)", "\n";
        yield taskSleep(10 * rand(1, 10));
        static::$raceData[$key] = $value;
    }
}

//$task = function() {
//    $tasks = [];
//    for ($i = 0; $i < 10; $i++) {
//        $tasks[] = OnceTest3::task("hello", $i);
//    }
//    yield parallel($tasks);
//    echo "final result: " . OnceTest3::get("hello"), "\n";
//};
//Task::execute($task());



class SpinLock1
{
    private $locked = false;

    /**
     * @var static[] map<string, SpinLock>
     */
    private static $lock;

    public function lock()
    {
        while (true) {
            if ($this->locked === true) {
                yield taskSleep(1);
                continue;
            }
            $this->locked = true;
            break;
        }
    }

    public function unlock()
    {
        $this->locked = false;
    }

    /**
     * @param $lockName
     * @return SpinLock1
     */
    public static function get($lockName)
    {
        if (!isset(static::$lock[$lockName])) {
            static::$lock[$lockName] = new static;
        }
        return static::$lock[$lockName];
    }
}


class SpinLock2
{
    private $locked = false;

    /**
     * @var static[] map<string, SpinLock>
     */
    private static $lock;

    private function lock_()
    {
        while (true) {
            if ($this->locked === true) {
                yield taskSleep(1);
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

    public static function lock($lockName)
    {
        yield static::get($lockName)->lock_();
    }

    public static function unlock($lockName)
    {
        static::get($lockName)->unlock_();
    }
}


class OnceTest4
{
    private static $raceData = [];

    public static function get($key)
    {
        return static::$raceData[$key];
    }

    public static function task($key, $id)
    {
        $lock = SpinLock1::get(__CLASS__);

        yield $lock->lock();
        if (!isset(static::$raceData[$key])) {
            yield static::asyncSetOnce($key, $id);
        }
        $lock->unlock();
    }

    public static function task1($key, $id)
    {
        yield SpinLock2::lock(__CLASS__);
        if (!isset(static::$raceData[$key])) {
            yield static::asyncSetOnce($key, $id);
        }
        SpinLock2::unlock(__CLASS__);
    }

    private static function asyncSetOnce($key, $value)
    {
        // echo __FUNCTION__ . "($key, $value)", "\n";
        yield taskSleep(10 * rand(1, 10));
        static::$raceData[$key] = $value;
    }
}

$task = function() {
    $tasks = [];
    for ($i = 0; $i < 10; $i++) {
        $tasks[] = OnceTest4::task("hello", $i);
    }
    yield parallel($tasks);
    echo "final result: " . OnceTest4::get("hello"), "\n";
};

//Task::execute($task());

$task1 = function() {
    $tasks = [];
    for ($i = 0; $i < 10; $i++) {
        $tasks[] = OnceTest4::task1("hello", $i);
    }
    yield parallel($tasks);
    echo "final result: " . OnceTest4::get("hello"), "\n";
};

//Task::execute($task1());







class SpinLock3
{
    private $locked = false;

    /**
     * @var static[] map<string, SpinLock>
     */
    private static $lock;

    private function lock_()
    {
        while (true) {
            if ($this->locked === true) {
                yield taskSleep(1);
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

    public $timerId;

    public static function lock($lockName, $lifeCycle = 100)
    {
        $lock = static::get($lockName);
        yield $lock->lock_();
        $lock->timerId = swoole_timer_after($lifeCycle, function($lockName) {
            static::unlock($lockName);
        }, $lockName);

//        Timer::after($lifeCycle, function() use($lockName) {
//            static::unlock($lockName);
//        }, static::lockTimerId($lock));
    }

    public static function unlock($lockName)
    {
        $lock = static::get($lockName);
        $ret = swoole_timer_clear($lock->timerId);
        var_dump($ret);

//        Timer::clearAfterJob(static::lockTimerId($lock));
        $lock->unlock_();
    }

    private static function lockTimerId(SpinLock3 $lock)
    {
        return spl_object_hash($lock) . "_lock";
    }
}



class OnceTest5
{
    private static $raceData = [];

    public static function get($key)
    {
        return static::$raceData[$key];
    }

    public static function task($key, $id)
    {
        yield SpinLock3::lock(__CLASS__);
        if (!isset(static::$raceData[$key])) {
            yield static::asyncSetOnce($key, $id);
        }
        SpinLock3::unlock(__CLASS__);
    }

    private static function asyncSetOnce($key, $value)
    {
        // echo __FUNCTION__ . "($key, $value)", "\n";
        yield taskSleep(10 * rand(1, 10));
        static::$raceData[$key] = $value;
    }
}

$task = function() {
    $tasks = [];
    for ($i = 0; $i < 10; $i++) {
        $tasks[] = OnceTest5::task("hello", $i);
    }
    yield parallel($tasks);
    echo "final result: " . OnceTest5::get("hello"), "\n";
};

//Task::execute($task());




class OnceTest6
{
    public static $afterUnlock = 100;

    private static $raceData = [];

    public static function get($key)
    {
        return static::$raceData[$key];
    }

    public static function task($key, $id)
    {
        yield Lock::lock(__CLASS__);
        if (!isset(static::$raceData[$key])) {
            yield static::asyncSetOnce($key, $id);
        }
        yield Lock::unlock(__CLASS__);
    }

    private static function asyncSetOnce($key, $value)
    {
        // echo __FUNCTION__ . "($key, $value)", "\n";
        yield taskSleep(10 * rand(1, 10));
        static::$raceData[$key] = $value;
    }
}

$task = function() {
    $tasks = [];
    for ($i = 0; $i < 10; $i++) {
        $tasks[] = OnceTest6::task("hello", $i);
    }
     try {
          yield parallel($tasks);
     } catch (\Exception $ex) {
         echo_exception($ex);
    }
     echo "final result: " . OnceTest6::get("hello"), "\n";
};

//Task::execute($task());





class OnceTest7
{
    public static $afterUnlock = 100;

    private static $raceData = [];

    public static function get($key)
    {
        return static::$raceData[$key];
    }

    public static function task($key, $id)
    {
        yield Lock::lock(__CLASS__);
        if (!isset(static::$raceData[$key])) {
            yield static::asyncSetOnce($key, $id);
        }
        yield Lock::unlock(__CLASS__);

        throw new \Exception("xx");
    }

    private static function asyncSetOnce($key, $value)
    {
        // echo __FUNCTION__ . "($key, $value)", "\n";
        yield taskSleep(10 * rand(1, 10));
        static::$raceData[$key] = $value;
    }
}

$task = function() {
    $tasks = [];
    for ($i = 0; $i < 10; $i++) {
        $tasks[] = OnceTest7::task("hello", $i);
    }
    try {
        // BUG 并行时异常只捕获第一个task
         yield parallel($tasks);

//        $taskWithCatch = function($i) {
//            try {
//                yield OnceTest7::task("hello", $i);
//            } catch (\Exception $ex) {
//                echo_exception($ex);
//            }
//        };
//        for ($i = 0; $i < 10; $i++) {
//            Task::execute($taskWithCatch($i));
//        }
//        swoole_timer_after(200, function() {
//            echo "final result: " . OnceTest7::get("hello"), "\n";
//        });
    } catch (\Exception $ex) {
        echo_exception($ex);
    }
//     echo "final result: " . OnceTest6::get("hello"), "\n";
};

//Task::execute($task());