<?php
namespace app\helpers;
class LogHelper
{

    public static function consoleMessage(string $message, bool $showMemory = false)
    {
        echo $message . PHP_EOL;
        if ($showMemory) {
            $memory = round(memory_get_usage() / 1024 / 1024, 2) . ' MB';
            echo "Used memory: {$memory}" .PHP_EOL;
        }
    }

    public static function consoleError(string $message)
    {
        die($message . PHP_EOL);
    }
}
