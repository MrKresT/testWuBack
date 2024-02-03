<?php

namespace app\helpers;

class FileHelper
{

    public static function loadFromUrl(string $url, string $filename, string $mode = 'w')
    {
        $fp = fopen($filename, $mode);
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_FILE, $fp);
        curl_exec($ch);
        curl_close($ch);
        fclose($fp);
    }

    public static function createDir(string $dir)
    {
        if (!file_exists($dir)) {
            if (!mkdir($dir, 0775, true) && !is_dir($dir)) {
                throw new \Exception("Directory {$dir} was not created");
            }
        }
    }

    public static function cleanDir(string $dir)
    {
        $files = glob($dir . '/*');
        foreach ($files as $file) {
            if (is_file($file)) {
                unlink($file);
            }
        }
    }
}
