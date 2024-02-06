<?php

namespace app\console;

use app\helpers\FileHelper;
use app\helpers\LogHelper;
use app\models\PostIndex;

/**
 * script for loading info from ukrposhta.ua to database
 *
 * php update-info.php 1 (with console log)
 */

//TODO зробити параметри для повного налаштування скрипта

require __DIR__ . '/../vendor/autoload.php';

$postmanUrl = 'https://www.ukrposhta.ua/files/shares/out/postindex.zip';

$pathPostindex = __DIR__ . '/../public/files/';

$nameFileZipPostindex = $pathPostindex . 'postindex.zip';

$nameFilePostindex = $pathPostindex . 'postindex.xlsx';

$withConsoleLog = (int)($argv[1] ?? 0);

try {
    $db = \app\models\DBConnection::connect();
} catch (\PDOException $e) {
    LogHelper::consoleError("Connection failed: " . $e->getMessage());
} catch (\Exception $e) {
    LogHelper::consoleError("Connection failed: " . $e->getMessage());
}
if (!file_exists($pathPostindex)) {
    try {
        FileHelper::createDir($pathPostindex);
    } catch (\Exception $e) {
        LogHelper::consoleError("Fail creating directory {$pathPostindex}: {$e->getMessage()}");
    }
}

//clean old files
try {
    FileHelper::cleanDir($pathPostindex);
} catch (\Exception $e) {
    LogHelper::consoleError("Fail cleaning directory {$pathPostindex}: {$e->getMessage()}");
}
$withConsoleLog && LogHelper::consoleMessage('Directory cleaned successfully', true);

//get file from url
try {
    FileHelper::loadFromUrl($postmanUrl, $nameFileZipPostindex, 'wb');
} catch (\Exception $e) {
    LogHelper::consoleError("Fail loading file from url {$postmanUrl}: {$e->getMessage()}");
}
$withConsoleLog && LogHelper::consoleMessage('File downloaded successfully', true);

//unzip files with info
if (!file_exists($nameFileZipPostindex)) {
    LogHelper::consoleError("File {$nameFileZipPostindex} not found");
}

try {
    $zip = new \ZipArchive();
    if ($zip->open($nameFileZipPostindex)) {
        $zip->extractTo($pathPostindex);
    }
    $zip->close();
} catch (\Exception $e) {
    LogHelper::consoleError("Unzip failed: " . $e->getMessage());
}
$withConsoleLog && LogHelper::consoleMessage("Unzip successful", true);

if (!file_exists($nameFilePostindex)) {
    LogHelper::consoleError("File {$nameFilePostindex} not found");
}
$withConsoleLog && LogHelper::consoleMessage('Start at ' . (new \DateTime())->format('Y-m-d H:i:s'), true);

try {

    $postIndexObj = new PostIndex($db);

    $withConsoleLog && LogHelper::consoleMessage("Connected successfully", true);

    //check if table exists and create it if not
    $postIndexObj->createIfExistTable();

    $withConsoleLog && LogHelper::consoleMessage("Table checked and created successfully", true);

    //File processing
    $postIndexObj->loadInfoFromXLSXFile($nameFilePostindex, $withConsoleLog);

} catch (\Exception $e) {
    LogHelper::consoleError($e->getMessage());
} catch (\PDOException $e) {
    LogHelper::consoleError("Error on DB: " . $e->getMessage());
}

$withConsoleLog && LogHelper::consoleMessage('End at ' . (new \DateTime())->format('Y-m-d H:i:s'), true);
