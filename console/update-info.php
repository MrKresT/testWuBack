<?php

namespace app\console;

use app\helpers\FileHelper;
use app\helpers\LogHelper;
use app\helpers\PostindexHelper;
use app\models\PostIndex;
use Box\Spout\Reader\Common\Creator\ReaderEntityFactory;

require __DIR__ . '/../vendor/autoload.php';
require __DIR__ . '/../components/autoloader.php';

$postmanUrl = 'https://www.ukrposhta.ua/files/shares/out/postindex.zip';
$pathPostindex = __DIR__ . '/../public/files/';
$nameFileZipPostindex = $pathPostindex . 'postindex.zip';
$nameFilePostindex = $pathPostindex . 'postindex.xlsx';

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
/*try {
    FileHelper::cleanDir($pathPostindex);
} catch (\Exception $e) {
    LogHelper::consoleError("Fail cleaning directory {$pathPostindex}: {$e->getMessage()}");
}
LogHelper::consoleMessage('Directory cleaned successfully', true);

//get file from url
try {
    FileHelper::loadFromUrl($PostindexUrl, $nameFileZipPostindex, 'wb');
} catch (\Exception $e) {
    LogHelper::consoleError("Fail loading file from url {$PostindexUrl}: {$e->getMessage()}");
}
LogHelper::consoleMessage('File downloaded successfully', true);*/

//unzip files with info
/*if (!file_exists($nameFileZipPostindex)) {
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
LogHelper::consoleMessage("Unzip successful", true);*/

if (!file_exists($nameFilePostindex)) {
    LogHelper::consoleError("File {$nameFilePostindex} not found");
}

try {

    $postIndexObj = new PostIndex($db);

    LogHelper::consoleMessage("Connected successfully", true);

//check if table exists and create it if not
    $postIndexObj->createIfExistTable();

    LogHelper::consoleMessage("Table checked and created successfully", true);

    $postIndexObj->loadInfoFromXLSXFile($nameFilePostindex, true);

    //associate columns of xlsx with fields of DB

} catch (\Exception $e) {
    LogHelper::consoleError($e->getMessage());
}catch (\PDOException $e) {
    LogHelper::consoleError("Error on DB: " . $e->getMessage());
}

