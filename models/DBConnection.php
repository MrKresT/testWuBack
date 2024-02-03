<?php
namespace app\models;

class DBConnection
{

    /**
     * Connects to the database using the configuration provided in 'db.php'.
     *
     * @return PDO Returns a PDO (PHP Data Object) connection to the database.
     *
     * @throws Exception If the 'dsn', 'username', or 'password' values are not set in the configuration.
     */
    public static function connect()
    {
        $dbConfig = require __DIR__ . '/../config/db.php';

        if (!isset($dbConfig['dsn'])) {
            throw new \Exception('dsn not set');
        }
        if (!isset($dbConfig['username'])) {
            throw new \Exception('username not set');
        }
        if (!isset($dbConfig['password'])) {
            throw new \Exception('password not set');
        }

        $connection = new \PDO($dbConfig['dsn'], $dbConfig['username'], $dbConfig['password']);
        $connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        return $connection;
    }

}
