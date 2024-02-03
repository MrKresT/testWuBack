<?php

namespace app\models;

use app\helpers\LogHelper;
use Box\Spout\Reader\Common\Creator\ReaderEntityFactory;

/**
 * Class for working with the PostIndex table.
 */
class PostIndex
{

    const ACTION_PROCESSING_INSERT = 'insert';
    const ACTION_PROCESSING_UPDATE = 'update';
    const ACTION_PROCESSING_SKIP = 'skip';
    const ACTION_PROCESSING_DELETE = 'delete';
    protected $_db;

    protected $tableName = 'post_info';

    protected $fieldKey = 'post_office_id';

    protected $countQueries = 3000;

    /**
     * @var array $fields
     *
     * The $fields array represents the structure of a database table.
     * Each key in the array represents a column name, and its corresponding value is an array
     * containing information about the column, such as data type and column label in Excel file.
     *
     * Example:
     * $fields = [
     *      'column_name' => [
     *          'type' => 'data_type',
     *          'columnXLSX' => 'column_label_in_Excel_file',
     *      ],
     *      ...
     * ]
     *
     * Key - Column name (string)
     * Value - Array containing column information (array)
     *
     * Available keys in the value array:
     *
     * - type: Represents the data type of the column (string)
     * - columnXLSX: Represents the label of the column as defined in an Excel file (string)
     *
     * Note: Some columns may not have a corresponding label in the Excel file.
     * In such cases, the columnXLSX value will be an empty string.
     *
     * Example:
     * $fields = [
     *      'post_office_id' => [
     *          'type' => 'VARCHAR (5) PRIMARY KEY NOT NULL',
     *          'columnXLSX' => 'Поштовий індекс відділення зв`язку (Post code of post office)'
     *      ],
     *      'region_ukr' => [
     *          'type' => 'varchar(255)',
     *          'columnXLSX' => 'Область',
     *      ],
     *      ...
     * ]
     */
    public $fields = [
        'post_office_id' => [
            'type' => 'VARCHAR (5) PRIMARY KEY NOT NULL',
            'columnXLSX' => 'Поштовий індекс відділення зв`язку (Post code of post office)'
        ],
        'region_ukr' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'Область',
        ],
        'district_old_ukr' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'Район (старий)',
        ],
        'district_new_ukr' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'Район (новий)',
        ],
        'settlement_ukr' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'Населений пункт',
        ],
        'postal_code' => [
            'type' => 'varchar(5)',
            'columnXLSX' => 'Поштовий індекс (Postal code)',
        ],
        'region_en' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'Region (Oblast)',
        ],
        'district_new_en' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'District new (Raion new)',
        ],
        'settlement_en' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'Settlement',
        ],
        'post_office_ukr' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'Вiддiлення зв`язку',
        ],
        'post_office_en' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'Post office',
        ],
    ];

    protected $requiredFields = [
        'created_manual' => [
            'type' => 'INT(1) NOT NULL DEFAULT 0',
            'columnXLSX' => '',
            'func' => 'setCreatedManual',
            'action' => [self::ACTION_PROCESSING_INSERT, self::ACTION_PROCESSING_DELETE],
        ],
        'created_at' => [
            'type' => 'DATETIME',
            'columnXLSX' => '',
            'func' => 'setCurrentDateTime',
            'action' => [self::ACTION_PROCESSING_INSERT],
        ],
        'updated_at' => [
            'type' => 'DATETIME',
            'columnXLSX' => '',
            'func' => 'setCurrentDateTime',
            'action' => [self::ACTION_PROCESSING_INSERT, self::ACTION_PROCESSING_UPDATE],
        ],
        'checked_at' => [
            'type' => 'DATETIME',
            'func' => 'setCurrentDateTime',
            'action' => [
                self::ACTION_PROCESSING_INSERT,
                self::ACTION_PROCESSING_UPDATE,
                self::ACTION_PROCESSING_SKIP,
                self::ACTION_PROCESSING_DELETE,
            ],
        ],
    ];

    protected function setCreatedManual()
    {
        return 0;
    }

    protected function setCurrentDateTime()
    {
        return (new \DateTime())->format('Y-m-d H:i:s');
    }

    /**
     * Constructor method for creating a new instance of the class.
     *
     * @param \PDO $db The database connection object.
     * @return void
     * @throws \Exception If no database connection object is provided.
     */
    public function __construct($db)
    {
        if (!$db) {
            throw new \Exception('No database connection');
        }

        $this->_db = $db;

    }

    /**
     * Loads information from an XLSX file and updates the table with the data.
     * If the file name is empty, an exception is thrown.
     * The XLSX file is opened and the first sheet is read.
     * If $withConsoleLog is set to true, a success message is logged to the console.
     * The table is updated with the data from the XLSX file.
     *
     * @param string $fileName The name of the XLSX file to load.
     * @param bool $withConsoleLog Optional. Set to true to log a success message to the console. Default is false.
     * @return void
     * @throws \Exception If no file name is provided.
     */
    public function loadInfoFromXLSXFile($fileName, $withConsoleLog = false)
    {
        if (!$fileName) {
            throw new \Exception('No file name');
        }
        $readerXLSX = ReaderEntityFactory::createXLSXReader();
        $readerXLSX->open($fileName);
        //we take the first sheet
        foreach ($readerXLSX->getSheetIterator() as $worksheet) {
            break;
        };
        $withConsoleLog && LogHelper::consoleMessage("File {$fileName} loaded successfully", true);

        $this->updateTablePostindex($worksheet, $withConsoleLog);
    }

    /**
     * Creates a table if it does not already exist in the database.
     * The table name and fields are defined in the class properties.
     * The method generates a SQL query and executes it using the database connection object.
     *
     * @return void
     */
    public function createIfExistTable()
    {
        $sql = "CREATE TABLE IF NOT EXISTS `{$this->tableName}` (";
        foreach ($this->getAllFields() as $field => $options) {
            $sql .= "`{$field}` {$options['type']}, ";
        }
        $sql = rtrim($sql, ', ');
        $sql .= ')';
        $this->_db->exec($sql);
    }


    /**
     * Associates the columns in an XLS worksheet with corresponding database fields.
     *
     * @param Worksheet $worksheet The XLS worksheet to associate columns with.
     * @param array $columnHeaderToFieldDB Reference to an array to store the column header to database field mapping.
     *
     * @return int The index of the last processed row in the worksheet.
     */
    protected function associateXLSColumnsWithDBFields($worksheet, &$columnHeaderToFieldDB)
    {
        foreach ($worksheet->getRowIterator() as $rowIndex => $row) {
            if (count($row->getCells()) === 0) {
                continue;
            }
            $cellIterator = $row->getCells();
            foreach ($cellIterator as $key => $cell) {
                if (!$value = $cell->getValue()) {
                    continue;
                }
                foreach ($this->fields as $field => $options) {
                    if ($options['columnXLSX'] && $value === $options['columnXLSX']) {
                        $columnHeaderToFieldDB[$key] = $field;
                    }
                }
            }
            break;
        }
        return $rowIndex + 1;
    }

    /**
     * Updates the postindex table with data from an XLS worksheet.
     *
     * @param Worksheet $worksheet The XLS worksheet containing the data.
     * @param bool $withConsoleLog (optional) Whether to output log messages to the console. Default is false.
     *
     * @return void
     * @throws \Exception If the field key is not found in the header column of the XLSX file.
     *
     */
    public function updateTablePostindex($worksheet, $withConsoleLog = false)
    {
        $insert = 0;
        $update = 0;
        $skip = 0;
        $columnHeaderToFieldDB = [];
        $rowIndex = $this->associateXLSColumnsWithDBFields($worksheet, $columnHeaderToFieldDB);

        $withConsoleLog && LogHelper::consoleMessage("Synchronization columns of xlsx with fields of DB was successful", true);

        $keyRowIndex = array_search($this->fieldKey, $columnHeaderToFieldDB);

        if ($keyRowIndex === false) {
            throw new \Exception("Not found field in header column in XLSX file");
        }

        $queryFindByKey = $this->_db->prepare("SELECT * FROM `{$this->tableName}` WHERE `{$this->fieldKey}` = :{$this->fieldKey}");
//        $queryFindByKeyIn = $this->_db->prepare("SELECT * FROM `{$this->tableName}` WHERE `{$this->fieldKey}` IN (:{$this->fieldKey}_ids)");
        $queryInsert = $this->_db->prepare($this->generateInsertQuery());

        $checkDateTimeStart = (new \DateTime())->format('Y-m-d H:i:s');

        $idsForUpdateSkip = [];
        //update DB
        foreach ($worksheet->getRowIterator() as $rowIndexCurrent => $row) {

            if ($rowIndex > $rowIndexCurrent || count($row->getCells()) === 0) {
                continue;
            }

            $cellIterator = $row->getCells();

            if (!$postOfficeId = $cellIterator[$keyRowIndex]->getValue()) {
                $skip++;
                continue;
            }

            $queryFindByKey->execute([
                $this->fieldKey => $postOfficeId,
            ]);

            $flgInsert = $queryFindByKey->rowCount() === 0;

            $valuesBD = $flgInsert ? [] : $queryFindByKey->fetch(\PDO::FETCH_ASSOC);

            $rowForDB = [];

            foreach ($columnHeaderToFieldDB as $numCell => $field) {
                if (($flgInsert || $valuesBD[$field] !== $cellIterator[$numCell]->getValue())) {
                    $rowForDB[$field] = $cellIterator[$numCell]->getValue();
                }
            }
            if (count($rowForDB) > 0) {
                if ($flgInsert) {
                    $rowForDB = array_merge($rowForDB, $this->handlingRequiredFields(self::ACTION_PROCESSING_INSERT));
                    $queryInsert->execute($rowForDB);
                    $insert++;
                } else {
                    $rowForDB = array_merge($rowForDB, $this->handlingRequiredFields(self::ACTION_PROCESSING_UPDATE));
                    $this->_db->exec($this->generateUpdateQuery($postOfficeId, $rowForDB));
                    $update++;
                }
            } else {
                $idsForUpdateSkip[] = $postOfficeId;
                $skip++;
                if ($this->checkAndApllySkipRow($idsForUpdateSkip)) {
                    $idsForUpdateSkip = [];
                }
            }
        }
        $this->checkAndApllySkipRow($idsForUpdateSkip, true);
        $this->findAndDeleteUnusedPostindex($checkDateTimeStart);

        $withConsoleLog && LogHelper::consoleMessage("Update data for DB was successful", true);
        $withConsoleLog && LogHelper::consoleMessage('Inserts: ' . $insert);
        $withConsoleLog && LogHelper::consoleMessage('Updates: ' . $update);
        $withConsoleLog && LogHelper::consoleMessage('Skips: ' . $skip);
        $withConsoleLog && LogHelper::consoleMessage('Rows: ' . $rowIndexCurrent);
    }

    protected function checkAndApllySkipRow($rowSkipped, $mustHave = false)
    {
        $res = false;
        if (count($rowSkipped) >= $this->countQueries || $mustHave) {
            $dataForDB = $this->handlingRequiredFields(self::ACTION_PROCESSING_SKIP);
            $sql = $this->generateUpdateQuery($rowSkipped, $dataForDB);
            $this->_db->exec($sql);
            $res = true;
        }
        return $res;
    }

    protected function handlingRequiredFields($process = self::ACTION_PROCESSING_INSERT)
    {
        $resArr = [];
        foreach ($this->requiredFields as $field => $options) {
            if (in_array($process, $options['action']) !== false) {
                $func = $options['func'];
                $resArr[$field] = $this->$func();
            }
        }
        return $resArr;
    }

    /**
     * Finds and deletes unused postindex records from the specified table.
     *
     * @param string $controlTime The control time to compare with DATETIME fields.
     *
     * @return void|bool Returns false if no records are deleted, otherwise nothing is returned.
     */
    protected function findAndDeleteUnusedPostindex($controlTime)
    {
        $sql = "DELETE FROM `{$this->tableName}` WHERE ";
        $condition = '';
        foreach ($this->requiredFields as $field => $options) {
            if (in_array(self::ACTION_PROCESSING_DELETE, $options['action'])) {
                if ($options['type'] === 'DATETIME') {
                    $condition .= "`{$field}` < '{$controlTime}' AND ";
                }
                if ($options['type'] === 'INT') {
                    $value = $options['func']();
                    $condition .= "`{$field}` = {$value} AND ";
                }
            }
        }
        //if the condition is empty do nothing, return false
        if (strlen($condition) === 0) {
            return false;
        }
        $condition = substr($condition, 0, -4);
        print_r($sql . $condition . "\n");

        $this->_db->exec($sql . $condition);
    }

    protected function getAllFields()
    {
        return array_merge($this->fields, $this->requiredFields);
    }

    protected function generateInsertQuery()
    {
        $sql = "INSERT INTO `{$this->tableName}` (";
        $sql .= implode(',', array_keys($this->getAllFields()));
        $sql .= ") VALUES (";
        foreach ($this->getAllFields() as $key => $value) {
            $sql .= ":{$key},";
        }
        $sql = rtrim($sql, ',');
        $sql .= ");";
        return $sql;
    }

    protected function generateUpdateQuery($keyValue, $row)
    {
        $sql = "UPDATE `{$this->tableName}` SET ";
        foreach ($row as $key => $value) {
            $sql .= "{$key} = \"{$value}\", ";
        }
        $sql = substr($sql, 0, -2);

        if (is_array($keyValue)) {
            $sql .= " WHERE `{$this->fieldKey}` IN (";
            foreach ($keyValue as $key => $value) {
                $sql .= $this->_db->quote($value) . ", ";
            }
            $sql = substr($sql, 0, -2);
            $sql .= ");";
        } else {
            $sql .= " WHERE `{$this->fieldKey}` = '{$keyValue}';";
        }

        return $sql;
    }


}
