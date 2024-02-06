<?php

namespace app\models;

use app\helpers\LogHelper;
use Box\Spout\Reader\Common\Creator\ReaderEntityFactory;
use Box\Spout\Reader\XLSX\Sheet;

/**
 * Class for working with the PostIndex table.
 */
class PostIndex
{

    const ACTION_PROCESSING_INSERT = 'insert';
    const ACTION_PROCESSING_UPDATE = 'update';
    const ACTION_PROCESSING_DELETE = 'delete';
    protected $_db;

    protected $tableName = 'post_info';

    protected $fieldKey = 'post_office_id';

    protected $countQueries = 5000;

    /**
     * @var array $fields
     *
     * The $fields array represents the structure of a database table.
     * Each key in the array represents a column name, and its corresponding value is an array
     * containing information about the column, such as data type and column label in Excel file and other information
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
     * - dictionary: Whether the column is a dictionary (boolean)
     * - lang: The language of the column (string)
     * - addressFieldOrder: The order of the address field in the column. Value is order in string of full address (integer)
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
        'region_ukr_id' => [
            'type' => ' INT NOT NULL',
            'dictionary' => true,
            'columnXLSX' => 'Область',
            'addressFieldOrder' => 1,
            'lang' => 'ukr',
        ],
        'district_old_ukr_id' => [
            'type' => ' INT NOT NULL',
            'dictionary' => true,
            'columnXLSX' => 'Район (старий)',
            'lang' => 'ukr',
        ],
        'district_new_ukr_id' => [
            'type' => ' INT NOT NULL',
            'dictionary' => true,
            'columnXLSX' => 'Район (новий)',
            'addressFieldOrder' => 2,
            'lang' => 'ukr',
        ],
        'settlement_ukr_id' => [
            'type' => ' INT NOT NULL',
            'dictionary' => true,
            'columnXLSX' => 'Населений пункт',
            'addressFieldOrder' => 3,
            'lang' => 'ukr',
        ],
        'postal_code' => [
            'type' => 'varchar(5)',
            'columnXLSX' => 'Поштовий індекс (Postal code)',
        ],
        'region_en_id' => [
            'type' => ' INT NOT NULL',
            'dictionary' => true,
            'columnXLSX' => 'Region (Oblast)',
            'lang' => 'en',
        ],
        'district_new_en_id' => [
            'type' => ' INT NOT NULL',
            'dictionary' => true,
            'columnXLSX' => 'District new (Raion new)',
            'lang' => 'en',
        ],
        'settlement_en_id' => [
            'type' => ' INT NOT NULL',
            'dictionary' => true,
            'columnXLSX' => 'Settlement',
            'lang' => 'en',
        ],
        'post_office_ukr' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'Вiддiлення зв`язку',
            'lang' => 'ukr',
        ],
        'post_office_en' => [
            'type' => 'varchar(255)',
            'columnXLSX' => 'Post office',
            'lang' => 'en',
        ],
    ];

    /**
     * @var array $requiredFields
     *
     * An array that declares the required fields for a specific operation.
     * The array is structured as follows:
     *
     * [
     *     'field_name' => [
     *         'type' => 'field_data_type',
     *         'columnXLSX' => 'xlsx_column_name',
     *         'func' => 'function_name',
     *         'action' => ['action1', 'action2', ...],
     *         'forUser' => 1 or 0,
     *     ],
     *     ...
     * ]
     *
     * - 'field_name': The name of the field.
     * - 'type': The data type of the field.
     * - 'columnXLSX': The name of the corresponding column in the XLSX file, empty if is not in XLSX.
     * - 'func': The name of the function associated with the field.
     * - 'action': An array of actions where the field is required.
     * - 'forUser': Indicates if the field is required for user input (1) or not (0).
     */
    protected $requiredFields = [
        'created_manual' => [
            'type' => 'INT(1) NOT NULL DEFAULT 0',
            'columnXLSX' => '',
            'func' => 'setCreatedManual',
            'action' => [self::ACTION_PROCESSING_INSERT, self::ACTION_PROCESSING_DELETE],
            'forUser' => 1,
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
    ];

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
     * Retrieves the dictionaries from the "fields" array.
     *
     * @return array An associative array containing the dictionaries.
     *                The keys represent the fields that have associated dictionaries,
     *                and the values represent the corresponding dictionary names without the "_id" suffix.
     */
    protected function getDictionaries()
    {
        $res = [];
        foreach ($this->fields as $field => $options) {
            if (array_key_exists('dictionary', $options) && $options['dictionary'] === true) {
                $res[$field] = preg_replace('/(_id)$/', '', $field);
            }
        }
        return $res;
    }

    /**
     * Gets the name of the table associated with this object.
     *
     * @return string The name of the table.
     */
    public function getTableName()
    {
        return $this->tableName;
    }

    /**
     * Returns the field key.
     *
     * @return string The field key.
     */
    public function getFieldKey()
    {
        return $this->fieldKey;
    }


    protected function setCreatedManual()
    {
        return 0;
    }

    protected function setCurrentDateTime()
    {
        return (new \DateTime())->format('Y-m-d H:i:s');
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
        //create dictionaries
        foreach ($this->getDictionaries() as $dictionary) {
            $sql = "CREATE TABLE IF NOT EXISTS `{$dictionary}` (";
            $sql .= "`id` INT  PRIMARY KEY NOT NULL AUTO_INCREMENT, ";
            $sql .= "`value` VARCHAR(255) NOT NULL default '') ";
            $this->_db->exec($sql);
        }
        //create main table
        $sql = "CREATE TABLE IF NOT EXISTS `{$this->tableName}` (";
        foreach ($this->getAllFields() as $field => $options) {
            $sql .= "`{$field}` {$options['type']}, ";
        }
        foreach ($this->getDictionaries() as $field => $dictionary) {
            $sql .= "FOREIGN KEY (`{$field}`) REFERENCES `{$dictionary}` (`id`), ";
        }
        $sql = rtrim($sql, ', ');
        $sql .= ')';
        $this->_db->exec($sql);
    }


    /**
     * Associates the columns in an XLS worksheet with corresponding database fields.
     *
     * @param Sheet $worksheet The XLS worksheet to associate columns with.
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
     * Checks if a given field is derived from a dictionary.
     *
     * @param string $field The field to check.
     *
     * @return bool Returns `true` if the field is derived from a dictionary, `false` otherwise.
     */
    protected function isFieldFromDictionary($field)
    {
        return array_key_exists('dictionary', $this->fields[$field]) && $this->fields[$field]['dictionary'] === true;
    }

    /**
     * Adds a value to a dictionary table.
     *
     * @param string $nameDictionary The name of the dictionary table.
     * @param mixed $value The value to be added to the dictionary.
     *
     * @return int The ID of the last inserted row.
     *
     * @throws \Exception If there is an error inserting the value to the dictionary.
     */
    public function addValueToDictionary($nameDictionary, $value)
    {
        $query = $this->_db->prepare("INSERT INTO `{$nameDictionary}` (`value`) VALUES (?)");
        if (!$query->execute([$value])) {
            throw new \Exception("Error insert value {$value} to dictionary {$nameDictionary}");
        }
        return $this->_db->lastInsertId();
    }

    /**
     * Checks if a value exists in a dictionary and updates the dictionary if necessary.
     *
     * @param string $nameDictionary The name of the dictionary to check and update.
     * @param mixed $value The value to check and potentially add to the dictionary.
     * @param array $dictionaries Reference to an array containing the dictionaries.
     *
     * @return int The index of the value in the dictionary. If the value does not exist in the dictionary, it will be added and its new index will be returned.
     */
    protected function checkAndUpdateDictionary($nameDictionary, $value, &$dictionaries)
    {
        if (($resultId = array_search($value, $dictionaries[$nameDictionary])) === false) {
            $resultId = $this->addValueToDictionary($nameDictionary, $value);
            $dictionaries[$nameDictionary][$resultId] = $value;
        }
        return $resultId;
    }

    /**
     * Generates a comma-separated list of SQL fields.
     *
     * @return string The comma-separated list of SQL fields.
     */
    protected function getListOfFieldsForSQL()
    {
        $fields = '';
        foreach ($this->fields as $field => $options) {
            $fields .= "`{$field}`,";
        }
        return rtrim($fields, ',');
    }

    /**
     * Updates the postindex table with data from an XLS worksheet.
     *
     * @param Sheet $worksheet The XLS worksheet containing the data to update the postindex table.
     * @param bool $withConsoleLog True if console logs should be displayed during the update process, false otherwise.
     *
     * @return void
     * @throws \Exception If the key field is not found in the header column of the XLSX file.
     */
    public function updateTablePostindex($worksheet, $withConsoleLog = false)
    {
        $columnHeaderToFieldDB = [];
        //get column header to field DB
        $rowIndex = $this->associateXLSColumnsWithDBFields($worksheet, $columnHeaderToFieldDB);

        $withConsoleLog && LogHelper::consoleMessage("Synchronization columns of xlsx with fields of DB was successful", true);

        //check key field in header XLSX
        $keyRowIndex = array_search($this->fieldKey, $columnHeaderToFieldDB);

        if ($keyRowIndex === false) {
            throw new \Exception("Not found field in header column in XLSX file");
        }

        //load data form DB from Dictionaries

        $fieldToDictionary = $this->getDictionaries();

        /**
         * full this structure
         * $dictionaries=[
         *      <name-dictionary> => [
         *              <id> => <value>,
         *      ]
         * ]
         */
        $dictionaries = [];
        foreach ($fieldToDictionary as $dictionary) {
            $dictionaries[$dictionary] = [];
            $query = $this->_db->query("SELECT * FROM `{$dictionary}`");
            $data = $query->fetchAll(\PDO::FETCH_ASSOC);
            foreach ($data as $row) {
                $dictionaries[$dictionary][$row['id']] = $row['value'];
            }
        }

        //for load data from XLS
        $postsFromFile = [];
        //for delete unused post indexes
        $usedPostIndexes = [];
        //update DB
        foreach ($worksheet->getRowIterator() as $rowIndexCurrent => $row) {

            if ($rowIndex > $rowIndexCurrent || count($row->getCells()) === 0) {
                continue;
            }

            $cellIterator = $row->getCells();

            //skip rows with empty cell of key
            if (!$postOfficeId = $cellIterator[$keyRowIndex]->getValue()) {
                continue;
            }

            //select post index for delete unused post indexes in DB
            $usedPostIndexes[] = $postOfficeId;
            $post = [];

            foreach ($columnHeaderToFieldDB as $numCell => $field) {

                //get data from XLS, if value from dictionary - check and update - input id of dictionary, else get value
                $post[$field] = $this->isFieldFromDictionary($field)
                    ? $this->checkAndUpdateDictionary($fieldToDictionary[$field], $cellIterator[$numCell]->getValue(), $dictionaries)
                    : $cellIterator[$numCell]->getValue();
            }

            $postsFromFile[$postOfficeId] = array_merge($post);

            //check and apply data, data check and load to DB become of chunks, which define in $this->countQueries
            if (count($postsFromFile) === $this->countQueries) {
                $this->checkAndApplyData($postsFromFile, $withConsoleLog);
                $postsFromFile = [];
            }
        }
        $withConsoleLog && LogHelper::consoleMessage("1");
        if (count($postsFromFile) > 0) {
            $this->checkAndApplyData($postsFromFile, $withConsoleLog);
        }

        $withConsoleLog && LogHelper::consoleMessage("Deleting unused indexes from DB");

        //delete unused post indexes
        $this->findAndDeleteUnusedPostindex($usedPostIndexes);

        $withConsoleLog && LogHelper::consoleMessage("Unused indexes from DB were deleted successfully", true);

        $withConsoleLog && LogHelper::consoleMessage("Update data for DB was successful", true);
        $withConsoleLog && LogHelper::consoleMessage('Rows: ' . $rowIndexCurrent);
    }

    /**
     * Checks and applies data from an array of posts to the database.
     *
     * @param array $postsFromFile The array of posts to check and apply.
     * @param bool $withConsoleLog (optional) Whether to log a console message. Defaults to false.
     *
     * @return void
     */
    protected function checkAndApplyData($postsFromFile, $withConsoleLog = false)
    {
        //load from DB records by post indexes from $postsFromFile
        $sql = "select {$this->getListOfFieldsForSQL()} from {$this->tableName} where {$this->fieldKey} in (";
        foreach ($postsFromFile as $key => $value) {
            $sql .= $this->_db->quote($value[$this->fieldKey]) . ",";
        }
        $sql = substr($sql, 0, -1);
        $sql .= ")";
        $query = $this->_db->query($sql);
        $data = $query->fetchAll(\PDO::FETCH_ASSOC);

        $multiQueryUpdate = '';
        $multiQueryInsert = '';
        foreach ($data as $rowDB) {
            $rowDBUpdate = [];
            //create array for update <field>=><new value>
            foreach ($rowDB as $field => $value) {
                if ($postsFromFile[$rowDB[$this->fieldKey]][$field] != $value) {
                    $rowDBUpdate[$field] = $value;
                }
            }
            //generate update query, if need
            if (count($rowDBUpdate) > 0) {
                $sql = $this->generateNotPrepareUpdateQuery($rowDB[$this->fieldKey], array_merge($rowDBUpdate, $this->handlingRequiredFields(self::ACTION_PROCESSING_UPDATE)));
                $multiQueryUpdate .= $sql;
            }
            //post index exists in BD, then delete from $postsFromFile
            unset($postsFromFile[$rowDB[$this->fieldKey]]);
        }
        //generate insert query, if there are still entries in $postsFromFile
        foreach ($postsFromFile as $key => $value) {
            $sql = $this->generateNotPrepareInsertQuery(array_merge($value, $this->handlingRequiredFields(self::ACTION_PROCESSING_INSERT)));
            $multiQueryInsert .= $sql;
        }
        //apply queries
        !empty($multiQueryUpdate) && $this->_db->exec($multiQueryUpdate);
        !empty($multiQueryInsert) && $this->_db->exec($multiQueryInsert);
        $withConsoleLog && LogHelper::consoleMessage('Chunk applied', true);
    }


    /**
     * Handles the required fields for a particular process.
     *
     * @param string $process The process to handle the required fields for. Defaults to ACTION_PROCESSING_INSERT.
     *
     * @return array An associative array containing the results of processing the required fields.
     */
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
     * Finds and deletes unused post indexes from the database.
     *
     * @param array $usedPostIndexes The array of used post indexes, which were loaded from the file.
     *
     * @return void
     */
    protected function findAndDeleteUnusedPostindex($usedPostIndexes)
    {
        if (count($usedPostIndexes)) {

            //select all post indexes from DB
            $query = $this->_db->query("SELECT `{$this->fieldKey}` FROM `{$this->tableName}`");
            $postIndexesInDB = $query->fetchAll(\PDO::FETCH_COLUMN);

            //define unused post indexes
            $forDelete = array_diff($postIndexesInDB, $usedPostIndexes);

            if (count($forDelete)) {
                //create sql for delete
                $sql = "DELETE FROM `{$this->tableName}` WHERE ";
                $sql .= "`{$this->fieldKey}` IN (";
                foreach ($forDelete as $value) {
                    $sql .= $this->_db->quote($value) . ",";
                }
                $sql = substr($sql, 0, -1);
                $sql .= ")";

                //add condition for deleted records, which did not add by user
                foreach ($this->requiredFields as $field => $options) {
                    if (in_array(self::ACTION_PROCESSING_DELETE, $options['action'])) {
                        if ($options['type'] === 'INT') {
                            $value = $options['func']();
                            $sql .= " AND `{$field}` = {$value} ";
                        }
                    }
                }
                //deleting
                $this->_db->exec($sql);
            }
        }
    }

    /**
     * Retrieves all fields from the class.
     *
     * @return array Returns an array containing all the fields and required fields.
     */
    public function getAllFields()
    {
        return array_merge($this->fields, $this->requiredFields);
    }


    /**
     * Generates a prepared SQL insert query for the given table.
     *
     * @return string The prepared SQL insert query.
     */
    public function generatePrepareInsertQuery()
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

    /**
     * Generates a non-prepared insert query based on the given row data.
     *
     * @param array $row The data to insert into the database.
     *
     * @return string The generated insert query.
     */
    public function generateNotPrepareInsertQuery($row)
    {
        $sql = "INSERT INTO `{$this->tableName}` (";
        $sql .= implode(',', array_keys($row));
        $sql .= ") VALUES (";
        foreach ($row as $key => $value) {
            $sql .= $this->_db->quote($value) . ",";
        }
        $sql = rtrim($sql, ',');
        $sql .= ");";
        return $sql;
    }


    /**
     * Generates an UPDATE query statement without preparing the values.
     *
     * @param mixed $keyValue The value(s) to filter the update query on. If an array, multiple values will be used with IN operator.
     * @param array $row The data to update in the database table.
     *
     * @return string The generated UPDATE query statement.
     */
    public function generateNotPrepareUpdateQuery($keyValue, $row)
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


    /**
     * Retrieves fields based on the specified language.
     *
     * @param string $lang (optional) The language to filter the fields by. Defaults to an empty string.
     *
     * @return array An array containing the fields based on the specified language and who not define lang.
     */
    public function getFieldsByLang($lang = '')
    {
        $resArr = [];
        foreach ($this->fields as $field => $option) {
            if (!$lang || (array_key_exists('lang', $option) && $option['lang'] === $lang)
                || !array_key_exists('lang', $option)) {
                $resArr[] = $field;
            }
        }
        return $resArr;
    }

    /**
     * Generates the join statement for a select query based on the provided fields.
     *
     * @param array $fields The fields to generate the join for.
     *
     * @return string The join statement for the select query.
     */
    protected function getJoinOfDictionariesForSelect($fields)
    {
        $dictionaries = $this->getDictionaries();
        $join = '';
        foreach ($fields as $field) {
            if ($this->isFieldFromDictionary($field)) {
                $join .= " INNER JOIN `{$dictionaries[$field]}`  AS `d_{$dictionaries[$field]}` ON `d_{$dictionaries[$field]}`.`id` = `{$this->tableName}`.`{$field}` ";
            }
        }
        return $join;
    }

    /**
     * Get a list of fields for the dictionaries that can be used in a SELECT query.
     *
     * @param array $fields The fields to include in the list.
     * @param bool $withAlias Whether to include aliases for the dictionary fields.
     * @return array The list of fields for the dictionaries.
     */
    protected function getListFieldOfDictionariesForSelect($fields, $withAlias = true)
    {
        $dictionaries = $this->getDictionaries();
        $resArr = [];
        foreach ($fields as $field) {
            if ($this->isFieldFromDictionary($field)) {
                $resArr[] = "`d_{$dictionaries[$field]}`.`value`" .
                    ($withAlias ? " AS `{$dictionaries[$field]}`" : "");
            } else {
                $resArr[] = "`{$field}`";
            }
        }
        return $resArr;
    }

    /**
     * Returns the base SELECT statement for querying records from the specified table.
     *
     * @param string $lang The language code for which to retrieve the fields.
     * @param bool $withAddress (optional) Whether to include the address field.
     * @return string The generated SQL query.
     */
    public function getBaseSelect($lang, $withAddress = false)
    {
//        $fields = $this->getListFieldOfDictionariesForSelect($this->getFieldsByLang($lang));
        $fields = $this->getFieldsWithoutDictionary();
        $fields[] = $this->getExpressionAddressField($lang);
        $join = $this->getJoinOfDictionariesForSelect($this->getFieldsByLang($lang));


        $sql = "SELECT ";
        $sql .= implode(',', $fields);
        $sql .= " FROM `{$this->tableName}` {$join}";
        return $sql;
    }

    /**
     * Returns an array of address fields in the specified language.
     *
     * @param string $lang (optional) The language code for which to retrieve the fields.
     * @return array An associative array of address fields ordered by their order property.
     */
    protected function arrayOfAddressFields($lang = '')
    {
        $arrAddr = [];
        $fieldsLang = $this->getFieldsByLang($lang);
        foreach ($this->fields as $field => $value) {
            if (in_array($field, $fieldsLang) && isset($value['addressFieldOrder'])) {
                $arrAddr[$value['addressFieldOrder']] = $field;
            }
        }
        ksort($arrAddr);
        return $arrAddr;
    }

    /**
     * Returns an array of fields from the current object's "fields" property, excluding those that are from a dictionary.
     *
     * @return array The array of fields without dictionary.
     */
    public function getFieldsWithoutDictionary()
    {
        $fields = [];
        foreach ($this->fields as $field => $value) {
            if (!$this->isFieldFromDictionary($field)) {
                $fields[] = $field;
            }
        }
        return $fields;
    }

    /**
     * Returns the concatenated address field expression for the specified language.
     *
     * @param string $lang (optional) The language code for which to retrieve the address fields. Defaults to an empty string.
     * @return string The generated address field expression.
     */
    public function getExpressionAddressField($lang = '')
    {
        $strAddr = '';
        $fields = $this->getListFieldOfDictionariesForSelect($this->arrayOfAddressFields($lang), false);
        foreach ($fields as $field) {
            $strAddr .= ($strAddr === '' ? '' : "', ',") . $field . ',';
        }
        $strAddr = substr($strAddr, 0, -1);
        return "CONCAT({$strAddr}) as address";
    }

    /**
     * Returns an array of address fields for the specified language.
     *
     * @param string $lang (optional) The language code for which to retrieve the fields.
     * @param bool $withAlias (optional) Whether to include the field aliases.
     * @return array An array of address fields.
     */
    public function getAddressFields($lang = '', $withAlias = true)
    {
        return array_values($this->getListFieldOfDictionariesForSelect($this->arrayOfAddressFields($lang), $withAlias));
    }

}
