<?php

namespace app\modules\v3\commands\fix\akb;

use app\modules\v3\models\AutoModificationAkb;
use SplFileObject;
use yii\base\Exception;
use yii\console\Controller;
use yii\db\Migration;
use yii\helpers\FileHelper;

/**
 * @noinspection LongInheritanceChainInspection
 */

/**
 * Commands for import akb data for running only dev environments
 */
class ImportCsvController extends Controller
{
    private const DIR_FILES_PATH = __DIR__ . '/files/';
    private const FILE_NAME_AKB = 'podbor_akb_202105251742.csv';
    private const FILE_NAME_AKB_RZ = 'params_akb_in_rozetka.csv';
    private const FILE_NAME_VENDORS_MODELS_TO_IMPORT = 'vendors_models_to_import.csv';
    private const FILE_CSV_DELIMITER_AKB = ';';
    private const FILE_CSV_DELIMITER_AKB_RZ = ',';

    /**
     * data from files for mapping
     */
    private $dataAkb;
    private $dataAkbRz;
    private $dataVendorsModels;

    /**
     * @var Migration
     */
    private $migration;

    /**
     * {@inheritdoc}
     */
    public function init()
    {
        $this->migration = new Migration();
        parent::init();
    }

    /**
     * For start ./yii /v3/fix/akb/import-csv
     * Import tyres
     * @throws Exception
     */
    public function actionIndex()
    {
        if (!YII_ENV_DEV) {
            printf("Only for running in dev environments\n");
            return;
        }
        ini_set('memory_limit', '-1');

        if ($this->checkTablesEmpty()) {
            printf("Start read data.csv\n");
            $this->initData();
            printf("Start import modifications\n");
            $this->importModifications();
        }
    }

    /**
     * Delete data from db
     * For start ./yii /v3/fix/akb/import-csv/clear-tables
     */
    public function actionClearTables()
    {
        AutoModificationAkb::deleteAll();
        printf("tables in schema 'akb' are empty\n");
    }

    /**
     * Check if tables are empty
     */
    private function checkTablesEmpty(): bool
    {
        $tableNotEmpty = AutoModificationAkb::find()->exists();
        if ($tableNotEmpty) {
            printf("Only for empty tables\n");
            return false;
        }
        return true;
    }

    /**
     * Method init start data for import
     * @throws Exception
     */
    private function initData()
    {
        $path = static::DIR_FILES_PATH;
        if (!is_dir($path)) {
            FileHelper::createDirectory($path);
        }
        $this->dataVendorsModels = $this->getDataFromCsvFileAkbRz(
            $path . static::FILE_NAME_VENDORS_MODELS_TO_IMPORT,
            static::FILE_CSV_DELIMITER_AKB_RZ
        );
        $this->dataAkb = $this->getDataFromCsvFileAkb(
            $path . static::FILE_NAME_AKB,
            static::FILE_CSV_DELIMITER_AKB
        );
        $this->dataAkbRz = $this->getDataFromCsvFileAkbRz(
            $path . static::FILE_NAME_AKB_RZ,
            static::FILE_CSV_DELIMITER_AKB_RZ
        );
    }

    /**
     * Import modifications to DB
     */
    private function importModifications()
    {
        $errors = [];
        foreach ($this->dataAkb as $row) {
            if (isset($row['polarnost'])) {
                $vendor = str_replace("'", "''", $row['vendor']);
                $model = str_replace("'", "''", $row['car']);
                $modification = str_replace("'", "''", $row['modification']);

                $emkostValueIds = $emkostValueNames = $tokXolProkValueIds = $tokXolProkValueNames = [];
                $polarnostValueId = $dlinaValueId = $shirinaValueId = $visotaValueId = $typeKlemValueId = null;
                $polarnostValueName = $typeKlemValueName = null;

                foreach ($this->dataAkbRz as $rowAkbRz) {
                    if ($rowAkbRz['ID Ð¿Ð°Ñ€Ð°Ð¼ÐµÑ‚Ñ€Ð°'] == AutoModificationAkb::PARAMS_AKB_RZ['emkost_option_id']) {
                        if (
                            $rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'] >= $row['emkost_min']
                            && $rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'] <= $row['emkost_max']
                        ) {
                            $emkostValueIds[] = $rowAkbRz['ID Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'];
                            $emkostValueNames[] = $rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'];
                        }
                    } elseif ($rowAkbRz['ID Ð¿Ð°Ñ€Ð°Ð¼ÐµÑ‚Ñ€Ð°'] == AutoModificationAkb::PARAMS_AKB_RZ['polarnost_option_id']) {
                        $polarnostValueName = AutoModificationAkb::PARAMS_AKB_RZ['polarnost_value_name'][$row['polarnost']] ?? null;
                        if (!empty($polarnostValueName) && $rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'] == $polarnostValueName) {
                            $polarnostValueId = $rowAkbRz['ID Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'];
                        }
                    } elseif ($rowAkbRz['ID Ð¿Ð°Ñ€Ð°Ð¼ÐµÑ‚Ñ€Ð°'] == AutoModificationAkb::PARAMS_AKB_RZ['dlina_option_id']) {
                        if ($rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'] == $row['dlina']) {
                            $dlinaValueId = $rowAkbRz['ID Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'];
                        }
                    } elseif ($rowAkbRz['ID Ð¿Ð°Ñ€Ð°Ð¼ÐµÑ‚Ñ€Ð°'] == AutoModificationAkb::PARAMS_AKB_RZ['shirina_option_id']) {
                        if ($rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'] == $row['shirina']) {
                            $shirinaValueId = $rowAkbRz['ID Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'];
                        }
                    } elseif ($rowAkbRz['ID Ð¿Ð°Ñ€Ð°Ð¼ÐµÑ‚Ñ€Ð°'] == AutoModificationAkb::PARAMS_AKB_RZ['visota_option_id']) {
                        if ($rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'] == $row['visota']) {
                            $visotaValueId = $rowAkbRz['ID Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'];
                        }
                    } elseif ($rowAkbRz['ID Ð¿Ð°Ñ€Ð°Ð¼ÐµÑ‚Ñ€Ð°'] == AutoModificationAkb::PARAMS_AKB_RZ['type_klem_option_id']) {
                        $typeKlemValueName = AutoModificationAkb::PARAMS_AKB_RZ['type_klem_value_name'][$row['type_klem']] ?? null;
                        if (!empty($typeKlemValueName) && $rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'] == $typeKlemValueName) {
                            $typeKlemValueId = $rowAkbRz['ID Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'];
                        }
                    } elseif ($rowAkbRz['ID Ð¿Ð°Ñ€Ð°Ð¼ÐµÑ‚Ñ€Ð°'] == AutoModificationAkb::PARAMS_AKB_RZ['tok_xol_prok_option_id']) {
                        if (
                            $rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'] >= $row['tok_xol_prok_min']
                            && $rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'] <= $row['tok_xol_prok_max']
                        ) {
                            $tokXolProkValueIds[] = $rowAkbRz['ID Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'];
                            $tokXolProkValueNames[] = $rowAkbRz['ÐÐ°Ð·Ð²Ð°Ð½Ð¸Ðµ Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ'];
                        }
                    }
                }
                $sEmkostValueIds = implode(',', $emkostValueIds);
                $sEmkostValueNames = implode(',', $emkostValueNames);
                $sTokXolProkValueIds = implode(',', $tokXolProkValueIds);
                $sTokXolProkValueNames = implode(',', $tokXolProkValueNames);

                try {
                    $this->migration->compact = true;
                    $this->migration->execute(
                        'INSERT INTO ' . AutoModificationAkb::tableName()
                        . "(\"id\", \"car_type\", \"vendor\", \"model\", \"modification\", \"year\", \"emkost_value_ids\",
                        \"emkost_value_names\", \"emkosts\", \"polarnost_value_id\", \"polarnost_value_name\",
                        \"polarnost\", \"dlina_value_id\", \"dlina_value_name\", \"dlina\", \"shirina_value_id\",
                        \"shirina_value_name\", \"shirina\", \"visota_value_id\", \"visota_value_name\", \"visota\",
                        \"type_klem_value_id\", \"type_klem_value_name\", \"type_klem\", \"tok_xol_prok_value_ids\",
                        \"tok_xol_prok_value_names\", \"tok_xol_proks\", \"artikul\")
                        VALUES ({$row['id']}, {$row['car_type']}, '{$vendor}', '{$model}', '{$modification}',
                        {$row['year']}, 
                        ARRAY[{$sEmkostValueIds}], ARRAY[{$sEmkostValueNames}], ARRAY[{$sEmkostValueNames}],
                        {$polarnostValueId}, '{$polarnostValueName}', {$row['polarnost']},
                        {$dlinaValueId}, {$row['dlina']}, {$row['dlina']}, 
                        {$shirinaValueId}, {$row['shirina']}, {$row['shirina']}, 
                        {$visotaValueId}, {$row['visota']}, {$row['visota']}, 
                        {$typeKlemValueId}, '{$typeKlemValueName}', '{$row['type_klem']}', 
                        ARRAY[{$sTokXolProkValueIds}], ARRAY[{$sTokXolProkValueNames}], ARRAY[{$sTokXolProkValueNames}],
                        '{$row['artikul']}');"
                    );
                } catch (\Exception $e) {
                    $errors[] = $e->getMessage();
                    continue;
                }
            }
        }
        var_dump($errors);
        echo 'Errors count = ' . count($errors);
    }

    /**
     * Get data from csv file by delimiter
     *
     * @param $path
     * @param $delimiter
     * @return array
     */
    private function getDataFromCsvFileAkb($path, $delimiter): array
    {
        $data = [];
        $file = new SplFileObject($path, 'r');
        $file->setCsvControl($delimiter);
        $headers = $file->fgetcsv();
        while (!$file->eof()) {
            $temp = [];
            $uniqueRow = '';
            foreach ($file->fgetcsv() as $key => $field) {
                $temp[$headers[$key]] = $field;
                if ($headers[$key] !== 'id') {
                    $uniqueRow .= $field;
                }
            }
            if (
                !array_key_exists('car_type', $temp) || $temp['car_type'] == 1
                || !array_key_exists('year', $temp) || $temp['year'] < 1990
                || !array_key_exists('vendor', $temp)
            ) {
                continue;
            }
            foreach ($this->dataVendorsModels as $row) {
                if ($temp['vendor'] == $row['vendor'] && $temp['car'] == $row['model']) {
                    $data[$uniqueRow] = $temp;
                    break;
                }
            }
        }
        return $data;
    }

    /**
     * Get data from csv file by delimiter
     *
     * @param $path
     * @param $delimiter
     * @return array
     */
    private function getDataFromCsvFileAkbRz($path, $delimiter): array
    {
        $data = [];
        $file = new SplFileObject($path, 'r');
        $file->setCsvControl($delimiter);
        $headers = $file->fgetcsv();
        while (!$file->eof()) {
            $temp = [];
            foreach ($file->fgetcsv() as $key => $field) {
                $temp[$headers[$key]] = $field;
            }
            $data[] = $temp;
        }
        return $data;
    }
}