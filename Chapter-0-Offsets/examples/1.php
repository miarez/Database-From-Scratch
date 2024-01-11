<?php
require_once '../../project-utils.php';
require_once '../src/DB.php';
use Chapter0\DB;

$db = new DB();
$table_name = "my_table";

# Insert multiple records
for($i = 0; $i < 100; $i++)
{
    $db->insert($table_name, "hello world");
}

# Full Table Scan All Records
$ans = $db->read($table_name);

echo sizeof($ans);
pp($ans, 1, "RESULT:");