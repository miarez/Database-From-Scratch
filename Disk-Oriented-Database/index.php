<?php


function pp(
    $a,
    int     $exit   =0,
    string  $label  =''
) : void
{
    echo "<PRE>";
    if($label) echo "<h5>$label</h5>";
    if($label) echo "<title>$label</title>";
    echo "<pre>";
    print_r($a);
    echo '</pre>';
    if($exit) exit();
}

class DiskOrientedDatabase {

    private array $index    = [];

    private array $buffer_pool = [];
    private const BUFFER_POOL_SIZE = 3;
    private string $db_file_path = 'database.txt';

    public function __construct()
    {
        # truncate DB everytime for test purposes
        file_put_contents($this->db_file_path, '');
    }

    public function print_indexes() : void
    {
        pp($this->index, 0, "Indices");
    }

    public function print_buffer_pool() : void
    {
        pp($this->buffer_pool, 0, "BUFFER POOL");
    }

    public function insert(
        array $record
    ): void
    {
        $this->index_record($record);

        # Write Record To Disk
        file_put_contents($this->db_file_path, json_encode($record)."\n", FILE_APPEND);
    }

    private function index_record(
        array $record
    ) : void
    {
        $offset = count(file($this->db_file_path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES));
        $this->index['name'][$record['name']][]        = $offset;
        $this->index['country'][$record['country']][]  = $offset;
    }

    public function read_where(
        string $field,
        string $value
    ) : array
    {
        $records = [];

        $db_offsets = $this->index[$field][$value];
        if(!isset($db_offsets)) return [];

        foreach($db_offsets as $offset_index=>$offset)
        {
            for($i = 0; $i < self::BUFFER_POOL_SIZE; $i++)
            {
                if(isset($this->buffer_pool[$i]['offset']) && $this->buffer_pool[$i]['offset'] === $offset)
                {
                    echo "<BR>Reading $field === $value From Buffer Pool<BR>";
                    $records[] = $this->buffer_pool[$i]['record'];
                    unset($db_offsets[$offset_index]);
                }
            }
        }

        $fileContents = file($this->db_file_path);

        foreach($db_offsets as $offset)
        {
            echo "<BR>Reading $field === $value From Disk<BR>";
            $record = json_decode($fileContents[$offset], true);
            $this->add_to_buffer_pool($offset, $record);
            $records[] = $record;
        }
        return $records;

    }

    private function add_to_buffer_pool(
        int $offset,
        array $record
    ){

        for($i = 0; $i < self::BUFFER_POOL_SIZE; $i++)
        {
            if(empty($this->buffer_pool[$i]))
            {
                echo "<BR>WRITE TO EMPTY BUFFER SLOT [$i]<BR>";
                $this->buffer_pool[$i] = [
                    "time" => microtime(),
                    "offset" => $offset,
                    "record" => $record
                ];
                return;
            }
        }

        // Use array_reduce to find the index of the smallest 'time'
        $array = $this->buffer_pool;
        $smallest_index = array_reduce(array_keys($array), function ($carry, $key) use ($array) {
            if (is_null($carry)) {
                return $key;
            }
            return $array[$key]['time'] < $array[$carry]['time'] ? $key : $carry;
        });


        echo "<BR>REPLACING BUFFER SLOT [$smallest_index]<BR>";

        $this->buffer_pool[$smallest_index] = [
            "time" => microtime(),
            "offset" => $offset,
            "record" => $record
        ];
    }
}

$db = new DiskOrientedDatabase();

$records = [
    ["name" => "Alice",     "country" => "US", "high score" => 110],
    ["name" => "Jonathan",  "country" => "US", "high score" => 100],
    ["name" => "Anita",     "country" => "ES", "high score" => 120],
    ["name" => "Samantha",  "country" => "US", "high score" => 80],
    ["name" => "Carlos",    "country" => "ES", "high score" => 90],
];

foreach($records as $record)
{
    $db->insert($record);
}


$db->read_where("name", "Jonathan");
$db->read_where("name", "Alice");
$db->read_where("name", "Anita");
$db->read_where("name", "Samantha");
$db->read_where("name", "Alice"); # should still be in buffer pool
$db->read_where("name", "Carlos"); # carlos will replace Alice in buffer pool as she is the older entry
$records = $db->read_where("country", "US"); # record #1 from buffer pool (samantha), record #2/#3 from disk as buffer is filled with ES
pp($records, 1, "US Records");
