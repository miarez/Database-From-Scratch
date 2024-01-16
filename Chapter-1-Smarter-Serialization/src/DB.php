<?php
namespace Chapter1;
use Exception;


class DB {
    private string $table_name;
    private string $table_record_file;
    private string $table_space_file;
    private string $table_schema_file;

    # The actual stream of data
    private $data_stream;
    private int $block_size = 48;
    private int $storage_size = 480000;

    # Meta info
    private array $block_space_used = [];
    private array $schema = [];

    public function __construct()
    {
    }

    private function initialize_table(
        $table_name
    ) : void
    {
        $this->table_name           = $table_name;
        $this->table_record_file    = "$this->table_name.data";
        $this->table_space_file     = "$this->table_name-space.data";
        $this->table_schema_file    = "$this->table_name-schema.json";

        $this->data_stream = fopen($this->table_record_file, 'c+');

        if(!file_exists($this->table_schema_file)){
            throw new DataMisalignedException("Schema Not Found");
        }
        $this->schema = json_decode(file_get_contents($this->table_schema_file), true);
        if(!is_array($this->schema)){
            throw new DataMisalignedException("INVALID SCHEMA");
        }

        if (file_exists($this->table_space_file))
        {
            if(is_array($block_space = unserialize(file_get_contents($this->table_space_file))))
            {
                $this->block_space_used = $block_space;
            }
        }
    }



    public function drop_table(
        $table_name
    ) : void
    {
        $this->initialize_table($table_name);
        unlink($this->table_record_file);
        unlink($this->table_space_file);
        $this->block_space_used = [];
    }

    public function table_details(
        $table_name
    ) : array
    {
        $this->initialize_table($table_name);
        return fstat($this->data_stream);
    }

    public function create_table(
        string $table_name,
        array $schema
    ) : void
    {
        $this->initialize_table($table_name);
        file_put_contents($this->table_schema_file, json_encode($schema));
    }
    public function read(
        string $table_name
    ) : array
    {
        $this->initialize_table($table_name);

        rewind($this->data_stream);
        $records = [];
        while (!feof($this->data_stream)) {
            $block = fread($this->data_stream, $this->block_size);
            if ($block) {
                $records = array_merge($records, $this->processBlock($block));
            }
        }
        return $records;
    }



    public function insert(
        $table_name,
        $record
    ): void {

        $this->initialize_table($table_name);

        $record = $this->serialize_record($record);

        $recordSize = strlen($record);

        $file_size = fstat($this->data_stream)['size'];

        $blockInfo = $this->findSuitableBlock($recordSize, $file_size);

        if (!$blockInfo['re_using_existing_block'] && $this->exceedsStorage($blockInfo['blockId'], $recordSize)) {
            throw new Exception("Insufficient storage space to insert the record.");
        }

        $this->writeRecord($record, $blockInfo['insertionPoint']);
        $this->updateBlockSpaceUsed($blockInfo['blockId'], $blockInfo['block_used_space']);
        $this->persistBlockSpaceUsed();
    }

    private function findSuitableBlock(int $recordSize, int $file_size): array {
        foreach ($this->block_space_used as $block_id => $block_used_space) {
            if ($this->block_size - $block_used_space >= $recordSize) {
                return $this->prepareBlockInfo($block_id, $block_used_space, $recordSize, true);
            }
        }
        return $this->prepareNewBlock($file_size, $recordSize);
    }

    private function prepareBlockInfo(int $blockId, int $spaceUsedInLastBlock, int $recordSize, bool $existing): array {
        $insertionPoint = ($blockId * $this->block_size) + $spaceUsedInLastBlock;
        $block_used_space = $spaceUsedInLastBlock + $recordSize;
        return [
            'blockId' => $blockId,
            'insertionPoint' => $insertionPoint,
            'block_used_space' => $block_used_space,
            're_using_existing_block' => $existing
        ];
    }

    private function prepareNewBlock(
        int $file_size,
        int $recordSize
    ): array {
        $blockId = intdiv($file_size, $this->block_size);
        $spaceUsedInLastBlock = $file_size % $this->block_size;

        if ($this->block_size - $spaceUsedInLastBlock < $recordSize) {
            $blockId++;
            $spaceUsedInLastBlock = 0;
        }

        return $this->prepareBlockInfo($blockId, $spaceUsedInLastBlock, $recordSize, false);
    }

    private function exceedsStorage(int $blockId, int $recordSize): bool {
        return (($blockId * $this->block_size) + $recordSize) > $this->storage_size;
    }

    private function writeRecord(string $record, int $insertionPoint): void {
        fseek($this->data_stream, $insertionPoint);
        fwrite($this->data_stream, $record);
    }

    private function updateBlockSpaceUsed(int $blockId, int $block_used_space): void {
        $this->block_space_used[$blockId] = $block_used_space;
    }

    private function persistBlockSpaceUsed(): void {
        file_put_contents($this->table_space_file, serialize($this->block_space_used));
    }


    private function processBlock(string $block): array {
        $records = [];
        $offset = 0;
        while ($offset < strlen($block)) {
            if ($offset + 4 > strlen($block)) { // Not enough bytes for another record's length prefix
                break;
            }
            $lengthArr = unpack('N', substr($block, $offset, 4)); // Extract length of the record
            $length = $lengthArr[1];
            $offset += 4; // Move past the length prefix

            if ($offset + $length > strlen($block)) { // The record exceeds block boundary
                break;
            }
            $recordData = substr($block, $offset, $length);
            if(strlen($recordData) > 0){
                $records[] = $this->deserializeRecord($recordData);
            }
            $offset += $length; // Move to the next record
        }

        return $records;
    }

    private function serialize_record($record) : string {
        $schema = $this->schema;
        $serialized = '';

        foreach ($schema as $column) {
            if (!isset($record[$column['column']])) {
                throw new DataMisalignedException("Missing value for column: " . $column['column']);
            }

            $value = $record[$column['column']];
            switch ($column['type']) {
                case 'INT':
                    $serialized .= pack('N', $value);
                    break;
                case 'VARCHAR':
                    $length = strlen($value);
                    $serialized .= pack('n', $length);
                    $serialized .= $value;
                    break;
                // Add more types as necessary
            }
        }

        $totalLength = strlen($serialized);
        $lengthPrefix = pack('N', $totalLength);
        return $lengthPrefix . $serialized;
    }

    private function deserializeRecord($binaryData) : array {
        $schema = $this->schema;
        $offset = 0;
        $record = [];

        foreach ($schema as $column) {
            switch ($column['type']) {
                case 'INT':
                    if ($offset + 4 > strlen($binaryData)) {
                        throw new LengthException("Not enough data for INT");
                    }
                    $record[$column['column']] = unpack('N', substr($binaryData, $offset, 4))[1];
                    $offset += 4;
                    break;
                case 'VARCHAR':
                    if ($offset + 2 > strlen($binaryData)) {
                        # Don't panic, this is because of empty blocks...
                        throw new Error("Invalid length for VARCHAR data in column: " . $column['column']);
                    }
                    $length = unpack('n', substr($binaryData, $offset, 2))[1];
                    $offset += 2;
                    if ($offset + $length > strlen($binaryData)) {
                        throw new Error("Invalid VARCHAR data for column: " . $column['column']);
                    }
                    $record[$column['column']] = substr($binaryData, $offset, $length);
                    $offset += $length;
                    break;
                // Add more types as necessary
            }
        }
        return $record;
    }

}


