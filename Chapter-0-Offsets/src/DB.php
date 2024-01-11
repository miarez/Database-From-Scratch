<?php
namespace Chapter0;
use Exception;

class DB {
    private string $table_record_file;

    # The actual stream of data
    private $data_stream;
    private int $block_size = 48;
    private int $storage_size = 480000;

    public function __construct(){}

    private function initialize_table(
        $table_name
    ) : void
    {
        $table_name1 = $table_name;
        $this->table_record_file    = "../data/$table_name1.data";
        # todo why c+?
        $this->data_stream = fopen($this->table_record_file, 'c+');
    }


    public function drop_table(
        $table_name
    ) : void
    {
        $this->initialize_table($table_name);
        unlink($this->table_record_file);
    }

    public function table_details(
        $table_name
    ) : array
    {
        $this->initialize_table($table_name);
        return fstat($this->data_stream);
    }


    /**
     * @throws Exception
     */
    public function insert(
        $table_name,
        $record
    ): void {

        $this->initialize_table($table_name);

        $record = $this->serialize_record($record);
        $recordSize = strlen($record);

        $file_size = $this->table_details($table_name)['size'];

        $blockInfo = $this->prepareNewBlock($file_size, $recordSize);

        if ($this->exceedsStorage($blockInfo['blockId'], $recordSize)) {
            throw new Exception("Insufficient storage space to insert the record.");
        }

        $this->writeRecord($record, $blockInfo['insertionPoint']);
    }


    private function prepareBlockInfo(
        int $blockId,
        int $spaceUsedInLastBlock,
        int $recordSize
    ): array {
        $insertionPoint = ($blockId * $this->block_size) + $spaceUsedInLastBlock;
        $block_used_space = $spaceUsedInLastBlock + $recordSize;
        return [
            'blockId' => $blockId,
            'insertionPoint' => $insertionPoint,
            'block_used_space' => $block_used_space
        ];
    }

    private function prepareNewBlock(
        int $file_size,
        int $recordSize
    ): array {
        $blockId                = intdiv($file_size, $this->block_size);
        $spaceUsedInLastBlock   = $file_size % $this->block_size;

        if ($this->block_size - $spaceUsedInLastBlock < $recordSize) {
            $blockId++;
            $spaceUsedInLastBlock = 0;
        }
        return $this->prepareBlockInfo($blockId, $spaceUsedInLastBlock, $recordSize);
    }

    private function exceedsStorage(int $blockId, int $recordSize): bool {
        return (($blockId * $this->block_size) + $recordSize) > $this->storage_size;
    }

    private function writeRecord(string $record, int $insertionPoint): void {
        fseek($this->data_stream, $insertionPoint);
        fwrite($this->data_stream, $record);
    }

    private function processBlock(string $block): array {
        $records = [];
        $offset = 0;
        while ($offset < strlen($block)) {
            if ($offset + 4 > strlen($block)) { // Not enough bytes for another record's length prefix
                break;
            }

            // Extract length of the record
            $lengthArr  = unpack('N', substr($block, $offset, 4));
            $length     = $lengthArr[1];
            $offset     += 4; // Move past the length prefix

            # The record exceeds block boundary
            if ($offset + $length > strlen($block)) {
                break;
            }

            $records[] = substr($block, $offset, $length);

            $offset += $length; // Move to the next record
        }
        return $records;
    }

    private function serialize_record(
        $record
    ) : string
    {
        $totalLength = strlen($record);
        $lengthPrefix = pack('N', $totalLength);
        return $lengthPrefix . $record;
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

}


