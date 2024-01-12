<?php
namespace Chapter3;

class DB {
    private string $table_record_file;
    private string $table_space_file;

    # The actual stream of data
    private $data_stream;
    private int $block_size = 48;
    private int $storage_size = 480000;

    private array $block_space_used = [];

    public function __construct(){}

    private function initialize_table(
        $table_name
    ) : void
    {

        $this->table_record_file    = "../data/$table_name.data";
        $this->table_space_file     = "../data/$table_name-space.data";

        $this->data_stream = fopen($this->table_record_file, 'c+');

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

        $blockInfo = $this->findSuitableBlock($recordSize, $file_size);

        if (!$blockInfo['re_using_existing_block'] && $this->exceedsStorage($blockInfo['blockId'], $recordSize)) {
            throw new \Exception("Insufficient storage space to insert the record.");
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

