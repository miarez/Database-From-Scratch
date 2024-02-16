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
class DB {
    public array $database;
    public array $locks;

    public function __construct() {
        $this->database = [
            0 => 5,
            1 => 6,
            2 => 15,
            3 => 9,
        ];
        $this->locks = []; // Track locks on rows
    }

    // Lock a row for a transaction
    public function lockRow($row_id, $transaction_id) {
        if (isset($this->locks[$row_id]) && $this->locks[$row_id] !== $transaction_id) {
            throw new Exception("Row is already locked by another transaction");
        }
        $this->locks[$row_id] = $transaction_id;
    }

    // Unlock a row
    public function unlockRow($row_id) {
        unset($this->locks[$row_id]);
    }

    // Check if a row is locked by a specific transaction
    public function isRowLockedByTransaction($row_id, $transaction_id) {
        return isset($this->locks[$row_id]) && $this->locks[$row_id] === $transaction_id;
    }
}

class Transaction {
    public int $id;
    public array $instructions = [];
    public DB $db;
    private array $localChanges = [];

    public function __construct($db) {
        $this->id = mt_rand(0, 1000);
        $this->db = $db;
    }

    public static function begin($db): self {
        return new Transaction($db);
    }

    public function bind_read($row_id): void {
        $this->instructions[] = [
            "type" => "read",
            "row_id" => $row_id,
        ];
    }

    public function bind_increment($row_id): void {
        // Check if the row is already locked by another transaction
        if ($this->db->isRowLockedByTransaction($row_id, $this->id)) {
            // The row is locked by the current transaction, so it's safe to proceed
        } else if (isset($this->db->locks[$row_id])) {
            // The row is locked by another transaction, so throw an exception
            throw new Exception("Row $row_id is already locked by another transaction");
        } else {
            // Lock the row for this transaction
            $this->db->lockRow($row_id, $this->id);
        }

        $this->instructions[] = [
            "type" => "increment",
            "row_id" => $row_id,
        ];
    }

    private function executeInstruction($instruction) {
        switch ($instruction['type']) {
            case 'read':
                // Read from local changes if exists, otherwise from DB
                if (array_key_exists($instruction['row_id'], $this->localChanges)) {
                    return $this->localChanges[$instruction['row_id']];
                } else {
                    return $this->db->database[$instruction['row_id']];
                }
                break;
            case 'increment':
                // Increment in local changes to simulate uncommitted transaction state
                $currentValue = $this->db->database[$instruction['row_id']] ?? 0; // Default to 0 if not set
                $this->localChanges[$instruction['row_id']] = $currentValue + 1;
                $this->db->lockRow($instruction['row_id'], $this->id); // Lock the row
                break;
        }
    }

    public function commit() {
        // Automatically process instructions before committing
        foreach ($this->instructions as $instruction) {
            $this->executeInstruction($instruction);
        }
        // Apply changes to the database and release locks
        foreach ($this->localChanges as $row_id => $value) {
            $this->db->database[$row_id] = $value; // Apply changes to the database
            $this->db->unlockRow($row_id); // Unlock the row
        }
        $this->instructions = []; // Clear instructions
        $this->localChanges = []; // Clear local changes
    }

    public function rollback() {
        // Unlock all rows this transaction has locked, without applying changes
        foreach ($this->instructions as $instruction) {
            if ($instruction['type'] === 'increment') {
                $this->db->unlockRow($instruction['row_id']);
            }
        }
        $this->instructions = []; // Clear instructions
        $this->localChanges = []; // Clear local changes
    }
}

# Connect to our database
$db = new DB();

# Simulate BEGIN statement
$trx_a = Transaction::begin($db);

# Should have no problem reading
$trx_a->bind_read(0);

# Should acquire a lock here for record #0
$trx_a->bind_increment(0);
$trx_a->bind_read(0);

$trx_b = Transaction::begin($db);
$trx_b->bind_read(0);

try {
    $trx_b->bind_increment(0);
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
}

$trx_a->commit(); // Commit A and release locks

# Should be ok to increment the row now as A has released the locks
$trx_b->bind_increment(0);

$trx_b->commit();


pp($db->database, 1, "FINAL RESULTS");



