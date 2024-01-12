<?php
require_once '../../project-utils.php';
require_once '../src/BTree.php';
use Chapter2\BTree;

// Instantiate the B-tree with a minimum degree
$tree = new BTree(3);

# Your array of first names
$firstNames = ["Olivia", "Emma", "Ava", "Sophia", "Isabella", "Charlotte", "Amelia", "Mia", "Harper", "Evelyn", "Abigail", "Emily", "Ella", "Olivia", "Scarlett", "Grace", "Chloe", "Camila", "Penelope", "Riley", "Layla", "Olivia"];

# Insert names into the B-tree with their indices
foreach ($firstNames as $row_offset => $name)
{
    $tree->insert($name, $row_offset);
}

# Search for a specific name and retrieve the index
$nameToSearch = "Olivia";
$foundIndex = $tree->search($nameToSearch);

pp($foundIndex, 1);

