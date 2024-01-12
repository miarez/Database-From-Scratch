<?php
namespace Chapter2;

class BTreeNode {
    public array $keys = [];
    public array $children = [];
    public bool $isLeaf;

    public function __construct($isLeaf) {
        $this->isLeaf = $isLeaf;
    }
}

class BTree {
    private array|null|BTreeNode $root;
    private int $t;  # Minimum degree (defines the range for the number of keys)

    public function __construct($t) {
        $this->root = null;
        $this->t = $t;
    }

    // Insert a new key and value to the B-tree
    public function insert(
        string $key,
        int $at_index
    ) : void
    {
        if ($this->root == null) {
            // Tree is empty, allocate root
            $this->root = new BTreeNode(true);
            $this->root->keys[0] = ['key' => $key, 'value' => $at_index];  // Insert key-value
        } else {
            // If root is full, then tree grows in height
            if (count($this->root->keys) == 2 * $this->t - 1) {
                $s = new BTreeNode(false);

                // Make old root as child of new root
                $s->children[0] = $this->root;

                // Split the old root and move 1 key to the new root
                $this->splitChild($s, 0, $this->root);

                // New root has two children now. Decide which of the two children is going to have a new key
                $i = 0;
                if ($s->keys[0]['key'] < $key) {
                    $i++;
                }
                $this->insertNonFull($s->children[$i], ['key' => $key, 'value' => $at_index]);

                // Change root
                $this->root = $s;
            } else {
                // If root is not full, call insertNonFull for root
                $this->insertNonFull($this->root, ['key' => $key, 'value' => $at_index]);
            }
        }
    }


    private function insertNonFull(
        BTreeNode $node,
        array $kv
    ) : void
    {
        // Initialize index as the rightmost element
        $i = count($node->keys) - 1;

        // If this is a leaf node
        if ($node->isLeaf) {
            // The leaf is where we finally decide to insert the key
            while ($i >= 0 && $node->keys[$i]['key'] > $kv['key']) {
                $i--;
            }

            // If the key already exists, append the index to the list of indices
            if ($i >= 0 && $node->keys[$i]['key'] == $kv['key']) {
                if (!is_array($node->keys[$i]['value'])) {
                    // Convert the existing value to an array if it's not already one
                    $node->keys[$i]['value'] = [$node->keys[$i]['value']];
                }
                $node->keys[$i]['value'][] = $kv['value'];
            } else {
                // Otherwise, insert a new key-value pair, where value is a list of indices
                array_splice($node->keys, $i + 1, 0, [['key' => $kv['key'], 'value' => [$kv['value']]]]);
            }
        } else {
            // Find the child that is going to have the new key
            while ($i >= 0 && $node->keys[$i]['key'] > $kv['key']) {
                $i--;
            }

            // Check if the found child is full
            if (count($node->children[$i + 1]->keys) == 2 * $this->t - 1) {
                // If the child is full, split it
                $this->splitChild($node, $i + 1, $node->children[$i + 1]);

                // After splitting, the middle key of $node->children[i] goes up and node->children[i] is split into two. See which of the two is going to have the new key
                if ($node->keys[$i + 1]['key'] < $kv['key']) {
                    $i++;
                }
            }
            $this->insertNonFull($node->children[$i + 1], $kv);
        }
    }

    // Function to split the child y of node x. i is the index of y in the child array of x.
    private function splitChild($x, $i, $y) {
        // Create a new node that is going to store (t-1) keys of y
        $z = new BTreeNode($y->isLeaf);
        for ($j = 0; $j < $this->t - 1; $j++) {
            $z->keys[$j] = $y->keys[$j + $this->t];
        }

        // Copy the last (t-1) children of y to z
        if (!$y->isLeaf) {
            for ($j = 0; $j < $this->t; $j++) {
                $z->children[$j] = $y->children[$j + $this->t];
            }
        }

        // Reduce the number of keys in y
        for ($j = count($y->keys); $j >= $this->t; $j--) {
            unset($y->keys[$j]);
        }

        // Move all children of x one step behind to create space for the new child
        for ($j = count($x->children) - 1; $j >= $i + 1; $j--) {
            $x->children[$j + 1] = $x->children[$j];
        }

        // Link the new child to this node
        $x->children[$i + 1] = $z;

        // A key of y will move to this node. Move all greater keys one space ahead
        for ($j = count($x->keys) - 1; $j >= $i; $j--) {
            $x->keys[$j + 1] = $x->keys[$j];
        }

        // Copy the middle key of y to this node
        $x->keys[$i] = $y->keys[$this->t - 1];

        // Remove the middle key from y
        unset($y->keys[$this->t - 1]);
    }

    // Function to search for a key in the tree
    public function search(
        string $key
    ) : null|array
    {

        return $this->root == null ? null : $this->_search($this->root, $key);
    }

    // Helper function to search for a key in a subtree rooted with this node
    private function _search($node, $key) {


        // Find the first key greater than or equal to the given key
        $i = 0;
        while ($i < count($node->keys) && $key > $node->keys[$i]['key']) {
            $i++;
        }


        // If the found key is equal to the key we are searching for, return its value
        if ($i < count($node->keys) && $node->keys[$i]['key'] == $key) {
            return $node->keys[$i]['value'];  // Now, this is a list of indices
        }
        // If the key is not found here and this is a leaf node, the key is not present in the tree
        if ($node->isLeaf) {
            return null;
        }

        // Go to the appropriate child
        return $this->_search($node->children[$i], $key);
    }
}


