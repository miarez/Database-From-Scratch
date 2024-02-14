<?php
include 'src/LeapFrog.php';


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
# sort by cost



class LeapFrog {
    private $arrays;

    public function __construct(array $arrays) {
        $this->arrays = $arrays;
    }

    private function advance($array, $currentValue) {
        foreach ($array as $value) {
            if ($value >= $currentValue) {
                return $value;
            }
        }
        return null; // No next value found
    }

    public function findMatches() {
        $matches = [];
        $firstArray = $this->arrays[0];

        foreach ($firstArray as $value) {
            $currentValue = $value;
            $isCommon = true;

            for ($i = 1; $i < count($this->arrays); $i++) {
                $nextValue = $this->advance($this->arrays[$i], $currentValue);

                if ($nextValue === null) {
                    $isCommon = false;
                    break;
                }

                if ($nextValue == $currentValue) {
                    continue;
                } elseif ($nextValue > $currentValue) {
                    $currentValue = $nextValue;
                    // Restart from the first array with the new $currentValue
                    $i = -1; // It will be incremented to 0 at the end of the loop
                }
            }

            if ($isCommon && !in_array($currentValue, $matches)) {
                $matches[] = $currentValue;
                // Move to the next value in the first array that is greater than the current value
                $currentValue = $this->advance(array_slice($firstArray, array_search($currentValue, $firstArray) + 1), $currentValue);
                if ($currentValue === null) break; // Exit if no next value
            }
        }

        return $matches;
    }
}

// Example usage
$leapFrog = new LeapFrog([
    [2, 13, 17, 20, 98],
    [1, 13, 22, 35, 98, 99],
    [1, 3, 13, 20, 35, 80, 98],
]);

$matches = $leapFrog->findMatches();
pp($matches);











echo "SUP DAWG";




