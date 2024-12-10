# deduplicate | identify duplicate files/directories

## Is this your use case?
You have a bunch of files in two directories and you want to identify what director and/or files you can safely delete?

## Context for creation
My brother has a lot of junk on his NAS and there are way too many duplicates to manage

## Approach:
Based on code from: https://stackoverflow.com/questions/748675/finding-duplicate-files-and-removing-them

## Features:

1. Searches directories for duplicates based on size, first 1KB of the file, or full file, if there are collisions (based on stackoverflow accepted answer)
1. Stores analysis of comparison for easy reuse
1. Searches matched files for whole directory duplication for easy deletion (useful when there are lots of small files in a directory)