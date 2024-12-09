#! /usr/bin/env python

# Originally taken from:
# https://stackoverflow.com/questions/748675/finding-duplicate-files-and-removing-them

#!/usr/bin/env python
from time import sleep
import sys
import os
import hashlib
import json
import csv

#stores our hashes
hashes_by_size = {}
hashes_on_1k = {}
hashes_full = {}

# log all duplicates
csvfile = open('findDuplicates.csv', 'w', newline='')
csvwriter = csv.writer(csvfile)
csvwriter.writerow(['filename1', 'filename2', 'size (MB)'])

def chunk_reader(fobj, chunk_size=1024):
    """Generator that reads a file in chunks of bytes"""
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


def get_hash(filename, first_chunk_only=False, hash=hashlib.sha1):
    hashobj = hash()
    file_object = open(filename, 'rb')

    if first_chunk_only:
        hashobj.update(file_object.read(1024))
    else:
        for chunk in chunk_reader(file_object):
            hashobj.update(chunk)
    hashed = hashobj.hexdigest()

    file_object.close()
    return hashed

def printinplace(str, file=sys.stdout):
    file.write('\r')
    file.write("%s" % str)
    file.flush


def check_for_duplicates(paths, hash=hashlib.sha1):

    print("Indexing files...")
    for path in paths:
        for dirpath, dirnames, filenames in os.walk(path):
            print("\tProcessing: %s" % os.path.abspath(dirpath))
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                try:
                    # if the target is a symlink (soft one), this will
                    # dereference it - change the value to the actual target file
                    full_path = os.path.realpath(full_path)
                    file_size = os.path.getsize(full_path)
                except (OSError,):
                    # not accessible (permissions, etc) - pass on
                    continue

                duplicate = hashes_by_size.get(file_size)

                if duplicate:
                    hashes_by_size[file_size].append(full_path)
                else:
                    hashes_by_size[file_size] = []  # create the list for this file size
                    hashes_by_size[file_size].append(full_path)

    print("Found %d files with similar file size, doing first level compare..." % len(hashes_by_size))
    # For all files with the same file size, get their hash on the 1st 1024 bytes

    for __, files in hashes_by_size.items():
        if len(files) < 2:
            continue    # this file size is unique, no need to spend cpy cycles on it

        for filename in files:
            try:
                small_hash = get_hash(filename, first_chunk_only=True)
            except (OSError,):
                # the file access might've changed till the exec point got here
                continue

            duplicate = hashes_on_1k.get(small_hash)
            if duplicate:
                hashes_on_1k[small_hash].append(filename)
            else:
                hashes_on_1k[small_hash] = []          # create the list for this 1k hash
                hashes_on_1k[small_hash].append(filename)

    # For all files with the hash on the 1st 1024 bytes, get their hash on the full file - collisions will be duplicates

    print("Found %d files with similar headers, doing deep compare..." % len(hashes_on_1k))
    # total megabytes
    total_dupe_size_mb = 0

    for __, files in hashes_on_1k.items():
        if len(files) < 2:
            continue    # this hash of fist 1k file bytes is unique, no need to spend cpy cycles on it

        for filename in files:
            try:
                full_hash = get_hash(filename, first_chunk_only=False)
            except (OSError,):
                # the file access might've changed till the exec point got here
                continue

            duplicate = hashes_full.get(full_hash)
            if duplicate:

              file_size_mb = os.path.getsize(filename)/(1024*1024)
              total_dupe_size_mb += file_size_mb

              printinplace("\tTotal duplicates found: %.02f MB" % (total_dupe_size_mb))
              csvwriter.writerow([filename, duplicate, "%.02f" % file_size_mb])
            else:
                hashes_full[full_hash] = filename

    if total_dupe_size_mb == 0:
        print("No duplicates found")

if sys.argv[1:]:
    check_for_duplicates(sys.argv[1:])
else:
    print("Please pass the paths to check as parameters to the script"
)

def dumparrays():
    with open("findDuplicates_hashes_by_size.json", "w") as filehandle:
      json.dump(hashes_by_size, filehandle)
    with open("findDuplicates_hashes_on_1k.json", "w") as filehandle:
      json.dump(hashes_on_1k, filehandle)
    with open("findDuplicates_hashes_full.json", "w") as filehandle:
      json.dump(hashes_full, filehandle)
    csvfile.close()

import atexit
atexit.register(dumparrays)
