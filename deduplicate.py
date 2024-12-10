#!/usr/bin/env python
import os
import json
import hashlib
import argparse
from collections import defaultdict
from pprint import pprint

HASH_STORAGE_DIR = "hash_storage"

if not os.path.exists(HASH_STORAGE_DIR):
    os.makedirs(HASH_STORAGE_DIR)

class HashAnalysis:
    """Handles file hashing and analysis for a specific directory."""

    def __init__(self, directory, debug=False):
        if directory:
            self.directory = os.path.abspath(directory)
            self.hash_file = os.path.join(HASH_STORAGE_DIR,
                                          f"{os.path.basename(self.directory)}.json")
        else:
            self.directory = 'compare'
        self.hashes_by_size = defaultdict(list)
        self.hashes_on_1k = defaultdict(list)
        self.hashes_full = defaultdict(list)
        self.rev_hashes_by_size = {}
        self.rev_hashes_on_1k = {}
        self.rev_hashes_full = {}
        self.loaded = False
        self.debug = debug

    @staticmethod
    def chunk_reader(fobj, chunk_size=1024):
        """Generator that reads a file in chunks of bytes."""
        while True:
            chunk = fobj.read(chunk_size)
            if not chunk:
                return
            yield chunk

    @staticmethod
    def get_hash(filename, first_chunk_only=False, hash=hashlib.sha1):
        """Compute a hash for the given file."""
        hashobj = hash()
        try:
            with open(filename, 'rb') as file_object:
                if first_chunk_only:
                    hashobj.update(file_object.read(1024))
                else:
                    for chunk in HashAnalysis.chunk_reader(file_object):
                        hashobj.update(chunk)
        except OSError:
            return None
        return hashobj.hexdigest()

    def get_lookup_hash(self, first_chunk_only):
        if first_chunk_only:
            return self.hashes_on_1k
        else:
            return self.hashes_full

    def load_hashes(self):
        """Load stored hashes for this directory if available."""
        try:
            with open(self.hash_file, 'r') as f:
                data = json.load(f)
            self.hashes_by_size.update(data.get('hashes_by_size', {}))
            self.rev_hashes_by_size.update(data.get('rev_hashes_by_size', {}))
            self.hashes_on_1k.update(data.get('hashes_on_1k', {}))
            self.rev_hashes_on_1k.update(data.get('rev_hashes_on_1k', {}))
            self.hashes_full.update(data.get('hashes_full', {}))
            self.rev_hashes_full.update(data.get('rev_hashes_full', {}))
            self.loaded = True
            if self.debug:
                print(f"Loaded hashes for {self.directory} from {self.hash_file}.")
        except FileNotFoundError:
            print(f"No stored hashes found for {self.directory}, will analyze.")

    def save_hashes(self):
        """Save hashes for this directory."""
        with open(self.hash_file, 'w') as f:
            json.dump({
                'hashes_by_size': self.hashes_by_size,
                'rev_hashes_by_size': self.rev_hashes_by_size,
                'hashes_on_1k': self.hashes_on_1k,
                'rev_hashes_on_1k': self.rev_hashes_on_1k,
                'hashes_full': self.hashes_full,
                'rev_hashes_full': self.rev_hashes_full,
            }, f, indent=2)
        if self.debug:
            print(f"Hashes for {self.directory} saved to {self.hash_file}.")

    def delete_hashes(self):
        """Delete hashes for this directory."""
        os.remove(self.hash_file)
        if self.debug:
            print(f"Deleted {self.hash_file} for {self.directory} hashes.")

    def print(self):
        if self.debug:
            print("==================================")
            print(f"directory: {self.directory}")
            print("==================================")
            print("\nhashes by size")
            pprint(self.hashes_by_size)
            print("\nhashes on 1k")
            pprint(self.hashes_on_1k)
            print("\nhashes full")
            pprint(self.hashes_full)

    def analyze(self):
        """Analyze this directory and compute file hashes."""
        if self.loaded:
            print(f"Hashes already loaded for {self.directory}. Skipping analysis.")
            return

        print(f"Analyzing directory: {self.directory}")
        for dirpath, _, filenames in os.walk(self.directory):
            for filename in filenames:
                full_path = os.path.realpath(os.path.join(dirpath, filename))
                try:
                    file_size = os.path.getsize(full_path)
                    self.hashes_by_size[file_size].append(full_path)
                    self.rev_hashes_by_size[full_path] = file_size
                except OSError:
                    continue

        for file_size, files in self.hashes_by_size.items():
            if len(files) < 2:
                continue
            for file in files:
                small_hash = self.get_hash(file, first_chunk_only=True)
                if small_hash:
                    self.hashes_on_1k[small_hash].append(file)
                    self.rev_hashes_on_1k[file] = small_hash

        for small_hash, files in self.hashes_on_1k.items():
            if len(files) < 2:
                continue
            for file in files:
                full_hash = self.get_hash(file, first_chunk_only=False)
                if full_hash:
                    self.hashes_full[full_hash].append(file)
                    self.rev_hashes_full[file] = full_hash

        self.save_hashes()


class DirectoryComparator:
    """Compares two directories using HashAnalysis instances."""

    def __init__(self, dir1, dir2, debug):
        self.analysis1 = HashAnalysis(dir1, debug)
        self.analysis2 = HashAnalysis(dir2, debug)
        self.debug = debug

    def clean(self):
        self.analysis1.delete_hashes()
        self.analysis2.delete_hashes()

    @staticmethod
    def merge_common_keys(dict1, dict2):
        # Find the common keys
        common_keys = dict1.keys() & dict2.keys()

        # Merge the values for the common keys
        merged_dict = {key: dict1[key] + dict2[key] for key in common_keys}

        return merged_dict

    def compare(self):
        """Compare the two directories for duplicate files."""
        self.analysis1.load_hashes()
        self.analysis2.load_hashes()

        self.analysis1.analyze()
        self.analysis2.analyze()

        self.analysis1.print()
        self.analysis2.print()

        total_dupe_size_mb = 0
        duplicate_count = 0

        print(f"Comparing directories:\n\t{self.analysis1.directory}\n\t{self.analysis2.directory}")
        # find the common keys in the size tables
        dict1 = self.analysis1.hashes_by_size
        dict2 = self.analysis2.hashes_by_size
        hashes_by_size = self.merge_common_keys(dict1, dict2)

        hashes_on_1k = defaultdict(list)
        # iterate to see if we have the hashes already
        rev_1k1 = self.analysis1.rev_hashes_on_1k
        rev_1k2 = self.analysis2.rev_hashes_on_1k
        for file_size, files in hashes_by_size.items():
            if len(files) < 2:
                continue
            for file in files:
                if file in rev_1k1.keys():
                    small_hash = rev_1k1[file]
                elif file in rev_1k2.keys():
                    small_hash = rev_1k2[file]
                else:
                    small_hash = self.get_hash(file, first_chunk_only=True)
                if small_hash:
                    hashes_on_1k[small_hash].append(file)

        hashes_full = defaultdict(list)
        rev_full1 = self.analysis1.rev_hashes_full
        rev_full2 = self.analysis2.rev_hashes_full
        rev_size1 = self.analysis1.rev_hashes_by_size
        rev_size2 = self.analysis2.rev_hashes_by_size
        for small_hash, files in hashes_on_1k.items():
            if len(files) < 2:
                continue
            for file in files:
                if file in rev_full1.keys():
                    full_hash = rev_full1[file]
                    file_size = rev_size1[file]
                elif file in rev_full2.keys():
                    full_hash = rev_full2[file]
                    file_size = rev_size2[file]
                else:
                    full_hash = self.get_hash(file, first_chunk_only=False)
                    file_size = os.path.getsize(file)
                if full_hash:
                    hashes_full[full_hash].append(file)
                    duplicate_count += 1
                    file_size_mb = int(file_size) / (1024 * 1024)
                    total_dupe_size_mb += file_size_mb
                    if self.debug:
                        print(f"Duplicate:{file} ({file_size_mb:.2f} MB)")

        # pprint(hashes_full)

        if duplicate_count == 0:
            print("No duplicates found.")
        else:
            print(f"Total duplicate size: {total_dupe_size_mb:.2f} MB")
            print(f"Total duplicates found: {duplicate_count}")

    def compare_hash(self, files1, files2, first_chunk_only):
        for file1 in files1:
            for file2 in files2:
                # Get the hash for file1
                hash1 = HashAnalysis.get_hash(file1, first_chunk_only)
                lookup_hash2 = self.analysis2.get_lookup_hash(first_chunk_only)
                if hash1 not in lookup_hash2:
                    # check the hash
                    hash2 = HashAnalysis.get_hash(file2, first_chunk_only)
                    if hash1 == hash2:
                        return (file1, file2)
                else:
                    lookup_hash2[hash1].append(file1)

        return (None, None)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find and compare duplicate files across directories.")
    parser.add_argument('-d', '--debug', action='store_true', help="Debug mode which deletes hashes and has extra printing.")
    parser.add_argument('--analyze', metavar='DIR', help="Analyze a specific directory.")
    parser.add_argument('--compare', nargs=2, metavar=('DIR1', 'DIR2'), help="Compare two directories for duplicates.")

    args = parser.parse_args()

    if args.analyze:
        analysis = HashAnalysis(args.analyze)
        analysis.analyze()

    if args.compare:
        dir1, dir2 = args.compare
        comparator = DirectoryComparator(dir1, dir2, args.debug)
        comparator.compare()
        if args.debug:
            comparator.clean()


