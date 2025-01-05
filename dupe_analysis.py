import sys
import gzip
import json
import hashlib
import itertools
from pprint import pprint, pformat
from dupe_utils import FileUtil, ProcessTimer
from collections import defaultdict

class DupeAnalysis:
    """Handles file hashing and analysis for a specific directory."""

    def __init__(self, dirs, debug=False, storage_dir='dd_analysis'):
        self.paths = set()
        self.paths_loaded = set()
        self.parents = set()
        self.storage_dir = storage_dir
        FileUtil.create_dir(self.storage_dir)
        for dir in dirs:
            path = FileUtil.fullpath(dir)
            self.paths.add(path)
            parent = FileUtil.parent(path)
            self.parents.add(parent)

            paths = sorted(self.paths)
            prefix = DupeAnalysis.hash_str_list(paths)
            self.storage_prefix = FileUtil.join(self.storage_dir, prefix)

        self.hashes_by_size = defaultdict(set)
        self.hashes_on_1k = defaultdict(set)
        self.hashes_full = defaultdict(set)
        self.rev_hashes_by_size = {}
        self.rev_hashes_on_1k = {}
        self.rev_hashes_full = {}

        self.empty_dirs = set()
        self.debug = debug

    def print(self):
        # if self.debug:
        #     print("==================================")
        #     print(f"directory: {self.directory}")
        #     print("==================================")
        #     print("\nhashes by size")
        #     pprint(self.hashes_by_size)
        #     print("\nhashes on 1k")
        #     pprint(self.hashes_on_1k)
        #     print("\nhashes full")
        #     pprint(self.hashes_full)
        return None

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
                    for chunk in DupeAnalysis.chunk_reader(file_object):
                        hashobj.update(chunk)
        except OSError:
            return None
        return hashobj.hexdigest()

    @staticmethod
    def hash_str_list(str_list, hash=hashlib.sha1):
        hashobj = hash()
        for str in str_list:
            hashobj.update(str.encode())
        return hashobj.hexdigest()

    def get_lookup_hash(self, first_chunk_only):
        if first_chunk_only:
            return self.hashes_on_1k
        else:
            return self.hashes_full

    @staticmethod
    def load_dict_set(to_dict_list, from_dict_list):
        for k, v in from_dict_list.items():
            to_dict_list[k] = set(v)

    @staticmethod
    def test_hash_file(paths, storage_dir, postfix='.json.gz'):
        paths = sorted(paths)
        prefix = DupeAnalysis.hash_str_list(paths)
        load_file = FileUtil.join(storage_dir, prefix + postfix)
        if FileUtil.exists(load_file):
            return load_file
        else:
            return None

    def load_hashes(self, dirs=None):
        """Load stored hashes for this directory if available."""
        if not dirs:
            dirs = self.paths
        path = DupeAnalysis.test_hash_file(dirs, self.storage_dir)
        if not path:
            print(f"INFO: No stored hashes found for {path}, will analyze.")
        else:
            with gzip.open(path, 'rt', encoding='UTF-8') as f:
                data = json.load(f)
            DupeAnalysis.load_dict_set(self.hashes_by_size, data.get('hashes_by_size', {}))
            self.rev_hashes_by_size = data.get('rev_hashes_by_size', {})
            DupeAnalysis.load_dict_set(self.hashes_on_1k, data.get('hashes_on_1k', {}))
            self.rev_hashes_on_1k = data.get('rev_hashes_on_1k', {})
            DupeAnalysis.load_dict_set(self.hashes_full, data.get('hashes_full', {}))
            self.rev_hashes_full = data.get('rev_hashes_full', {})
            # we don't touch self.paths
            self.paths_loaded = set(data.get('paths', []))
            self.parents = set(data.get('parents', []))
            self.empty_dirs = set(data.get('empty_dirs', []))
            if self.debug:
                print(f"INFO: Loaded hashes for {pformat(self.paths)} from {path}.")

        return self.return_all()

    def save_hashes(self):
        paths = sorted(self.paths)
        prefix = DupeAnalysis.hash_str_list(paths)
        self.storage_prefix = FileUtil.join(self.storage_dir, prefix)

        """Save hashes for this directory."""
        with gzip.open(self.storage_prefix + '.json.gz', 'wt', encoding='UTF-8') as f:
            json.dump({
                'hashes_by_size': self.hashes_by_size,
                'rev_hashes_by_size': self.rev_hashes_by_size,
                'hashes_on_1k': self.hashes_on_1k,
                'rev_hashes_on_1k': self.rev_hashes_on_1k,
                'hashes_full': self.hashes_full,
                'rev_hashes_full': self.rev_hashes_full,
                'paths': self.paths,
                'parents': self.parents,
                'empty_dirs': self.empty_dirs,
            }, f, default=list)
        with open(self.storage_prefix + '.txt', 'w') as t:
            t.write(pformat(self.paths))

        if self.debug:
            print(f"INFO: Hashes for {pformat(self.paths)} saved to {self.storage_prefix}.")

    def delete_hashes(self):
        """Delete hashes for this directory."""
        FileUtil.delete(self.storage_prefix + '.json.gz')
        FileUtil.delete(self.storage_prefix + '.txt')
        if self.debug:
            print(f"INFO: Deleted {self.storage_prefix} for {pformat(self.paths)} hashes.")

    def load_other_hashes(self):
        while self.paths_not_loaded():
            # attempt partial load; search for permutations of dirs
            # in a greedy way
            unique_perms = set(itertools.permutations(
                self.paths_not_loaded()))
            sorted_perms = sorted(unique_perms, key=len)
            load_file = None
            dirs = None
            for perm in sorted_perms:
                load_file = DupeAnalysis.test_hash_file(perm,
                                                        self.storage_dir)
                if load_file:
                    dirs = perm
                    break
            if load_file:
                print(f"INFO: Partial load of {pformat(dirs)}. Performing Merge.")
                analysis2 = DupeAnalysis(dirs)
                analysis2.load_hashes()
                self.merge(analysis2)
            else:
                # we exit this function as there was nothing left to load
                break

    def paths_not_loaded(self):
        return self.paths - self.paths_loaded

    def return_all(self):
        return (self.hashes_full, self.rev_hashes_by_size,
                self.paths, self.empty_dirs, self.parents)

    def load(self):
        """attempt to load various combinations of json past runs."""
        ret = self.load_hashes()
        if self.paths_not_loaded:
            self.load_other_hashes()
            if self.paths_not_loaded() == self.paths:
                self.analyze()
            else:
                while self.paths_not_loaded():
                    analysis2 = DupeAnalysis(paths_remaining)
                    analysis2.analyze()
                    self.merge(analysis2)

        return self.return_all()

    def analyze(self):
        """Analyze this directory and compute file hashes."""

        print(f"Analyzing directories: {pformat(self.paths_not_loaded())}")
        timer = ProcessTimer(start=True)

        print(f"\tPass 1: by filesize", end=' ')
        subtimer = ProcessTimer(start=True)
        for path in self.paths_not_loaded():
            for dirpath, dirs, filenames in FileUtil.walk(path):
                for filename in filenames:
                    full_path = FileUtil.join(dirpath, filename)
                    try:
                        file_size = FileUtil.size(full_path)
                    except OSError:
                        print(f"**ERROR**: unable to get size for: {full_path}", file=sys.stderr)
                        file_size = 0
                    finally:
                        self.hashes_by_size[file_size].add(full_path)
                        self.rev_hashes_by_size[full_path] = file_size
                # find any empty dirs
                if len(dirs) == 0 and len(filenames) == 0:
                    # print('found empty', dirpath)
                    self.empty_dirs.add(dirpath)
        subtimer.stop()
        print(f"[{subtimer.elapsed_readable()}]")

        print(f"\tPass 2: by hash (1k)", end=' ')
        subtimer = ProcessTimer(start=True)
        for file_size, files in self.hashes_by_size.items():
            if len(files) < 2:
                continue
            for file in files:
                small_hash = self.get_hash(file, first_chunk_only=True)
                if small_hash:
                    self.hashes_on_1k[small_hash].add(file)
                    self.rev_hashes_on_1k[file] = small_hash
        subtimer.stop()
        print(f"[{subtimer.elapsed_readable()}]")

        print(f"\tPass 3: by hash (full)", end=' ')
        subtimer = ProcessTimer(start=True)
        for small_hash, files in self.hashes_on_1k.items():
            if len(files) < 2:
                continue
            for file in files:
                full_hash = self.get_hash(file, first_chunk_only=False)
                if full_hash:
                    self.hashes_full[full_hash].add(file)
                    self.rev_hashes_full[file] = full_hash
        subtimer.stop()
        print(f"[{subtimer.elapsed_readable()}]")

        timer.stop()
        print(f"\tTotal Analysis Time: {timer.elapsed_readable()}")

        self.paths_loaded = self.paths_not_loaded()

        self.save_hashes()

        return self.return_all()

    def merge_hashes_by_size(self, analysis2):
        print(f"\tPass 1: by filesize", end=' ')
        timer = ProcessTimer(start=True)
        hbs2 = analysis2.hashes_by_size

        # Find the common keys
        common_keys = self.hashes_by_size.keys() & hbs2.keys()

        # Merge the values for the common keys
        for key in common_keys:
            self.hashes_by_size[key].update(hbs2[key])

        # populate the reverse lookup
        self.rev_hashes_by_size.update(analysis2.rev_hashes_by_size)

        # add the empty dirs
        self.empty_dirs.update(analysis2.empty_dirs)
        timer.stop()
        print(f"[{timer.elapsed_readable()}]")

    def merge_hashes_on_1k(self, analysis2):
        print(f"\tPass 2: by hash (1k)", end=' ')
        timer = ProcessTimer(start=True)
        for file_size, files in self.hashes_by_size.items():
            if len(files) < 2:
                continue
            for file in files:
                if file in self.rev_hashes_on_1k.keys():
                    small_hash = self.rev_hashes_on_1k[file]
                elif file in analysis2.rev_hashes_on_1k.keys():
                    small_hash = analysis2.rev_hashes_on_1k[file]
                else:
                    small_hash = DupeAnalysis.get_hash(file, first_chunk_only=True)

                if small_hash:
                    self.hashes_on_1k[small_hash].add(file)
                    self.rev_hashes_on_1k[file] = small_hash
                else:
                    print(f"**ERROR**: unable to get 1k hash for: {file}", file=sys.stderr)
        timer.stop()
        print(f"[{timer.elapsed_readable()}]")

    def merge_hashes_on_full(self, analysis2):
        print(f"\tPass 3: by hash (full)", end=' ')
        timer = ProcessTimer(start=True)

        rev_full1 = self.rev_hashes_full
        rev_full2 = analysis2.rev_hashes_full
        rev_size1 = self.rev_hashes_by_size
        rev_size2 = analysis2.rev_hashes_by_size

        for small_hash, files in self.hashes_on_1k.items():
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
                    # new collision
                    full_hash = DupeAnalysis.get_hash(file, first_chunk_only=False)
                    # the file should be in one of the file size hashes
                    if file in rev_size1:
                        file_size = rev_size1[file]
                    else:
                        file_size = rev_size2[file]

                if full_hash:
                    self.hashes_full[full_hash].add(file)
                    self.rev_hashes_full[file] = full_hash
                    # also update our master sizes dict
                    self.rev_hashes_by_size[file] = file_size
                else:
                    print(f"**ERROR**: unable to get full hash for: {file}", file=sys.stderr)

        timer.stop()
        print(f"[{timer.elapsed_readable()}]")

    def fully_loaded(self):
        return self.paths == self.paths_loaded

    def merge(self, analysis2):
        print(f"Merging analysis")
        self.load()
        analysis2.load()
        if not self.fully_loaded() or not analysis2.fully_loaded():
            print(f"**ERROR**: Hashes not loaded, Run DupeAnalysis.analyze() first.")
            return

        timer = ProcessTimer(start=True)
        self.merge_hashes_by_size(analysis2)
        self.merge_hashes_on_1k(analysis2)
        self.merge_hashes_on_full(analysis2)

        timer.stop()
        print(f"\tTotal Analysis Time: {timer.elapsed_readable()}")

        # update the storage_prefix
        prefix = DupeAnalysis.hash_str_list(self.paths)
        self.storage_prefix = FileUtil.join(self.storage_dir, prefix)

        self.save_hashes()
