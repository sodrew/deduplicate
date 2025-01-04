#!/usr/bin/env python
import gzip
import json
import hashlib
import argparse
import sys
import itertools
from collections import defaultdict
from pprint import pprint, pformat
from utils import FileUtil, ProcessTimer


class HashAnalysis:
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
            prefix = HashAnalysis.hash_str_list(paths)
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
                    for chunk in HashAnalysis.chunk_reader(file_object):
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

    def load_hashes(self, prefix=None):
        """Load stored hashes for this directory if available."""
        if not prefix:
            prefix = self.storage_prefix
        try:
            with gzip.open(prefix + '.json.gz', 'rt', encoding='UTF-8') as f:
                data = json.load(f)
            HashAnalysis.load_dict_set(self.hashes_by_size, data.get('hashes_by_size', {}))
            HashAnalysis.load_dict_set(self.rev_hashes_by_size, data.get('rev_hashes_by_size', {}))
            HashAnalysis.load_dict_set(self.hashes_on_1k, data.get('hashes_on_1k', {}))
            HashAnalysis.load_dict_set(self.rev_hashes_on_1k, data.get('rev_hashes_on_1k', {}))
            HashAnalysis.load_dict_set(self.hashes_full, data.get('hashes_full', {}))
            HashAnalysis.load_dict_set(self.rev_hashes_full, data.get('rev_hashes_full', {}))
            # we don't touch self.paths
            self.paths_loaded = set(data.get('paths', []))
            self.parents = set(data.get('parents', []))
            self.empty_dirs = set(data.get('empty_dirs', []))
            if self.debug:
                print(f"INFO: Loaded hashes for {pformat(self.paths)} from {prefix}.")
        except FileNotFoundError:
            print(f"INFO: No stored hashes found for {pformat(self.paths)}, will analyze.")
        finally:
            return self.paths_loaded

    def save_hashes(self):
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

    def check_partial_load(self):
        return self.paths_loaded

    def analyze(self):
        """Analyze this directory and compute file hashes."""
        # if we loaded all paths, skip this
        paths_remaining = self.paths - self.paths_loaded
        if not paths_remaining:
            if self.debug:
                print(f"INFO: Hashes already loaded for {pformat(self.paths)}. Skipping analysis.")
            return
        # else:
        #     # attempt partial load for each dir
        #     unique_perms = set(itertools.permutations(paths_remaining))
        #     sorted_perms = sorted(unique_perms, key=len)
        #     # do so in a greedy way
        #     for perms in sorted_perms:
        #         if self.load_hashes(perms):
        #             break

        # # now recheck and do a full_load
        # paths_remaining = self.paths - self.paths_loaded

        # if paths_remaining:
        #     analysis2 = HashAnalysis(paths_remaining)
        #     analysis2.analyze()
        #     print(f"INFO: Hashes loaded for {pformat(self.paths_loaded)}. Performing Merge.")
        #     return self.merge(analysis2)

        print(f"Analyzing directories: {pformat(paths_remaining)}")
        timer = ProcessTimer(start=True)

        print(f"\tPass 1: by filesize", end=' ')
        subtimer = ProcessTimer(start=True)
        for path in paths_remaining:
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

        self.paths_loaded = paths_remaining
        self.paths = self.paths_loaded
        self.save_hashes()

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
                    small_hash = HashAnalysis.get_hash(file, first_chunk_only=True)

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

        hash_file_size = {}

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
                    full_hash = HashAnalysis.get_hash(file, first_chunk_only=False)
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
                    # create the hash to file size dict
                    hash_file_size[full_hash] = file_size
                else:
                    print(f"**ERROR**: unable to get full hash for: {file}", file=sys.stderr)

        timer.stop()
        print(f"[{timer.elapsed_readable()}]")
        return hash_file_size

    def fully_loaded(self):
        return self.paths == self.paths_loaded

    def merge(self, analysis2):
        print(f"Merging analysis")
        if not self.fully_loaded() or not analysis2.fully_loaded():
            print(f"**ERROR**: Hashes not loaded, Run HashAnalysis.analyze() first.")
            return

        timer = ProcessTimer(start=True)
        self.merge_hashes_by_size(analysis2)
        self.merge_hashes_on_1k(analysis2)
        hash_file_size = self.merge_hashes_on_full(analysis2)

        timer.stop()
        print(f"\tTotal Analysis Time: {timer.elapsed_readable()}")

        # update the storage_prefix
        prefix = HashAnalysis.hash_str_list(self.paths)
        self.storage_prefix = FileUtil.join(self.storage_dir, prefix)

        self.save_hashes()

        return hash_file_size

class DupeFile:
    def __init__(self, file, hash='', size=0):
        # self.parent_dd = None
        self.path = file
        self.parent = FileUtil.parent(file)
        path_parts = FileUtil.splitpath(file)
        self.depth = len(path_parts)
        self.hash = hash
        self.size = size
        self.is_deleted = False
        self.is_kept = False
        self.duplicates = set()

    def set_dupes(self, df_list):
        for df in df_list:
            if df != self:
                self.duplicates.add(df)

    def delete(self):
        deletes = set()
        if not self.is_deleted and not self.is_kept:
            deletes.add(self)
            # print('delete', self.path)
            self.is_deleted = True
        return deletes

    def keep(self, dwd):
        deletes = set()
        keeps = set()
        if not self.is_deleted:
            self.is_kept = True
            # print('keep', self.path)
            # delete the duplicates
            for dupe in self.duplicates:
                deletes.update(dupe.delete())
            keeps.add(self)
        return keeps, deletes

    def __repr__(self):
        # return f"\n DupeFile({pformat(vars(self), indent=2, width=1)})"
        return f"DupeFile({self.path})"

class DupeDir(DupeFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.parent_dd = None
        self.subdir_dupes = set()
        # self.subdir_part_dupes = []
        self.subdir_uniqs = []
        self.file_dupes = set()
        self.file_uniqs = []
        self.count = 0
        self.count_total = 0
        self.extra = 0
        self.extra_total = 0
        self.is_full_dupe = False
        self.dupe_children = set()
        # self.is_superset = False
        self.manual = True
        # self.is_root = False

    def __repr__(self):
        # return f"\n DupeDir({pformat(vars(self), indent=2, width=1)})"
        # return f"{self.depth}: {self.path}: {self.count_total}"
        x = {'extra': self.extra_total,
             'size': self.size,
             'count': self.count_total,
             'is_deleted': self.is_deleted,
             }
        return f"DupeDir({self.path}: {x})"

    def is_empty(self):
        return (self.has_no_extras() and
                self.has_no_dupedirs() and
                self.has_no_dupefiles())

    def has_no_extras(self):
        return (not(self.has_nondupe_files()) and
                not(self.has_nondupe_subdirs()))

    def has_no_dupedirs(self):
        if len(self.subdir_dupes) > 0:
            for sd in self.subdir_dupes:
                if not sd.is_deleted:
                    return False
        return True

    def has_no_dupefiles(self):
        if len(self.file_dupes) > 0:
            for fd in self.file_dupes:
                if not fd.is_deleted:
                    return False
        return True

    def has_nondupe_files(self):
        return len(self.file_uniqs) > 0

    def has_nondupe_subdirs(self):
        return len(self.subdir_uniqs) > 0

    def load_fs(self, dupe_files, dupe_dirs):
        all_dupedirs_are_full = False
        for dirpath, dirs, filenames in FileUtil.walk(self.path):
            for filename in filenames:
                full_path = FileUtil.join(dirpath, filename)
                if full_path in dupe_files:
                    df = dupe_files[full_path]
                    self.file_dupes.add(df)
                    self.size += df.size
                    self.count += 1
                else:
                    self.file_uniqs.append(full_path)
                    self.extra += 1
            self.count_total += self.count
            self.extra_total += self.extra
            for dir in dirs:
                full_path = FileUtil.join(dirpath, dir)
                if full_path in dupe_dirs:
                    # print('fp', full_path)
                    dd = dupe_dirs[full_path]
                    self.subdir_dupes.add(dd)
                    # dd.parent_dd = self
                    all_dupedirs_are_full = all_dupedirs_are_full and dd.is_full_dupe
                    self.count_total += dd.count_total
                    self.size += dd.size
                else:
                    self.subdir_uniqs.append(full_path)
                    self.extra_total += 1
            # don't recurse through sub directories
            break

        # we do this next part under the assumption
        #  that are doing a leaf first approach
        self.manual = False

        if self.has_no_dupedirs():
            all_dupedirs_are_full = True

        self.is_full_dupe = (
            self.has_no_extras() and
            all_dupedirs_are_full)

        self.is_superset = (
            not(self.has_no_extras()) and
            all_dupedirs_are_full)

    def fill_parents(self, dupe_dirs, dwd_depth, stop_dirs):
        parent = self.parent
        prev_dd = self
        while parent not in stop_dirs:
            if parent not in dupe_dirs:
                # print('fillp', parent)
                dd = DupeDir(parent)
                dupe_dirs[parent] = dd
                dwd_depth[dd.depth].append(dd)
            else:
                dd = dupe_dirs[parent]

            if prev_dd not in dd.dupe_children:
                dd.dupe_children.add(prev_dd)

            if dd.manual:
                dd.count_total += self.count_total
                dd.size += self.size
                dd.extra_total += self.extra_total
            parent = dd.parent
            prev_dd = dd

    @staticmethod
    def max_overlap(str1, str2):
        # Initialize the matrix for dynamic programming
        dp = [[0] * (len(str2) + 1) for _ in range(len(str1) + 1)]
        max_length = 0
        end_index = 0

        # Fill the matrix
        for i in range(1, len(str1) + 1):
            for j in range(1, len(str2) + 1):
                if str1[i - 1] == str2[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1] + 1
                    if dp[i][j] > max_length:
                        max_length = dp[i][j]
                        end_index = i

        # Extract the largest overlapping substring
        largest_substring = str1[end_index - max_length:end_index]
        return largest_substring

    @staticmethod
    def calc_max(dupedir_list, past_kept=None):
        filtered_list = [d for d in dupedir_list if not d.is_deleted]
        if len(filtered_list) == 0:
            return None

        # pprint(past_kept)
        # see if we match the
        if past_kept:
            # print('past_kept')
            # pprint(past_kept)
            # we need to consider past choices as an additional weight
            weighted = defaultdict(list)
            for fl in filtered_list:
                count = 0
                for pk in past_kept:
                    # print(fl.path, pk, len(DupeDir.max_overlap(fl.path, pk)))
                    count += len(DupeDir.max_overlap(fl.path, pk))

                weighted[count].append(fl)
            # pprint(weighted)
            keys = sorted(weighted.keys())
            keys.reverse()
            # print('filtered')
            # pprint(filtered_list)
            filtered_list = weighted[keys[0]]
            # print('weighted')
            # pprint(weighted)

        # use the sort approach of largest file count
        sorted_arr = sorted(filtered_list,
                            key=lambda d: (
                                d.count_total,
                                d.extra_total,
                                # d.size,
                                # prefer shallower directory
                                d.parent[::-1],
                            ),
                            reverse=True)
        # print('final_max', sorted_arr[0])
        return sorted_arr[0]

    def check_delete(self):
        if not self.is_deleted and self.is_empty():
            self.is_deleted = True
        return self.is_deleted

    def decrement_dupes(self, df, dwd):
        self.count -= 1
        self.count_total -= 1
        # self.size -= df.size
        self.check_delete()
        if self.parent in dwd:
            dd = dwd[self.parent]
            dd.decrement_dupes(df, dwd)

    def increment_dupes(self, df, dwd):
        self.count += 1
        self.count_total += 1
        # self.size -= df.size
        # self.check_delete()
        if self.parent in dwd:
            dd = dwd[self.parent]
            dd.increment_dupes(df, dwd)

    def keep(self, accum, delete_lookup, dwd):
        # do directory deletes
        keeps = set()
        deletes = set()
        size = 0
        # print('keep()', self.path)
        for dupe in self.file_dupes:
            ks, ds = dupe.keep(dwd)
            keeps.update(ks)
            deletes.update(ds)
            for k in ks:
                if k.parent in dwd:
                    dd = dwd[k.parent]
                    dd.increment_dupes(k, dwd)
            for d in ds:
                # print(d.path)
                # update who this is deleted by
                delete_lookup[d.path] = self.path
                # update dir counts
                if d.parent in dwd:
                    dd = dwd[d.parent]
                    dd.decrement_dupes(d, dwd)
                size += d.size

        if len(keeps) > 0:
            accum[self.path] = keeps, deletes, size
            # print('DupeDir.keep():', self.path,
            #       pformat(keeps),
            #       pformat(deletes))
            return (self.path, keeps, deletes)
        else:
            # move on to subdirs if empty
            dd = DupeDir.calc_max(self.dupe_children, accum.keys())
            if dd:
                return dd.keep(accum, delete_lookup, dwd)
            else:
                return (None, set(), set())

    def check_single_parent(self):
        # print('checking', self.path, self.parent)
        for dirpath, dirs, filenames in FileUtil.walk(self.parent):
            if len(filenames) == 0 and len(dirs) == 1:
                full_path = FileUtil.join(dirpath, dirs[0])
                if full_path == self.path:
                    dd = DupeDir(self.parent, None)
                    dd.subdir_dupes.add(self)
                    return dd
            break
        return None


class DirectoryComparator:
    """Compares two directories using HashAnalysis instances."""

    def __init__(self, dir1, dir2, debug):
        self.analysis1 = HashAnalysis([dir1], debug)
        if dir1 == dir2:
            self.analysis2 = self.analysis1
        else:
            self.analysis2 = HashAnalysis([dir2], debug)
        self.size_lookup = {}
        self.hash_file_size = {}
        self.debug = debug
        self.timer = ProcessTimer(start=True)

    def clean(self):
        self.timer.stop()
        print(f'Total Execution Time: {self.timer.elapsed_readable()}')
        if self.debug:
            self.analysis1.delete_hashes()
            self.analysis2.delete_hashes()

    def execute(self, exec_delete=False):
        try:
            final_dirs = self.analyze()

            print(f"-------------------------------")
            print(f"Results")
            print(f"-------------------------------")
            if not final_dirs:
                print("\nNo duplicates found")
            else:
                all_sizes = 0
                # output the directories
                ordered_keys = sorted(final_dirs)
                all_deletes = set()
                for dpath in ordered_keys:
                   print(f"\nKeep dir:   {dpath}")
                   keeps, deletes, sizes = final_dirs[dpath]
                   for k in keeps:
                       print(f"  keep file:{k.path}")
                   print(f"  Deleting: {FileUtil.human_readable(sizes)}")
                   size = 0
                   for d in deletes:
                       print(f"            {d.path}")
                       size += d.size
                       # print(d.path, size)
                       if exec_delete:
                           FileUtil.delete(d.path)
                   all_deletes.update(deletes)
                   all_sizes += size
                print(f'\nConsolidated delete list: {FileUtil.human_readable(all_sizes)}')
                for d in sorted(all_deletes, key=lambda d: d.path):
                   print(f"{d.path}|{FileUtil.human_readable(d.size)}")

        except Exception as e:
            print(f"**ERROR**: Exception:{type(e).__name__} {e}", file=sys.stderr)
            raise e
        finally:
            self.clean()


    def delete(self):
        self.execute(exec_delete=True)

    def compare(self):
        self.execute(exec_delete=False)

    def analyze(self):
        """Compare the two directories for duplicate files."""
        loaded1 = self.analysis1.load_hashes()
        loaded2 = self.analysis2.load_hashes()

        print(f"-------------------------------")
        if loaded1 and loaded2:
            print(f"Initial Analysis: Skipped since prior analyses loaded")
        else:
            print(f"Analysis")
            print(f"-------------------------------")
            self.analysis1.analyze()
            # self.analysis1.print()
            self.analysis2.analyze()
            # self.analysis2.print()
        print(f"-------------------------------")

        # merge the two saved analyses
        self.hash_file_size = self.analysis1.merge(self.analysis2)
        self.size_lookup = self.analysis1.rev_hashes_by_size
        hashes_full = self.analysis1.hashes_full
        # pprint(hashes_full)

        # pprint(hashes_full)
        if not hashes_full:
            return {}

        # loop through the duplicates by hash.
        # strip out non-duplicates.
        # lay the base of dupedir and dupefile objects.


        # dupefiles[path] = DupeFile(path)
        dupefiles = {}
        # dupedirs[path] = DupeDir(path)
        dirs_w_dupes = {}
        dirs_w_dupes_by_depth = defaultdict(list)

        # add our root dirs as they may not have dupes
        for path in self.analysis1.paths:
            dd = DupeDir(path, None)
            dirs_w_dupes[dd.path] = dd
            dirs_w_dupes_by_depth[dd.depth].append(dd)

        # create the dupe file objects
        for hash, files in hashes_full.items():
            if len(files) < 2:
                continue
            obj_list = set()
            for path in files:
                if path not in dupefiles:
                    df = DupeFile(path, hash,
                                  self.hash_file_size[hash])
                    dupefiles[path] = df
                    obj_list.add(df)

                parent = dupefiles[path].parent
                # print('p', parent)
                if parent not in dirs_w_dupes:
                    dd = DupeDir(parent, None)
                    dirs_w_dupes[parent] = dd
                    dirs_w_dupes_by_depth[dd.depth].append(dd)
                    sp = dd.check_single_parent()
                    # print('sp', sp)
                    if sp:
                        dirs_w_dupes[sp.path] = sp
                        dirs_w_dupes_by_depth[sp.depth].append(sp)

            # set the duplicates
            for df in obj_list:
                df.set_dupes(obj_list)

        # pprint(dupefiles)

        # add in empty dirs
        for dir in self.analysis1.empty_dirs:
            dd = DupeDir(dir, None)
            dirs_w_dupes[dir] = dd
            dirs_w_dupes_by_depth[dd.depth].append(dd)
        # for dir in self.analysis2.empty_dirs:
        #     dd = DupeDir(dir, None)
        #     dirs_w_dupes[dir] = dd
        #     dirs_w_dupes_by_depth[dd.depth].append(dd)


        # determine if dupe dirs are completely duplicate
        #  check against filesystem for other files
        #  check to see if subdirs are complete dupes
        #  check to see if dir is a superset (has other non dupe files)
        ordered_keys = sorted(dirs_w_dupes_by_depth.keys())
        rev_ordered_keys = ordered_keys.copy()
        rev_ordered_keys.reverse()

        # fill in empty parent dirs to aggregate
        #  sizes and counts.
        for key in rev_ordered_keys:
            for dd in dirs_w_dupes_by_depth[key]:
                dd.fill_parents(dirs_w_dupes,
                                dirs_w_dupes_by_depth,
                                self.analysis1.parents)

        # because we may update dirs_w_dupes_by_depth in fill_parents
        # we update the keys
        ordered_keys = sorted(dirs_w_dupes_by_depth.keys())
        rev_ordered_keys = ordered_keys.copy()
        rev_ordered_keys.reverse()

        for key in rev_ordered_keys:
            for dd in dirs_w_dupes_by_depth[key]:
                dd.load_fs(dupefiles, dirs_w_dupes)
                # print(dd.size, dd.path)

        # get the highest directory level of each dir family of dupes
        key = next(iter(ordered_keys))
        start_list = dirs_w_dupes_by_depth[key]
        # determine which dir to start with
        d = DupeDir.calc_max(start_list)
        # print('d', d)
        final_output = {}
        # delete_lookup used by DupeDir.keep()
        delete_lookup = {}
        # generate the first pass of dupe finding (hardest part)
        kept, kepts, dels = d.keep(final_output, delete_lookup, dirs_w_dupes)
        reviewed = set()
        reviewed.update(kepts)
        reviewed.update(dels)

        all_dupes = set()
        all_dupes.update(dupefiles.values())
        # pprint(all_dupes)

        remaining_dupes = all_dupes - reviewed
        # print('analyze()', pformat(all_dupes))

        # do more passes until dupes are all found
        while len(remaining_dupes) > 0:
            new_dwd_depth = defaultdict(list)
            # create new depth lookup
            for df in remaining_dupes:
                new_dwd_depth[df.depth - 1].append(dirs_w_dupes[df.parent])

            # print('new_dwd_depth', pformat(new_dwd_depth))
            ordered_keys = sorted(new_dwd_depth.keys())
            if ordered_keys:
                key = next(iter(ordered_keys))
                start_list = new_dwd_depth[key]
                # print('start_list', pformat(start_list))
                d = DupeDir.calc_max(start_list, final_output.keys())
                # print('calc', d)
                kept, kepts, dels = d.keep(final_output, delete_lookup, dirs_w_dupes)
                reviewed.update(kepts)
                reviewed.update(dels)
                # print('pass ', debug_count)
                remaining_dupes = all_dupes - reviewed


        # pprint(final_output)
        # pprint(delete_lookup)
        # pprint(dirs_w_dupes_by_depth)

        # clean up dirs that are empty in the final_output
        for key in rev_ordered_keys:
            for dd in dirs_w_dupes_by_depth[key]:
                # print('o-dd', dd.path, dd.is_deleted, dd.is_empty())
                # print('    ', dd.has_no_extras(), dd.has_nondupe_files(),
                #       dd.has_nondupe_subdirs())
                # pprint(dd.subdir_dupes)
                # pprint(dd.subdir_uniqs)
                if dd.check_delete():
                    # print('i-dd', dd.path, dd.is_deleted)
                    # clean up files that are children of deleted dirs
                    first_time = True
                    for d in dd.file_dupes:
                        kept = delete_lookup[d.path]
                        keeps, deletes, sizes = final_output[kept]
                        if d in deletes:
                            # print('found', d.path, dd.path)
                            deletes.remove(d)
                            # we only do this once if there are multiple
                            # substitutions
                            if first_time:
                                deletes.add(dd)
                                first_time = False
                    # clean up subdirs that are children of deleted dirs
                    for sd in dd.subdir_dupes:
                        for kept, vals in final_output.items():
                            keeps, deletes, sizes = vals
                            if sd in deletes:
                                # print('found', sd.path, dd.path)
                                deletes.remove(sd)
                                if first_time:
                                    deletes.add(dd)
                                    first_time = False
                    # this has no files or subdirs
                    # if first_time:
                    #     print('first_time', dd.path)

        return final_output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find and compare duplicate files across directories.")
    parser.add_argument('--debug', action='store_true', help="Debug mode which deletes hashes and has extra printing.")
    parser.add_argument('--analyze', metavar='DIR', help="Analyze a specific directory.")
    parser.add_argument('--compare', nargs=2, metavar=('DIR1', 'DIR2'), help="Compare two directories for duplicates.")

    parser.add_argument('--delete', nargs=2, metavar=('DIR1', 'DIR2'), help="Delete duplicates in a directory.")

    args = parser.parse_args()

    # print(args.debug)
    if args.analyze:
        analysis = HashAnalysis([args.analyze])
        analysis.analyze()

    if args.compare:
        dir1, dir2 = args.compare
        comparator = DirectoryComparator(dir1, dir2, args.debug)
        comparator.compare()

    if args.delete:
        dir1, dir2 = args.delete
        comparator = DirectoryComparator(dir1, dir2, args.debug)
        comparator.delete()


