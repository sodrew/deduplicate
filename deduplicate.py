#!/usr/bin/env python
import os
import shutil
import json
import hashlib
import argparse
from collections import defaultdict
from pprint import pprint

class FileUtil:
    @staticmethod
    def fullpath(filename):
        return os.path.abspath(filename)

    @staticmethod
    def join(path, filename):
        return os.path.abspath(os.path.join(path, filename))

    @staticmethod
    def parent(path):
        return os.path.dirname(path)

    @staticmethod
    def splitpath(path):
        return path.split(os.sep)

    @staticmethod
    def joinpath(parts):
        return os.sep.join(parts)

    @staticmethod
    def create_dir(path):
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def delete(path):
       try:
           os.remove(path)
       except IsADirectoryError:
           # maybe this is a directory
           shutil.rmtree(path)
       except FileNotFoundError:
           raise Exception(f"Delete target doesn't exist: {path}")

    @staticmethod
    def size(path):
        return os.path.getsize(path)


class HashAnalysis:
    """Handles file hashing and analysis for a specific directory."""

    def __init__(self, directory, debug=False, storage='dd_analysis'):
        if directory:
            self.directory = FileUtil.fullpath(directory)
            FileUtil.create_dir(storage)
            parts = FileUtil.splitpath(self.directory)
            hash_file_name = '-'.join(parts) + '.json'
            self.hash_file = FileUtil.join(storage, hash_file_name)
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

        FileUtil.delete(self.hash_file)
        if self.debug:
            print(f"Deleted {self.hash_file} for {self.directory} hashes.")

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

    def analyze(self):
        """Analyze this directory and compute file hashes."""
        if self.loaded:
            print(f"Hashes already loaded for {self.directory}. Skipping analysis.")
            return

        print(f"Analyzing directory: {self.directory}")
        for dirpath, _, filenames in os.walk(self.directory):
            for filename in filenames:
                full_path = FileUtil.join(dirpath, filename)
                try:
                    file_size = FileUtil.size(full_path)
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


class DupeFile:
    def __init__(self, file, hash='', size=0):
        self.parent_dd = None
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

    def keep(self):
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
        from pprint import pformat
        return f"\n DupeFile({pformat(vars(self), indent=2, width=1)})"

class DupeDir(DupeFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.parent_dd = None
        self.subdir_dupes = set()
        # self.subdir_part_dupes = []
        self.subdir_uniqs = []
        self.file_dupes = set()
        self.file_uniqs = []
        self.size_total = 0
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
        # from pprint import pformat
        # return f"\n DupeDir({pformat(vars(self), indent=2, width=1)})"
        # return f"{self.depth}: {self.path}: {self.count_total}"
        x = {'extra': self.extra_total,
             'size': self.size_total,
             'count': self.count_total}
        return f"{self.path}: {x}"

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
        for dirpath, dirs, filenames in os.walk(self.path):
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
            self.size_total += self.size
            self.extra_total += self.extra
            for dir in dirs:
                full_path = FileUtil.join(dirpath, dir)
                if full_path in dupe_dirs:
                    dd = dupe_dirs[full_path]
                    self.subdir_dupes.add(dd)
                    # dd.parent_dd = self
                    all_dupedirs_are_full = all_dupedirs_are_full and dd.is_full_dupe
                    self.count_total += dd.count_total
                    self.size_total += dd.size_total
                else:
                    self.subdir_uniqs.append(full_path)
                    self.extra_total += dd.extra_total
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

    def fill_parents(self, dupe_dirs, stop_dirs):
        parent = self.parent
        prev_dd = self
        while parent not in stop_dirs:
            if parent not in dupe_dirs:
                dd = DupeDir(parent)
                dupe_dirs[parent] = dd
            else:
                dd = dupe_dirs[parent]

            if prev_dd not in dd.dupe_children:
                dd.dupe_children.add(prev_dd)

            if dd.manual:
                dd.count_total += self.count_total
                dd.size_total += self.size_total
                dd.extra_total += self.extra_total
            parent = dd.parent
            prev_dd = dd


    def calc_max(dupedir_list):
        if len(dupedir_list) > 0:
            sorted_arr = sorted(dupedir_list,
                                key=lambda d: (
                                    d.count_total,
                                    d.extra_total,
                                    d.size_total),
                                reverse=True)
            return sorted_arr[0]
        else:
            return None

    def check_delete(self):
        if self.is_empty():
            self.is_deleted = True
        return self.is_deleted

    def keep(self, accum, delete_lookup):
        # do directory deletes
        keeps = set()
        deletes = set()
        # print('keep', self.path)
        for dupe in self.file_dupes:
            ks, ds = dupe.keep()
            keeps.update(ks)
            deletes.update(ds)
            for d in ds:
                # print(d.path)
                delete_lookup[d.path] = self.path
        if len(keeps) > 0:
            accum[self.path] = keeps, deletes
        # move on to the next directory
        d = DupeDir.calc_max(self.dupe_children)
        if d:
            d.keep(accum, delete_lookup)


class DirectoryComparator:
    """Compares two directories using HashAnalysis instances."""

    def __init__(self, dir1, dir2, debug):
        self.analysis1 = HashAnalysis(dir1, debug)
        if dir1 == dir2:
            self.analysis2 = self.analysis1
        else:
            self.analysis2 = HashAnalysis(dir2, debug)
        self.size_lookup = {}
        self.hash_file_size = {}
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

    def merge_analyses(self):
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
                    small_hash = HashAnalysis.get_hash(file, first_chunk_only=True)
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
                    # new collision
                    full_hash = HashAnalysis.get_hash(file, first_chunk_only=False)
                    # the file should be in one of the file size hashes
                    if file in rev_size1:
                        file_size = rev_size1[file]
                    else:
                        file_size = rev_size2[file]
                # now build the output
                hashes_full[full_hash].append(file)
                self.hash_file_size[full_hash] = file_size
                self.size_lookup[file] = file_size

        # pprint(hashes_full)

        return hashes_full

    @staticmethod
    def readable_size(size):
        # Define the units
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']

        # Initialize the index for units
        unit_index = 0

        # Loop to find the appropriate unit
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1

        # Return the formatted string
        return f"{size:.2f} {units[unit_index]}"


    def execute(self, can_delete=False):
        final_dirs = self.analyze()

        if not final_dirs:
            print("No duplicates found")

        sizes = {}
        # output the directories
        ordered_keys = sorted(final_dirs)
        for dpath in ordered_keys:
           print(f"Keep: {dpath}")
           keeps, deletes = final_dirs[dpath]
           for k in keeps:
               print(f"\t{k.path}")
           print(f"Deleting:")
           size = 0
           for d in deletes:
               print(f"\t{d.path}")
               size += d.size
               if can_delete:
                   FileUtil.delete(d.path)
           sizes[dpath] = size
        for dpath, size in sizes.items():
            print(f"Saved: {self.readable_size(size)} by deleting duplicates of {dpath}")


    def delete(self):
        self.execute(can_delete=True)

    def compare(self):
        self.execute(can_delete=False)

    def analyze(self):
        """Compare the two directories for duplicate files."""
        self.analysis1.load_hashes()
        self.analysis2.load_hashes()

        self.analysis1.analyze()
        # self.analysis1.print()
        self.analysis2.analyze()
        # self.analysis2.print()

        # merge the two saved analyses
        hashes_full = self.merge_analyses()
        # pprint(hashes_full)
        if not hashes_full:
            return {}

        # loop through the duplicates by hash.
        # strip out non-duplicates.
        # lay the base of dupedir and dupefile objects.
        parent1 = FileUtil.parent(self.analysis1.directory)
        parent2 = FileUtil.parent(self.analysis2.directory)
        stop_dirs = [parent1, parent2]

        # dupefiles[path] = DupeFile(path)
        dupefiles = {}
        # dupedirs[path] = DupeDir(path)
        dirs_w_dupes = {}
        dirs_w_dupes_by_depth = defaultdict(list)

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
                if parent not in dirs_w_dupes:
                    dd = DupeDir(parent, None)
                    dirs_w_dupes[parent] = dd
                    dirs_w_dupes_by_depth[dd.depth].append(dd)

            # set the duplicates
            for df in obj_list:
                df.set_dupes(obj_list)
        # determine if dupe dirs are completely duplicate
        #  check against filesystem for other files
        #  check to see if subdirs are complete dupes
        #  check to see if dir is a superset (has other non dupe files)
        ordered_keys = sorted(dirs_w_dupes_by_depth.keys())
        rev_ordered_keys = ordered_keys.copy()
        rev_ordered_keys.reverse()
        # supersets = {}
        for key in rev_ordered_keys:
            for dd in dirs_w_dupes_by_depth[key]:
                dd.load_fs(dupefiles, dirs_w_dupes)
                # if dd.is_superset:
                #     supersets[dd.path] = dd

        # fill in empty parent dirs to aggregate
        #  sizes and counts.
        for key in rev_ordered_keys:
            for dd in dirs_w_dupes_by_depth[key]:
                dd.fill_parents(dirs_w_dupes, stop_dirs)

        # determine highest directory of each dir family of dupes
        key = next(iter(ordered_keys))
        start_list = dirs_w_dupes_by_depth[key]
        d = DupeDir.calc_max(start_list)
        # print(d)
        final_dirs = {}
        delete_lookup = {}
        d.keep(final_dirs, delete_lookup)

        # clean up dirs that are empty
        for key in rev_ordered_keys:
            for dd in dirs_w_dupes_by_depth[key]:
                if dd.check_delete():
                    for d in dd.file_dupes:
                        kept = delete_lookup[d.path]
                        keeps, deletes = final_dirs[kept]
                        if d in deletes:
                            deletes.remove(d)
                            deletes.add(dd)
                    # for sd in dd.subdir_dupes:
                    #     kept = delete_lookup[sd.path]
                    #     keeps, deletes = final_dirs[kept]
                    #     if sd in deletes:
                    #         deletes.remove(sd)
                    #         deletes.add(dd)

        return final_dirs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find and compare duplicate files across directories.")
    parser.add_argument('-d', '--debug', action='store_true', help="Debug mode which deletes hashes and has extra printing.")
    parser.add_argument('--analyze', metavar='DIR', help="Analyze a specific directory.")
    parser.add_argument('--compare', nargs=2, metavar=('DIR1', 'DIR2'), help="Compare two directories for duplicates.")

    parser.add_argument('--delete', nargs=2, metavar=('DIR1', 'DIR2'), help="Delete duplicates in a directory.")

    args = parser.parse_args()

    if args.analyze:
        analysis = HashAnalysis(args.analyze)
        analysis.analyze()
        if args.debug:
            comparator.clean()

    if args.compare:
        dir1, dir2 = args.compare
        comparator = DirectoryComparator(dir1, dir2, args.debug)
        comparator.compare()
        if args.debug:
            comparator.clean()

    if args.delete:
        dir1, dir2 = args.delete
        comparator = DirectoryComparator(dir1, dir2, args.debug)
        comparator.delete()
        if args.debug:
            comparator.clean()


