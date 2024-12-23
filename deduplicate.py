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


class DupeFile:
    def __init__(self, file, hash='', size=0):
        self.path = file
        path_parts = file.split('/')[:-1]
        self.parent = '/'.join(path_parts)
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


    def has_no_extras(self):
        return (not(self.has_nondupe_files()) and
                not(self.has_nondupe_subdirs()))

    def has_no_dupedirs(self):
        return len(self.subdir_dupes) == 0

    def has_no_dupefiles(self):
        return len(self.file_dupes) == 0

    def has_nondupe_files(self):
        return len(self.file_uniqs) > 0

    def has_nondupe_subdirs(self):
        return len(self.subdir_uniqs) > 0

    def load_fs(self, dupe_files, dupe_dirs):
        all_dupedirs_are_full = False
        for dirpath, dirs, filenames in os.walk(self.path):
            for filename in filenames:
                full_path = os.path.realpath(os.path.join(dirpath, filename))
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
                full_path = os.path.realpath(os.path.join(dirpath, dir))
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


    def calc_max(dupedir_list, strategy):
        m_count = m_size =  m_extra = -1
        count_d = size_d = extra_d = None
        for d in dupedir_list:
            if d.count_total > m_count:
                count_d = d
            if d.size_total > m_size:
                size_d = d
            if d.extra_total > m_extra:
                extra_d = d
        if strategy == 'count':
            return count_d
        if strategy == 'size':
            return size_d
        if strategy == 'extra':
            return extra_d
        raise Exception('invalid strategy')

    def delete(self, strategy):
        super().delete(strategy)

    def keep(self, accum, strategy):
        # do directory deletes
        keeps = set()
        deletes = set()
        # print('keep', self.path)
        for dupe in self.file_dupes:
            ks, ds = dupe.keep()
            keeps.update(ks)
            deletes.update(ds)
        if len(keeps) > 0:
            accum[self.path] = keeps, deletes
        # move on to the next directory
        d = DupeDir.calc_max(self.dupe_children, strategy)
        if d:
            d.keep(accum, strategy)



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

    def analyze_directories(self, hashes_full):
        dirs = {}
        #     def group_files_by_directory(self, file_list):
        # """Groups files by their parent directory."""
        # dir_map = defaultdict(list)
        # for file in file_list:
        #     directory = os.path.dirname(file)
        #     dir_map[directory].append(file)
        # return dir_map


    def get_parent_dir(self, file):
        return '/'.join(file.split('/')[:-1])

    def is_full_dir_of_dupes(self, dupe_files, potential_dupe_dir, file):
        """Checks to make sure all files in a dir are duplicates, however, it does not check each subdir.  it only aggregates the subdirs.  subdirs must be checked separately. """
        dir_file_contents = []
        ret_dirs = []
        dir_size = 0
        # print(f"potential_dupe_dir: {potential_dupe_dir} for {file}")
        for dirpath, dirs, filenames in os.walk(potential_dupe_dir):
            for filename in filenames:
                full_path = os.path.realpath(os.path.join(dirpath, filename))
                if full_path not in dupe_files:
                    return (None, None, None, None)
                dir_file_contents.append(full_path)
                dir_size += self.size_lookup[full_path]
            for dir in dirs:
                full_path = os.path.realpath(os.path.join(dirpath, dir))
                ret_dirs.append(full_path)
            # don't recurse through sub directories
            break
        return (potential_dupe_dir, ret_dirs, dir_file_contents, dir_size)

    def dirs_full_of_dupes(self, dupe_files, dupe_file_depth):
                # capture a new hash to store our dupe tree
        dupe_dirs = defaultdict(dict)
        dupe_dir_depth = defaultdict(list)

        # iterate through each through starting with deepest dir
        ordered_depth_keys = sorted(dupe_file_depth)
        revkeys = ordered_depth_keys.copy()
        revkeys.reverse()
        for key in revkeys:
            for file in dupe_file_depth[key]:
                potential_dupe_dir = self.get_parent_dir(file)
                # avoid doing reanalysis
                if potential_dupe_dir not in dupe_dirs:
                    dupe_dir, subdirs, dupe_contents, size = self.is_full_dir_of_dupes(dupe_files,
                                                                        potential_dupe_dir,
                                                                        file)
                    if dupe_dir:
                        dupe_dirs[dupe_dir] = {'subdirs': subdirs,
                                               'files': dupe_contents,
                                               'count': len(dupe_contents),
                                               'size': size}
                        dupe_dir_depth[len(subdirs)].append(dupe_dir)
                        # print(f"found dupe dir: {dupe_dir}")
        # pprint(dupe_dirs)
        # pprint(dupe_dir_depth)
        return dupe_dirs, dupe_dir_depth

    def execute(self, can_delete=False):
        processed_files = {}
        dupe_files = self.analyze()
        for dupe_file, dupe_file_list in dupe_files.items():
            if dupe_file not in processed_files:
                print(f"keeping: {dupe_file}")
                for delete in dupe_file_list:
                    if dupe_file != delete:
                        print(f"\tdeleting: {delete}")
                        processed_files[delete] = dupe_file
                        if can_delete:
                            try:
                                os.remove(delete)
                            except FileNotFoundError:
                                print(f"error: FNF {delete}")

    def delete(self):
        self.execute(can_delete=True)

    def compare(self):
        self.execute()

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

        # loop through the duplicates by hash.
        # strip out non-duplicates.
        # lay the base of dupedir and dupefile objects.

        parent1 = '/'.join(self.analysis1.directory.split('/')[:-1])
        parent2 = '/'.join(self.analysis2.directory.split('/')[:-1])
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
        strategy = 'count'
        d = DupeDir.calc_max(start_list, strategy)
        final_dirs = {}
        d.keep(final_dirs, strategy)

        # output the directories
        ordered_keys = sorted(final_dirs)
        for dpath in ordered_keys:
           print(f"Keep: {dpath}")
           keeps, deletes = final_dirs[dpath]
           for k in keeps:
               print(f"\t{k.path}")
           print(f"Deleting:")
           for d in deletes:
               print(f"\t{d.path}")

        return {}
        dupe_dirs, dupe_dir_depth = self.dirs_full_of_dupes(dupe_files, dupe_file_depth)

        pprint(dupe_dir_depth)
        # find which validated dirs are dupes of one another
        processed_dirs = []
        true_dupe_dirs = defaultdict(list)
        ordered_dir_depth = sorted(dupe_dir_depth)
        for depth in ordered_dir_depth:
            for dir in dupe_dir_depth[depth]:
                if dir in processed_dirs:
                    continue
                # skip dirs that have subdirs that aren't full of dupes
                should_skip = False
                for sd in dupe_dirs[dir]['subdirs']:
                    if sd not in dupe_dirs:
                        should_skip = True
                        break
                test_filelist = dupe_dirs[dir]['files']
                if should_skip or not test_filelist:
                    processed_dirs.append(dir)
                    break
                # valid dir, find it's true dupes
                for child_file in dupe_files[test_filelist[0]]:
                    potdir = self.get_parent_dir(child_file)
                    # print(potdir)
                    if (dir != potdir and potdir in dupe_dirs and
                        dupe_dirs[potdir]['size'] == dupe_dirs[dir]['size'] and
                        dupe_dirs[potdir]['count'] == dupe_dirs[dir]['count']):
                        true_dupe_dirs[dir].append(potdir)
                    processed_dirs.append(potdir)
                processed_dirs.append(dir)

        pprint(true_dupe_dirs)

        # need to parse through dupe
        #  retain the files in the same dir
        #  but anchored by the largest dirs

        # so, first find occurrences by directory

        # processed_files = {}
        # occs = defaultdict((0, list))
        # for f, ds in dupe_files.items():
        #     if f in processed_files:
        #         continue
        #     parent = self.get_parent_dir(f)
        #     if parent in true_dupe_dirs:

        # categorized = defaultdict(list)



        dupe_files.update(true_dupe_dirs)
                        # # clean up dupe files dict
                        # for f in dupe_dirs[potdir]['files']:
                        #     dupe_files.pop(f)
                        # for f in dupe_dirs[dir]['files']:
                        #     dupe_files.pop(f)



        # safe_dir = defaultdict(list)
        # # now that we have full dupe directories of files
        # # we need to check to make sure that nested directories are safe to delete
        # for parent, contents in dupe_dirs.items():
        #     dirs, files, file_count, file_size = contents
        #     for dir in dirs:
        #         if dir not in dupe_dirs:
        #             break
        #     safe_dir[parent] = contents

        # pprint(safe_dir)

        # create a dict of dir and its duplicates
        # dupe_dirs = defaultdict(list)

        # check each duplicate file starting at the highest level
        #  check to see if the current file's directory is duplicate
        #    with the directories of the duplicates
        #    if duplicate directory is also duplicate
        #    check the subdirs and ensure they are also fully duplicate
        #    to choose which dir to keep, it would be the highest file count

        # all files that we have already processed (and can ignore)
        # value will resolve to the file that is kept
        # processed_dupes = {}

        # for key in ordered_depth_keys:
        #     # start at root directories
        #     for file in dupe_file_depth[key]:
        #         # skip files we've processed
        #         if file in processed_dupes:
        #             continue
        #         # check to see if this is part of dir of dupes
        #         parent = self.get_parent_dir(file)
        #         # skip dirs we've processed
        #         if parent in processed_dupes:
        #             continue

        #         # this directory has at least one duplicate for all files.
        #         if parent in dupe_dirs:
        #             # mark all files as processed
        #             for dir_file in safe_dir[parent]:
        #                 process_dupes[dir_file] = file
        #         # go through the duplicates of this file
        #         for dupe in dupe_files[file]:
        #             # skip files we've processed
        #             if dupe in processed_dupes:
        #                 continue
        #             # check to see if this is part of dir of dupes
        #             parent = self.get_parent_dir(dupe)
        #             if parent in safe_dir:
        #                 # check
        #                 processed_dupes[dupe]


        # list out those files that don't have directories

                    # file_size = os.path.getsize(file)
                    # duplicate_count += 1
                    # file_size_mb = int(file_size) / (1024 * 1024)
                    # total_dupe_size_mb += file_size_mb
                    # if self.debug:
                    #     print(f"Duplicate:{file} ({file_size_mb:.2f} MB)")
        # if duplicate_count == 0:
        #     print("No duplicates found.")
        #     return

        # print(f"Total duplicate size: {total_dupe_size_mb:.2f} MB")
        # print(f"Total duplicates found: {duplicate_count}")

        # build dictionary of duplicate files by path
        # start with the leaf files (longest paths)
        ## examine each duplicate and add to dict based on path depth
        # see if they encompass a full directory
        ## for the highest depth file, ls the directory and check that each of the other files exists in the hash
        ### check the parent directory for duplicate files
        # check the entire directory against any of the files they are duplicate with
        return dupe_files




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


