#!/usr/bin/env python
import argparse
import sys
import csv
from tqdm import tqdm
from collections import defaultdict
from pprint import pprint, pformat
from dupe_utils import FileUtil, ProcessTimer
from dupe_analysis import DupeAnalysis

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
            # print('delete():', self.path)
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
        self.kept = 0
        self.kept_total = 0
        self.is_full_dupe = False
        self.dupe_children = set()
        # self.is_superset = False
        self.manual = True
        # self.is_root = False

    def __repr__(self):
        # return f"\n DupeDir({pformat(vars(self), indent=2, width=1)})"
        # return f"{self.depth}: {self.path}: {self.count_total}"
        x = {'kept': self.kept_total,
             'extra': self.extra_total,
             'count': self.count_total,
             'keepable': self.get_first_keepable(),
             'size': self.size,
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

    def has_no_unkept_dupefiles(self):
        if len(self.file_dupes) > 0:
            for fd in self.file_dupes:
                if not fd.is_deleted and not fd.is_kept:
                    return False
        return True

    def load_fs(self, da, dupe_files, dupe_dirs):
        all_dupedirs_are_full = False
        ret = da.get_dir_info(self.path)
        filenames = ret['files']
        dirs = ret['subdirs']
        for filename in filenames:
            full_path = filename
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
            full_path = dir
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

    def get_first_keepable(self):
        # print(self.path, self.is_deleted, self.count_total)
        if self.count_total < 1 or self.is_deleted:
            return 0

        if not self.is_deleted:
            # print('get_first_keepable', self.path)
            if (self.has_no_unkept_dupefiles() or
                not self.has_no_dupedirs() and self.has_no_dupefiles()):
                for sd in self.subdir_dupes:
                    found = sd.get_first_keepable()
                    if found > 0:
                        return found
            else:
                return len(self.path)

        return 0


    def get_keepable_dirs(self):
        """
        Returns self, or the first subdirectory that is a dir
        containing dupe files that can be kept
        """
        if self.count_total < 1:
            return set()
            # raise Exception('get_keepable_dirs: called dir without dupes')
        if not self.is_deleted:
            # print('get_keepable_dirs()', self.path)
            if (self.has_no_unkept_dupefiles() or
                not self.has_no_dupedirs() and self.has_no_dupefiles()):
                keepable_dupes = set()
                for sd in self.subdir_dupes:
                    keepable_dupes.update(sd.get_keepable_dirs())
                return keepable_dupes
            else:
                return set([self])



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
        # need to select the parent directory with highest count.
        # weighted by past choices, which means the highest kept.
        # once that's selected, figure out which dirs are actually
        #   keepable and plough through.

        # filter out deletes.
        # print('calc_max(): dupedir_list\n', pformat(dupedir_list))
        filtered_list = set()
        for d in dupedir_list:
            if not d.is_deleted and d.get_first_keepable() > 0:
                filtered_list.add(d)

        if len(filtered_list) == 0:
            return None

        # print('calc_max(): filtered_list\n', pformat(filtered_list))
        class reversor:
            def __init__(self, obj):
                self.obj = obj

            def __eq__(self, other):
                return other.obj == self.obj

            def __lt__(self, other):
                return other.obj < self.obj

        # sort to find the best directory
        sorted_arr = sorted(filtered_list,
                            key=lambda d: (
                                reversor(d.kept_total),
                                reversor(d.count_total),
                                reversor(d.extra_total),
                                d.get_first_keepable(),
                                d.path,
                                # d.size,
                                # # prefer shallower directory
                                # d.parent[::-1],
                                # reversor(d.path),
                            ))# ,
                            # reverse=True)
        # print('calc_max(): sorted_arr\n', pformat(sorted_arr))

        keepable = None
        for d in sorted_arr:
            dirs = d.get_keepable_dirs()
            if dirs:
                keepable = next(iter(dirs))
                break

        # also seek out children that would have actual files to keep
        # for d in dupedir_list:
        #     filtered_list.update(d.get_keepable_dirs())

        # if past_kept:
        #     past_kept = set(past_kept)
        #     print('calc_max(): past_kept\n', pformat(past_kept))
        #     # we need to consider past choices as an additional weight
        #     weighted = defaultdict(list)
        #     for fl in filtered_list:
        #         if fl.path in past_kept:
        #             continue
        #         count = 0
        #         for pk in past_kept:
        #             # print(fl.path, pk, len(DupeDir.max_overlap(fl.path, pk)))
        #             count += len(DupeDir.max_overlap(fl.path, pk))

        #         weighted[count].append(fl)
        #     keys = sorted(weighted.keys())
        #     keys.reverse()
        #     print('calc_max(): weighted\n', pformat(weighted))
        #     filtered_list = weighted[keys[0]]
        #     print('calc_max(): weighted_top\n', pformat(filtered_list))

        # print('calc_max(): final_max\n', keepable)
        return keepable

    def check_delete(self):
        if not self.is_deleted and self.is_empty():
            self.is_deleted = True
        return self.is_deleted

    def decrement_dupes(self, df, dwd):
        self.count -= 1
        self.count_total -= 1
        # self.size -= df.size
        self.check_delete()
        next_parent = self.parent
        # sometimes we need to skip dirs
        while next_parent != '/':
            if next_parent in dwd:
                dd = dwd[next_parent]
                dd.decrement_dupes(df, dwd)
                break
            next_parent = FileUtil.parent(next_parent)

    def increment_dupes(self, df, dwd):
        self.kept += 1
        self.kept_total += 1
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
            # print('keep(): found')
            accum[self.path] = keeps, deletes, size
            # print('DupeDir.keep():', self.path,
            #       pformat(keeps),
            #       pformat(deletes))
            return (self.path, keeps, deletes)
        else:
            print('keep(): none found')
            # move on to subdirs if empty
            dd = DupeDir.calc_max(self.dupe_children, accum.keys())
            if dd:
                return dd.keep(accum, delete_lookup, dwd)
            else:
                return (None, set(), set())

    def check_single_parent(self, da):
        # print('checking', self.path, self.parent)
        ret = da.get_dir_info(self.parent)
        filenames = ret['files']
        dirs = ret['subdirs']
        if len(filenames) == 0 and len(dirs) == 1:
            full_path = dirs[0]
            if full_path == self.path:
                dd = DupeDir(self.parent, None)
                dd.subdir_dupes.add(self)
                return dd
        return None


class DupeDedupe:
    """Determines optimal delete for DupeAnalysis instances."""

    def __init__(self, dirs, depth_first=True, debug=False):
        self.dirs = dirs
        self.debug = debug
        self.depth_first = depth_first
        self.timer = ProcessTimer(start=True)

    def analyze(self):
        """Compare the two directories for duplicate files."""

        print(f"-------------------------------")
        print(f"Analysis")
        print(f"-------------------------------")
        da = DupeAnalysis(debug=self.debug)
        da.load(self.dirs)
        rets = da.get_duplicates()

        print(f"-------------------------------")

        hashes_full = rets['dupes']
        rev_hashes_by_size = rets['sizes']
        empty_dirs = rets['empty_dirs']
        paths = rets['paths']
        zeroes = rets['zeroes']
        parents = [FileUtil.parent(path) for path in paths]

        # if self.debug:
        #     da.delete_hashes()

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
        for path in paths:
            dd = DupeDir(path, None)
            dirs_w_dupes[dd.path] = dd
            dirs_w_dupes_by_depth[dd.depth].append(dd)

        print('\tCreating objects')
        # create the dupe file objects

        with tqdm(total=len(hashes_full), unit='file', unit_scale=True,
                  ncols=80, desc=f"\tProcessing") as pbar:
            for hash, files in hashes_full.items():
                # if len(files) < 2:
                #     continue
                obj_list = set()
                for path in files:
                    if path not in dupefiles:
                        # print(f'\r\t  Processing: {parent}', end='')
                        df = DupeFile(path, hash,
                                      rev_hashes_by_size[path])
                        dupefiles[path] = df
                        obj_list.add(df)

                    parent = dupefiles[path].parent
                    # print('p', parent)
                    if parent not in dirs_w_dupes:
                        # print(f'\r\t  Processing: {parent}', end='')
                        dd = DupeDir(parent, None)
                        dirs_w_dupes[parent] = dd
                        dirs_w_dupes_by_depth[dd.depth].append(dd)
                        sp = dd.check_single_parent(da)
                        # print('sp', sp)
                        if sp:
                            dirs_w_dupes[sp.path] = sp
                            dirs_w_dupes_by_depth[sp.depth].append(sp)

                # set the duplicates
                for df in obj_list:
                    df.set_dupes(obj_list)

                pbar.update(1)


        # pprint(dupefiles)

        # add in empty dirs
        for dir in empty_dirs:
            dd = DupeDir(dir, None)
            dirs_w_dupes[dir] = dd
            dirs_w_dupes_by_depth[dd.depth].append(dd)

        # determine if dupe dirs are completely duplicate
        #  check against filesystem for other files
        #  check to see if subdirs are complete dupes
        #  check to see if dir is a superset (has other non dupe files)
        ordered_keys = sorted(dirs_w_dupes_by_depth.keys())
        rev_ordered_keys = ordered_keys.copy()
        rev_ordered_keys.reverse()

        print('\tFilling in parents')
        # fill in empty parent dirs to aggregate
        #  sizes and counts.
        for key in rev_ordered_keys:
            for dd in dirs_w_dupes_by_depth[key]:
                dd.fill_parents(dirs_w_dupes,
                                dirs_w_dupes_by_depth,
                                parents)

        # because we may update dirs_w_dupes_by_depth in fill_parents
        # we update the keys
        ordered_keys = sorted(dirs_w_dupes_by_depth.keys())
        rev_ordered_keys = ordered_keys.copy()
        rev_ordered_keys.reverse()

        with tqdm(total=len(rev_ordered_keys), unit='file',
                  unit_scale=True,
                  ncols=80, desc=f"\tLoading file system") as pbar1:
            for key in rev_ordered_keys:
                with tqdm(total=len(dirs_w_dupes_by_depth[key]),
                          unit='file', unit_scale=True,
                          leave=False,
                          ncols=80, desc=f"\t  Processing") as pbar2:
                    for dd in dirs_w_dupes_by_depth[key]:
                        dd.load_fs(da, dupefiles, dirs_w_dupes)
                        pbar2.update(1)
                pbar1.update(1)


        # get the highest directory level of each dir family of dupes
        key = next(iter(ordered_keys))
        start_list = dirs_w_dupes_by_depth[key]
        pprint(start_list)
        # determine which dir to start with
        d = DupeDir.calc_max(start_list)
        print(f'\tFound first keep dir: {d.path}')
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
            print(f'\tRemaining dupes to process: {len(remaining_dupes)}')
            # check whether we can find more in the area
            #  where kepts are already done to further concentrate
            #  the kepts
            if self.depth_first:
                # print('here')
                d = DupeDir.calc_max(kept.parent, final_output.keys())
            else:
                d = DupeDir.calc_max(start_list, final_output.keys())
            if not d:
                new_dwd_depth = defaultdict(list)
                # create new depth lookup
                for df in remaining_dupes:
                    new_dwd_depth[df.depth - 1].append(dirs_w_dupes[df.parent])

                # print('new_dwd_depth', pformat(new_dwd_depth))
                ordered_keys = sorted(new_dwd_depth.keys())
                if ordered_keys:
                    for key in ordered_keys:
                        start_list = new_dwd_depth[key]
                        # print('start_list', pformat(start_list))
                        d = DupeDir.calc_max(start_list, final_output.keys())
                        if d:
                            break
                # print('calc', d)
            if not d:
                break


            kept, kepts, dels = d.keep(final_output, delete_lookup, dirs_w_dupes)
            reviewed.update(kepts)
            reviewed.update(dels)
            # print('pass ', debug_count)
            remaining_dupes = all_dupes - reviewed

        pprint(f'remaining_dupes\n{pformat(remaining_dupes)}')

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

    def execute(self, exec_delete=False):
        try:
            final_dirs = self.analyze()

            print(f"-------------------------------")
            print(f"Results")
            print(f"-------------------------------")
            if not final_dirs:
                print("\nNo duplicates found")
            else:
                with open('dupe_list.csv', 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(['delete', 'keep',
                                        'size (MB)', 'size(B)'])
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
                           csvwriter.writerow([d.path, dpath, "%.02f" % (d.size/1024/1024), d.size])
                       all_deletes.update(deletes)
                       all_sizes += size
                    self.timer.stop()
                    print(f'Total Execution Time: {self.timer.elapsed_readable()}')
                    print(f'\nConsolidated delete list: {FileUtil.human_readable(all_sizes)}')
                with open('deletes.csv', 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(['delete', 'size (MB)', 'size (B)'])
                    for d in sorted(all_deletes, key=lambda d: d.path):
                        csvwriter.writerow([d.path,
                                            "%.02f" % (d.size/1024/1024),
                                            d.size])

        except Exception as e:
            print(f"**ERROR**: Exception:{type(e).__name__} {e}", file=sys.stderr)
            raise e



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find and compare duplicate files across directories.")
    parser.add_argument('dirs', type=str, nargs='+', help="Directories to act on.")
    parser.add_argument('--debug', action='store_true', help="Debug mode which deletes analyses and has extra printed detail.")
    parser.add_argument('--delete', action='store_true', help="Delete duplicates in a directory.")
    # parser.add_argument('--merge', metavar='DIR', help="Merge a specific directory and save the results -- no analysis provided.")

    args = parser.parse_args()

    if args.dirs:
        # if args.merge:
        #     da1 = DupeAnalysis([args.directory], args.debug)
        #     da2 = DupeAnalysis([args.merge], args.debug)
        #     da1.merge(da2)
        # else:
        da = DupeDedupe(args.dirs, args.debug)
        da.execute(args.delete)
    else:
        parser.print_help()


