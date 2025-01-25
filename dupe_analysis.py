import hashlib
import os
import sqlite3
import itertools
import fnmatch
import re
import subprocess
from tqdm import tqdm
from pprint import pprint, pformat
from dupe_utils import ProcessTimer

class DupeAnalysis:
    """Handles file hashing and analysis for directories, optimized with layered hashing."""

    def __init__(self, debug=False, complete_hash=False,
                 db_root='dd_analysis',
                 batch_limit=1000, excludes=[]):

        self.paths = None
        self.db_root = os.path.abspath(db_root)
        self.db_path = None
        self.conn = None
        self.cursor = None
        self.debug = debug
        self.complete_hash = complete_hash
        self.excludes = excludes
        self.excl_re = re.compile(r'|'.join([fnmatch.translate(x)
                                  for x in excludes]) or r'$.')
        self.zero_hash = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
        self.batch_limit = batch_limit
        if self.debug:
            self.batch_limit = 2

        os.makedirs(self.db_root, exist_ok=True)

    @staticmethod
    def _get_db_path(directories, db_root):
        sorted_dirs = sorted(map(os.path.abspath, directories))
        hash_value = hashlib.sha1('|'.join(sorted_dirs).encode()).hexdigest()
        db_filename = f"{hash_value}.db"
        return os.path.join(db_root, db_filename)

    def _set_db_path(self):
        self.db_path = self._get_db_path(self.paths, self.db_root)

    @staticmethod
    def _connect_db(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        return conn, cursor

    @staticmethod
    def _init_db(db_path):
        conn, cursor = DupeAnalysis._connect_db(db_path)
        cursor.executescript("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            path TEXT UNIQUE,
            depth INTEGER,
            dirpath TEXT,
            name TEXT,
            size INTEGER,
            beg_hash TEXT,
            rev_hash TEXT,
            full_hash TEXT
        );

        CREATE TABLE IF NOT EXISTS dirs (
            id INTEGER PRIMARY KEY,
            dirpath TEXT,
            subdir TEXT
        );

        CREATE TABLE IF NOT EXISTS empty_dirs (
            id INTEGER PRIMARY KEY,
            path TEXT UNIQUE
        );

        CREATE INDEX IF NOT EXISTS idx_files_dirpath ON files(dirpath);
        CREATE INDEX IF NOT EXISTS idx_files_depth ON files(depth);
        CREATE INDEX IF NOT EXISTS idx_files_size ON files(size);
        CREATE INDEX IF NOT EXISTS idx_files_beg_hash ON files(beg_hash);
        CREATE INDEX IF NOT EXISTS idx_files_rev_hash ON files(rev_hash);
        CREATE INDEX IF NOT EXISTS idx_files_full_hash ON files(full_hash);

        CREATE INDEX IF NOT EXISTS idx_dirs_dirpath ON dirs(dirpath);
        """)
        conn.commit()
        return conn, cursor

    @staticmethod
    def _exists(dirs, db_root):
        db_path = DupeAnalysis._get_db_path(dirs, db_root)
        exists = os.path.exists(db_path)
        return (exists, db_path)

    def load(self, dirs):
        self.paths = {os.path.abspath(dir) for dir in dirs}
        exists, db_path = DupeAnalysis._exists(self.paths, self.db_root)

        # self.paths = ['/volume1/Photos']
        # exists = True
        # db_path = '6f598c9f70b4ec41973449688788aabdb0bad847.db'
        # self.paths = ['/volume1/NetBackup']
        # exists = True
        # db_path = '03915609392d48546581ed2c04d879ef02283ca8.db'

        print(f"Attempting load of {self.paths}")
        if exists:
            self.db_path = db_path
            print(f"\tLoading existing database for {self.paths} from {self.db_path}")
            self.conn, self.cursor = DupeAnalysis._connect_db(self.db_path)
            return
        else:
            # base case: do analysis
            if len(self.paths) == 1:
                self.db_path = db_path
                print(f"\tCreating database {self.db_path} for {self.paths}")
                self.conn, self.cursor = DupeAnalysis._init_db(db_path)
                self.analyze(batch_limit=self.batch_limit)
                return
            else:
                # attempt partial load; search for permutations of dirs
                # in a greedy way
                print(f"\tSearching for any individual databases for {self.paths}")
                paths_not_loaded = self.paths
                dbs_found = {}
                path_count = len(paths_not_loaded) - 1
                while paths_not_loaded and path_count > 0:
                    combs = itertools.combinations(paths_not_loaded, path_count)
                    found = set()
                    for comb in combs:
                        sc = set(comb)
                        exists, db_path = DupeAnalysis._exists(sc, self.db_root)
                        if exists:
                            dbs_found[db_path] = sc
                            found = sc
                            break
                    paths_not_loaded = paths_not_loaded - found
                    path_count -= 1

                # print('paths_not_loaded', pformat(paths_not_loaded))
                # create new ones if there are still some not found
                # we only do one at a time so we can combine results easily
                if paths_not_loaded:
                    for path in paths_not_loaded:
                        # print('path', path)
                        sp = set()
                        sp.add(path)
                        da = DupeAnalysis(self.debug, complete_hash=self.complete_hash, db_root=self.db_root, excludes=self.excludes)
                        da.load(sp)
                        da.close()
                        dbs_found[da.db_path] = sp

                # print('dbs_found', pformat(dbs_found))
                # add in all of the found paths
                self._merge(dbs_found)


    def analyze(self, batch_limit=1000):
        print(f"Analyzing: {self.paths}")
        timer = ProcessTimer(start=True)

        batch_fs = []
        batch_fs_empty = []
        batch_ds = []
        batch_ds_empty = []
        total_size = self._get_total_size()
        batch_db_calls = batch_limit > 1
        with tqdm(total=total_size,
                  unit='B', unit_scale=True, unit_divisor=1024,
                  ncols=80, desc="\t[Pass 0] load filesizes") as pbar:
            for path in self.paths:
                for root, dirs, files in os.walk(path):
                    filtered_files = []
                    for fname in files:
                        depth = fname.count(os.sep)
                        path = os.path.join(root, fname)
                        # exclude files
                        if self.excl_re.match(path):
                            continue
                        filtered_files.append(path)
                        try:
                            file_size = os.path.getsize(path)
                        except OSError:
                            file_size = -1

                        if batch_db_calls:
                            if file_size == 0:
                                batch_fs_empty.append((path,
                                                   depth,
                                                   root,
                                                   fname))
                            else:
                                batch_fs.append((path, depth,
                                              root,
                                              fname, file_size))
                            if len(batch_fs) >= batch_limit:
                                self._insert_files_batch(batch_fs)
                                batch_fs = []

                            # print(batch_limit, len(batch_fs_empty))
                            if len(batch_fs_empty) >= batch_limit:
                                self._insert_files_empty_batch(batch_fs_empty)
                                batch_fs_empty = []
                        else:
                            self._insert_file(path, depth, root, fname, file_size)
                        pbar.update(file_size)

                    # exclude dirs
                    dirs[:] = [os.path.join(root, d) for d in dirs]
                    dirs[:] = [d for d in dirs
                               if not self.excl_re.match(d)]
                    if dirs:
                        if batch_db_calls:
                            batch_ds.append((root, dirs))

                            if len(batch_ds) >= batch_limit:
                                self._insert_dirs_batch(batch_ds)
                                batch_ds = []
                        else:
                            self._insert_dirs(root, dirs)
                    else:
                        if not filtered_files:
                            if batch_db_calls:
                                batch_ds_empty.append((root,))

                                if len(batch_ds_empty) >= batch_limit:
                                    self._insert_dirs_empty(batch_ds_empty)
                                    batch_ds_empty = []
                            else:
                                self._insert_dirs_empty([(root,)])

        if batch_db_calls:
            if batch_fs:
                self._insert_files_batch(batch_fs)
            if batch_fs_empty:
                self._insert_files_empty_batch(batch_fs_empty)
            if batch_ds:
                self._insert_dirs_batch(batch_ds)
            if batch_ds_empty:
                self._insert_dirs_empty(batch_ds_empty)

        self._compute_hashes()
        print(f"\tTotal Analysis Time: {timer.elapsed_readable()}")

    def _get_total_size(self):
        total_size = 0
        for path in self.paths:
            print(f'\tCalculating total size of directory: {path}')
            with subprocess.Popen(['du', '-sb', path],
                                    stdout=subprocess.PIPE) as proc:
                try:
                    output, errs = proc.communicate(timeout=60)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    output, errs = proc.communicate()
                if output:
                    output = output.split()[0]
                    total_size += int(output)
        return total_size

    def _insert_file(self, path, depth, dirpath, name, size):
        self.cursor.execute("""
            INSERT INTO files (path, depth, dirpath, name, size)
            VALUES (?, ?, ?, ?, ?)
        """, (path, depth, dirpath, name, size))
        self.conn.commit()

    def _insert_files_batch(self, batch):
        self.cursor.executemany("""
            INSERT INTO files (path, depth, dirpath, name, size)
            VALUES (?, ?, ?, ?, ?)
        """, batch)
        self.conn.commit()

    def _insert_files_empty_batch(self, batch_zero):
        self.cursor.executemany(f"""
            INSERT INTO files (path, depth, dirpath, name, size, beg_hash, rev_hash, full_hash)
            VALUES (?, ?, ?, ?, 0, '{self.zero_hash}', '{self.zero_hash}', '{self.zero_hash}')
        """, batch_zero)
        self.conn.commit()

    def _insert_dirs(self, path, dirs):
        new_dirs = [(d,) for d in dirs]
        self.cursor.executemany(f"""
            INSERT INTO dirs (dirpath, subdir)
            VALUES ('{path}', ?)
        """, new_dirs)
        self.conn.commit()

    def _insert_dirs_batch(self, batch_ds):
        all_inserts = []
        for p, ds in batch_ds:
            all_inserts.extend([(p, d) for d in ds])
        self.cursor.executemany(f"""
            INSERT INTO dirs (dirpath, subdir)
            VALUES (?, ?)
        """, all_inserts)
        self.conn.commit()

    def _insert_dirs_empty(self, paths):
        self.cursor.executemany("""
            INSERT INTO empty_dirs (path)
            VALUES (?)
        """, paths)
        self.conn.commit()

    def _compute_hashes(self):
        self._compute_hash('size', 'beg_hash',
                           '[Pass 1] beginning hash')

        self._compute_hash('beg_hash', 'rev_hash',
                           '[Pass 2] end & mid hash')

        if self.complete_hash:
            self._compute_hash('rev_hash', 'full_hash',
                               '[Pass 3] full file hash')
    def _compute_hash(self, old, new, msg):
        self.cursor.execute(
            DupeAnalysis._generate_hash_sql(old, new))
        rows = self.cursor.fetchall()
        with tqdm(total=len(rows), unit='file', unit_scale=True,
                  ncols=80, desc=f"\t{msg}") as pbar:

            for row in rows:
                fid, size, path = row
                # print(path, size, new)
                hash = DupeAnalysis.get_hash(path, size, new)
                self._update_file_hashes(fid, hash, new)
                pbar.update(1)

    @staticmethod
    def _generate_hash_sql(old, new):
        return f"""
        SELECT id, size, path
        FROM files
        WHERE {old}
        IN
        (
        SELECT {old}
        FROM files
        WHERE {old} IS NOT NULL
        AND size > 0
        GROUP BY {old}
        HAVING COUNT(id) > 1
        )
        AND {new} IS NULL
        """

    def _update_file_hashes(self, fid, hash, position):
        # print(fid, hash, position)
        sql = f"""
        UPDATE files
        SET {position} = '{hash}'
        WHERE id = {fid}
        """
        # print(sql)
        self.cursor.execute(sql)
        self.conn.commit()

    @staticmethod
    def chunk_reader(fobj, chunk_size):
        """Generator that reads a file in chunks of bytes."""
        while True:
            chunk = fobj.read(chunk_size)
            if not chunk:
                return
            yield chunk

    @staticmethod
    def get_hash(filename, filesize, position,
                 chunk=1024, hash=hashlib.sha1):
        if filesize == 0:
            return self.zero_hash

        hashobj = hash()
        try:
            with open(filename, 'rb') as f:
                if position == 'beg_hash':
                    hashobj.update(f.read(chunk))
                elif position == 'rev_hash':
                    f.seek(max(0, filesize - chunk))
                    hashobj.update(f.read(chunk))
                    f.seek(max(0, filesize // 2 - chunk // 2))
                    hashobj.update(f.read(chunk))
                elif position == 'full_hash':
                    for chunk in DupeAnalysis.chunk_reader(f, chunk):
                        hashobj.update(chunk)
                else:
                    raise Exception('invalid position')
        except OSError:
            return None
        return hashobj.hexdigest()

    def _copy_data(self, source_db_path):
        conn, cursor = DupeAnalysis._connect_db(source_db_path)
        cursor.execute("SELECT path, depth, dirpath, name, size, beg_hash, rev_hash, full_hash FROM files")
        for row in cursor.fetchall():
            self.cursor.execute("""
                INSERT OR IGNORE INTO files (path, depth, dirpath, name, size, beg_hash, rev_hash, full_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
        cursor.execute("SELECT path FROM empty_dirs")
        for row in cursor.fetchall():
            self.cursor.execute("INSERT OR IGNORE INTO empty_dirs (path) VALUES (?)", row)

        cursor.execute("SELECT dirpath, subdir FROM dirs")
        for row in cursor.fetchall():
            self.cursor.execute("INSERT OR IGNORE INTO dirs (dirpath, subdir) VALUES (?, ?)", row)
        conn.close()

    def _merge(self, dbs_found):
        """
        Helper function to load() which merges all database files together
        """

        # sub function to copy data between two dbs

        # merge is only called when something couldn't be immediately loaded
        #  because it relies on other items that need to be merged

        # merge the list of path sets into one set
        self.paths = set().union(*dbs_found.values())
        self._set_db_path()
        self.conn, self.cursor = DupeAnalysis._init_db(self.db_path)
        # Copy data from the databases into the output database
        print(f"Merging existing database for:")
        timer = ProcessTimer(start=True)
        for db_path, dirs in dbs_found.items():
            print(f"\t{dirs} from {db_path}")
            self._copy_data(db_path)
            self.conn.commit()

        print(f"Recomputing hashes for merged data")
        self._compute_hashes()
        print(f"\tTotal Merge Time: {timer.elapsed_readable()}")

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
        # if self.debug:
        #     os.remove(self.db_path)

    def dump_db(self):
        """Output the current database content for testing."""
        files = []
        empty_dirs = []

        # Fetch files
        for row in self.cursor.execute("""
        SELECT path, depth, dirpath, name, size, beg_hash, rev_hash, full_hash
        FROM files
        ORDER BY path ASC
            """):
            files.append({
                "path": row[0],
                "depth": row[1],
                "dirpath": row[2],
                "name": row[3],
                "size": row[4],
                "beg_hash": row[5],
                "rev_hash": row[6],
                "full_hash": row[7],
            })

        # Fetch empty directories
        for row in self.cursor.execute("SELECT path FROM empty_dirs"):
            empty_dirs.append(row[0])

        return {"files": files, "empty_dirs": empty_dirs}

    def _query_duplicates(self, hash):
        duplicates = {}
        sizes = {}
        zeroes = []
        # self.cursor.execute(f"""
        # SELECT {hash},
        # GROUP_CONCAT(path || '|' || size)
        # FROM files
        # WHERE {hash} IS NOT NULL
        # AND size > 0
        # GROUP BY {hash}
        # HAVING COUNT(id) > 1
        # """)
        self.cursor.execute(f"""
        SELECT {hash},
        GROUP_CONCAT(path || '::' || size, '||')
        FROM files
        WHERE {hash} IS NOT NULL
        GROUP BY {hash}
        HAVING COUNT(id) > 1
        """)
        for row in self.cursor.fetchall():
            paths = []
            # print('row', row[1])
            for r in row[1].split('||'):
                # print('r', r)
                path, size = r.split('::')
                paths.append(path)
                sizes[path] = int(size)
            duplicates[row[0]] = paths
        return duplicates, sizes

    def get_dir_info(self, directory):
        depth = directory.count(os.sep)

        self.cursor.execute(f"""
        SELECT path
        FROM files
        WHERE dirpath = ?
        """, (directory,))

        files = [f[0] for f in self.cursor.fetchall()]

        self.cursor.execute(f"""
        SELECT subdir
        FROM dirs
        WHERE dirpath = ?
        """, (directory,))

        subdirs = [s[0] for s in self.cursor.fetchall()]

        # print(f"get_dir_info(): {depth}, {directory}\n{pformat({'files': files, 'subdirs': subdirs})}")
        return {'files': files, 'subdirs': subdirs}

    # def get_dir_info(self, directory):
    #     dir_len = len(directory)
    #     self.cursor.execute(f"""
    #     SELECT
    #         CASE
    #             WHEN dirpath = ? THEN 'file'
    #             WHEN dirpath LIKE ? THEN 'subdir'
    #         END AS type,
    #         CASE
    #             WHEN dirpath = ? THEN path
    #             WHEN dirpath LIKE ? THEN
    #             substr(path, 1, ? +
    #                 instr(substr(path, ? + 2, length(path)-?), '/'))
    #         END AS item
    #     FROM files
    #     WHERE dirpath = ? OR dirpath LIKE ?
    #     """, (directory,
    #           f"{directory}/%",
    #           directory,
    #           f"{directory}/%",
    #           dir_len, dir_len, dir_len,
    #           directory, f"{directory}/%"))

    #     files = []
    #     subdirs = set()

    #     for row in self.cursor.fetchall():
    #         type_, item = row
    #         if type_ == 'file':
    #             files.append(item)
    #         elif type_ == 'subdir':
    #             subdirs.add(item)

    #     # print('get_dir_info()', directory, pformat(files), pformat(subdirs))
    #     return {'files': files, 'subdirs': list(subdirs)}

    def get_duplicates(self):
        """
        Identify and return duplicates based on hashes.
        :return: Dictionary with duplicates grouped by their full hash or fast full hash.
        """

        ret = None
        if self.complete_hash:
            # Fetch files grouped by full hash
            ret = self._query_duplicates('full_hash')
        else:
            ret =  self._query_duplicates('rev_hash')
        duplicates, sizes = ret

        empty_dirs = []
        for row in self.cursor.execute("SELECT path FROM empty_dirs"):
            empty_dirs.append(row[0])

        zeroes = []
        for row in self.cursor.execute("SELECT path FROM files WHERE size=0"):
            zeroes.append(row[0])

        return {
            'dupes': duplicates,
            'zeroes': zeroes,
            'sizes': sizes,
            'paths': self.paths,
            'empty_dirs': empty_dirs
        }
