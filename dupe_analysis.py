import hashlib
import os
import sqlite3
import itertools
from pprint import pprint, pformat

class DupeAnalysis:
    """Handles file hashing and analysis for directories, optimized with layered hashing."""

    def __init__(self, debug=False,
                 db_root='dd_analysis', complete_hash=False):
        self.paths = None
        self.db_root = db_root
        self.db_path = None
        self.conn = None
        self.cursor = None
        self.debug = debug
        self.complete_hash = complete_hash

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
            size INTEGER,
            beg_hash TEXT,
            rev_hash TEXT,
            full_hash TEXT,
            name TEXT
        );

        CREATE TABLE IF NOT EXISTS empty_dirs (
            id INTEGER PRIMARY KEY,
            path TEXT UNIQUE
        );

        CREATE INDEX IF NOT EXISTS idx_files_size ON files(size);
        CREATE INDEX IF NOT EXISTS idx_files_beg_hash ON files(beg_hash);
        CREATE INDEX IF NOT EXISTS idx_files_beg_hash ON files(rev_hash);
        CREATE INDEX IF NOT EXISTS idx_files_full_hash ON files(full_hash);
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

        if exists:
            self.db_path = db_path
            print(f"Loading existing database for {self.paths} from {self.db_path}")
            self.conn, self.cursor = DupeAnalysis._connect_db(self.db_path)
            return
        else:
            # base case: do analysis
            if len(self.paths) == 1:
                self.db_path = db_path
                print(f"Creating database for {self.paths} from {self.db_path}")
                self.conn, self.cursor = DupeAnalysis._init_db(db_path)
                self.analyze()
                return
            else:
                # attempt partial load; search for permutations of dirs
                # in a greedy way
                print(f"Searching for any individual databases from {self.paths}")
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
                        da = DupeAnalysis(self.debug, self.db_root, self.complete_hash)
                        da.load(sp)
                        da.close()
                        dbs_found[da.db_path] = sp

                # print('dbs_found', pformat(dbs_found))
                # add in all of the found paths
                self._merge(dbs_found)


    def analyze(self):
        # print(self.paths)
        for path in self.paths:
            for dirpath, dirs, filenames in os.walk(path):
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    try:
                        file_size = os.path.getsize(full_path)
                        self._insert_file(full_path, file_size, filename)
                    except OSError:
                        continue

                if not dirs and not filenames:
                    self._insert_empty_dir(dirpath)

        self._compute_hashes()

    def _insert_file(self, path, size, name):
        self.cursor.execute("""
            INSERT OR IGNORE INTO files (path, size, name)
            VALUES (?, ?, ?)
        """, (path, size, name))
        self.conn.commit()

    def _insert_empty_dir(self, path):
        self.cursor.execute("""
            INSERT OR IGNORE INTO empty_dirs (path)
            VALUES (?)
        """, (path,))
        self.conn.commit()

    def _compute_hashes(self, full_hash=False):
        self._compute_hash('size', 'beg_hash')
        self._compute_hash('beg_hash', 'rev_hash')

        if full_hash:
            self._compute_hash('rev_hash', 'full_hash')

    def _compute_hash(self, old, new):
        res = self.cursor.execute(
            DupeAnalysis._generate_hash_sql(old, new))
        if res:
            for row in self.cursor.fetchall():
                fid, size, path = row
                hash = DupeAnalysis.get_hash(path, size, new)
                self._update_file_hashes(fid, hash, new)

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
    def get_hash(filename, filesize, position,
                 chunk=1024, hash=hashlib.sha1):
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
                    while chunk := f.read(chunk):
                        hashobj.update(chunk)
                else:
                    raise Exception('invalid position')
        except OSError:
            return None
        return hashobj.hexdigest()

    def _copy_data(self, source_db_path):
        conn, cursor = DupeAnalysis._connect_db(source_db_path)
        cursor.execute("SELECT path, size, beg_hash, rev_hash, full_hash, name FROM files")
        for row in cursor.fetchall():
            self.cursor.execute("""
                INSERT OR IGNORE INTO files (path, size, beg_hash, rev_hash, full_hash, name)
                VALUES (?, ?, ?, ?, ?, ?)
            """, row)
        cursor.execute("SELECT path FROM empty_dirs")
        for row in cursor.fetchall():
            self.cursor.execute("INSERT OR IGNORE INTO empty_dirs (path) VALUES (?)", row)
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
        for db_path, dirs in dbs_found.items():
            print(f"\t {dirs} from {db_path}")
            self._copy_data(db_path)
            self.conn.commit()

        self._compute_hashes()


        # # Step 1: Identify potential duplicates across the merged data
        # cursor.execute("""
        #     SELECT a.path, a.size, a.beg_hash, a.rev_hash, a.full_hash, b.path
        #     FROM files a
        #     JOIN files b
        #     ON a.size = b.size
        #     AND a.beg_hash = b.beg_hash
        #     AND a.rev_hash = b.rev_hash
        #     AND a.(a.end_hash IS NULL OR a.end_hash = b.end_hash)
        #     AND a.path != b.path
        # """)
        # potential_duplicates = cursor.fetchall()

        # # Step 2: Recompute missing hashes for potential duplicates
        # for row in potential_duplicates:
        #     file_a, size, beg_hash, mid_hash, end_hash, full_hash, fast_full_hash, file_b = row
        #     if mid_hash is None:
        #         mid_hash = self.get_hash(file_a, position='middle')
        #         end_hash = self.get_hash(file_a, position='end')
        #         self._update_file_hashes_in_db(cursor, file_a, mid_hash=mid_hash, end_hash=end_hash)

        #     if full_hash is None and size <= self.file_size_limit:
        #         full_hash = self.get_hash(file_a, partial=False)
        #         self._update_file_hashes_in_db(cursor, file_a, full_hash=full_hash)
        #     elif fast_full_hash is None and size > self.file_size_limit:
        #         fast_full_hash = f"{beg_hash}:{mid_hash}:{end_hash}"
        #         self._update_file_hashes_in_db(cursor, file_a, fast_full_hash=fast_full_hash)

        # conn.commit()
        # conn.close()
        # return output_db_path

    def _update_file_hashes_in_db(self, cursor, path, beg_hash=None, mid_hash=None, end_hash=None, full_hash=None, fast_full_hash=None):
        """
        Update hash values for a file directly in the database.

        :param cursor: SQLite cursor for the output database.
        :param path: File path to update.
        :param beg_hash: Beginning hash value.
        :param mid_hash: Middle hash value.
        :param end_hash: End hash value.
        :param full_hash: Full file hash.
        :param fast_full_hash: Fast full hash for large files.
        """
        cursor.execute("""
            UPDATE files
            SET beg_hash = COALESCE(?, beg_hash),
                mid_hash = COALESCE(?, mid_hash),
                end_hash = COALESCE(?, end_hash),
                full_hash = COALESCE(?, full_hash),
                fast_full_hash = COALESCE(?, fast_full_hash)
            WHERE path = ?
        """, (beg_hash, mid_hash, end_hash, full_hash, fast_full_hash, path))


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
                SELECT path, size, beg_hash, rev_hash, full_hash, name
                FROM files ORDER BY path ASC
            """):
            files.append({
                "path": row[0],
                "size": row[1],
                "beg_hash": row[2],
                "rev_hash": row[3],
                "full_hash": row[4],
                "name": row[5],
            })

        # Fetch empty directories
        for row in self.cursor.execute("SELECT path FROM empty_dirs"):
            empty_dirs.append(row[0])

        return {"files": files, "empty_dirs": empty_dirs}

    def get_duplicates(self):
        """
        Identify and return duplicates based on hashes.
        :return: Dictionary with duplicates grouped by their full hash or fast full hash.
        """
        duplicates = {}

        # Fetch files grouped by full hash
        self.cursor.execute("""
            SELECT full_hash, GROUP_CONCAT(path)
            FROM files
            WHERE full_hash IS NOT NULL
            GROUP BY full_hash
            HAVING COUNT(*) > 1
        """)
        for row in self.cursor.fetchall():
            duplicates[row[0]] = row[1].split(',')

        # Fetch files grouped by fast full hash for large files
        self.cursor.execute("""
            SELECT rev_hash, GROUP_CONCAT(path)
            FROM files
            WHERE rev_hash IS NOT NULL
            GROUP BY rev_hash
            HAVING COUNT(*) > 1
        """)
        for row in self.cursor.fetchall():
            duplicates[row[0]] = row[1].split(',')

        return duplicates
