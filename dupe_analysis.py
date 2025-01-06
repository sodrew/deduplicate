import hashlib
import os
import sqlite3
from pprint import pprint, pformat

class DupeAnalysis:
    """Handles file hashing and analysis for directories, optimized with layered hashing."""

    def __init__(self, dirs, debug=False, storage_dir='dd_analysis', file_size_limit=10 * 1024 * 1024, validate_suspect=False):
        self.paths = {os.path.abspath(dir) for dir in dirs}
        self.storage_dir = storage_dir
        self.db_path = self._get_db_path(self.paths, storage_dir)
        self.debug = debug
        self.file_size_limit = file_size_limit
        self.validate_suspect = validate_suspect

        os.makedirs(self.storage_dir, exist_ok=True)

    @staticmethod
    def _get_db_path(directories, storage_dir):
        sorted_dirs = sorted(map(os.path.abspath, directories))
        hash_value = hashlib.sha1('|'.join(sorted_dirs).encode()).hexdigest()
        db_filename = f"analysis_{hash_value}.db"
        return os.path.join(storage_dir, db_filename)

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

    def load(self):
        if os.path.exists(self.db_path):
            print(f"Loading existing database for {self.paths} from {self.db_path}")
            self.conn, self.cursor = DupeAnalysis._connect_db(self.db_path)
        else:
            print(f"No database found for {self.paths}, starting new analysis.")
            self.conn, self.cursor = DupeAnalysis._init_db(self.db_path)
            self.analyze()

    def analyze(self):
        print(self.paths)
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

    @staticmethod
    def _generate_hash_sql(old, new):
        return f"""
        SELECT id, path
        FROM files
        WHERE {old}
        IN
        (
        SELECT {old}
        FROM files
        WHERE {old} IS NOT NULL
        AND {new} IS NULL
        GROUP BY {old}
        HAVING COUNT(id) > 1
        )
        """
            # return (f"SELECT id, path FROM files WHERE {old} IN "
            #     f"(SELECT {old} FROM files WHERE {old} IS NOT NULL "
            #     f"AND {new} IS NULL GROUP BY {old} HAVING COUNT(id) > 1")


    def _compute_hashes(self, full_hash=False):
        res = self.cursor.execute(
            DupeAnalysis._generate_hash_sql('size', 'beg_hash'))
        if res:
            for row in self.cursor.fetchall():
                fid, path = row
                beg_hash = self.get_hash(path, partial=True)
                self._update_file_hashes(fid, beg_hash=beg_hash)

        res = self.cursor.execute(
            DupeAnalysis._generate_hash_sql('beg_hash', 'rev_hash'))
        if res:
            for row in self.cursor.fetchall():
                fid, path = row
                mid_hash = self.get_hash(path, position='middle')
                end_hash = self.get_hash(path, position='end')
                rev_hash = f"{end_hash}:{mid_hash}"
                self._update_file_hashes(fid, rev_hash=rev_hash)

        if not full_hash:
            return

        res = self.cursor.execute(
            DupeAnalysis._generate_hash_sql('rev_hash', 'full_hash'))
        if res:
            for row in self.cursor.fetchall():
                fid, path = row
                full_hash = self.get_hash(path, partial=False)
                self._update_file_hashes(fid, full_hash=full_hash)

    def _update_file_hashes(self, fid, beg_hash=None, rev_hash=None, full_hash=None):
        self.cursor.execute("""
            UPDATE files
            SET beg_hash = COALESCE(?, beg_hash),
                rev_hash = COALESCE(?, rev_hash),
                full_hash = COALESCE(?, full_hash)
            WHERE id = ?
        """, (beg_hash, rev_hash, full_hash, fid))
        self.conn.commit()

    @staticmethod
    def get_hash(filename, partial=False, position=None, hash=hashlib.sha1):
        hashobj = hash()
        try:
            with open(filename, 'rb') as f:
                if partial:
                    hashobj.update(f.read(1024))
                elif position == 'middle':
                    f.seek(max(0, os.path.getsize(filename) // 2 - 512))
                    hashobj.update(f.read(1024))
                elif position == 'end':
                    f.seek(max(0, os.path.getsize(filename) - 1024))
                    hashobj.update(f.read(1024))
                else:
                    while chunk := f.read(1024):
                        hashobj.update(chunk)
        except OSError:
            return None
        return hashobj.hexdigest()

    def merge(self, other_dirs):
        """
        Merge the current analysis with another set of directories.

        :param other_dirs: Another set of directories to merge with.
        :return: Path to the new merged database.
        """
        other_dirs = {os.path.abspath(dir) for dir in other_dirs}
        combined_dirs = self.paths | other_dirs
        output_db_path = self._get_db_path(combined_dirs, self.storage_dir)

        if os.path.exists(output_db_path):
            print(f"Merged database already exists at {output_db_path}")
            return output_db_path

        other_db_path = self._get_db_path(other_dirs, self.storage_dir)
        conn, cursor = DupeAnalysis._init_db(other_db_path)

        # Copy data from both source databases into the output database
        def copy_data(source_db_path):
            conn, cursor = DupeAnalysis._connect_db(source_db_path)
            cursor.execute("SELECT * FROM files")
            for row in cursor.fetchall():
                cursor_output.execute("""
                    INSERT OR IGNORE INTO files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
            cursor.execute("SELECT * FROM empty_dirs")
            for row in cursor.fetchall():
                cursor_output.execute("INSERT OR IGNORE INTO empty_dirs VALUES (?, ?)", row)
            conn.close()

        copy_data(self.db_path)
        copy_data(other_db_path)
        conn.commit()

        # Step 1: Identify potential duplicates across the merged data
        cursor.execute("""
            SELECT a.path, a.size, a.beg_hash, a.mid_hash, a.end_hash, a.full_hash, a.fast_full_hash, b.path
            FROM files a
            JOIN files b
            ON a.size = b.size
            AND a.beg_hash = b.beg_hash
            AND (a.mid_hash IS NULL OR a.mid_hash = b.mid_hash)
            AND (a.end_hash IS NULL OR a.end_hash = b.end_hash)
            AND a.path != b.path
        """)
        potential_duplicates = cursor.fetchall()

        # Step 2: Recompute missing hashes for potential duplicates
        for row in potential_duplicates:
            file_a, size, beg_hash, mid_hash, end_hash, full_hash, fast_full_hash, file_b = row
            if mid_hash is None:
                mid_hash = self.get_hash(file_a, position='middle')
                end_hash = self.get_hash(file_a, position='end')
                self._update_file_hashes_in_db(cursor, file_a, mid_hash=mid_hash, end_hash=end_hash)

            if full_hash is None and size <= self.file_size_limit:
                full_hash = self.get_hash(file_a, partial=False)
                self._update_file_hashes_in_db(cursor, file_a, full_hash=full_hash)
            elif fast_full_hash is None and size > self.file_size_limit:
                fast_full_hash = f"{beg_hash}:{mid_hash}:{end_hash}"
                self._update_file_hashes_in_db(cursor, file_a, fast_full_hash=fast_full_hash)

        conn.commit()
        conn.close()
        return output_db_path

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
        if self.debug:
            os.remove(self.db_path)


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
