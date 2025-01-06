import os
import random
from pathlib import Path
import shutil
import unittest
from pprint import pprint, pformat
from dupe_analysis import DupeAnalysis

class TestDupeAnalysis(unittest.TestCase):
    test_root = "test"  # Set a fixed directory for tests
    had_exception = False

    def setUp(self):
        """Set up the test root directory."""
        # print('setUp', self.id(), self.had_exception)
        if self.__class__.had_exception:
            self.skipTest('another test had an exception')
        if os.path.exists(self.test_root):
            shutil.rmtree(self.test_root)
        os.makedirs(self.test_root)

    # def tearDown(self):
    #     """Clean up the test root directory."""
    #     if os.path.exists(self.test_root):
    #         shutil.rmtree(self.test_root)

    def create_folder(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)


    def create_file(self, path, target_size):
        if target_size == -1:
            target_size = TestDupeAnalysis.human_size_to_bytes('32B')
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with open(path, 'w') as f:
            while count < target_size / 8:
                # Generate a random number and write it to the file
                random_number = random.randint(10000000, 99999999)
                f.write(f"{random_number}")
                count += 1

    @staticmethod
    def human_size_to_bytes(size_str):
        # Define size units and their corresponding byte values
        units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4, "PB": 1024**5}

        # Extract the numerical part and the unit from the input string
        size_str = size_str.strip().upper()
        num = int(''.join(filter(str.isdigit, size_str)))
        unit = ''.join(filter(str.isalpha, size_str))

        # Convert to bytes
        if unit in units:
            return int(num * units[unit])
        else:
            raise ValueError(f"Unrecognized unit: {unit}")

    def generate_file_structure(self, input):
        for path in input:
            path = os.path.join(self.test_root, path)
            name = path
            src = None
            if '==' in path:  # make a duplicate
                name, src = path.split('==', 1)
                src = os.path.join(self.test_root, src)

            is_dir = name[-1] == os.sep
            if is_dir:
                if src:
                    shutil.copytree(src, name)
                else:
                    self.create_folder(name)
            else:
                if src:
                    size = -1
                    pad = -1
                    if ':' in src:
                        src, size = src.split(':', 1)
                        if '+' in size:
                            size, pad = size.split('+', 1)
                            pad = TestDupeAnalysis.human_size_to_bytes(pad)
                        size = TestDupeAnalysis.human_size_to_bytes(size)
                    shutil.copy(src, name)
                    if size > 0:
                        # truncate file
                        with open(name, 'r+') as f:
                            f.seek(size)
                            f.truncate()
                    if pad > 0:
                        # pad file
                        with open(name, 'r+') as f:
                            f.seek(size)
                            count = 0
                            while count < pad / 8:

                                # Generate a random number and write it to the file
                                random_number = random.randint(10000000, 99999999)
                                f.write(f"{random_number}")
                                count += 1
                else:
                    size = -1
                    if ':' in name:
                        name, size = name.split(':', 1)
                        size = TestDupeAnalysis.human_size_to_bytes(size)
                    self.create_file(name, size)

    def validate_duplicates(self, actual, expected):
        """Validate duplicates against expected output."""
        sorted_vals = sorted(actual.values())

        aset = set(sorted_vals)
        self.assertTrue(aset in expected)

        # a = set(actual)
        # e = set(expected)
        # self.assertEqual(a-e, set(), f"\nextra: {pformat(a-e)}")
        # self.assertEqual(e-a, set(), f"\nmissing:{pformat(e-a)}")

    def execute(self, input, dirs, expected):
        self.generate_file_structure(input)
        dirs = [os.path.join(self.test_root, d) for d in dirs]
        analysis = DupeAnalysis(dirs, debug=True)
        analysis.load()
        db = analysis.dump_db()
        pprint(db)
        actual = analysis.get_duplicates()
        pprint(actual)
        analysis.close()

        self.validate_duplicates(actual, expected)

    def test_single_folder_duplicates(self):
        """Test duplicate detection within a single folder."""
        input = [
            'folder1/file1a.txt:3KB',
            'folder1/file1b.txt==folder1/file1a.txt',
            'folder1/file1c.txt==folder1/file1a.txt:2KB+1KB',
            'folder1/file2.txt:32B',
            'folder1/file3.txt:64B',
            'folder1/file4.txt:128B',
            'folder1/file5.txt:256B',
            'folder1/file6.txt:512B',
            'folder1/file7.txt:4KB',
        ]

        dirs = [
            'folder1'
        ]

        expected = [
            [
                'folder1/file1a.txt',
                'folder1/file1b.txt',
                ],
        ]
        self.execute(input, dirs, expected)

    # def test_multiple_folders_duplicates(self):
    #     """Test duplicate detection across multiple folders."""
    #     setup_file_structure(self.TEST_BASE, {
    #         "folder1": ["file1.txt"],
    #         "folder2": ["folder1/file1.txt", "file2.txt"]
    #     })

    #     analysis = DupeAnalysis([f"{self.TEST_BASE}/folder1", f"{self.TEST_BASE}/folder2"], debug=True)
    #     analysis.load()

    #     expected_duplicates = {
    #         "some_hash_key": [
    #             f"{self.TEST_BASE}/folder1/file1.txt",
    #             f"{self.TEST_BASE}/folder2/file1.txt"
    #         ]
    #     }
    #     self.validate_duplicates(analysis, expected_duplicates)

    # def test_large_file_fast_hash(self):
    #     """Test fast hash detection for large files."""
    #     large_file_path = f"{self.TEST_BASE}/folder1/large_file.txt"
    #     os.makedirs(f"{self.TEST_BASE}/folder1", exist_ok=True)
    #     with open(large_file_path, "w") as f:
    #         f.write("content" * 1024 * 1024)  # 6MB file

    #     analysis = DupeAnalysis([f"{self.TEST_BASE}/folder1"], debug=True, file_size_limit=5 * 1024 * 1024)
    #     analysis.load()

    #     duplicates = analysis.get_duplicates()
    #     self.assertIn("some_fast_full_hash", duplicates)

    # def test_partial_and_full_merge(self):
    #     """Test merging of databases with and without duplicates."""
    #     setup_file_structure(self.TEST_BASE, {
    #         "folder1": ["file1.txt"],
    #         "folder2": ["file1.txt", "file2.txt"],
    #         "folder3": ["file3.txt", "folder1/file1.txt"]
    #     })

    #     analysis1 = DupeAnalysis([f"{self.TEST_BASE}/folder1"], debug=True)
    #     analysis1.load()

    #     analysis2 = DupeAnalysis([f"{self.TEST_BASE}/folder2"], debug=True)
    #     analysis2.load()

    #     merged_db = analysis1.merge([f"{self.TEST_BASE}/folder2"])
    #     merged_analysis = DupeAnalysis([f"{self.TEST_BASE}/folder1", f"{self.TEST_BASE}/folder2"], debug=True)
    #     merged_analysis.db_path = merged_db
    #     merged_analysis.load()

    #     expected_duplicates = {
    #         "some_hash_key": [
    #             f"{self.TEST_BASE}/folder1/file1.txt",
    #             f"{self.TEST_BASE}/folder2/file1.txt"
    #         ]
    #     }
    #     self.validate_duplicates(merged_analysis, expected_duplicates)
