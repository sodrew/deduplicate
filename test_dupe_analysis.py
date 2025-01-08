import os
import random
from pathlib import Path
import shutil
import unittest
from pprint import pprint, pformat
from dupe_analysis import DupeAnalysis

class TestDupeAnalysis(unittest.TestCase):
    test_root = "test"  # Set a fixed directory for tests
    db_root = "test_dbs"
    had_exception = False

    def setUp(self):
        """Set up the test root directory."""
        # print('setUp', self.id(), self.had_exception)
        if self.__class__.had_exception:
            self.skipTest('another test had an exception')
        self.test_root = os.path.abspath(self.test_root)
        if os.path.exists(self.test_root):
            shutil.rmtree(self.test_root)
        os.makedirs(self.test_root)
        self.db_root = os.path.abspath(self.db_root)
        if os.path.exists(self.db_root):
            shutil.rmtree(self.db_root)
        os.makedirs(self.db_root)

    def func(self):
        return self.id().split('.')[2]

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
                    os.makedirs(os.path.dirname(name), exist_ok=True)
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
        found = {}
        for e in expected:
            efull = set()
            for es in e:
                efull.add(os.path.join(self.test_root, es))
            for a in actual.values():
                a = set(a)
                # print('a:', pformat(a), '\nefull', pformat(efull))
                if a == efull:
                    key = '|'.join(sorted(list(efull)))
                    found[key] = a
                    break

        self.assertTrue(len(found.keys()) == len(expected))
        self.assertTrue(len(actual) == len(expected))

        # a = set(actual)
        # e = set(expected)
        # self.assertEqual(a-e, set(), f"\nextra: {pformat(a-e)}")
        # self.assertEqual(e-a, set(), f"\nmissing:{pformat(e-a)}")

    def execute_default(self, dirs):
        analysis = DupeAnalysis(debug=True, db_root=self.db_root)
        analysis.load(dirs)
        pprint(analysis.dump_db())
        actual = analysis.get_duplicates()
        pprint(actual)
        analysis.close()
        return actual

    def execute_merge(self, dirs1, dirs2):
        analysis1 = DupeAnalysis(debug=True, db_root=self.db_root)
        analysis1.load(dirs1)
        # pprint(analysis1.dump_db())
        analysis1.close()
        analysis2 = DupeAnalysis(debug=True, db_root=self.db_root)
        analysis2.load(dirs1 + dirs2)
        # pprint(analysis2.dump_db())
        actual = analysis2.get_duplicates()
        # pprint(actual)
        analysis2.close()
        return actual

    def execute(self, input, expected, dirs, input2=None, dirs2=None):
        print(f"\n==={self.func()}===================================================================")
        self.generate_file_structure(input)
        dirs = [os.path.join(self.test_root, d) for d in dirs]
        if input2 and dirs2:
            self.generate_file_structure(input2)
            dirs2 = [os.path.join(self.test_root, d) for d in dirs2]
            actual = self.execute_merge(dirs, dirs2)
        else:
            actual = self.execute_default(dirs)
        print('\n======================================================================')
        self.validate_duplicates(actual, expected)

    # def test_simple_duplicate(self):
    #     input = [
    #         'folder1/file1a.txt',
    #         'folder1/file1b.txt==folder1/file1a.txt',
    #         'folder1/file2.txt',
    #         'folder1/file3.txt',
    #     ]

    #     expected = [
    #         [
    #             'folder1/file1a.txt',
    #             'folder1/file1b.txt',
    #             ],
    #     ]

    #     dirs = [
    #         'folder1'
    #     ]

    #     self.execute(input, expected, dirs)

    # def test_different_sizes(self):
    #     input = [
    #         'folder1/file1a.txt:3KB',
    #         'folder1/file1b.txt==folder1/file1a.txt',
    #         'folder1/file1c.txt==folder1/file1a.txt:2KB+1KB',
    #         'folder1/file1d.txt==folder1/file1a.txt:1KB+2KB',
    #         'folder1/file2.txt:32B',
    #         'folder1/file3.txt:64B',
    #         'folder1/file4.txt:128B',
    #         'folder1/file5.txt:256B',
    #         'folder1/file6.txt:512B',
    #         'folder1/file7.txt:4KB',
    #     ]

    #     expected = [
    #         [
    #             'folder1/file1a.txt',
    #             'folder1/file1b.txt',
    #             ],
    #     ]

    #     dirs = [
    #         'folder1'
    #     ]

    #     self.execute(input, expected, dirs)

    # def test_separate_dirs(self):
    #     input = [
    #         'folder1/file1a.txt',
    #         'folder2/file1b.txt==folder1/file1a.txt',
    #     ]

    #     expected = [
    #         [
    #             'folder1/file1a.txt',
    #             'folder2/file1b.txt',
    #             ],
    #     ]

    #     dirs = [
    #         'folder1',
    #         'folder2',
    #     ]

    #     self.execute(input, expected, dirs)

    # def test_nested_dirs(self):
    #     input = [
    #         'folder1/file1a.txt',
    #         'folder1/subfolder1/file1a.txt==folder1/file1a.txt',
    #     ]

    #     expected = [
    #         [
    #             'folder1/file1a.txt',
    #             'folder1/subfolder1/file1a.txt',
    #             ],
    #     ]

    #     dirs = [
    #         'folder1',
    #     ]

    #     self.execute(input, expected, dirs)

    # def test_empty_root_nested_dirs(self):
    #     input = [
    #         'folder1/subfolder1/file1a.txt',
    #         'folder1/subfolder2/file1a.txt==folder1/subfolder1/file1a.txt',
    #     ]

    #     expected = [
    #         [
    #             'folder1/subfolder1/file1a.txt',
    #             'folder1/subfolder2/file1a.txt',
    #             ],
    #     ]

    #     dirs = [
    #         'folder1',
    #     ]

    #     self.execute(input, expected, dirs)

    def test_empty_root_nested_dirs2(self):
        input = [
            'folder1/subfolder1/file1a.txt',
            'folder1/subfolder2/file1a.txt==folder1/subfolder1/file1a.txt',
            'folder2/file1a.txt==folder1/subfolder1/file1a.txt',
            'folder2/file2.txt',
        ]

        expected = [
            [
                'folder1/subfolder1/file1a.txt',
                'folder1/subfolder2/file1a.txt',
                'folder2/file1a.txt',
                ],
        ]

        dirs = [
            'folder1',
            'folder2',
        ]

        self.execute(input, expected, dirs)

    # def test_db_merge(self):
    #     input = [
    #         'folder1/file1a.txt',
    #         'folder1/file1b.txt==folder1/file1a.txt',
    #         'folder1/file2.txt',
    #     ]

    #     input2 = [
    #         'folder2/file1a.txt',
    #         'folder2/file1b.txt==folder2/file1a.txt',
    #         'folder2/file2.txt==folder1/file2.txt',
    #     ]

    #     expected = [
    #         [
    #             'folder1/file1a.txt',
    #             'folder1/file1b.txt',
    #             ],
    #         [
    #             'folder2/file1a.txt',
    #             'folder2/file1b.txt',
    #             ],
    #         [
    #             'folder1/file2.txt',
    #             'folder2/file2.txt',
    #             ],
    #     ]

    #     dirs = [
    #         'folder1',
    #     ]

    #     dirs2 = [
    #         'folder2',
    #     ]

    #     self.execute(input, expected, dirs, input2, dirs2)
