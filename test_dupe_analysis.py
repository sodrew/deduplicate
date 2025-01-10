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

    @staticmethod
    def create_folder(path):
        Path(path).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def create_file(path, src=None):
        path, size, seek = TestDupeAnalysis._parse_file_and_size(path)
        opath = Path(path)
        opath.parent.mkdir(parents=True, exist_ok=True)
        opath.touch()
        # print('create_file', path)
        return path, size

    @staticmethod
    def _parse_file_and_size(path):
        # print('parse', path)
        seek = 0
        size = TestDupeAnalysis.human_size_to_bytes('6KB')
        if ':' in path:
            path, size = path.split(':', 1)
            if '-' in size:
                seek, size = size.split('-', 1)
                seek = TestDupeAnalysis.human_size_to_bytes(seek)
                size = TestDupeAnalysis.human_size_to_bytes(size)
                size = size - seek
            else:
                size = TestDupeAnalysis.human_size_to_bytes(size)
        # print('path', path, size)
        return path, size, seek

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

    def write_content(self, path, size=-1, src=None, seek=0):
        # print('write_content', seek, path, size, src)
        def pad(f, size):
            count = 0
            while count < size / 8:
                # Generate a random number and write it to the file
                random_number = random.randint(10000000, 99999999)
                f.write(f"{random_number}")
                count += 1
            return size

        copy_size = 0
        if path:
             path = os.path.join(self.test_root, path)
             with open(path, 'r+') as f:
                f.seek(seek)
                if not src:
                    # fsize = os.path.getsize(path) - seek
                    # if size > fsize:
                    #     size = fsize
                    pad(f, size)
                    copy_size = size
                else:
                    src, copy_size, src_seek = TestDupeAnalysis._parse_file_and_size(src)
                    src = os.path.join(self.test_root, src)
                    # print('parsed', src, copy_size)
                    fsize = os.path.getsize(src) - src_seek
                    if copy_size > fsize:
                        copy_size = fsize
                    with open(src, 'r') as s:
                        s.seek(src_seek)
                        data = s.read(copy_size)
                        f.write(data)
        else:
            with open(path, 'r+') as f:
                f.seek(size)
                pad(f, size)

        return seek + copy_size


    def generate_file_structure(self, input):
        """
            'folder1/file1a.txt',
            target = folder1/file1a.txt

            'folder1/file1a.txt:3KB',
            target = folder1/file1a.txt:3KB

            'folder1/file1b.txt==folder1/file1a.txt',
            target = folder1/file1b.txt
            source = folder1/file1a.txt

            'folder1/file1d.txt==folder1/file1a.txt:1KB|2KB',
            target = folder1/file1d.txt
            source = folder1/file1a.txt:1KB

        """
        for path in input:
            path = os.path.join(self.test_root, path)
            target = path
            src = None

            if '==' in path:  # make a duplicate
                target, src = path.split('==', 1)
                src = os.path.join(self.test_root, src)

            is_dir = target[-1] == os.sep
            if is_dir:
                if src:
                    shutil.copytree(src, target)
                else:
                    self.create_folder(target)
            else:
                target, size = self.create_file(target)
                seek = 0
                if src:
                    srcs = [src]
                    if '+' in src:
                        srcs = src.split('+')
                    for src in srcs:
                        # print(target, src, seek)
                        seek = self.write_content(target, src=src, seek=seek)
                else:
                    seek = self.write_content(target, size, src, seek=seek)



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

    def execute_default(self, dirs, complete_hash):
        analysis = DupeAnalysis(debug=True, db_root=self.db_root, complete_hash=complete_hash)
        analysis.load(dirs)
        # pprint(analysis.dump_db())
        actual, sizes = analysis.get_duplicates()
        # pprint(actual)
        analysis.close()
        return actual

    def execute_merge(self, dirs1, dirs2, complete_hash):
        analysis1 = DupeAnalysis(debug=True, db_root=self.db_root, complete_hash=complete_hash)
        analysis1.load(dirs1)
        # pprint(analysis1.dump_db())
        analysis1.close()
        analysis2 = DupeAnalysis(debug=True, db_root=self.db_root, complete_hash=complete_hash)
        analysis2.load(dirs1 + dirs2)
        # pprint(analysis2.dump_db())
        actual, sizes = analysis2.get_duplicates()
        # pprint(actual)
        analysis2.close()
        return actual

    def execute(self, input, expected, dirs, input2=None, dirs2=None, complete_hash=False):
        print(f"\n==={self.func()}===================================================================")
        self.generate_file_structure(input)
        dirs = [os.path.join(self.test_root, d) for d in dirs]
        if input2 and dirs2:
            self.generate_file_structure(input2)
            dirs2 = [os.path.join(self.test_root, d) for d in dirs2]
            actual = self.execute_merge(dirs, dirs2, complete_hash=complete_hash)
        else:
            actual = self.execute_default(dirs, complete_hash=complete_hash)
        print('\n======================================================================')
        self.validate_duplicates(actual, expected)

    def test_simple_duplicate(self):
        input = [
            'folder1/file1a.txt',
            'folder1/file1b.txt==folder1/file1a.txt',
            'folder1/file2.txt',
            'folder1/file3.txt',
        ]

        expected = [
            [
                'folder1/file1a.txt',
                'folder1/file1b.txt',
                ],
        ]

        dirs = [
            'folder1'
        ]

        self.execute(input, expected, dirs)

    def test_different_sizes(self):
        input = [
            'folder1/file1a.txt:3KB',
            'folder1/file1b.txt==folder1/file1a.txt',
            'folder1/pad1.txt:1KB',
            'folder1/pad2.txt:2KB',
            'folder1/file1c.txt==folder1/file1a.txt:2KB+folder1/pad1.txt',
            'folder1/file1d.txt==folder1/file1a.txt:1KB+folder1/pad2.txt',
            'folder1/file2.txt:32B',
            'folder1/file3.txt:64B',
            'folder1/file4.txt:128B',
            'folder1/file5.txt:256B',
            'folder1/file6.txt:512B',
            'folder1/file7.txt:4KB',
        ]

        expected = [
            [
                'folder1/file1a.txt',
                'folder1/file1b.txt',
                ],
        ]

        dirs = [
            'folder1'
        ]

        self.execute(input, expected, dirs)

    def test_separate_dirs(self):
        input = [
            'folder1/file1a.txt',
            'folder2/file1b.txt==folder1/file1a.txt',
        ]

        expected = [
            [
                'folder1/file1a.txt',
                'folder2/file1b.txt',
                ],
        ]

        dirs = [
            'folder1',
            'folder2',
        ]

        self.execute(input, expected, dirs)

    def test_nested_dirs(self):
        input = [
            'folder1/file1a.txt',
            'folder1/subfolder1/file1a.txt==folder1/file1a.txt',
        ]

        expected = [
            [
                'folder1/file1a.txt',
                'folder1/subfolder1/file1a.txt',
                ],
        ]

        dirs = [
            'folder1',
        ]

        self.execute(input, expected, dirs)

    def test_empty_root_nested_dirs(self):
        input = [
            'folder1/subfolder1/file1a.txt',
            'folder1/subfolder2/file1a.txt==folder1/subfolder1/file1a.txt',
        ]

        expected = [
            [
                'folder1/subfolder1/file1a.txt',
                'folder1/subfolder2/file1a.txt',
                ],
        ]

        dirs = [
            'folder1',
        ]

        self.execute(input, expected, dirs)

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

    def test_db_merge(self):
        input = [
            'folder1/file1a.txt',
            'folder1/file1b.txt==folder1/file1a.txt',
            'folder1/file2.txt',
        ]

        input2 = [
            'folder2/file1a.txt',
            'folder2/file1b.txt==folder2/file1a.txt',
            'folder2/file2.txt==folder1/file2.txt',
        ]

        expected = [
            [
                'folder1/file1a.txt',
                'folder1/file1b.txt',
                ],
            [
                'folder2/file1a.txt',
                'folder2/file1b.txt',
                ],
            [
                'folder1/file2.txt',
                'folder2/file2.txt',
                ],
        ]

        dirs = [
            'folder1',
        ]

        dirs2 = [
            'folder2',
        ]

        self.execute(input, expected, dirs, input2, dirs2)

    def test_db_merge2(self):
        input = [
            'folder1/file1a.txt',
            'folder1/file1b.txt==folder1/file1a.txt',
            'folder1/file2.txt',
            'folder2/file1a.txt',
            'folder2/file1b.txt==folder2/file1a.txt',
            'folder2/file3.txt',
        ]

        input2 = [
            'folder3/file1a.txt',
            'folder3/file1b.txt==folder3/file1a.txt',
            'folder3/file3.txt==folder1/file2.txt',
            'folder4/file4.txt',
            'folder4/file5.txt==folder3/file1a.txt',
            'folder4/file6.txt==folder2/file3.txt',
        ]

        expected = [
            [
                'folder1/file1a.txt',
                'folder1/file1b.txt',
                ],
            [
                'folder2/file1a.txt',
                'folder2/file1b.txt',
                ],
            [
                'folder3/file1a.txt',
                'folder3/file1b.txt',
                'folder4/file5.txt',
                ],
            [
                'folder1/file2.txt',
                'folder3/file3.txt',
                ],
            [
                'folder2/file3.txt',
                'folder4/file6.txt',
                ],

        ]

        dirs = [
            'folder1', 'folder2',
        ]

        dirs2 = [
            'folder3', 'folder4',
        ]

        self.execute(input, expected, dirs, input2, dirs2)

    def test_complete_hash(self):
        input = [
            'folder1/file1a.txt:5KB',
            'folder1/file1b.txt==folder1/file1a.txt',
            'folder1/pad1.txt:1KB',
            'folder1/pad2.txt:2KB',
            'folder1/file1c.txt==folder1/file1a.txt:1KB+folder1/pad1.txt+folder1/file1a.txt:2KB-3KB+folder1/pad1.txt+folder1/file1a.txt:4KB-5KB',
            'folder1/file1d.txt==folder1/file1a.txt:1KB+folder1/pad2.txt',
            'folder1/file2.txt:32B',
            'folder1/file3.txt:64B',
            'folder1/file4.txt:128B',
            'folder1/file5.txt:256B',
            'folder1/file6.txt:512B',
            'folder1/file7.txt:4KB',
        ]

        expected = [
            [
                'folder1/file1a.txt',
                'folder1/file1b.txt',
                # 'folder1/file1c.txt',
                ],
        ]

        dirs = [
            'folder1'
        ]

        # self.execute(input, expected, dirs)
        self.execute(input, expected, dirs, complete_hash=True)

    def test_complete_hash_false(self):
        input = [
            'folder1/file1a.txt:5KB',
            'folder1/file1b.txt==folder1/file1a.txt',
            'folder1/pad1.txt:1KB',
            'folder1/pad2.txt:2KB',
            'folder1/file1c.txt==folder1/file1a.txt:1KB+folder1/pad1.txt+folder1/file1a.txt:2KB-3KB+folder1/pad1.txt+folder1/file1a.txt:4KB-5KB',
            'folder1/file1d.txt==folder1/file1a.txt:1KB+folder1/pad2.txt',
            'folder1/file2.txt:32B',
            'folder1/file3.txt:64B',
            'folder1/file4.txt:128B',
            'folder1/file5.txt:256B',
            'folder1/file6.txt:512B',
            'folder1/file7.txt:4KB',
        ]

        expected = [
            [
                'folder1/file1a.txt',
                'folder1/file1b.txt',
                'folder1/file1c.txt',
                ],
        ]

        dirs = [
            'folder1'
        ]

        self.execute(input, expected, dirs)
