import os
import shutil
import subprocess
import unittest
from pathlib import Path
import pprint
import argparse

class DuplicateDeletionTest(unittest.TestCase):
    script_path = "deduplicate.py"  # Replace with the actual path to your script
    json_root = "dd_analysis"
    test_root = "test"  # Set a fixed directory for tests
    preserve_root = "test_preserve"  # Set a fixed directory for preserve
    defargs = ['--delete', 'test/folder1', 'test/folder2']
    had_exception = False

    def setUp(self):
        """Set up the test root directory."""
        # print('setUp', self.id(), self.had_exception)
        if self.__class__.had_exception:
            self.skipTest('another test had an exception')
        if os.path.exists(self.test_root):
            shutil.rmtree(self.test_root)
        os.makedirs(self.test_root)
        # if we disable debug we need to clean this up
        if os.path.exists(self.json_root):
            shutil.rmtree(self.json_root)
        os.makedirs(self.json_root)

    # def tearDown(self):
    #     """Clean up the test root directory."""
    #     if os.path.exists(self.test_root):
    #         shutil.rmtree(self.test_root)

    def run_script(self, arguments):
        """Run the delete script with the specified arguments and store the output."""
        command = ["python3", self.script_path] + arguments
        result = subprocess.run(command, capture_output=True, text=True)
        self._last_script_output = result.stdout
        self._last_script_error = result.stderr
        self._last_command = command
        return result.stdout, result.stderr

    def func(self):
        return self.id().split('.')[2]

    def _print_script_output(self, input, output):
        """Print the script output when a test fails."""
        print(f"\n==={self.func()}===================================================================")
        print(f"\n--- Test Input: --- \n{pprint.pformat(input)}")
        print(f"\n--- Test Output: ---\n"
              f"{pprint.pformat(output)}\n")
        # print(f"--- Command ---\n{' '.join(self._last_command)}")
        print(f"--- Script Output: ---\n{self._last_script_output}")
        if self._last_script_error:
            print(f"\n--- Script Error ---\n{self._last_script_error}")
        print('\n======================================================================')

    def create_folder(self, path):
        """Create an empty folder."""
        Path(path).mkdir(parents=True, exist_ok=True)

    def create_file(self, path, content=""):
        """Create a file with the specified content."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(content)

    def generate_input(self, input):
        for file in input:
            file = os.path.join(self.test_root, file)
            if file[-1] == os.sep:
                self.create_folder(file)
            else:
                basename = os.path.basename(file)
                frag = basename.split('_')
                if frag and len(frag) > 0:
                    frag = frag[0]
                else:
                    frag = basename
                self.create_file(file, frag)

        # always copy over the input
        path = os.path.join(self.preserve_root, 'input_tmp')
        if os.path.exists(path):
            shutil.rmtree(path)
        shutil.copytree(self.test_root, path)


    def execute(self, input, output, output_detail=False,
                defargs=None):
        output2 = set()
        for o in output:
            output2.add(os.path.join(self.test_root, o))
        self.assertTrue(len(output) == len(output2),
                         f"Duplicates found in expected output")

        self.generate_input(input)

        if not defargs:
            defargs = self.defargs

        # print('defargs', defargs, self.id())
        ret = self.run_script(defargs)

        self.validate_output(input, output2, output_detail)

        return ret

    def validate_output(self, input, output2, output_detail):
        try:
            # cycle through the actual files
            actual_files = set()
            actual_file_dirs = set()
            actual_dirs = set()
            for root, dirs, files in os.walk(self.test_root):
                # print('root', root)
                for file in files:
                    fpath = os.path.join(root, file)
                    # print('fpath', fpath)
                    with open(fpath, "r") as fp:
                        file_contents = fp.readline().strip()
                        found = os.path.join(root, file_contents)
                        actual_files.add(found)
                        # print('file', found)
                        # print('dir', os.path.dirname(found))
                        actual_file_dirs.add(os.path.dirname(found))
                for dir in dirs:
                    dpath = os.path.join(root, dir)
                    actual_dirs.add(dpath)

            # print('1', actual_dirs)
            actual_dirs = actual_dirs - actual_file_dirs
            # print('2', actual_dirs)
            new_dirs = set()
            for ad in actual_dirs:
                # ignore dirs that are in path of files
                found = False
                for af in actual_files:
                    if ad in af[:len(ad)]:
                        found = True
                        # print(ad, af)
                        break
                if not found:
                    new_dirs.add(ad + os.sep)
            actual_files.update(new_dirs)

            diff = actual_files - output2
            diff2 = output2 - actual_files
            # print(new_dirs, diff)

            output2_list = sorted(output2)
            msg = ""
            if len(diff) > 0:
                msg += f"Extra: {sorted(diff)}\n"
            if len(diff2) > 0:
                msg += f"Miss:  {sorted(diff2)}\n"
            self.assertFalse(len(msg) != 0,
                             # f"\nExpect:\n"
                             # f"{pprint.pformat(output2_list)}\n"
                             f"\nFound:\n"
                             f"{pprint.pformat(sorted(actual_files))}\n"
                             f"\n{msg}"
                             )
            has_exception = None
        except AssertionError as e:
            has_exception = e
        finally:
            if has_exception:
                self.__class__.had_exception = True
                output_detail = True
            if output_detail:
                self._print_script_output(input, output2_list)

                path = os.path.join(self.preserve_root, 'input_tmp')
                path2 = os.path.join(self.preserve_root, 'input')
                if os.path.exists(path2):
                    shutil.rmtree(path2)
                os.rename(path, path2)

                path = os.path.join(self.preserve_root, 'jsongz')
                if os.path.exists(path):
                    shutil.rmtree(path)
                shutil.copytree(self.json_root, path)

                path = os.path.join(self.preserve_root, 'output')
                if os.path.exists(path):
                    shutil.rmtree(path)
                shutil.copytree(self.test_root, path)
            elif args.show_all:
                self._print_script_output(input, output2_list)

            if has_exception:
                raise has_exception


    def test_simple(self):
        input = [
            'folder1/file1_a',
            'folder1/file1_b',
            'folder2/file2',
            ]

        output = [
            'folder1/file1',
            'folder2/file2',
            ]

        self.execute(input, output)


    def test_empty_dirs(self):
        input = [
            'folder1/',
            'folder2/',
            ]

        output = [
            'folder1/',
            'folder2/',
            ]

        self.execute(input, output)

    def test_sep_directories(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder2/file1',
            'folder2/file2',
            ]

        output = [
            'folder1/file1',
            'folder1/file2',
            ]

        self.execute(input, output)

    def test_superset(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder1/file3',
            'folder2/file1',
            'folder2/file2',
            ]

        output = [
            'folder1/file1',
            'folder1/file2',
            'folder1/file3',
            ]

        self.execute(input, output)

    def test_superset2(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder2/file1',
            'folder2/file2',
            'folder2/file3',
            ]

        output = [
            'folder2/file1',
            'folder2/file2',
            'folder2/file3',
            ]

        self.execute(input, output)

    def test_nested(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder2/file1',
            'folder2/file2',
            ]

        output = [
            'folder1/file1',
            'folder1/file2',
            ]

        self.execute(input, output)

    def test_nested2(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child2/file1',
            'folder1/child2/file2',
            'folder2/file1',
            'folder2/file2',
            'folder2/child1/file1',
            'folder2/child2/file2',
            ]

        output = [
            'folder1/file1',
            'folder1/file2',
            ]

        self.execute(input, output)

    def test_nested3(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child2/file1',
            'folder1/child2/file2',
            'folder1/child2/file3',
            'folder2/file1',
            'folder2/file2',
            'folder2/child1/file1',
            'folder2/child2/file2',
            ]

        output = [
            'folder1/file1',
            'folder1/file2',
            'folder1/child2/file3',
            ]

        self.execute(input, output)

    def test_nested_deep(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child2/file1',
            'folder1/child2/file2',
            'folder1/child2/grand1/file1',
            'folder1/child2/grand1/file2',
            'folder1/child2/grand1/greatgrand1/file1',
            'folder1/child2/grand1/greatgrand1/file2',
            'folder2/file1',
            'folder2/file2',
            'folder2/child1/file1',
            'folder2/child2/file2',
            'folder2/child2/grand1/greatgrand1/file2',
            ]

        output = [
            'folder1/file1',
            'folder1/file2',
            ]

        self.execute(input, output)

    def test_nested_deep2(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child2/file1',
            'folder1/child2/file2',
            'folder1/child2/grand1/file1',
            'folder1/child2/grand1/file2',
            'folder1/child2/grand1/greatgrand1/',
            'folder2/file1',
            'folder2/file2',
            'folder2/child1/file1',
            'folder2/child2/file2',
            'folder2/child2/grand1/greatgrand1/file2',
            ]

        output = [
            'folder1/file1',
            'folder1/file2',
            ]

        self.execute(input, output)

    def test_nested_deep3(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child2/file3',
            'folder1/file1',
            'folder1/file2',
            'folder2/child2/grand2/file1',
            'folder2/child2/grand2/file2',
            'folder2/child1/grand1/file3',
            ]

        output = [
            'folder1/file1',
            'folder1/file2',
            'folder1/child2/file3',
            ]

        self.execute(input, output)

    def test_nested_deep4(self):
        input = [
            'folder1/file1',
            'folder2/child1/grand1/file1',
            'folder2/child2/grand2/file2',
            'folder2/child3/grand2/file3',
            ]

        output = [
            'folder2/child1/grand1/file1',
            'folder2/child2/grand2/file2',
            'folder2/child3/grand2/file3',
            ]

        self.execute(input, output)

    def test_nested_deep5(self):
        input = [
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child1/file3',
            'folder2/child1/grand1/file1',
            'folder2/child2/grand2/file2',
            'folder2/child3/grand2/file3',
            ]

        output = [
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child1/file3',
            ]

        self.execute(input, output)

    def test_separate_dupes(self):
        input = [
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child1/file3',
            'folder1/child2/file4',
            'folder1/child2/file5',
            'folder1/child2/file6',
            'folder2/child2/grand1/file1',
            'folder2/child2/grand2/file2',
            'folder2/child2/grand3/file3',
            'folder2/child2/grand4/file4',
            'folder2/child2/grand5/file5',
            'folder2/file6',
            ]

        output = [
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child1/file3',
            'folder1/child2/file4',
            'folder1/child2/file5',
            'folder1/child2/file6',
            ]

        self.execute(input, output, output_detail=True)

    def test_separate_dupes2(self):
        input = [
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child1/file3',
            'folder1/child2/file4',
            'folder1/child2/file5',
            'folder1/child2/file6',
            'folder2/child1/grand1/file1',
            'folder2/child1/grand2/file2',
            'folder2/child1/grand3/file3',
            'folder2/child1/grand4/file4',
            'folder2/child2/grand5/file5',
            'folder2/file6',
            'folder2/file7',
            ]

        output = [
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder1/child1/file3',
            'folder1/child2/file4',
            'folder1/child2/file5',
            'folder2/file6',
            'folder2/file7',
            ]

        self.execute(input, output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--show_all', action='store_true', required=False)
    args = parser.parse_args()
    unittest.main(argv=['first-arg-is-ignored'])
