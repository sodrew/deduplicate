import os
import shutil
import subprocess
import unittest
from pathlib import Path


class DuplicateDeletionTest(unittest.TestCase):
    script_path = "deduplicate.py"  # Replace with the actual path to your script
    test_root = "test"  # Set a fixed directory for tests

    def setUp(self):
        """Set up the test root directory."""
        if os.path.exists(self.test_root):
            shutil.rmtree(self.test_root)
        os.makedirs(self.test_root)

    def tearDown(self):
        """Clean up the test root directory."""
        if os.path.exists(self.test_root):
            shutil.rmtree(self.test_root)

    def run_script(self, args):
        """Run the delete script with the specified arguments and store the output."""
        command = ["python3", self.script_path] + args
        result = subprocess.run(command, capture_output=True, text=True)
        self._last_script_output = result.stdout
        self._last_script_error = result.stderr
        self._last_command = command
        return result.stdout, result.stderr

    def func(self):
        return self.id().split('.')[2]

    def _print_script_output(self):
        """Print the script output when a test fails."""
        print('\n======================================================================')
        # print(f"--- Command ---\n{' '.join(self._last_command)}")
        print(f"--- Script Output: {self.func()} ---\n{self._last_script_output}")
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


    def execute(self, input):
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

        return self.run_script(["--debug", "--delete", 'test/folder1', 'test/folder2'])

    def validate_output(self, output):
        try:
            # add the test root
            output2 = set()
            for o in output:
                output2.add(os.path.join(self.test_root, o))
            self.assertTrue(len(output) == len(output2),
                             f"Duplicates found in expected output")

            # cycle through the actual files
            actual_files = set()
            actual_file_dirs = set()
            actual_dirs = set()
            for root, dirs, files in os.walk(self.test_root):
                for file in files:
                    fpath = os.path.join(root, file)
                    with open(fpath, "r") as fp:
                        file_contents = fp.readline().strip()
                        found = os.path.join(root, file_contents)
                        actual_files.add(found)
                        actual_file_dirs.add(os.path.dirname(found))
                for dir in dirs:
                    dpath = os.path.join(root, dir)
                    actual_dirs.add(dpath)

            actual_dirs = actual_dirs - actual_file_dirs
            new_dirs = set()
            for ad in actual_dirs:
                new_dirs.add(ad + os.sep)
            actual_files.update(new_dirs)

            diff = actual_files - output2
            diff2 = output2 - actual_files
            # print(new_dirs, diff)

            output2_list = sorted(output2)
            output = ""
            if len(diff) > 0:
                output = f"Extra: {sorted(diff)}\n"
            if len(diff2) > 0:
                output += f"Miss:  {sorted(diff2)}\n"
            self.assertFalse(len(output) != 0,
                             f"\nExpect:{output2_list}\n{output}"
                             )
        except AssertionError as e:
            # Print script output only on failure
            self._print_script_output()
            raise e


    def test_simple(self):
        input = [
            'folder1/file1_a',
            'folder1/file1_b',
            'folder2/file2',
            ]

        self.execute(input)

        self.validate_output([
            'folder1/file1',
            'folder2/file2',
            ])


    def test_empty_dirs(self):
        input = [
            'folder1/',
            'folder2/',
            ]

        self.execute(input)

        self.validate_output([
            'folder1/',
            'folder2/',
            ])

    def test_sep_directories(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder2/file1',
            'folder2/file2',
            ]

        self.execute(input)

        self.validate_output([
            'folder1/file1',
            'folder1/file2',
            ])


    def test_superset(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder1/file3',
            'folder2/file1',
            'folder2/file2',
            ]

        self.execute(input)

        self.validate_output([
            'folder1/file1',
            'folder1/file2',
            'folder1/file3',
            ])

    def test_superset2(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder2/file1',
            'folder2/file2',
            'folder2/file3',
            ]

        self.execute(input)

        self.validate_output([
            'folder2/file1',
            'folder2/file2',
            'folder2/file3',
            ])

    def test_nested(self):
        input = [
            'folder1/file1',
            'folder1/file2',
            'folder1/child1/file1',
            'folder1/child1/file2',
            'folder2/file1',
            'folder2/file2',
            ]

        self.execute(input)

        self.validate_output([
            'folder1/file1',
            'folder1/file2',
            ])

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

        self.execute(input)

        self.validate_output([
            'folder1/file1',
            'folder1/file2',
            ])

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

        self.execute(input)

        self.validate_output([
            'folder1/file1',
            'folder1/file2',
            'folder1/child2/file3',
            ])

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

        self.execute(input)

        self.validate_output([
            'folder1/file1',
            'folder1/file2',
            ])


if __name__ == "__main__":
    unittest.main()
