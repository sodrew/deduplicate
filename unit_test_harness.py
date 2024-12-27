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

    def create_file(self, path, content=""):
        """Create a file with the specified content."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(content)

    def create_folder(self, path):
        """Create an empty folder."""
        Path(path).mkdir(parents=True, exist_ok=True)

    def run_script(self, args):
        """Run the delete script with the specified arguments and store the output."""
        command = ["python3", self.script_path] + args
        result = subprocess.run(command, capture_output=True, text=True)
        self._last_script_output = result.stdout
        self._last_script_error = result.stderr
        self._last_command = command
        return result.stdout, result.stderr

    def validate_files(self, expected_files):
        """Validate that the expected files/folders exist."""
        try:
            actual_files = []
            for root, _, files in os.walk(self.test_root):
                for file in files:
                    full_path = os.path.realpath(os.path.join(root, file))
                    with open(full_path, "r") as f:
                        actual_files.append(f.readline().strip())

            actual_set = set(actual_files)
            self.assertTrue(len(actual_files) == len(expected_files),
                            f"File count mismatch: {len(actual_files)} != {len(expected_files)}")
            missing_files = expected_files - actual_set
            extra_files = actual_set - expected_files
            self.assertFalse(missing_files, f"Missing files: {missing_files}")
            self.assertFalse(extra_files, f"Unexpected extra files: {extra_files}")
        except AssertionError as e:
            # Print script output only on failure
            self._print_script_output()
            raise e

    def validate_dirs(self, expected_dirs):
        """Validate that the expected files/folders exist."""
        try:
            actual_dirs = []
            for root, dirs, files in os.walk(self.test_root):
                for dir in dirs:
                    actual_dirs.append(dir)

            actual_set = set(actual_dirs)
            self.assertTrue(len(actual_dirs) == len(expected_dirs),
                            f"Dir count mismatch: {len(actual_dirs)} != {len(expected_dirs)}")

            # enable OR functionality
            for ad in actual_dirs:
                if ad not in expected_dirs:
                    found = False
                    for d in expected_dirs:
                        if '|' in d:
                            possibledirs = d.split('|')
                            if ad in possibledirs:
                                found = True
                                break
                else:
                    found = True

                self.assertTrue(found, f"Unexpected extra dir: {ad}")

        except AssertionError as e:
            # Print script output only on failure
            self._print_script_output()
            raise e

    def _print_script_output(self):
        """Print the script output when a test fails."""
        print(f"\n--- Command ---\n{' '.join(self._last_command)}")
        print(f"\n--- Script Output ---\n{self._last_script_output}")
        if self._last_script_error:
            print(f"\n--- Script Error ---\n{self._last_script_error}")

    # def test_simple_duplicate_file(self):

    #     folder1 = os.path.join(self.test_root, "folder1")
    #     self.create_file(os.path.join(folder1, "file1.txt"), "duplicate")
    #     self.create_file(os.path.join(folder1, "file2.txt"), "duplicate")

    #     # Run the script
    #     self.run_script(["--debug", "--delete", folder1, folder1])

    #     # Validate
    #     self.validate_files(
    #         {
    #             "duplicate",
    #         }
    #     )

    # def test_simple_duplicate_file_with_unique_file(self):
    #     """Test case: duplicate folders with some unique content."""
    #     folder1 = os.path.join(self.test_root, "folder1")
    #     folder2 = os.path.join(self.test_root, "folder2")
    #     self.create_file(os.path.join(folder1, "file1.txt"), "duplicate")

    #     self.create_file(os.path.join(folder2, "file1.txt"), "duplicate")
    #     self.create_file(os.path.join(folder2, "file2.txt"), "unique")

    #     # Run the script
    #     self.run_script(["--debug", "--delete", folder1, folder2])

    #     # Validate
    #     self.validate_files(
    #         {
    #             "duplicate",
    #             "unique",
    #         }
    #     )

    # def test_nested_duplicate_files(self):
    #     folder1 = os.path.join(self.test_root, "folder1")
    #     child1 = os.path.join(folder1, "child1")
    #     folder2 = os.path.join(self.test_root, "folder2")
    #     self.create_file(os.path.join(folder1, "file1.txt"), "x")

    #     self.create_file(os.path.join(child1, "file1.txt"), "x")
    #     self.create_file(os.path.join(child1, "file2.txt"), "y")
    #     self.create_file(os.path.join(child1, "file3.txt"), "y")

    #     self.create_file(os.path.join(folder2, "file2.txt"), "z")

    #     # Run the script
    #     self.run_script(["--debug", "--delete", folder1, folder2])

    #     # Validate
    #     self.validate_files(
    #         {
    #             "x",
    #             "y",
    #             "z",
    #         }
    #     )

    # def test_empty_dirs(self):
    #     """Test case: empty folders."""
    #     folder1 = os.path.join(self.test_root, "folder1")
    #     folder2 = os.path.join(self.test_root, "folder2")
    #     self.create_folder(folder1)
    #     self.create_folder(folder2)

    #     # Run the script
    #     self.run_script(["--debug", "--delete", folder1, folder2])

    #     # Validate
    #     self.validate_files(set())

    # def test_simple_dirs(self):
    #     folder1 = os.path.join(self.test_root, "folder1")
    #     self.create_folder(folder1)
    #     folder2 = os.path.join(self.test_root, "folder2")
    #     self.create_folder(folder2)
    #     self.create_file(os.path.join(folder1, "file1.txt"), "x")
    #     self.create_file(os.path.join(folder1, "file2.txt"), "y")
    #     self.create_file(os.path.join(folder2, "file1.txt"), "x")
    #     self.create_file(os.path.join(folder2, "file2.txt"), "y")

    #     # Run the script
    #     self.run_script(["--debug", "--delete", folder1, folder2])

    #     # Validate
    #     self.validate_files(
    #         {
    #             "x",
    #             "y",
    #         }
    #     )
    #     self.validate_dirs({"folder1|folder2"})

    # def test_superset_dirs(self):
    #     folder1 = os.path.join(self.test_root, "folder1")
    #     self.create_folder(folder1)
    #     folder2 = os.path.join(self.test_root, "folder2")
    #     self.create_folder(folder2)
    #     self.create_file(os.path.join(folder1, "file1.txt"), "x")
    #     self.create_file(os.path.join(folder1, "file2.txt"), "y")
    #     self.create_file(os.path.join(folder2, "file1.txt"), "x")
    #     self.create_file(os.path.join(folder2, "file2.txt"), "y")
    #     self.create_file(os.path.join(folder2, "file3.txt"), "z")

    #     # Run the script
    #     self.run_script(["--debug", "--delete", folder1, folder2])

    #     # Validate
    #     self.validate_files(
    #         {
    #             "x",
    #             "y",
    #             "z",
    #         }
    #     )
    #     self.validate_dirs({"folder2"})

    # def test_both_superset_dirs(self):
    #     folder1 = os.path.join(self.test_root, "folder1")
    #     self.create_folder(folder1)
    #     folder2 = os.path.join(self.test_root, "folder2")
    #     self.create_folder(folder2)
    #     self.create_file(os.path.join(folder1, "file1.txt"), "x")
    #     self.create_file(os.path.join(folder1, "file2.txt"), "y")
    #     self.create_file(os.path.join(folder1, "file4.txt"), "a")
    #     self.create_file(os.path.join(folder2, "file1.txt"), "x")
    #     self.create_file(os.path.join(folder2, "file2.txt"), "y")
    #     self.create_file(os.path.join(folder2, "file3.txt"), "z")

    #     # Run the script
    #     self.run_script(["--debug", "--delete", folder1, folder2])

    #     # Validate
    #     self.validate_files(
    #         {
    #             "x",
    #             "y",
    #             "z",
    #             "a",
    #         }
    #     )
    #     self.validate_dirs({"folder1", "folder2"})

    def create_file2(self, path, content=""):
        """Create a file with the specified content."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(content)


    def execute(self, input):
        for file in input:
            file = os.path.join(self.test_root, file)
            basename = os.path.basename(file)
            frag = basename.split('_')
            if frag and len(frag) > 0:
                frag = frag[0]
            else:
                frag = basename
            self.create_file2(file, frag)

        return self.run_script(["--debug", "--delete", 'test/folder1', 'test/folder2'])

    def validate_output(self, output):
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
        actual_files.update(actual_dirs)

        diff = actual_files - output2
        diff2 = output2 - actual_files
        diff.update(diff2)
        output2_list = sorted(output2)
        actual_files_list = sorted(actual_files)
        self.assertFalse(diff,
                         f"\nExpect:{output2_list}\n"
                         f"Found: {actual_files_list}\n"
                         f"Diff:  {sorted(diff)}")

    # def test_new_simple_duplicate_file(self):
    #     input = [
    #         'folder1/file1_a',
    #         'folder1/file1_b',
    #         'folder2/file2',
    #         ]

    #     self.execute(input)

    #     self.validate_output([
    #         'folder1/file1',
    #         'folder2/file2',
    #         ])

    # def test_new_simple_duplicate_file2(self):
    #     input = [
    #         'folder1/file1',
    #         'folder1/file2',
    #         'folder2/file1',
    #         'folder2/file2',
    #         ]

    #     self.execute(input)

    #     self.validate_output([
    #         'folder2/file1',
    #         'folder2/file2',
    #         ])

    def test_new_superset(self):
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


if __name__ == "__main__":
    unittest.main()
