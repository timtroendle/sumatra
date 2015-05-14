"""
Unit tests for the sumatra.datastore module
"""

from __future__ import with_statement
import unittest
import shutil
import os
import datetime
import hashlib
from pathlib import Path
from sumatra.datastore import FileSystemDataStore, ArchivingFileSystemDataStore, get_data_store, DataKey
from sumatra.datastore.base import DataStore
from sumatra.datastore.filesystem import DataFile
from sumatra.core import TIMESTAMP_FORMAT


class TestFileSystemDataStore(unittest.TestCase):

    def setUp(self):
        self.root_dir = Path('kusehgcfscuzhfqizuchgsireugvcsi').absolute()
        if self.root_dir.exists():
            shutil.rmtree(self.root_dir.as_posix())
        assert not self.root_dir.exists()
        self.ds = FileSystemDataStore(self.root_dir)
        self.now = datetime.datetime.now()
        test_dir = self.root_dir / 'test_dir'
        test_dir.mkdir(parents=True)
        self.test_files = set([self.root_dir / 'test_file1',
                               self.root_dir / 'test_file2',
                               test_dir / 'test_file3'])
        self.test_data = b'licgsnireugcsenrigucsic\ncrgqgjch,kgch'
        for path in self.test_files:
            with path.open('wb') as f:
                f.write(self.test_data)

    def tearDown(self):
        shutil.rmtree(self.root_dir.as_posix())
        del self.ds

    def test__init__should_create_root_if_it_doesnt_exist(self):
        self.assert_(self.root_dir.exists())

    def test__str__should_return_root(self):
        self.assertEqual(str(self.ds), self.root_dir.as_posix())

    def test__get_state__should_return_dict_containing_root(self):
        self.assertEqual(self.ds.__getstate__(), {'root': self.root_dir.as_posix()})

    def test__find_new_data__should_return_list_of_keys_matching_new_files(self):
        self.assertEqual(set(key.path for key in self.ds.find_new_data(self.now)),
                         self.test_files)

    def test__find_new_data_with_future_timestamp__should_return_empty_list(self):
        tomorrow = self.now + datetime.timedelta(1)
        self.assertEqual(set(self.ds.find_new_data(tomorrow)),
                         set([]))

    def test__get_content__should_return_short_file_content(self):
        digest = hashlib.sha1(self.test_data).hexdigest()
        key = DataKey('test_file1', digest)
        content = self.ds.get_content(key)
        self.assertEqual(content, self.test_data)

    def test__get_content__should_truncate_long_files(self):
        digest = hashlib.sha1(self.test_data).hexdigest()
        key = DataKey('test_file1', digest)
        content = self.ds.get_content(key, max_length=10)
        self.assertEqual(content, self.test_data[:10])

    def test__delete__should_remove_files(self):
        file1 = self.root_dir / 'test_file1'
        assert file1.exists()
        digest = hashlib.sha1(self.test_data).hexdigest()
        keys = [DataKey(path, digest) for path in self.test_files]
        self.ds.delete(*keys)
        self.assert_(not file1.exists())


class TestArchivingFileSystemDataStore(unittest.TestCase):

    def setUp(self):
        self.root_dir = os.path.abspath('zusehgcfscuzhfqizuchgsireugvcsi')
        self.archive_dir = os.path.abspath("test_archive")
        # remove root and archive directories
        for path in (self.root_dir, self.archive_dir):
            if os.path.exists(path):
                shutil.rmtree(path)
            assert not os.path.exists(path)

        self.ds = ArchivingFileSystemDataStore(self.root_dir, self.archive_dir)
        self.now = datetime.datetime.now()
        os.mkdir(os.path.join(self.root_dir, 'test_dir'))
        self.test_files = set(['test_file1', 'test_file2', 'test_dir/test_file3'])
        self.test_data = b'licgsnireugcsenrigucsic\ncrgqgjch,kgch'
        for filename in self.test_files:
            with open(os.path.join(self.root_dir, filename), 'wb') as f:
                f.write(self.test_data)

    def tearDown(self):
        for path in (self.root_dir, self.archive_dir):
            if os.path.exists(path):
                shutil.rmtree(path)
        del self.ds

    def test__init__should_create_root_and_archive_if_they_dont_exist(self):
        self.assert_(os.path.exists(self.root_dir))

    def test__str__should_return_root(self):
        self.assertEqual(str(self.ds), self.root_dir)

    def test__get_state__should_return_dict_containing_root_and_archive_store(self):
        self.assertEqual(self.ds.__getstate__(),
                         {'root': self.root_dir, 'archive': self.archive_dir})

    def test__find_new_data__should_return_list_of_keys_matching_new_files(self):
        self.assertEqual(set("/".join(key.path.split("/")[1:]) for key in self.ds.find_new_data(self.now)),
                         self.test_files)

    def test__find_new_data_with_future_timestamp__should_return_empty_list(self):
        tomorrow = self.now + datetime.timedelta(1)
        self.assertEqual(set(self.ds.find_new_data(tomorrow)),
                         set([]))

    def test__archive__should_create_a_tarball(self):
        self.ds._archive('test', self.test_files)
        self.assert_(os.path.exists(os.path.join(self.archive_dir, 'test.tar.gz')))
        self.assert_(not os.path.exists(os.path.join(self.root_dir, 'test.tar.gz')))

    def test__archive__should_delete_original_files_if_requested(self):
        assert os.path.exists(os.path.join(self.root_dir, 'test_file1'))
        self.ds._archive('test', self.test_files, delete_originals=True)
        self.assert_(not os.path.exists(os.path.join(self.root_dir, 'test_file1')))

    def test__get_content__should_return_short_file_content(self):
        self.ds.find_new_data(self.now)
        digest = hashlib.sha1(self.test_data).hexdigest()
        key = DataKey('%s/test_file1' % self.now.strftime(TIMESTAMP_FORMAT), digest)
        content = self.ds.get_content(key)
        self.assertEqual(content, self.test_data)

    def test__get_content__should_truncate_long_files(self):
        self.ds.find_new_data(self.now)
        digest = hashlib.sha1(self.test_data).hexdigest()
        key = DataKey('%s/test_file1' % self.now.strftime(TIMESTAMP_FORMAT), digest)
        content = self.ds.get_content(key, max_length=10)
        self.assertEqual(content, self.test_data[:10])


class MockDataStore(object):
        root = os.getcwd()

class TestDataFile(unittest.TestCase):

    def setUp(self):
        self.test_file = Path('test_file1')
        self.test_data = b'licgsnireugcsenrigucsic\ncrgqgjch,kgch'
        with self.test_file.open('wb') as f:
            f.write(self.test_data)
        self.data_file = DataFile(self.test_file, MockDataStore())

    def tearDown(self):
        self.test_file.unlink()

    def test_init(self):
        pass

    def test_str__should_return_path(self):
        self.assertEqual(str(self.data_file), self.data_file.path.as_posix())

    def test_content(self):
        self.assertEqual(self.data_file.content, self.test_data)

    def test_sorted_content(self):
        self.assertEqual(self.data_file.sorted_content,
                         b'crgqgjch,kgch\nlicgsnireugcsenrigucsic')
        self.test_file.with_name(self.test_file.name + ',sorted').unlink()

    def test_eq(self):
        same_data_file = DataFile(self.test_file, MockDataStore())
        self.assertEqual(self.data_file, same_data_file)
        test_file2 = Path('test_file2')
        with test_file2.open('wb') as f:
            f.write(self.data_file.sorted_content)
        sorted_data_file = DataFile(test_file2, MockDataStore())
        self.assertEqual(self.data_file, sorted_data_file)
        test_file2.unlink()
        test_file2.with_name(test_file2.name + ',sorted').unlink()

    def test_ne(self):
        test_file3 = Path('test_file3')
        with test_file3.open("wb") as f:
            f.write(b"ucyfgnauygfcangf\niauff\ngiurg\n")
        other_data_file = DataFile(test_file3, MockDataStore())
        self.assertNotEqual(self.data_file, other_data_file)
        test_file3.unlink()


class TestModuleFunctions(unittest.TestCase):

    def test__get_data_store__should_return_DataStore_object(self):
        root_dir = 'kuqeyfgneuqygvn'
        ds = FileSystemDataStore(root_dir)
        self.assert_(isinstance(get_data_store('FileSystemDataStore', {'root': root_dir}), DataStore))
        if os.path.exists(root_dir):
            os.rmdir(root_dir)

    def test__get_data_store__should_raise_KeyError_if_wrong_type(self):
        self.assertRaises(KeyError, get_data_store, 'FooDataStore', {})

    def test__get_data_store__should_raise_Exception_if_wrong_parameters(self):
        self.assertRaises(TypeError, get_data_store, 'FileSystemDataStore', {'foo': 'kcjghnqlecg'})
        self.assertRaises((TypeError, AttributeError), get_data_store, 'FileSystemDataStore', {'root': 42})


if __name__ == '__main__':
    unittest.main()
