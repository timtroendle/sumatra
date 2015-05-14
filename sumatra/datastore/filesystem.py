"""
Datastore based on files written to and retrieved from a local filesystem.


:copyright: Copyright 2006-2014 by the Sumatra team, see doc/authors.txt
:license: CeCILL, see LICENSE for details.
"""

import os
import datetime
import mimetypes
from subprocess import Popen
import warnings
from pathlib import Path
from ..core import registry
from .base import DataStore, DataItem, IGNORE_DIGEST


class DataFile(DataItem):
    """A file-like object, that represents a file in a local filesystem."""
    # current implementation just for real files

    def __init__(self, path, store):
        self.path = Path(path)
        self.full_path = Path(store.root) / self.path
        if self.full_path.exists():
            self.size = self.full_path.stat().st_size
        else:
            raise IOError("File %s does not exist" % self.full_path)
        self.name = self.full_path.name
        self.extension = self.full_path.suffix
        self.mimetype, self.encoding = mimetypes.guess_type(self.full_path.as_posix())

    def get_content(self, max_length=None):
        with self.full_path.open('rb') as f:
            if max_length:
                content = f.read(max_length)
            else:
                content = f.read()
        return content
    content = property(fget=get_content)

    @property
    def sorted_content(self):
        sorted_path = "%s,sorted" % self.full_path
        if not os.path.exists(sorted_path):
            cmd = "sort %s > %s" % (self.full_path, sorted_path)
            job = Popen(cmd, shell=True)
            job.wait()
        f = open(sorted_path, 'rb')
        content = f.read()
        f.close()
        if len(content) != self.size:  # sort adds a \n if the file does not end with one
            assert len(content) == self.size + 1
            content = content[:-1]
        return content

    # should probably override save_copy() from base class,
    # as a filesystem copy will be much faster


class FileSystemDataStore(DataStore):
    """
    Represents a locally-mounted filesystem. The root of the data store will
    generally be a subdirectory of the real filesystem.
    """
    data_item_class = DataFile

    def __init__(self, root):
        self.root = Path(root).absolute()

    def __str__(self):
        return self.root.as_posix()

    def __getstate__(self):
        return {'root': self.root.as_posix()}

    def __setstate__(self, state):
        self.__init__(**state)

    @property
    def root(self):
        return self._root

    @root.setter
    def root(self, value):
        self._root = Path(value)
        if not self._root.exists():
            try:
                self._root.mkdir(parents=True)
            except OSError:
                pass  # should perhaps emit warning

    def _find_new_data_files(self, timestamp, ignoredirs=[".smt", ".hg", ".svn", ".git", ".bzr"]):
        """Finds newly created/changed files in dataroot."""
        # The timestamp-based approach creates problems when running several
        # experiments at once, since datafiles created by other experiments may
        # be mixed in with this one.
        # For this reason, concurrently running computations should each use
        # their own datastore, each with a different root.
        timestamp = timestamp.replace(microsecond=0)  # Round down to the nearest second
        # Find and add new data files
        new_files = []
        for root, dirs, files in os.walk(self.root.as_posix()):
            for igdir in ignoredirs:
                if igdir in dirs:
                    dirs.remove(igdir)
            for file in files:
                full_path = Path(root) / file
                last_modified = datetime.datetime.fromtimestamp(full_path.stat().st_mtime)
                if last_modified >= timestamp:
                    new_files.append(full_path)
        return new_files

    def find_new_data(self, timestamp):
        """Finds newly created/changed data items"""
        return [DataFile(path, self).generate_key()
                for path in self._find_new_data_files(timestamp)]

    def get_data_item(self, key):
        """
        Return the file that matches the given key.
        """
        try:
            df = self.data_item_class(key.path, self)
        except IOError:
            raise KeyError("File %s does not exist." % key.path)
        if key.digest != IGNORE_DIGEST and df.digest != key.digest:
            raise KeyError("Digests do not match.")  # add info about file sizes?
        return df

    def delete(self, *keys):
        """
        Delete the files corresponding to the given keys.
        """
        for key in keys:
            try:
                data_item = self.get_data_item(key)
            except KeyError:
                warnings.warn("Tried to delete %s, but it did not exist." % key)
            else:
                Path(data_item.full_path).unlink()

    def contains_path(self, path):
        return (self.root / path).is_file()


registry.register(FileSystemDataStore)
