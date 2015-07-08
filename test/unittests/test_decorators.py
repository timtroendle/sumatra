"""
Unit tests for the sumatra.decorator module
"""

import unittest
import shutil
import tempfile
from pathlib import Path
from textwrap import dedent
import os
import git
import sys

from sumatra.decorators import capture
from sumatra import commands as smtcmd
from sumatra.parameters import SimpleParameterSet


class TestCaptureDecorator(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix='sumatra-test-')
        self.cwd_before_test = os.getcwd()
        os.chdir(self.dir)
        self.repo = git.Repo.init(self.dir)
        with open((Path(self.dir) / 'params.yaml').as_posix(), "w") as f:
            f.writelines(["testing: 10"])
        with open((Path(self.dir) / 'main.py').as_posix(), "w") as f:
            f.writelines(dedent("""
                import sys
                #from sumatra.parameters import build_parameters
                from sumatra.decorators import capture

                @capture
                def main(parameters):
                    print "Hi"

                #parameter_file = sys.argv[1]
                #parameters = build_parameters(parameter_file)
                #main(parameters)
                """))
        self.repo.index.add(["params.yaml", "main.py"])
        self.repo.index.commit("initial commit")
        os.chdir(self.dir)
        sys.path.append(self.dir)
        sys.modules['__main__'].__file__ = (Path(self.dir) / 'main.py').as_posix()
        smtcmd.init(["test_project"])

    def tearDown(self):
        os.chdir(self.cwd_before_test)
        shutil.rmtree(self.dir)

    def test_captures_record(self):
        print(os.listdir('.'))
        import main
        #parameter_file = sys.argv[1]
        #parameters = build_parameters(parameter_file)
        main.main(SimpleParameterSet(""))
        #main.main(None)
        assert True
