"""
The parameters module handles different parameter file formats.

The original idea was that all parameter files will be converted to a single
internal parameter format, the NeuroTools ParameterSet class. This will allow
fancy searching/comparisons based on parameters. However, we don't do this at
the moment, the only methods that are used are `update()` and `save()`

Classes
-------

NTParameterSet:
    handles parameter files in the NeuroTools parameter set format, based on
    nested dictionaries.
SimpleParameterSet:
    handles parameter files in a simple "name = value" format, with no nesting
    or grouping.
ConfigParserParameterSet
    handles parameter files in traditional config file format, as parsed by the
    standard Python :mod:`ConfigParser` module.
JSONParameterSet
    handles parameter files in JSON format
YAMLParameterSet
    handles parameter files in YAML format


:copyright: Copyright 2006-2014 by the Sumatra team, see doc/authors.txt
:license: CeCILL, see LICENSE for details.
"""

from __future__ import with_statement, absolute_import
from __future__ import unicode_literals
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import object
import os.path
import shutil
import abc
import re
from itertools import filterfalse
from pathlib import Path
try:
    from StringIO import StringIO # this is necessary because Python2-ConfigParser can't handle unicode
except ImportError: # Python 3
    from io import StringIO
from future.utils import with_metaclass
from configparser import SafeConfigParser, MissingSectionHeaderError, NoOptionError
import json
try:
    import yaml
    yaml_loaded = True
except ImportError:
    yaml_loaded = False
import parameters
from .core import registry

POP_NONE = "eiutbocqnluiegnclqiuetyvbietcbdgsfzpq"

class ParameterSet(with_metaclass(abc.ABCMeta, object)):
    required_attributes = ("update", "save")
    list_pattern = re.compile(r'^\s*\[.*\]\s*$')
    tuple_pattern = re.compile(r'^\s*\(.*\)\s*$')
    casts = (yaml.load, ) ## good behavior for all bool, at cost of dependency

    def _new_param_check(self, name, value):
        try:
            self.values[name]
        except KeyError:
            raise ValueError("")


    def parse_command_line_parameter(self, p):
        """Parse command line parameter

        Uses ParameterSet format-specific type parsers stored in self.casts

        Raises ValueError with args tuple containing name, value if parameter name
        isn't in self.values.
        """
        pos = p.find('=')
        if pos == -1:
            raise Exception("Not a valid command line parameter. String must be of form 'name=value'")
        name = p[:pos]
        value = p[pos + 1:]

        if self.list_pattern.match(value) or self.tuple_pattern.match(value):
            value = eval(value)
        else:
            for cast in self.casts:
                try:
                    value = cast(value)
                    break
                except ValueError:
                    pass
        try:
            self._new_param_check(name, value)
        except ValueError as v:
            raise ValueError(str(v), name,  value)
            ## attempt to pass undefined param -- let commands.py deal with

        return {name: value}


class YAMLParameterSet(ParameterSet):
    """
    Handles parameter files in YAML format, as parsed by the
    PyYAML module
    """
    name = ".yaml"

    def __init__(self, initialiser):
        """
        Create a new parameter set from a file or string.
        """
        if yaml_loaded:
            try:
                if os.path.exists(initialiser):
                    with open(initialiser) as fid:
                        self.values = yaml.load(fid)
                    self.source_file = initialiser
                else:
                    if initialiser:
                        self.values = yaml.load(initialiser)
                    else:
                        self.values = {}
            except yaml.YAMLError:
                raise SyntaxError("Misformatted YAML file")
            if not isinstance(self.values, dict):
                raise SyntaxError("YAML file cannot be represented as a dict")
        else:
            raise ImportError("Cannot import PyYAML module")

    def __str__(self):
        return self.pretty()

    def __getitem__(self, name):
        return self.values[name]

    def __eq__(self, other):
        return self.as_dict() == other.as_dict()

    def __ne__(self, other):
        return not self.__eq__(other)

    def pretty(self, expand_urls=False):
        """
        Return a string representation of the parameter set, suitable for
        creating a new, identical parameter set.

        expand_urls is present for compatibility with NTParameterSet, and is
                    not used.
        """

        output = yaml.dump(self.values, indent=4)
        return output

    def as_dict(self):
        return self.values

    def save(self, filename, add_extension=False):
        if add_extension:
            filename += ".yaml"
        with open(filename, "w") as f:
            yaml.dump(self.values, f)
        return filename

    def update(self, E, **F):
        self.values.update(E, **F)
    update.__doc__ = dict.update.__doc__

    def pop(self, key, d=None):
        if key in self.values:
            return self.values.pop(key)
        else:
            return d


class NTParameterSet(parameters.ParameterSet, ParameterSet):
    # just a re-name, to clarify things
    name = ".ntparameterset"


class SimpleParameterSet(ParameterSet):
    """
    Handles parameter files in a simple "name = value" format, with no nesting or grouping.
    """
    name = ".simpleparameterset"
    casts = (int, float)

    COMMENT_CHAR = u"#"

    def __init__(self, initialiser):
        """
        Create a new parameter set from a file or string. In both cases,
        parameters should be separated by newlines.
        """
        self.values = {}
        self.types = {}
        self.comments = {}
        if isinstance(initialiser, dict):
            for name, value in initialiser.items():
                self._add_or_update_parameter(name=name, value=value)
        elif SimpleParameterSet._is_valid_file(initialiser):
            with open(initialiser) as f:
                for line in filterfalse(SimpleParameterSet._empty_or_comment, f.readlines()):
                    name, value, comment = self._parse_parameter_from_line(line)
                    self._add_or_update_parameter(name=name, value=value, comment=comment)
            self.source_file = initialiser
        else:
            try:
                for line in filterfalse(SimpleParameterSet._empty_or_comment, initialiser.split(u"\n")):
                    name, value, comment = self._parse_parameter_from_line(line)
                    self._add_or_update_parameter(name=name, value=value, comment=comment)
            except (AttributeError, TypeError):
                raise TypeError("Parameter set initialiser must be a filename, string or dict.")

    @staticmethod
    def _is_valid_file(path):
        try:
            path = Path(path)
            return path.exists() and path.is_file()
        except TypeError:
            return False

    @classmethod
    def _empty_or_comment(cls, line):
        line = str(line.strip())
        return len(line) == 0 or line.startswith(cls.COMMENT_CHAR)

    def _parse_parameter_from_line(self, line):
        line = str(line.strip())
        if u"=" in line:
            parts = line.split(u"=")
            name = parts[0].strip()
            value = "=".join(parts[1:])
            try:
                if SimpleParameterSet._value_represents_string(value):
                    value = str(eval(value))
                else:
                    value = eval(value)
            except NameError:
                value = str(value)
            except TypeError as err:  # e.g. null bytes
                raise SyntaxError("File is not a valid simple parameter file. %s" % err)
            if self.COMMENT_CHAR in line:
                comment = self.COMMENT_CHAR.join(line.split(self.COMMENT_CHAR)[1:])  # this fails if the value is a string containing COMMENT_CHAR
            else:
                comment = None
        else:
            raise SyntaxError("File is not a valid simple parameter file. This line caused the error: %s" % line)
        return name, value, comment

    @staticmethod
    def _value_represents_string(value):
        single_quote = u"'"
        double_quote = u'"'
        stripped = value.strip()
        return (stripped.startswith(single_quote) and stripped.endswith(single_quote)) \
            or (stripped.startswith(double_quote) and stripped.endswith(double_quote))

    def _add_or_update_parameter(self, name, value, comment=None):
        if not isinstance(value, (int, float, str, list)):
            raise TypeError("value must be a numeric value or a string")
        self.values[name] = value
        self.types[name] = type(value)
        if comment is not None:
            self.comments[name] = comment

    def __str__(self):
        return self.pretty()

    def __getitem__(self, name):
        return self.values[name]

    def __eq__(self, other):
        return ((self.values == other.values) and (self.types == other.types))

    def __ne__(self, other):
        return not self.__eq__(other)

    def pop(self, k, d=POP_NONE):
        if k in self.values:
            v = self.values.pop(k)
            self.types.pop(k)
            self.comments.pop(k, None)
            return v
        elif d is not POP_NONE:
            return d
        else:
            raise KeyError("%s not found" % k)

    def pretty(self, expand_urls=False):
        """
        Return a string representation of the parameter set, suitable for
        creating a new, identical parameter set.

        expand_urls is present for compatibility with NTParameterSet, and is
                    not used.
        """
        output = []
        for name, value in self.values.items():
            value_type = self.types[name]
            if issubclass(value_type, str):
                output.append('%s = "%s"' % (name, value))
            else:
                output.append('%s = %s' % (name, value))
            if name in self.comments:
                output[-1] += ' #%s' % self.comments[name]
        return "\n".join(output)

    def as_dict(self):
        return self.values.copy()

    def save(self, filename, add_extension=False):
        if add_extension:
            filename += ".param"
        if os.path.exists(filename):
            shutil.copy(filename, filename + ".orig")
        with open(filename, 'w') as f:
            f.write(self.pretty())
        return filename

    def update(self, E, **F):
        if hasattr(E, "items"):
            for name, value in E.items():
                self._add_or_update_parameter(name, value)
        else:
            for name, value in E:
                self._add_or_update_parameter(name, value)
        for name, value in F.items():
            self._add_or_update_parameter(name, value)
    update.__doc__ = dict.update.__doc__


class ConfigParserParameterSet(SafeConfigParser, ParameterSet):
    """
    Handles parameter files in traditional config file format, as parsed by the
    standard Python ConfigParser module. Note that this format does not
    distinguish numbers from string representations of those numbers, so all
    parameter values are treated as strings.
    """
    name = ".cfg"
    casts = (str, )

    def __init__(self, initialiser):
        """
        Create a new parameter set from a file or string.
        """
        SafeConfigParser.__init__(self)
        try:
            if os.path.exists(initialiser):
                self.read(initialiser)
                self.source_file = initialiser
            else:
                input = StringIO(str(initialiser))  # configparser has some problems with unicode. Using str() is a crude, and probably partial fix.
                input.seek(0)
                self.readfp(input)
        except MissingSectionHeaderError:
            raise SyntaxError("Initialiser contains no section headers")

    def __str__(self):
        return self.pretty()

    def __getitem__(self, name):
        if "." in name:
            section, option = name.split(".")
            return self.get(section, option)
        elif self.has_option("sumatra", name):
            return self.get("sumatra", name)
        else:
            return dict(self.items(name))

    def __eq__(self, other):
        return self.as_dict() == other.as_dict()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __deepcopy__(self, memo):
        # deepcopy of a SafeConfigParser fails under Python 2.7, so we
        # implement this simple version which avoids copying SRE_Pattern objects
        return ConfigParserParameterSet(self.pretty())

    def pretty(self, expand_urls=False):
        """
        Return a string representation of the parameter set, suitable for
        creating a new, identical parameter set.

        expand_urls is present for compatibility with NTParameterSet, and is
                    not used.
        """
        output = StringIO()
        self.write(output)
        return output.getvalue()

    def as_dict(self):
        D = {}
        for section in self.sections():
            D[section] = dict(self.items(section))
        return D

    def save(self, filename, add_extension=False):
        if add_extension:
            filename += ".cfg"
        with open(filename, "w") as f:
            self.write(f)
        return filename

    def update(self, E, **F):
        def _update(name, value):
            if "." in name:
                section, option = name.split(".")
            else:
                section = "sumatra"  # used for extra parameters added by sumatra
                option = name
            if not self.has_section(section):
                self.add_section(section)
            if not isinstance(value, str):
                value = str(value)
            self.set(section, option, value)
        if hasattr(E, "items"):
            for name, value in E.items():
                _update(name, value)
        else:
            for name, value in E:
                _update(name, value)
        for name, value in F.items():
            _update(name, value)
    update.__doc__ = dict.update.__doc__

    def pop(self, name, d=POP_NONE):
        if "." in name:
            section, option = name.split(".")
            try:
                value = self.get(section, option)
                self.remove_option(section, option)
            except NoOptionError:
                if d is not POP_NONE:
                    value = d
                else:
                    raise KeyError('name')
            return value
        elif self.has_option("sumatra", name):
            value = self.get("sumatra", name)
            self.remove_option("sumatra", name)
        # should we allow popping an entire section?
        else:
            value = d
        return value

    def _new_param_check(self, name, value):
        raise ValueError("Config file: parameter name checking not implemented!")

class JSONParameterSet(ParameterSet):
    """
    Handles parameter files in JSON format, as parsed by the
    standard Python json module.
    """
    name = ".json"
    casts = (json.loads, )

    def __init__(self, initialiser):
        """
        Create a new parameter set from a file or string.
        """
        try:
            if os.path.exists(initialiser):
                with open(initialiser) as fid:
                    self.values = json.load(fid)
                self.source_file = initialiser
            else:
                if initialiser:
                    self.values = json.loads(initialiser)
                else:
                    self.values = {}
        except ValueError:
            raise SyntaxError("Misformatted JSON file")

    def __str__(self):
        return self.pretty()

    def __getitem__(self, name):
        return self.values[name]

    def __eq__(self, other):
        return self.as_dict() == other.as_dict()

    def __ne__(self, other):
        return not self.__eq__(other)

    def pretty(self, expand_urls=False):
        """
        Return a string representation of the parameter set, suitable for
        creating a new, identical parameter set.

        expand_urls is present for compatibility with NTParameterSet, and is
                    not used.
        """

        output = json.dumps(self.values, sort_keys=True, indent=4)
        return output

    def as_dict(self):
        return self.values

    def save(self, filename, add_extension=False):
        if add_extension:
            filename += ".json"
        with open(filename, "w") as f:
            json.dump(self.values, f)
        return filename

    def update(self, E, **F):
        self.values.update(E, **F)
    update.__doc__ = dict.update.__doc__

    def pop(self, key, d=None):
        if key in self.values:
            return self.values.pop(key)
        else:
            return d


registry.add_component_type(ParameterSet)

registry.register(JSONParameterSet)
if yaml_loaded:
    registry.register(YAMLParameterSet)
registry.register(NTParameterSet)
registry.register(ConfigParserParameterSet)
registry.register(SimpleParameterSet)


def build_parameters(filename):
    body, ext = os.path.splitext(filename)
    parameters = None
    extension_map = registry.components[ParameterSet]
    if ext in extension_map:
        parameter_set_class = extension_map[ext]
        parameters = parameter_set_class(filename)
    else:
        for parameter_set_class in extension_map.values():
            try:
                parameters = parameter_set_class(filename)
            except (SyntaxError, NameError, UnicodeDecodeError):
                pass
    return parameters
