# object_format.py provides types and functions for working with
# requested output formats such as JSON, YAML, etc.

import enum
import json
import sys

from typing import (
    Any,
    Dict,
    Optional,
)

import yaml

# this uses a version check as opposed to a try/except because this
# form makes mypy happy and try/except doesn't.
if sys.version_info >= (3, 8):
    from typing import runtime_checkable, Protocol
else:
    from typing_extensions import runtime_checkable, Protocol


DEFAULT_JSON_INDENT: int = 2


class Format(enum.Enum):
    plain = "plain"
    json = "json"
    json_pretty = "json-pretty"
    yaml = "yaml"
    xml_pretty = "xml-pretty"
    xml = "xml"


# SimpleData is a type alias for Any unless we can determine the
# exact set of subtypes we want to support. But it is explicit!
SimpleData = Any


@runtime_checkable
class SimpleDataProvider(Protocol):
    def to_simplified(self) -> SimpleData:
        """Return a simplified representation of the current object.
        The simplified representation should be trivially serializable.
        """
        ...  # pragma: no cover


@runtime_checkable
class JSONDataProvider(Protocol):
    def to_json(self) -> Any:
        """Return a python object that can be serialized into JSON.
        This function does _not_ return a JSON string.
        """
        ...  # pragma: no cover


@runtime_checkable
class YAMLDataProvider(Protocol):
    def to_yaml(self) -> Any:
        """Return a python object that can be serialized into YAML.
        This function does _not_ return a string of YAML.
        """
        ...  # pragma: no cover


class JSONFormatter(Protocol):
    def format_json(self) -> str:
        """Return a JSON formatted representation of an object."""
        ...  # pragma: no cover


class YAMLFormatter(Protocol):
    def format_yaml(self) -> str:
        """Return a JSON formatted representation of an object."""
        ...  # pragma: no cover


class ObjectFormatAdapter:
    """A format adapater for a single object.
    Given an input object, this type will adapt the object, or a simplified
    representation of the object, to either JSON or YAML when the format_json or
    format_yaml methods are used.

    If the compatible flag is true and the object provided to the adapter has
    methods such as `to_json` and/or `to_yaml` these methods will be called in
    order to get a JSON/YAML compatible simplified representation of the
    object.

    If the above case is not satisfied and the object provided to the adapter
    has a method `to_simplified`, this method will be called to acquire a
    simplified representation of the object.

    If none of the above cases is true, the object itself will be used for
    serialization. If the object can not be safely serialized an exception will
    be raised.

    NOTE: Some code may use methods named like `to_json` to return a JSON
    string. If that is the case, you should not use that method with the
    ObjectFormatAdapter. Do not set compatible=True for objects of this type.
    """

    def __init__(
        self,
        obj: Any,
        json_indent: Optional[int] = DEFAULT_JSON_INDENT,
        compatible: bool = False,
    ) -> None:
        self.obj = obj
        self._compatible = compatible
        self.json_indent = json_indent

    def _fetch_json_data(self) -> Any:
        # if the data object provides a specific simplified representation for
        # JSON (and compatible mode is enabled) get the data via that method
        if self._compatible and isinstance(self.obj, JSONDataProvider):
            return self.obj.to_json()
        # otherwise we use our specific method `to_simplified` if it exists
        if isinstance(self.obj, SimpleDataProvider):
            return self.obj.to_simplified()
        # and fall back to the "raw" object
        return self.obj

    def format_json(self) -> str:
        """Return a JSON formatted string representing the input object."""
        return json.dumps(
            self._fetch_json_data(), indent=self.json_indent, sort_keys=True
        )

    def _fetch_yaml_data(self) -> Any:
        if self._compatible and isinstance(self.obj, YAMLDataProvider):
            return self.obj.to_yaml()
        # nothing specific to YAML was found. use the simplified representation
        # for JSON, as all valid JSON is valid YAML.
        return self._fetch_json_data()

    def format_yaml(self) -> str:
        """Return a YAML formatted string representing the input object."""
        return yaml.safe_dump(self._fetch_yaml_data())
