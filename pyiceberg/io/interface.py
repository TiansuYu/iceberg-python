from __future__ import annotations

import os
from abc import ABC, abstractmethod
from functools import cache
from io import SEEK_SET
from types import TracebackType
from typing import (
    Optional,
    Protocol,
    Type,
    Union,
    runtime_checkable, Tuple,
)
from urllib.parse import urlparse

from fsspec import AbstractFileSystem
from pyarrow._fs import FileSystem

from pyiceberg.typedef import Properties, EMPTY_DICT


@runtime_checkable
class InputStream(Protocol):
    """A protocol for the file-like object returned by InputFile.open(...).

    This outlines the minimally required methods for a seekable input stream returned from an InputFile
    implementation's `open(...)` method. These methods are a subset of IOBase/RawIOBase.
    """

    @abstractmethod
    def read(self, size: int = 0) -> bytes: ...

    @abstractmethod
    def seek(self, offset: int, whence: int = SEEK_SET) -> int: ...

    @abstractmethod
    def tell(self) -> int: ...

    @abstractmethod
    def close(self) -> None: ...

    def __enter__(self) -> InputStream:
        """Provide setup when opening an InputStream using a 'with' statement."""

    @abstractmethod
    def __exit__(
            self, exctype: Optional[Type[BaseException]], excinst: Optional[BaseException],
            exctb: Optional[TracebackType]
    ) -> None:
        """Perform cleanup when exiting the scope of a 'with' statement."""


@runtime_checkable
class OutputStream(Protocol):  # pragma: no cover
    """A protocol for the file-like object returned by OutputFile.create(...).

    This outlines the minimally required methods for a writable output stream returned from an OutputFile
    implementation's `create(...)` method. These methods are a subset of IOBase/RawIOBase.
    """

    @abstractmethod
    def write(self, b: bytes) -> int: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def __enter__(self) -> OutputStream:
        """Provide setup when opening an OutputStream using a 'with' statement."""

    @abstractmethod
    def __exit__(
            self, exctype: Optional[Type[BaseException]], excinst: Optional[BaseException],
            exctb: Optional[TracebackType]
    ) -> None:
        """Perform cleanup when exiting the scope of a 'with' statement."""


class InputFile(ABC):
    """A base class for InputFile implementations.

    Args:
        location (str): A URI or a path to a local file.

    Attributes:
        location (str): The URI or path to a local file for an InputFile instance.
        exists (bool): Whether the file exists or not.
    """

    def __init__(self, location: str):
        self._location = location

    @abstractmethod
    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""

    @property
    def location(self) -> str:
        """The fully-qualified location of the input file."""
        return self._location

    @abstractmethod
    def exists(self) -> bool:
        """Check whether the location exists.

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error.
        """

    @abstractmethod
    def open(self, seekable: bool = True) -> InputStream:
        """Return an object that matches the InputStream protocol.

        Args:
            seekable: If the stream should support seek, or if it is consumed sequential.

        Returns:
            InputStream: An object that matches the InputStream protocol.

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error.
            FileNotFoundError: If the file at self.location does not exist.
        """


class OutputFile(ABC):
    """A base class for OutputFile implementations.

    Args:
        location (str): A URI or a path to a local file.

    Attributes:
        location (str): The URI or path to a local file for an OutputFile instance.
        exists (bool): Whether the file exists or not.
    """

    def __init__(self, location: str):
        self._location = location

    @abstractmethod
    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""

    @property
    def location(self) -> str:
        """The fully-qualified location of the output file."""
        return self._location

    @abstractmethod
    def exists(self) -> bool:
        """Check whether the location exists.

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error.
        """

    @abstractmethod
    def to_input_file(self) -> InputFile:
        """Return an InputFile for the location of this output file."""

    @abstractmethod
    def create(self, overwrite: bool = False) -> OutputStream:
        """Return an object that matches the OutputStream protocol.

        Args:
            overwrite (bool): If the file already exists at `self.location`
                and `overwrite` is False a FileExistsError should be raised.

        Returns:
            OutputStream: An object that matches the OutputStream protocol.

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error.
            FileExistsError: If the file at self.location already exists and `overwrite=False`.
        """


GenericFileSystem = Union[AbstractFileSystem, FileSystem]


class FileIO(ABC):
    """A base class for FileIO implementations."""

    properties: Properties  # TODO: ClassVar???

    def __init__(self, properties: Properties = EMPTY_DICT):
        self.properties = properties

    @abstractmethod
    def new_input(self, location: str) -> InputFile:
        """Get an InputFile instance to read bytes from the file at the given location.

        Args:
            location (str): A URI or a path to a local file.
        """

    @abstractmethod
    def new_output(self, location: str) -> OutputFile:
        """Get an OutputFile instance to write bytes to the file at the given location.

        Args:
            location (str): A URI or a path to a local file.
        """

    @abstractmethod
    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given path.

        Args:
            location (Union[str, InputFile, OutputFile]): A URI or a path to a local file--if an InputFile instance or
                an OutputFile instance is provided, the location attribute for that instance is used as the URI to delete.

        Raises:
            PermissionError: If the file at location cannot be accessed due to a permission error.
            FileNotFoundError: When the file at the provided location does not exist.
        """

    def fs_by_uri(self, uri: str) -> GenericFileSystem:
        """Get the file system name from the location uri.

        Args:
            uri (str): A URI or a path to a local file.

        Returns:
            str: An instance of either fsspec.AbstractFileSystem or pyarrow._fs.FileSystem.
        """
        scheme = self.parse_uri(uri)[0]
        return self.fs_by_scheme(scheme)

    @abstractmethod
    @cache
    def fs_by_scheme(self, scheme: str) -> GenericFileSystem:
        """Get the file system name from the scheme.

        Args:
            scheme (str): The scheme of the URI.

        Returns:
            str: An instance of either fsspec.AbstractFileSystem or pyarrow._fs.FileSystem.
        """
        return NotImplemented

def parse_location(uri: str) -> Tuple[str, str, str]:
    """Parse the URI into a tuple of scheme, netloc, and path.

    Args:
        uri (str): A URI or a path to a local file.

    Returns:
        Tuple[str, str, str]: A tuple of the scheme, netloc, and path.
    """
    uri = urlparse(uri)
    if not uri.scheme:
        return "file", uri.netloc, os.path.abspath(uri)
    elif uri.scheme in ("hdfs", "viewfs"):
        return uri.scheme, uri.netloc, uri.path
    else:
        return uri.scheme, uri.netloc, f"{uri.netloc}{uri.path}"
