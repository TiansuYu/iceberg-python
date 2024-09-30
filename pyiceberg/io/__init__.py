# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Base FileIO classes for implementing reading and writing table files.

The FileIO abstraction includes a subset of full filesystem implementations. Specifically,
Iceberg needs to read or write a file at a given location (as a seekable stream), as well
as check if a file exists. An implementation of the FileIO abstract base class is responsible
for returning an InputFile instance, an OutputFile instance, and deleting a file given
its location.
"""

from __future__ import annotations

import logging
import warnings
from typing import (
    Optional,
    TypeVar, )

from pyiceberg.io.interface import FileIO, InputFile, OutputFile, InputStream, OutputStream
from pyiceberg.typedef import EMPTY_DICT, Properties

logger = logging.getLogger(__name__)

__all__ = ["FileIO", "InputFile", "OutputFile", "InputStream", "OutputStream", "load_file_io"]

# FileIO client kwargs
AWS_REGION = "client.region"
AWS_ACCESS_KEY_ID = "client.access-key-id"
AWS_SECRET_ACCESS_KEY = "client.secret-access-key"
AWS_SESSION_TOKEN = "client.session-token"
S3_ENDPOINT = "s3.endpoint"
S3_ACCESS_KEY_ID = "s3.access-key-id"
S3_SECRET_ACCESS_KEY = "s3.secret-access-key"
S3_SESSION_TOKEN = "s3.session-token"
S3_REGION = "s3.region"
S3_PROXY_URI = "s3.proxy-uri"
S3_CONNECT_TIMEOUT = "s3.connect-timeout"
S3_SIGNER_URI = "s3.signer.uri"
S3_SIGNER_ENDPOINT = "s3.signer.endpoint"
S3_SIGNER_ENDPOINT_DEFAULT = "v1/aws/s3/sign"
HDFS_HOST = "hdfs.host"
HDFS_PORT = "hdfs.port"
HDFS_USER = "hdfs.user"
HDFS_KERB_TICKET = "hdfs.kerberos_ticket"
ADLFS_CONNECTION_STRING = "adlfs.connection-string"
ADLFS_ACCOUNT_NAME = "adlfs.account-name"
ADLFS_ACCOUNT_KEY = "adlfs.account-key"
ADLFS_SAS_TOKEN = "adlfs.sas-token"
ADLFS_TENANT_ID = "adlfs.tenant-id"
ADLFS_CLIENT_ID = "adlfs.client-id"
ADLFS_ClIENT_SECRET = "adlfs.client-secret"
GCS_TOKEN = "gcs.oauth2.token"
GCS_TOKEN_EXPIRES_AT_MS = "gcs.oauth2.token-expires-at"
GCS_PROJECT_ID = "gcs.project-id"
GCS_ACCESS = "gcs.access"
GCS_CONSISTENCY = "gcs.consistency"
GCS_CACHE_TIMEOUT = "gcs.cache-timeout"
GCS_REQUESTER_PAYS = "gcs.requester-pays"
GCS_SESSION_KWARGS = "gcs.session-kwargs"
GCS_ENDPOINT = "gcs.endpoint"
GCS_DEFAULT_LOCATION = "gcs.default-bucket-location"
GCS_VERSION_AWARE = "gcs.version-aware"
PYARROW_USE_LARGE_TYPES_ON_READ = "pyarrow.use-large-types-on-read"

LOCATION = "location"
WAREHOUSE = "warehouse"

FSSPEC = "fsspec"
PYARROW = "pyarrow"
ARROW_FILE_IO = "pyiceberg.io.pyarrow.PyArrowFileIO"
FSSPEC_FILE_IO = "pyiceberg.io.fsspec.FsspecFileIO"
PY_IO_IMPL = "py-io-impl"
DEFAULT_PY_IO_IMPL = PYARROW

FileIOType = TypeVar("FileIOType", bound=FileIO)


def _py_io_impl_argument_parser(py_io_impl: str) -> str:
    """Keep backward compatibility with the old py-io-impl property"""
    if py_io_impl in (FSSPEC, PYARROW):
        return py_io_impl
    elif py_io_impl == FSSPEC_FILE_IO:
        warnings.warn("The 'py-io-impl' value 'pyiceberg.io.fsspec.FsspecFileIO' is being deprecated. "
                      "Please use 'fsspec' instead.")
        return FSSPEC
    elif py_io_impl == ARROW_FILE_IO:
        warnings.warn("The 'py-io-impl' value 'pyiceberg.io.pyarrow.PyArrowFileIO' is being deprecated. "
                      "Please use 'pyarrow' instead.")
        return PYARROW
    else:
        ValueError(f"Unknown value '{py_io_impl}' for {PY_IO_IMPL}. Accepts only: 'fsspec', 'pyarrow', or the "
                   f"deprecating params: 'pyiceberg.io.fsspec.FsspecFileIO', 'pyiceberg.io.pyarrow.PyArrowFileIO'")


def load_file_io(properties: Properties = EMPTY_DICT, location: Optional[str] = None) -> FileIO:
    py_io_impl = properties.get(PY_IO_IMPL, DEFAULT_PY_IO_IMPL)
    py_io_impl = _py_io_impl_argument_parser(py_io_impl)
    # lazily import pyarrow or fsspec
    if py_io_impl == PYARROW:
        from pyiceberg.io.pyarrow import PyArrowFileIO
        file_io = PyArrowFileIO(properties)
        if location and file_io.fs_by_uri(location) is None:
            warnings.warn(f"Uri scheme {location} is not supported by PyArrowFileIO. "
                          "Attempts to load by FsspecFileIO instead.")
            from pyiceberg.io.fsspec import FsspecFileIO
            file_io = FsspecFileIO(properties)
    else:
        from pyiceberg.io.fsspec import FsspecFileIO
        file_io = FsspecFileIO(properties)
        if location and file_io.fs_by_uri(location) is None:
            warnings.warn(f"Uri scheme {location} is not supported by FsspecFileIO. "
                          "Attempts to load by PyArrowFileIO instead.")
            from pyiceberg.io.pyarrow import PyArrowFileIO
            file_io = PyArrowFileIO(properties)
    return file_io
