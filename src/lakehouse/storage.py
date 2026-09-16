"""Filesystem helpers that behave identically for a local lake and a gs:// lake."""

from __future__ import annotations

import json
import os
from typing import Any

from pyarrow import fs as pafs


def filesystem_for(uri: str) -> tuple[pafs.FileSystem, str]:
    if "://" not in uri:
        return pafs.LocalFileSystem(), os.path.abspath(uri)
    return pafs.FileSystem.from_uri(uri)


def exists(uri: str) -> bool:
    fs, path = filesystem_for(uri)
    return fs.get_file_info(path).type != pafs.FileType.NotFound


def _write_bytes(data: bytes, uri: str) -> None:
    fs, path = filesystem_for(uri)
    if isinstance(fs, pafs.LocalFileSystem):
        fs.create_dir(os.path.dirname(path), recursive=True)
        tmp = f"{path}.tmp"
        with fs.open_output_stream(tmp) as out:
            out.write(data)
        fs.move(tmp, path)  # atomic rename: readers never see a partial file
    else:
        # A GCS object only becomes visible when its upload finalizes, so a direct write
        # is already atomic. No directory markers are created.
        with fs.open_output_stream(path) as out:
            out.write(data)


def put_file(local_path: str, dest_uri: str) -> None:
    """Copy a local file to the lake atomically, overwriting any previous version."""
    fs, path = filesystem_for(dest_uri)
    local = pafs.LocalFileSystem()
    if isinstance(fs, pafs.LocalFileSystem):
        fs.create_dir(os.path.dirname(path), recursive=True)
        tmp = f"{path}.tmp"
        pafs.copy_files(local_path, tmp, source_filesystem=local, destination_filesystem=fs)
        fs.move(tmp, path)
    else:
        pafs.copy_files(local_path, path, source_filesystem=local, destination_filesystem=fs)


def write_json(obj: Any, uri: str) -> None:
    _write_bytes(json.dumps(obj, indent=2, sort_keys=True, default=str).encode(), uri)


def read_json(uri: str) -> Any:
    fs, path = filesystem_for(uri)
    with fs.open_input_stream(path) as f:
        return json.loads(f.read())


def delete_if_exists(uri: str) -> None:
    fs, path = filesystem_for(uri)
    if fs.get_file_info(path).type != pafs.FileType.NotFound:
        fs.delete_file(path)
