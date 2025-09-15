"""
services.name_processing

This module provides the NameProcessingService class for handling large JSON datasets
of first and last names in a memory-efficient streaming fashion. It supports:

- Converting large JSON input into NDJSON (newline-delimited JSON) files.
- Disk-backed external sorting of NDJSON files.
- Merging sorted first and last names into "full_names" and "unpaired" arrays
  in a streaming way.
- Input validation for malformed JSON data with a dedicated InvalidInputError.

The service is designed to be used independently of FastAPI.
"""
import json
import heapq
import ijson
import tempfile
from pathlib import Path
from tempfile import TemporaryDirectory
from ijson.common import IncompleteJSONError
from typing import Generator


class InvalidInputError(Exception):
    """
    Raised when input JSON is invalid.

    Attributes:
        message (str): Safe, generic message for client consumption.
        status_code (int): HTTP status code, default 400.
        raw_error (Exception | None): Original exception object for logging purposes.
    """
    def __init__(
            self,
            message: str = "Invalid JSON input", raw_error: Exception | None = None
        ):
        super().__init__(message)
        self.message = message         # safe, generic message for client
        self.status_code = 400
        self.raw_error = raw_error     # keep raw exception for logging later

class NameProcessingService:
    """
    Service for handling name processing tasks.

    Each request gets its own temporary working directory under `base_tmp`.
    The service provides streaming conversion, external sorting, and merging
    of first and last names to support large datasets without exceeding memory.
    """

    def __init__(self, base_tmp: str = "/tmp"):
        """
        Initialize the service.

        Args:
            base_tmp (str): Base directory for temporary request-scoped files.
        """
        self.base_tmp = Path(base_tmp)

    def convert_to_ndjson_stream(
            self,
            file_path: str,
            batch_size: int = 100
        ) -> Generator[bytes, None, None]:
        """
        Convert large JSON to NDJSON, sort, and merge into final output arrays.

        This method yields bytes in a streaming fashion to support large JSON
        input without exceeding memory.

        Args:
            file_path (str): Path to the input JSON file.
            batch_size (int): Number of lines to buffer before yielding.

        Yields:
            bytes: Chunk of NDJSON formatted output.

        Raises:
            InvalidInputError: If the input JSON is malformed or cannot be processed.
        """
        output_dir = Path(tempfile.mkdtemp(prefix="ndjson_", dir=self.base_tmp))
        first_names_path = output_dir / "first_names.ndjson"
        last_names_path = output_dir / "last_names.ndjson"

        # Step 1: Convert JSON -> NDJSON
        try:
            with open(file_path, "rb") as infile, \
                open(first_names_path, "w", encoding="utf-8") as f_first:

                buffer = []
                for item in ijson.items(infile, "first_names.item"):
                    line = json.dumps(item) + "\n"
                    f_first.write(line)
                    buffer.append(line)
                    if len(buffer) >= batch_size:
                        yield "".join(buffer).encode("utf-8")
                        buffer.clear()
                if buffer:
                    yield "".join(buffer).encode("utf-8")

                infile.seek(0)
                with open(last_names_path, "w", encoding="utf-8") as f_last:
                    buffer = []
                    for item in ijson.items(infile, "last_names.item"):
                        line = json.dumps(item) + "\n"
                        f_last.write(line)
                        buffer.append(line)
                        if len(buffer) >= batch_size:
                            yield "".join(buffer).encode("utf-8")
                            buffer.clear()
                    if buffer:
                        yield "".join(buffer).encode("utf-8")
        except (ValueError, IncompleteJSONError) as e:
            raise InvalidInputError(raw_error=e)

        # Step 2: Automatically sort each NDJSON file
        yield b"# Sorting first_names...\n"
        first_sorted = self.external_sort_ndjson(first_names_path, batch_size=batch_size)

        yield b"# Sorting last_names...\n"
        last_sorted = self.external_sort_ndjson(last_names_path, batch_size=batch_size)

        # Step 3: Stream merged API JSON
        yield b"{\n  \"full_names\": [\n"
        yield from self.merge_full_names(first_sorted, last_sorted, batch_size=batch_size)
        yield b"  ],\n  \"unpaired\": [\n"
        yield from self.merge_unpaired(first_sorted, last_sorted, batch_size=batch_size)
        yield b"  ]\n}\n"

        yield f"# Pipeline finished. Results in {output_dir}\n".encode("utf-8")

    def external_sort_ndjson(
        self,
        ndjson_path: Path,
        batch_size: int = 100,
        sorted_suffix: str = ".sorted.ndjson"
    ) -> Path:
        """
        Perform an external disk-backed sort of an NDJSON file in
        order to generate a file containing sequential entries
        (Ordered by their ID from smaller to larger).

        Args:
            ndjson_path (Path): Path to the NDJSON file to sort.
            batch_size (int): Number of records to sort in memory at a time.
            sorted_suffix (str): Suffix to append to the sorted file name.

        Returns:
            Path: Path to the sorted NDJSON file.
        """
        ndjson_path = Path(ndjson_path)
        sorted_path = ndjson_path.with_name(ndjson_path.stem + sorted_suffix)

        with TemporaryDirectory(dir=ndjson_path.parent) as tmpdir:
            chunk_paths = []

            # Chunking: read in memory-limited pieces, sort, write to temp files
            chunk = []
            with ndjson_path.open("r", encoding="utf-8") as infile:
                for line in infile:
                    line = line.strip()
                    if not line:
                        continue
                    item = json.loads(line)
                    chunk.append(item)
                    if len(chunk) >= batch_size:
                        chunk.sort(key=lambda x: x[1])
                        chunk_file = Path(tmpdir) / f"chunk_{len(chunk_paths)}.ndjson"
                        with chunk_file.open("w", encoding="utf-8") as cf:
                            for obj in chunk:
                                cf.write(json.dumps(obj) + "\n")
                        chunk_paths.append(chunk_file)
                        chunk.clear()
                if chunk:
                    chunk.sort(key=lambda x: x[1])
                    chunk_file = Path(tmpdir) / f"chunk_{len(chunk_paths)}.ndjson"
                    with chunk_file.open("w", encoding="utf-8") as cf:
                        for obj in chunk:
                            cf.write(json.dumps(obj) + "\n")
                    chunk_paths.append(chunk_file)

            # Merge sorted chunks
            def gen_file_lines(path: Path):
                with path.open("r", encoding="utf-8") as f:
                    for line in f:
                        yield json.loads(line)

            merged_iter = heapq.merge(
                *(gen_file_lines(p) for p in chunk_paths),
                key=lambda x: x[1]
            )

            # Write merged output
            with sorted_path.open("w", encoding="utf-8") as out:
                for obj in merged_iter:
                    out.write(json.dumps(obj) + "\n")

        return sorted_path

    def iter_ndjson(self, path: Path):
        """
        A merge helper.
        Generator that yields each JSON object from an NDJSON file.

        Args:
            path (Path): Path to the NDJSON file.

        Yields:
            dict: JSON object from the NDJSON file.
        """
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)

    def merge_full_names(
        self,
        first_path: Path,
        last_path: Path,
        batch_size: int = 100
        ) -> Generator[bytes, None, None]:
        """
        Merge first and last names into the 'full_names' array.

        Args:
            first_path (Path): Path to the sorted first_names NDJSON.
            last_path (Path): Path to the sorted last_names NDJSON.
            batch_size (int): Number of merged entries to buffer before yielding.

        Yields:
            bytes: JSON-formatted entries for 'full_names' array.
        """
        first_iter = self.iter_ndjson(first_path)
        last_iter = self.iter_ndjson(last_path)

        first_current = next(first_iter, None)
        last_current = next(last_iter, None)

        first = True
        buffer = []

        while first_current and last_current:
            if first_current[1] == last_current[1]:
                full = {"first": first_current[0], "last": last_current[0], "id": first_current[1]}
                line = ("    " if first else "    ,") + json.dumps(full) + "\n"
                buffer.append(line)
                if len(buffer) >= batch_size:
                    yield "".join(buffer).encode("utf-8")
                    buffer.clear()
                first_current = next(first_iter, None)
                last_current = next(last_iter, None)
                first = False
            elif first_current[1] < last_current[1]:
                first_current = next(first_iter, None)
            else:
                last_current = next(last_iter, None)

        if buffer:
            yield "".join(buffer).encode("utf-8")

    def merge_unpaired(
            self,
            first_path: Path,
            last_path: Path,
            batch_size: int = 100
        ) -> Generator[bytes, None, None]:
        """
        Merge first and last names that do not have matching IDs into 'unpaired'.

        Args:
            first_path (Path): Path to the sorted first_names NDJSON.
            last_path (Path): Path to the sorted last_names NDJSON.
            batch_size (int): Number of unpaired entries to buffer before yielding.

        Yields:
            bytes: JSON-formatted entries for 'unpaired' array.
        """
        first_iter = self.iter_ndjson(first_path)
        last_iter = self.iter_ndjson(last_path)

        first_current = next(first_iter, None)
        last_current = next(last_iter, None)

        first = True
        buffer = []

        while first_current or last_current:
            if first_current and (not last_current or first_current[1] < last_current[1]):
                rec = {"first": first_current[0], "id": first_current[1]}
                line = ("    " if first else "    ,") + json.dumps(rec) + "\n"
                buffer.append(line)
                first_current = next(first_iter, None)
                first = False
            elif last_current and (not first_current or last_current[1] < first_current[1]):
                rec = {"last": last_current[0], "id": last_current[1]}
                line = ("    " if first else "    ,") + json.dumps(rec) + "\n"
                buffer.append(line)
                last_current = next(last_iter, None)
                first = False
            else:
                first_current = next(first_iter, None)
                last_current = next(last_iter, None)

            if len(buffer) >= batch_size:
                yield "".join(buffer).encode("utf-8")
                buffer.clear()

        if buffer:
            yield "".join(buffer).encode("utf-8")