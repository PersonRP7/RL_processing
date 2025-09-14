import heapq
import ijson
import json
import tempfile
from pathlib import Path
from typing import Iterator


class NameProcessingService:
    def __init__(self, base_tmp: str = "/tmp"):
        """
        Service for handling name processing tasks.
        Each request will get its own temp namespace dir under base_tmp.
        """
        self.base_tmp = Path(base_tmp)

    def convert_to_ndjson_stream(self, file_path: str, batch_size: int = 100):
        """
        Convert large JSON to NDJSON files in a unique request-scoped temp dir.
        Yields lines for streaming back to client.
        After writing, automatically sorts both NDJSON outputs by ID.
        """
        output_dir = Path(tempfile.mkdtemp(prefix="ndjson_", dir=self.base_tmp))
        first_names_path = output_dir / "first_names.ndjson"
        last_names_path = output_dir / "last_names.ndjson"

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

        # --- sort the files once they are fully written ---
        sorted_first = self.external_sort_ndjson(first_names_path)
        sorted_last = self.external_sort_ndjson(last_names_path)

        yield f"# Files written to {output_dir}\n".encode("utf-8")
        yield f"# Sorted files: {sorted_first}, {sorted_last}\n".encode("utf-8")

    def external_sort_ndjson(self, file_path: Path, key_index: int = 1, chunk_size: int = 100_000) -> Path:
        """
        External merge sort on NDJSON file by a key index (disk-backed).
        Returns path to sorted file.
        """
        file_path = Path(file_path)
        output_path = file_path.with_suffix(".sorted.ndjson")
        temp_files = []

        # --- Step 1: break into sorted chunks ---
        with open(file_path, "r", encoding="utf-8") as infile:
            chunk = []
            for line in infile:
                item = json.loads(line)
                chunk.append(item)
                if len(chunk) >= chunk_size:
                    temp_files.append(self._write_sorted_chunk(chunk, key_index))
                    chunk.clear()
            if chunk:  # leftover
                temp_files.append(self._write_sorted_chunk(chunk, key_index))

        # --- Step 2: merge sorted chunks ---
        def iter_file(file: Path) -> Iterator[list]:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    yield json.loads(line)

        sorted_iters = [iter_file(p) for p in temp_files]
        with open(output_path, "w", encoding="utf-8") as outfile:
            for item in heapq.merge(*sorted_iters, key=lambda x: x[key_index]):
                outfile.write(json.dumps(item) + "\n")

        # cleanup
        for p in temp_files:
            p.unlink()

        return output_path

    def _write_sorted_chunk(self, chunk: list, key_index: int) -> Path:
        """Helper: sort chunk and write to temp file."""
        chunk.sort(key=lambda x: x[key_index])
        fd, path = tempfile.mkstemp(suffix=".ndjson", prefix="chunk_")
        Path(path).write_text("\n".join(json.dumps(item) for item in chunk), encoding="utf-8")
        return Path(path)