import json
import tempfile
import ijson
from pathlib import Path
from heapq import merge

class NameProcessingService:
    def __init__(self, base_tmp: str = "/tmp"):
        self.base_tmp = Path(base_tmp)

    def convert_to_ndjson_stream(self, file_path: str, batch_size: int = 100):
        """
        Convert large JSON to NDJSON files in a unique request-scoped temp dir,
        sort them externally, then merge into full_names + unpaired,
        yielding bytes for streaming back to client.
        """
        output_dir = Path(tempfile.mkdtemp(prefix="ndjson_", dir=self.base_tmp))
        first_path = output_dir / "first_names.ndjson"
        last_path = output_dir / "last_names.ndjson"

        # Step 1: convert JSON -> NDJSON
        with open(file_path, "rb") as infile, \
             open(first_path, "w", encoding="utf-8") as f_first, \
             open(last_path, "w", encoding="utf-8") as f_last:

            buffer_first, buffer_last = [], []

            # stream first_names
            for item in ijson.items(infile, "first_names.item"):
                line = json.dumps(item) + "\n"
                f_first.write(line)
                buffer_first.append(line)
                if len(buffer_first) >= batch_size:
                    yield "".join(buffer_first).encode("utf-8")
                    buffer_first.clear()

            if buffer_first:
                yield "".join(buffer_first).encode("utf-8")
                buffer_first.clear()

            infile.seek(0)

            # stream last_names
            for item in ijson.items(infile, "last_names.item"):
                line = json.dumps(item) + "\n"
                f_last.write(line)
                buffer_last.append(line)
                if len(buffer_last) >= batch_size:
                    yield "".join(buffer_last).encode("utf-8")
                    buffer_last.clear()

            if buffer_last:
                yield "".join(buffer_last).encode("utf-8")
                buffer_last.clear()

        # Step 2: sort NDJSON files externally
        first_sorted = self.external_sort_ndjson(first_path, batch_size)
        last_sorted = self.external_sort_ndjson(last_path, batch_size)

        # Step 3: merge sorted files into full_names + unpaired
        merged_stream = self.merge_full_and_unpaired(first_sorted, last_sorted, batch_size)
        yield from merged_stream

        # Done
        yield f"# All processing complete. Temp dir: {output_dir}\n".encode("utf-8")

    def external_sort_ndjson(self, ndjson_path: Path, batch_size: int = 100) -> Path:
        """
        Sort a NDJSON file externally by the ID (second element).
        Returns path to sorted NDJSON file.
        """
        sorted_path = ndjson_path.with_name(ndjson_path.stem + ".sorted.ndjson")

        # Read lines in memory-limited batches, sort, write to temp sorted chunks
        chunk_files = []
        buffer = []

        with ndjson_path.open("r", encoding="utf-8") as f:
            for line in f:
                buffer.append(json.loads(line))
                if len(buffer) >= batch_size:
                    buffer.sort(key=lambda x: x[1])
                    chunk_file = tempfile.NamedTemporaryFile(delete=False, dir=ndjson_path.parent, mode="w", encoding="utf-8")
                    for item in buffer:
                        chunk_file.write(json.dumps(item) + "\n")
                    chunk_file.close()
                    chunk_files.append(chunk_file.name)
                    buffer.clear()

            if buffer:
                buffer.sort(key=lambda x: x[1])
                chunk_file = tempfile.NamedTemporaryFile(delete=False, dir=ndjson_path.parent, mode="w", encoding="utf-8")
                for item in buffer:
                    chunk_file.write(json.dumps(item) + "\n")
                chunk_file.close()
                chunk_files.append(chunk_file.name)
                buffer.clear()

        # Merge all sorted chunks
        def iter_chunk(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    yield json.loads(line)

        iterators = [iter_chunk(cf) for cf in chunk_files]
        with sorted_path.open("w", encoding="utf-8") as out:
            for item in merge(*iterators, key=lambda x: x[1]):
                out.write(json.dumps(item) + "\n")

        # Cleanup chunk files
        for cf in chunk_files:
            Path(cf).unlink()

        return sorted_path

    def merge_full_and_unpaired(self, first_sorted_path: Path, last_sorted_path: Path, batch_size: int = 100):
        """
        Merge two sorted NDJSON files into full_names.ndjson and unpaired.ndjson.
        Yields each line as bytes for streaming in real time.
        """
        full_path = first_sorted_path.parent / "full_names.ndjson"
        unpaired_path = first_sorted_path.parent / "unpaired.ndjson"

        def iter_ndjson(path: Path):
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        yield json.loads(line)

        first_iter = iter_ndjson(first_sorted_path)
        last_iter = iter_ndjson(last_sorted_path)

        buffer_full, buffer_unpaired = [], []

        try:
            first_current = next(first_iter)
        except StopIteration:
            first_current = None
        try:
            last_current = next(last_iter)
        except StopIteration:
            last_current = None

        with full_path.open("w", encoding="utf-8") as f_full, \
             unpaired_path.open("w", encoding="utf-8") as f_unpaired:

            while first_current or last_current:
                if first_current and last_current:
                    id_first, id_last = first_current[1], last_current[1]
                    if id_first == id_last:
                        line = json.dumps([first_current[0], last_current[0], id_first]) + "\n"
                        f_full.write(line)
                        buffer_full.append(line)
                        first_current = next(first_iter, None)
                        last_current = next(last_iter, None)
                    elif id_first < id_last:
                        line = json.dumps(first_current) + "\n"
                        f_unpaired.write(line)
                        buffer_unpaired.append(line)
                        first_current = next(first_iter, None)
                    else:
                        line = json.dumps(last_current) + "\n"
                        f_unpaired.write(line)
                        buffer_unpaired.append(line)
                        last_current = next(last_iter, None)
                elif first_current:
                    line = json.dumps(first_current) + "\n"
                    f_unpaired.write(line)
                    buffer_unpaired.append(line)
                    first_current = next(first_iter, None)
                elif last_current:
                    line = json.dumps(last_current) + "\n"
                    f_unpaired.write(line)
                    buffer_unpaired.append(line)
                    last_current = next(last_iter, None)

                # yield in batches
                if len(buffer_full) >= batch_size:
                    yield "".join(buffer_full).encode("utf-8")
                    buffer_full.clear()
                if len(buffer_unpaired) >= batch_size:
                    yield "".join(buffer_unpaired).encode("utf-8")
                    buffer_unpaired.clear()

            if buffer_full:
                yield "".join(buffer_full).encode("utf-8")
            if buffer_unpaired:
                yield "".join(buffer_unpaired).encode("utf-8")

        yield f"# Merged full_names -> {full_path}\n".encode("utf-8")
        yield f"# Merged unpaired -> {unpaired_path}\n".encode("utf-8")