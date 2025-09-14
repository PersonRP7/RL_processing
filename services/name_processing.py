import ijson
import json
from pathlib import Path

def convert_to_ndjson_stream(file_path: str, output_dir: str = "/tmp", batch_size: int = 100):
    file_path = Path(file_path)
    first_names_path = Path(output_dir) / "first_names.ndjson"
    last_names_path = Path(output_dir) / "last_names.ndjson"

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