import ijson
import json
import tempfile
from pathlib import Path


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

        # At the end, yield info about where files are stored (for debugging)
        yield f"# Files written to {output_dir}\n".encode("utf-8")
