import ijson
import json
from pathlib import Path

# def convert_to_ndjson(file_path: str, output_dir: str = "/tmp") -> tuple[str, str]:
#     """
#     Convert a large JSON file with 'first_names' and 'last_names' arrays
#     into two NDJSON files, written line by line to avoid memory blowup.

#     Args:
#         file_path: Path to input JSON file.
#         output_dir: Directory where NDJSON files will be stored.

#     Returns:
#         (first_names_path, last_names_path)
#     """
#     file_path = Path(file_path)
#     first_names_path = Path(output_dir) / "first_names.ndjson"
#     last_names_path = Path(output_dir) / "last_names.ndjson"

#     with open(file_path, "rb") as infile, \
#          open(first_names_path, "w", encoding="utf-8") as f_first, \
#          open(last_names_path, "w", encoding="utf-8") as f_last:

#         # Stream first_names
#         for item in ijson.items(infile, "first_names.item"):
#             f_first.write(json.dumps(item) + "\n")

#         # Reset file pointer and stream last_names
#         infile.seek(0)
#         for item in ijson.items(infile, "last_names.item"):
#             f_last.write(json.dumps(item) + "\n")

#     return str(first_names_path), str(last_names_path)


# def convert_to_ndjson_stream(file_path: str, output_dir: str = "/tmp"):
#     """
#     Stream items from large JSON file as NDJSON while also writing them to disk.
#     Yields each line as bytes for StreamingResponse.
#     """
#     file_path = Path(file_path)
#     first_names_path = Path(output_dir) / "first_names.ndjson"
#     last_names_path = Path(output_dir) / "last_names.ndjson"

#     with open(file_path, "rb") as infile, \
#          open(first_names_path, "w", encoding="utf-8") as f_first:

#         for item in ijson.items(infile, "first_names.item"):
#             line = json.dumps(item) + "\n"
#             f_first.write(line)
#             yield line.encode("utf-8")  # stream back to client

#         infile.seek(0)
#         with open(last_names_path, "w", encoding="utf-8") as f_last:
#             for item in ijson.items(infile, "last_names.item"):
#                 line = json.dumps(item) + "\n"
#                 f_last.write(line)
#                 yield line.encode("utf-8")

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