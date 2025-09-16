"""API data generator functionality

Used to generate the API payloads of variable size,
covering all the defined API use cases.
"""

import json
import random
import string
import os
from pathlib import Path


def random_name(length: int = 5) -> str:
    """
    Generate a random, capitalized name-like string.

    Args:
        length (int): Number of characters in the generated name.

    Returns:
        str: A capitalized pseudo-name string.
    """
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length)).capitalize()


def generate_test_json(
    target_size_mb: int,
    num_first: int,
    num_last: int,
    overlap_ratio: float = 0.5,
    output_path: str = "test_input.json",
    avg_name_len: int = 5
) -> Path:
    """
    Generate a JSON file with `first_names` and `last_names` entries.

    The size of the file is approximately controlled by `target_size_mb`.
    Names are randomly generated, with unique IDs, except for overlaps
    controlled by `overlap_ratio`.

    Args:
        target_size_mb (int): Desired output size in MB.
        num_first (int): Base number of first names (scaled up to reach target size).
        num_last (int): Base number of last names (scaled up to reach target size).
        overlap_ratio (float): Fraction of min(num_first, num_last) that will share IDs.
        output_path (str): File path where the JSON will be saved.
        avg_name_len (int): Average character length of generated names.

    Returns:
        Path: Path to the generated JSON file.
    """
    target_bytes = target_size_mb * 1024 * 1024

    # Estimate rough record size: name + ID + JSON overhead
    approx_record_size = avg_name_len + 20

    # Determine scaling factor to reach target size
    base_records = max(1, num_first + num_last)
    scale = max(1, target_bytes // (base_records * approx_record_size))

    num_first *= scale
    num_last *= scale

    # Generate random non-sequential IDs
    ids_first = random.sample(range(1, 1_000_000_000), num_first)
    ids_last = random.sample(range(1_000_000_001, 2_000_000_000), num_last)

    # Apply overlap
    overlap_size = int(min(num_first, num_last) * overlap_ratio)
    if overlap_size > 0:
        overlap_ids = random.sample(ids_first, overlap_size)
        for i in range(overlap_size):
            ids_last[i] = overlap_ids[i]

    # Build records
    first_names = [[random_name(avg_name_len), id_] for id_ in ids_first]
    last_names = [[random_name(avg_name_len), id_] for id_ in ids_last]

    random.shuffle(first_names)
    random.shuffle(last_names)

    data = {"first_names": first_names, "last_names": last_names}

    output_path = Path(output_path)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"{output_path}: {size_mb:.2f} MB")

    return output_path


if __name__ == "__main__":
    TARGET_SIZE_MB = 1  # <--- control size here

    generate_test_json(TARGET_SIZE_MB, 3, 3, overlap_ratio=1.0, output_path="case_match.json")
    generate_test_json(TARGET_SIZE_MB, 3, 3, overlap_ratio=0.0, output_path="case_unpaired.json")
    generate_test_json(TARGET_SIZE_MB, 3, 0, output_path="case_only_first.json")
    generate_test_json(TARGET_SIZE_MB, 0, 3, output_path="case_only_last.json")
    generate_test_json(TARGET_SIZE_MB, 0, 0, output_path="case_empty.json")