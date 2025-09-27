import ijson
from services.name_processing import InvalidInputError


class StreamingValidator:
    """
    Incrementally validates incoming JSON for the /combine-names endpoint
    while it's being written to a file.

    Raises InvalidInputError immediately on the first malformed chunk.
    """

    def __init__(self):
        self._seen_first_names = False
        self._seen_last_names = False
        self._buffer = b""  # Buffer to hold partial chunks

    def feed(self, chunk: bytes):
        """
        Feed a chunk of bytes to the validator.

        Args:
            chunk: A bytes object from the request stream.

        Raises:
            InvalidInputError: If malformed JSON or invalid array item is detected.
        """
        self._buffer += chunk

        try:
            parser = ijson.parse(self._buffer)
            for prefix, event, value in parser:
                # Detect top-level keys
                if prefix == "first_names" and event == "start_array":
                    self._seen_first_names = True
                elif prefix == "last_names" and event == "start_array":
                    self._seen_last_names = True

                # Validate array items
                if event == "start_array" and prefix.endswith(".item"):
                    item = []
                    for p2, e2, v2 in parser:
                        if e2 in ("string", "number"):
                            item.append(v2)
                        elif e2 == "end_array":
                            break
                    if len(item) != 2:
                        raise InvalidInputError(
                            f"Invalid item structure in {prefix}: {item}"
                        )

        except ijson.JSONError:
            # The chunk may be incomplete — don't raise yet
            pass

    def finish(self):
        """
        Finish parsing the remaining buffer.

        Raises:
            InvalidInputError: If top-level keys are missing or JSON is malformed.
        """
        try:
            parser = ijson.parse(self._buffer)
            # Consume parser fully
            for _, _, _ in parser:
                pass

            if not self._seen_first_names and not self._seen_last_names:
                raise InvalidInputError(
                    "Input must contain at least one of 'first_names' or 'last_names'."
                )
        except ijson.JSONError as e:
            raise InvalidInputError("Malformed JSON input.", raw_error=e)


def streaming_validator():
    return StreamingValidator()