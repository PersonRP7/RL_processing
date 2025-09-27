"""
Streaming JSON input validator.

This module provides a streaming validation mechanism for JSON input,
specifically tailored for requests containing "first_names" and "last_names"
fields. It is designed to validate large JSON payloads incrementally as they
are received, without loading the entire content into memory.

The main class, `StreamingValidator`, allows feeding the incoming JSON data
chunk by chunk using the `feed()` method and finalizing validation with
`finish()`. On encountering invalid JSON structures or missing required fields,
it raises `InvalidInputError` immediately, enabling early termination of
processing and returning a 400-level HTTP response.

This approach is useful for:

- Streaming large JSON requests efficiently.
- Catching malformed input early before further processing.
- Decoupling validation logic from I/O or logging concerns.

Typical usage:

    from validators.input_validator import streaming_validator

    validator = streaming_validator()
    for chunk in request_stream:
        validator.feed(chunk)
    validator.finish()  # Raises error if any issues found
"""
import ijson
from services.name_processing import InvalidInputError


class StreamingValidator:
    """
    Incrementally validates incoming JSON
    while it's being written to a file.

    Raises InvalidInputError immediately on the first malformed chunk.
    """

    def __init__(self):
        self._seen_first_names = False
        self._seen_last_names = False
        self._buffer = b""  # Buffer to hold partial chunks

    def feed(self, chunk: bytes):
        """
        Feed a chunk of bytes to the streaming JSON validator.

        This method allows the validator to process the request body in a streaming
        fashion, handling arbitrarily large JSON documents without reading them
        fully into memory. Incoming data may arrive in partial or incomplete chunks,
        so parsing errors from incomplete JSON are temporarily ignored until more
        data is fed.

        Args:
            chunk (bytes): A portion of the request body from the streaming request.

        Raises:
            InvalidInputError: If a complete structural element (e.g., a JSON array item)
                has been fully received and is invalid, or if the final JSON structure
                is malformed when `finish()` is called.

        Notes:
            - Partial or incomplete chunks are allowed because the validator cannot
            determine whether the JSON is truly malformed until enough data is available.
            - A complete structural element (like an array item) is considered "fully
            received" when all of its constituent tokens (start, values, end) are present.
            Only then is it validated. This ensures that the validator does not raise
            errors on chunks that merely contain incomplete JSON fragments.
            - The validator internally buffers chunks and only parses enough to
            validate complete elements, avoiding false positives on incomplete data.
            """
        self._buffer += chunk

        try:
            parser = ijson.parse(self._buffer)
            # ijson.parse generates a prefix,key,value tuple
            # for every event in the JSON document
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
        Complete validation after all chunks have been fed.

        This method should be called once all streaming JSON data has been
        provided via `feed()`. It finalizes the parsing of any remaining
        buffered data and performs top-level validation to ensure the input
        meets required structural expectations.

        Raises:
            InvalidInputError: If the JSON is malformed, or if the required
                top-level keys ('first_names' or 'last_names') are missing.
                This ensures that even if individual chunks were valid,
                the overall JSON structure is correct.

        Notes:
            - Should always be called after the last chunk of data is fed.
            - This is the point where missing required keys are detected,
              since they may not appear until the entire document has been
              streamed.
            - Any remaining partial elements in the buffer are parsed and
              validated here.
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
    """
    Create and return a new StreamingValidator instance.

    This convenience function provides a simple way to obtain a streaming
    JSON validator, which can then be used to feed JSON data in chunks and
    perform incremental validation.

    Returns:
        StreamingValidator: A new validator instance ready for streaming input.
    """
    return StreamingValidator()