# User Acceptance Test (UAT) Payloads

This directory contains utilities for generating JSON payloads used to test
the `/combine-names` FastAPI endpoint. The script is **not part of the FastAPI app**
itself and can be run with any Python 3.8+ interpreter.

 **Warning**
Running the script will generate **five JSON files** in this directory. Each file will
be approximately sized according to the `TARGET_SIZE_MB` variable in
`payload_generator.py`. Ensure you have sufficient disk space before running.

---
## Usage

To use the ```uat``` utility, a Python interpreter 3.8+ and the ```cURL``` package are required.
If you do not have them in your environment, the utility can be run from inside the running ```container```:

In the project root:

- ```docker compose up```
- ```cd uat```
- ```uv run python3.12 payload_generator.py```

or

```<your_python_interpreter> payload_generator.py```

This produces the following files, which can be used to send ```cURL``` requests to the server:

*case_match.json — first and last names with perfectly overlapping IDs*
```
curl -i -X POST http://localhost:5678/combine-names \
     -H "Content-Type: application/json" \
     --data-binary @case_match.json
```

*case_unpaired.json — unmatched entities*
```
curl -i -X POST http://localhost:5678/combine-names \
     -H "Content-Type: application/json" \
     --data-binary @case_unpaired.json
```

*case_only_first.json — only first_names, no last_names*
```
curl -i -X POST http://localhost:5678/combine-names \
     -H "Content-Type: application/json" \
     --data-binary @case_only_first.json
```

*case_only_last.json — only last_names, no first_names*
```
curl -i -X POST http://localhost:5678/combine-names \
     -H "Content-Type: application/json" \
     --data-binary @case_only_last.json
```

*case_empty.json — both arrays empty*
```
curl -i -X POST http://localhost:5678/combine-names \
     -H "Content-Type: application/json" \
     --data-binary @case_empty.json
```

**Notes**
---
The script does not generate malformed JSON files. Any malformed request will
elicit a 400 Bad Request response from the API.

This UAT utility is self-contained and does not depend on the FastAPI app runtime.