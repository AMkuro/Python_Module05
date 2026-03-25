# Numeric Validation Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `NumericProcessor.validate()` and `NumericProcessor.process()` share one strict contract so invalid numeric inputs are rejected before processing.

**Architecture:** Keep the change local to `stream_processor.py` and add standard-library `unittest` coverage in `tests/`. The processor will only accept non-empty `list` or `tuple` values whose elements are `int` or `float`, excluding `bool`, so validation and processing rely on the same assumptions.

**Tech Stack:** Python 3, `unittest`

---

### Task 1: Lock the numeric contract with failing tests

**Files:**
- Create: `tests/test_stream_processor.py`
- Test: `tests/test_stream_processor.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_numeric_validate_rejects_bool_values(self):
    self.assertFalse(self.processor.validate([True, False]))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_stream_processor -v`
Expected: failures for `bool`, `complex`, and non-sequence rejection cases

### Task 2: Implement the strict numeric validation

**Files:**
- Modify: `stream_processor.py`
- Test: `tests/test_stream_processor.py`

- [ ] **Step 1: Write minimal implementation**

```python
if not isinstance(data, (list, tuple)):
    return False
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_stream_processor -v`
Expected: PASS
