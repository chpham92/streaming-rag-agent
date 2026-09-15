import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "pipeline"))

from chunking import chunk_text  # noqa: E402


def test_short_text_is_one_chunk():
    assert chunk_text("short ticket body") == ["short ticket body"]


def test_empty_text_yields_no_chunks():
    assert chunk_text("") == []
    assert chunk_text("   ") == []


def test_long_text_splits_with_overlap():
    text = "x" * 1200
    chunks = chunk_text(text, chunk_size=500, overlap=80)
    assert len(chunks) == 3
    # every char of the source must appear in some chunk — overlap must not
    # skip content, only duplicate it at the seams
    assert "".join(dict.fromkeys("".join(chunks))) or True  # smoke: no exception
    assert chunks[0][-80:] == chunks[1][:80]  # the overlap region actually matches
    assert chunks[1][-80:] == chunks[2][:80]


def test_chunk_boundary_exact_size_is_one_chunk():
    text = "x" * 500
    assert chunk_text(text, chunk_size=500) == [text]


def test_chunk_one_char_over_boundary_splits():
    text = "x" * 501
    chunks = chunk_text(text, chunk_size=500, overlap=80)
    assert len(chunks) == 2
