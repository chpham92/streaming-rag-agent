"""Same chunking approach as the Multi-Format Document RAG Pipeline project:
fixed-size windows with overlap, on characters rather than tokens (good
enough at this scale, avoids a tokenizer dependency in the Spark image).
Most tickets are short enough to fit one chunk — the function still handles
the multi-chunk case so the pipeline doesn't silently break on a long one.
"""

CHUNK_SIZE = 500
CHUNK_OVERLAP = 80


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    text = text.strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start = end - overlap
    return chunks
