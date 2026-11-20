import pytest

from saleor_rag.prompts import (
    NO_RETRIEVAL_SYSTEM_PROMPT,
    RAG_SYSTEM_PROMPT,
    REFUSAL,
    build_messages,
    is_refusal,
)

QUESTION = "Can I change the channel of a checkout?"
CHUNKS = [{"chunk_text": "Checkout > Lifecycle"}, {"chunk_text": "Checkout > Channels"}]


@pytest.mark.parametrize(
    ("chunks", "system_prompt"),
    [(CHUNKS, RAG_SYSTEM_PROMPT), (None, NO_RETRIEVAL_SYSTEM_PROMPT)],
    ids=["rag", "no_retrieval"],
)
def test_build_messages(chunks, system_prompt):
    system, user = build_messages(QUESTION, chunks)

    assert system == {"role": "system", "content": system_prompt}
    assert user["role"] == "user"
    assert user["content"].endswith(QUESTION)
    has_excerpts = chunks is not None
    assert ("[1] Checkout > Lifecycle" in user["content"]) is has_excerpts
    assert ("[2] Checkout > Channels" in user["content"]) is has_excerpts


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        (REFUSAL, True),
        (f"  {REFUSAL.upper()}\n", True),
        (f"Sorry. {REFUSAL} Try the API reference.", True),
        ("No, the channel cannot be changed [1].", False),
        ("", False),
    ],
    ids=["exact", "case_and_whitespace", "inside_longer_answer", "normal_answer", "empty"],
)
def test_is_refusal(answer, expected):
    assert is_refusal(answer) is expected
