import json
import re
from dataclasses import dataclass

SCORE_RANGE = range(1, 6)

JUDGE_SYSTEM_PROMPT = """You grade answers of a documentation assistant for the Saleor e-commerce platform.
You get the question, a reference answer written from the documentation, the assistant's answer,
and, when the assistant used retrieval, the documentation excerpts it was given.

Score each criterion from 1 (bad) to 5 (perfect):
- correctness: the answer agrees with the reference answer and has no wrong facts. A refusal scores 1 if the reference has an answer, and 5 if the reference says the topic is not covered.
- relevance: the answer addresses the question that was asked.
- groundedness: every claim in the answer is supported by the excerpts. Use null when no excerpts are given.

Reply with one JSON object only, no other text:
{"correctness": <1-5>, "relevance": <1-5>, "groundedness": <1-5 or null>, "reason": "<one sentence>"}"""

JSON_OBJECT = re.compile(r"\{.*\}", re.DOTALL)


@dataclass(frozen=True)
class Verdict:
    correctness: int
    relevance: int
    groundedness: int | None
    reason: str


def build_judge_messages(
    question: str, reference: str, answer: str, excerpts: str | None
) -> list[dict]:
    parts = [
        f"Question:\n{question}",
        f"Reference answer:\n{reference}",
        f"Assistant answer:\n{answer}",
        (
            f"Excerpts given to the assistant:\n{excerpts}"
            if excerpts
            else "No excerpts were given to the assistant."
        ),
    ]
    return [
        {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
        {"role": "user", "content": "\n\n".join(parts)},
    ]


def content_text(content) -> str:
    """Text of a chat response. Reasoning models return a list of blocks, where only the text blocks are the answer."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            block.get("text", "")
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        )
    raise TypeError(f"Unexpected response content type: {type(content).__name__}")


def _score(value, name: str, nullable: bool = False) -> int | None:
    if value is None and nullable:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value not in SCORE_RANGE
    ):
        raise ValueError(f"{name} must be an integer from 1 to 5, got {value!r}")
    return value


def parse_verdict(text: str, has_excerpts: bool) -> Verdict:
    """Reads the judge's JSON. Fails on anything outside the agreed format instead of guessing a score."""
    match = JSON_OBJECT.search(text)
    if not match:
        raise ValueError(f"No JSON object in the judge response: {text[:200]!r}")
    data = json.loads(match.group(0))
    groundedness = _score(data.get("groundedness"), "groundedness", nullable=True)
    return Verdict(
        correctness=_score(data.get("correctness"), "correctness"),
        relevance=_score(data.get("relevance"), "relevance"),
        # without excerpts there is nothing to be grounded in, whatever the judge says
        groundedness=groundedness if has_excerpts else None,
        reason=str(data.get("reason", "")).strip(),
    )


def expected_doc_rank(
    retrieved_doc_ids: list[str], expected_doc_id: str | None
) -> int | None:
    """1-based position of the first chunk from the expected page, None if it is not in the top-k."""
    if expected_doc_id is None:
        return None
    for rank, doc_id in enumerate(retrieved_doc_ids, start=1):
        if doc_id == expected_doc_id:
            return rank
    return None
