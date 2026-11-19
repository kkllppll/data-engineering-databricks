import json

import pytest

from saleor_rag.judge import Verdict, content_text, expected_doc_rank, parse_verdict

VALID = {"correctness": 5, "relevance": 4, "groundedness": 3, "reason": "Matches."}
VALID_JSON = json.dumps(VALID)
MISSING = object()


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        (VALID_JSON, "Matches."),
        (f"Here is my verdict:\n{VALID_JSON}\nThanks.", "Matches."),
        (f"```json\n{VALID_JSON}\n```", "Matches."),
        (json.dumps({**VALID, "reason": "Uses {checkoutCreate} correctly."}), "Uses {checkoutCreate} correctly."),
    ],
    ids=["clean", "prose_around", "json_fence", "braces_in_reason"],
)
def test_parse_verdict_finds_json(text, reason):
    assert parse_verdict(text, has_excerpts=True) == Verdict(
        correctness=5, relevance=4, groundedness=3, reason=reason
    )


@pytest.mark.parametrize(
    ("field", "value", "has_excerpts"),
    [
        ("correctness", 0, True),
        ("correctness", 6, True),
        ("relevance", "5", True),
        ("correctness", 4.0, True),
        ("relevance", True, True),
        ("correctness", MISSING, True),
        ("relevance", MISSING, True),
        ("groundedness", 7, False),
    ],
    ids=["zero", "six", "string", "float", "bool", "missing_correctness", "missing_relevance",
         "groundedness_out_of_range_without_excerpts"],
)
def test_parse_verdict_rejects_invalid_scores(field, value, has_excerpts):
    data = dict(VALID)
    if value is MISSING:
        del data[field]
    else:
        data[field] = value

    with pytest.raises(ValueError, match=field):
        parse_verdict(json.dumps(data), has_excerpts)


@pytest.mark.parametrize(
    ("has_excerpts", "value", "expected"),
    [(True, None, None), (True, 4, 4), (False, 5, None)],
    ids=["null_with_excerpts", "score_with_excerpts", "score_without_excerpts"],
)
def test_groundedness(has_excerpts, value, expected):
    text = json.dumps({**VALID, "groundedness": value})

    assert parse_verdict(text, has_excerpts).groundedness == expected


def test_parse_verdict_without_json_fails():
    with pytest.raises(ValueError):
        parse_verdict("The answer is correct, 5 out of 5.", has_excerpts=True)


@pytest.mark.parametrize(
    ("retrieved", "expected_doc_id", "rank"),
    [
        (["checkout/lifecycle", "order/expiration", "order/expiration"], "order/expiration", 2),
        (["checkout/lifecycle"], "taxes/overview", None),
        ([], "taxes/overview", None),
        (["checkout/lifecycle"], None, None),
    ],
    ids=["first_match", "not_found", "nothing_retrieved", "no_expected_page"],
)
def test_expected_doc_rank(retrieved, expected_doc_id, rank):
    assert expected_doc_rank(retrieved, expected_doc_id) == rank


def test_content_text_keeps_only_text_blocks():
    content = [
        {"type": "reasoning", "summary": [{"type": "summary_text", "text": "thinking"}]},
        {"type": "text", "text": '{"correctness"'},
        "stray",
        {"type": "text", "text": ": 5}"},
    ]

    assert content_text(content) == '{"correctness": 5}'
    with pytest.raises(TypeError):
        content_text(None)
