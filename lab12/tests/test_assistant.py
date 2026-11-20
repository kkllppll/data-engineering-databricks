import json
from types import SimpleNamespace as NS

import pytest
from databricks.sdk.service.serving import ChatMessageRole

from saleor_rag.assistant import RESULT_COLUMNS, Assistant, to_log_row
from saleor_rag.prompts import REFUSAL, build_messages

EMBEDDING = "embedding-endpoint"
LLM = "llm-endpoint"
INDEX = "catalog.schema.chunks_index"
QUESTION = "Can I change the channel of a checkout?"
VECTOR = [0.1, 0.2, 0.3]
USAGE = NS(prompt_tokens=800, completion_tokens=20)
ROWS = [
    [
        "checkout/lifecycle#0000",
        "checkout/lifecycle",
        "Lifecycle",
        "Checkout > Lifecycle\n\nThe channel can't be changed.",
        "https://github.com/saleor/saleor-docs/blob/abc/docs/developer/checkout/lifecycle.mdx",
        "0.0042",
    ],
    [
        "checkout/overview#0001",
        "checkout/overview",
        "Channels",
        "Checkout > Channels\n\nA checkout belongs to one channel.",
        "https://github.com/saleor/saleor-docs/blob/abc/docs/developer/checkout/overview.mdx",
        0.0031,
    ],
]


class FakeServingEndpoints:
    def __init__(self, answer, usage):
        self.answer = answer
        self.usage = usage
        self.calls = []

    def query(self, name, **kwargs):
        self.calls.append({"name": name, **kwargs})
        if name == EMBEDDING:
            return NS(data=[NS(embedding=VECTOR)])
        return NS(choices=[NS(message=NS(content=self.answer))], usage=self.usage)


class FakeVectorSearchIndexes:
    def __init__(self, data_array):
        self.data_array = data_array
        self.calls = []

    def query_index(self, **kwargs):
        self.calls.append(kwargs)
        # the service appends the score as the last column
        columns = [NS(name=name) for name in [*RESULT_COLUMNS, "score"]]
        return NS(manifest=NS(columns=columns), result=NS(data_array=self.data_array))


def make_client(answer="No, the channel cannot be changed [1].", usage=USAGE, data_array=ROWS):
    return NS(
        serving_endpoints=FakeServingEndpoints(answer, usage),
        vector_search_indexes=FakeVectorSearchIndexes(data_array),
    )


def make_assistant(client):
    return Assistant(client, EMBEDDING, INDEX, LLM, max_tokens=600)


@pytest.mark.parametrize(
    ("topic", "filters_json"),
    [("checkout", '{"topic": "checkout"}'), (None, None)],
    ids=["topic", "no_topic"],
)
def test_retrieve_queries_index(topic, filters_json):
    client = make_client()

    make_assistant(client).retrieve(QUESTION, top_k=5, topic=topic)

    assert client.serving_endpoints.calls == [{"name": EMBEDDING, "input": [QUESTION]}]
    assert client.vector_search_indexes.calls == [
        {
            "index_name": INDEX,
            "columns": RESULT_COLUMNS,
            "query_vector": VECTOR,
            "num_results": 5,
            "filters_json": filters_json,
        }
    ]


@pytest.mark.parametrize(("data_array", "count"), [(ROWS, 2), (None, 0)], ids=["rows", "no_rows"])
def test_retrieve_maps_rows_by_column_names(data_array, count):
    chunks = make_assistant(make_client(data_array=data_array)).retrieve(QUESTION, top_k=5)

    assert len(chunks) == count
    if count:
        assert chunks[0] == {
            "chunk_id": "checkout/lifecycle#0000",
            "doc_id": "checkout/lifecycle",
            "section": "Lifecycle",
            "chunk_text": "Checkout > Lifecycle\n\nThe channel can't be changed.",
            "source_url": ROWS[0][4],
            "score": "0.0042",
        }


def test_answer_with_retrieval():
    client = make_client()

    result = make_assistant(client).answer(QUESTION, top_k=5, topic="checkout")

    assert result.mode == "rag"
    assert result.top_k == 5
    assert result.topic_filter == "checkout"
    assert [chunk["chunk_id"] for chunk in result.chunks] == [row[0] for row in ROWS]
    llm_call = client.serving_endpoints.calls[-1]
    assert llm_call["name"] == LLM
    assert [m.role for m in llm_call["messages"]] == [ChatMessageRole.SYSTEM, ChatMessageRole.USER]
    assert "[1] Checkout > Lifecycle" in llm_call["messages"][1].content
    assert llm_call["temperature"] == 0.0
    assert llm_call["max_tokens"] == 600
    assert json.loads(result.prompt) == build_messages(QUESTION, result.chunks)


def test_answer_without_retrieval_makes_no_search_calls():
    client = make_client()

    result = make_assistant(client).answer(QUESTION, top_k=5, use_retrieval=False)

    assert client.vector_search_indexes.calls == []
    assert [call["name"] for call in client.serving_endpoints.calls] == [LLM]
    assert result.mode == "no_retrieval"
    assert result.top_k is None
    assert result.chunks == []
    assert json.loads(result.prompt) == build_messages(QUESTION, None)


@pytest.mark.parametrize(
    ("answer", "refused"),
    [(f"  {REFUSAL}\n", True), ("No, the channel cannot be changed [1].\n", False)],
    ids=["refusal", "answer"],
)
def test_answer_refused_flag(answer, refused):
    result = make_assistant(make_client(answer=answer)).answer(QUESTION, top_k=5)

    assert result.answer == answer.strip()
    assert result.refused is refused


def test_answer_without_usage_has_no_token_counts():
    result = make_assistant(make_client(usage=None)).answer(QUESTION, top_k=5)

    assert result.prompt_tokens is None
    assert result.completion_tokens is None


@pytest.mark.parametrize(
    ("use_retrieval", "chunk_ids", "scores"),
    [
        (True, ["checkout/lifecycle#0000", "checkout/overview#0001"], [0.0042, 0.0031]),
        (False, [], []),
    ],
    ids=["rag", "no_retrieval"],
)
def test_to_log_row(use_retrieval, chunk_ids, scores):
    result = make_assistant(make_client()).answer(QUESTION, top_k=5, use_retrieval=use_retrieval)

    row = to_log_row(result, asked_by="reader@example.com", source="eval")

    assert row["request_id"] == result.request_id
    assert row["asked_by"] == "reader@example.com"
    assert row["source"] == "eval"
    assert row["mode"] == result.mode
    assert row["retrieved_chunk_ids"] == chunk_ids
    assert row["retrieved_scores"] == scores
    assert all(isinstance(score, float) for score in row["retrieved_scores"])
    assert row["prompt_tokens"] == 800
    assert row["error"] is None
