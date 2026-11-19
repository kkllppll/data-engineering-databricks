import json
import time
import uuid
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from saleor_rag.prompts import build_messages, is_refusal

RESULT_COLUMNS = ["chunk_id", "doc_id", "section", "chunk_text", "source_url"]


@dataclass
class AssistantAnswer:
    request_id: str
    question: str
    mode: str
    topic_filter: str | None
    top_k: int | None
    chunks: list[dict]
    prompt: str
    answer: str
    refused: bool
    llm_endpoint: str
    prompt_tokens: int | None
    completion_tokens: int | None
    latency_ms: int
    error: str | None = field(default=None)


class Assistant:

    def __init__(
        self,
        client: WorkspaceClient,
        embedding_endpoint: str,
        index_name: str,
        llm_endpoint: str,
        max_tokens: int,
    ):
        self.client = client
        self.embedding_endpoint = embedding_endpoint
        self.index_name = index_name
        self.llm_endpoint = llm_endpoint
        self.max_tokens = max_tokens

    def retrieve(
        self, question: str, top_k: int, topic: str | None = None
    ) -> list[dict]:
        """Top-k chunks closest to the question, optionally only inside one topic."""
        # the question must be embedded by the same model as the chunks, or the distances mean nothing
        vector = (
            self.client.serving_endpoints.query(
                name=self.embedding_endpoint, input=[question]
            )
            .data[0]
            .embedding
        )
        response = self.client.vector_search_indexes.query_index(
            index_name=self.index_name,
            columns=RESULT_COLUMNS,
            query_vector=vector,
            num_results=top_k,
            filters_json=json.dumps({"topic": topic}) if topic else None,
        )
        names = [column.name for column in response.manifest.columns]
        return [dict(zip(names, row)) for row in (response.result.data_array or [])]

    def answer(
        self,
        question: str,
        top_k: int,
        topic: str | None = None,
        use_retrieval: bool = True,
    ) -> AssistantAnswer:
        started = time.monotonic()
        chunks = self.retrieve(question, top_k, topic) if use_retrieval else []
        messages = build_messages(question, chunks if use_retrieval else None)

        response = self.client.serving_endpoints.query(
            name=self.llm_endpoint,
            messages=[
                ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"])
                for m in messages
            ],
            # the same question gives the same answer, so runs can be compared
            temperature=0.0,
            max_tokens=self.max_tokens,
        )
        text = response.choices[0].message.content.strip()
        usage = response.usage

        return AssistantAnswer(
            request_id=str(uuid.uuid4()),
            question=question,
            mode="rag" if use_retrieval else "no_retrieval",
            topic_filter=topic,
            top_k=top_k if use_retrieval else None,
            chunks=chunks,
            prompt=json.dumps(messages, ensure_ascii=False),
            answer=text,
            refused=is_refusal(text),
            llm_endpoint=self.llm_endpoint,
            prompt_tokens=usage.prompt_tokens if usage else None,
            completion_tokens=usage.completion_tokens if usage else None,
            latency_ms=int((time.monotonic() - started) * 1000),
        )


def to_log_row(result: AssistantAnswer, asked_by: str, source: str) -> dict:
    """One row of the assistant_log table, asked_at is added by Spark."""
    return {
        "request_id": result.request_id,
        "asked_by": asked_by,
        "source": source,
        "mode": result.mode,
        "question": result.question,
        "topic_filter": result.topic_filter,
        "top_k": result.top_k,
        "retrieved_chunk_ids": [chunk["chunk_id"] for chunk in result.chunks],
        "retrieved_scores": [float(chunk["score"]) for chunk in result.chunks],
        "prompt": result.prompt,
        "answer": result.answer,
        "refused": result.refused,
        "llm_endpoint": result.llm_endpoint,
        "prompt_tokens": result.prompt_tokens,
        "completion_tokens": result.completion_tokens,
        "latency_ms": result.latency_ms,
        "error": result.error,
    }
