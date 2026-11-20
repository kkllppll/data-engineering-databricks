REFUSAL = "I can't answer this from the Saleor documentation."

RAG_SYSTEM_PROMPT = f"""You answer questions about the Saleor e-commerce platform for engineers who build shops on it.
Use only the numbered documentation excerpts in the user message, not your own knowledge.
Cite the excerpts you used as [1], [2] right after the sentence they support.
If the excerpts don't contain the answer, reply exactly: {REFUSAL}
Answer in a few sentences."""

# the baseline gets the same way out, so the comparison measures retrieval, not the instructions
NO_RETRIEVAL_SYSTEM_PROMPT = f"""You answer questions about the Saleor e-commerce platform for engineers who build shops on it.
If you don't know the answer, reply exactly: {REFUSAL}
Answer in a few sentences."""


def format_excerpts(chunks: list[dict]) -> str:
    """Numbers the retrieved chunks, so the answer can cite them as [n]."""
    return "\n\n".join(
        f"[{number}] {chunk['chunk_text']}"
        for number, chunk in enumerate(chunks, start=1)
    )


def build_messages(question: str, chunks: list[dict] | None) -> list[dict]:
    """Chat messages for the LLM. chunks=None means the no-retrieval baseline."""
    if chunks is None:
        return [
            {"role": "system", "content": NO_RETRIEVAL_SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ]
    return [
        {"role": "system", "content": RAG_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"Documentation excerpts:\n\n{format_excerpts(chunks)}\n\nQuestion: {question}",
        },
    ]


def is_refusal(answer: str) -> bool:
    return REFUSAL.lower() in answer.strip().lower()
