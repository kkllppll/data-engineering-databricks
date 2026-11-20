import pytest

from saleor_rag.chunking import Chunk, chunk_page, pack_blocks, split_blocks, split_sections


def blocks_of(*sizes: int) -> list[str]:
    """One-line paragraphs of the given lengths, each with its own letter."""
    return [chr(ord("A") + i) * size for i, size in enumerate(sizes)]


def test_split_sections_builds_heading_path():
    text = (
        "Intro line.\n\n"
        "## Checkout\nAbout checkout.\n\n"
        "### Create\nCreate it.\n\n"
        "## Orders\nAbout orders."
    )

    assert split_sections(text) == [
        ("", "Intro line."),
        ("Checkout", "About checkout."),
        ("Checkout > Create", "Create it."),
        ("Orders", "About orders."),
    ]


@pytest.mark.parametrize("fence", ["```", "~~~"])
def test_hash_line_inside_code_block_is_not_a_heading(fence):
    text = f"## Setup\n{fence}bash\n# install the CLI\npip install x\n{fence}\nDone."

    assert split_sections(text) == [
        ("Setup", f"{fence}bash\n# install the CLI\npip install x\n{fence}\nDone.")
    ]


def test_split_blocks_keeps_code_block_with_blank_lines_whole():
    body = "Text.\n\n```python\na = 1\n\nb = 2\n```\n\nAfter."

    assert split_blocks(body) == ["Text.", "```python\na = 1\n\nb = 2\n```", "After."]


def test_overlap_repeats_last_paragraph():
    a, b, c = blocks_of(100, 100, 100)

    chunks = pack_blocks([a, b, c], max_chars=250, overlap_chars=120)

    assert chunks == [f"{a}\n\n{b}", f"{b}\n\n{c}"]


def test_overlap_skipped_when_last_paragraph_is_longer_than_overlap():
    a, b, c = blocks_of(100, 150, 100)

    chunks = pack_blocks([a, b, c], max_chars=260, overlap_chars=120)

    assert chunks == [f"{a}\n\n{b}", c]


@pytest.mark.parametrize(
    ("sizes", "max_chars", "overlap_chars"),
    [
        ((100, 100, 100, 100), 250, 120),
        ((240, 5, 240), 250, 200),
        ((90, 90, 90), 100, 95),
        ((400, 30, 30), 100, 50),
    ],
    ids=["regular", "overlap_close_to_max", "overlap_larger_than_room", "long_block"],
)
def test_chunks_never_exceed_max_chars(sizes, max_chars, overlap_chars):
    chunks = pack_blocks(blocks_of(*sizes), max_chars, overlap_chars)

    assert chunks
    assert all(len(chunk) <= max_chars for chunk in chunks)


def test_long_block_is_cut_at_line_ends():
    lines = [f"{i:02d}" + "x" * 28 for i in range(10)]
    block = "\n".join(lines)

    chunks = pack_blocks([block], max_chars=100, overlap_chars=0)

    assert len(chunks) > 1
    assert all(len(chunk) <= 100 for chunk in chunks)
    assert all(line in lines for chunk in chunks for line in chunk.splitlines())
    assert "\n".join(chunks) == block


def test_line_longer_than_max_chars_is_cut_hard():
    line = "".join(chr(ord("A") + i % 26) for i in range(250))

    chunks = pack_blocks([line], max_chars=100, overlap_chars=0)

    assert [len(chunk) for chunk in chunks] == [100, 100, 50]
    assert "".join(chunks) == line


@pytest.mark.parametrize(("length", "kept"), [(29, False), (30, True)])
def test_min_chars_is_checked_without_the_header(length, kept):
    chunks = chunk_page(
        "Checkout", "## Lifecycle\n" + "x" * length,
        max_chars=1500, overlap_chars=200, min_chars=30,
    )

    assert len(chunks) == (1 if kept else 0)


@pytest.mark.parametrize(
    ("text", "section", "header"),
    [
        ("## Payments\nBody text of the section.", "Payments", "Saleor > Payments"),
        ("Body text of a page without headings.", "", "Saleor"),
    ],
    ids=["with_section", "without_section"],
)
def test_chunk_starts_with_title_and_section(text, section, header):
    body = text.splitlines()[-1]

    chunks = chunk_page("Saleor", text, max_chars=1500, overlap_chars=200, min_chars=10)

    assert chunks == [Chunk(section=section, text=f"{header}\n\n{body}")]
