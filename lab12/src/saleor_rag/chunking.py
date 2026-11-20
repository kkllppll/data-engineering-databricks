import re
from dataclasses import dataclass

HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
FENCE = re.compile(r"^\s*(```|~~~)")


@dataclass(frozen=True)
class Chunk:
    section: str
    text: str


def split_sections(text: str) -> list[tuple[str, str]]:
    """Splits markdown by headings into (heading path, body) pairs.

    Lines starting with # inside code blocks are comments, not headings.
    """
    sections = []
    path: list[tuple[int, str]] = []
    lines: list[str] = []
    in_code = False

    def flush():
        body = "\n".join(lines).strip()
        if body:
            sections.append((" > ".join(title for _, title in path), body))
        lines.clear()

    for line in text.splitlines():
        if FENCE.match(line):
            in_code = not in_code
        heading = None if in_code else HEADING.match(line)
        if heading:
            flush()
            level = len(heading.group(1))
            path = [(lvl, title) for lvl, title in path if lvl < level]
            path.append((level, heading.group(2).strip("` ")))
        else:
            lines.append(line)
    flush()
    return sections


def split_blocks(body: str) -> list[str]:
    """Splits a section into paragraphs. A code block stays one block, even with blank lines inside."""
    blocks = []
    current: list[str] = []
    in_code = False
    for line in body.splitlines():
        if FENCE.match(line):
            in_code = not in_code
        if not line.strip() and not in_code:
            if current:
                blocks.append("\n".join(current))
                current = []
            continue
        current.append(line)
    if current:
        blocks.append("\n".join(current))
    return blocks


def _split_long_block(block: str, max_chars: int) -> list[str]:
    """Cuts a block longer than max_chars at line ends, or hard if one line is too long."""
    pieces = []
    current = ""
    for line in block.splitlines():
        while len(line) > max_chars:
            if current:
                pieces.append(current)
                current = ""
            pieces.append(line[:max_chars])
            line = line[max_chars:]
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > max_chars:
            pieces.append(current)
            current = line
        else:
            current = candidate
    if current:
        pieces.append(current)
    return pieces


def pack_blocks(blocks: list[str], max_chars: int, overlap_chars: int) -> list[str]:
    """Packs whole paragraphs into chunks up to max_chars.

    The last paragraphs of a chunk, up to overlap_chars, are repeated at the start of the next one,
    so a fact on the border is whole in at least one chunk.
    """
    units = [piece for block in blocks for piece in _split_long_block(block, max_chars)]
    chunks = []
    current: list[str] = []
    for unit in units:
        if current and len("\n\n".join(current + [unit])) > max_chars:
            chunks.append("\n\n".join(current))
            overlap: list[str] = []
            for previous in reversed(current):
                if len("\n\n".join([previous] + overlap + [unit])) > max_chars:
                    break
                if len("\n\n".join([previous] + overlap)) > overlap_chars:
                    break
                overlap.insert(0, previous)
            current = overlap
        current.append(unit)
    if current:
        chunks.append("\n\n".join(current))
    return chunks


def chunk_page(
    title: str, text: str, max_chars: int, overlap_chars: int, min_chars: int
) -> list[Chunk]:
    """Chunks one cleaned page. Every chunk starts with 'title > section', so it reads on its own."""
    chunks = []
    for section, body in split_sections(text):
        header = f"{title} > {section}" if section else title
        for piece in pack_blocks(split_blocks(body), max_chars, overlap_chars):
            if len(piece) >= min_chars:
                chunks.append(Chunk(section=section, text=f"{header}\n\n{piece}"))
    return chunks
