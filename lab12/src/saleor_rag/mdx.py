import re

FRONT_MATTER = re.compile(r"\A---\s*\n(.*?)\n---\s*\n", re.DOTALL)
# only the top level title, not the indented seo title
TITLE_LINE = re.compile(r"^title:\s*(.+?)\s*$", re.MULTILINE)

FENCE = re.compile(r"^\s*(?:```|~~~)\s*([\w-]*)")
JSX_COMMENT = re.compile(r"\{/\*.*?\*/\}", re.DOTALL)
IMAGE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
LINK = re.compile(r"\[([^\]]+)\]\([^)]*\)")
HTML_IMG = re.compile(r"<img\b[^>]*/?>")
# a line that holds only JSX tags, e.g. <Tabs>, </Tab>, <Card title="..." href="...">
JSX_TAG_LINE = re.compile(r"^\s*(</?[A-Z][\w.]*\b[^>]*>\s*)+$")
ADMONITION_OPEN = re.compile(r"^:::(\w+)(?:\[(.*)\])?\s*$")
ADMONITION_CLOSE = re.compile(r"^:::\s*$")
TAB_VALUE = re.compile(r'^\s*<Tab\s+value="([^"]+)"\s*>\s*$')
CARD_TITLE = re.compile(r'^\s*<Card\b[^>]*\btitle="([^"]+)"')
BLANK_RUNS = re.compile(r"\n{3,}")


def split_front_matter(text: str) -> tuple[str, str]:
    """Returns (front matter, body). Front matter is empty when the page has none."""
    match = FRONT_MATTER.match(text)
    if not match:
        return "", text
    return match.group(1), text[match.end() :]


def page_title(text: str) -> str | None:
    front_matter, _ = split_front_matter(text)
    match = TITLE_LINE.search(front_matter)
    if not match:
        return None
    return match.group(1).strip("'\"")


def _clean_prose_line(line: str) -> str | None:
    """Cleans one line outside code blocks. None means the line is dropped."""
    tab = TAB_VALUE.match(line)
    if tab:
        return f"{tab.group(1)}:"
    card = CARD_TITLE.match(line)
    if card:
        return card.group(1)
    admonition = ADMONITION_OPEN.match(line)
    if admonition:
        kind, inline_text = admonition.groups()
        label = f"{kind.capitalize()}:"
        return f"{label} {_clean_inline(inline_text)}" if inline_text else label
    line = _clean_inline(line)
    # checked after inline cleaning, so <Frame><img/></Frame> becomes an empty tag line and is dropped
    if ADMONITION_CLOSE.match(line) or JSX_TAG_LINE.match(line):
        return None
    return line


def _clean_inline(text: str) -> str:
    text = HTML_IMG.sub("", text)
    text = IMAGE.sub("", text)
    return LINK.sub(r"\1", text)


def clean_body(body: str, drop_code_languages: frozenset[str] = frozenset()) -> str:
    """Turns an MDX page body into plain markdown text for embedding.

    Code blocks are kept, because questions often name a mutation or a field,
    except the languages in drop_code_languages (e.g. json response examples, mermaid diagrams),
    which are long and carry no facts that the text around them doesn't.
    """
    body = JSX_COMMENT.sub("", body)
    lines = []
    in_code = False
    skip_code = False
    for line in body.splitlines():
        fence = FENCE.match(line)
        if fence:
            if not in_code:
                skip_code = fence.group(1).lower() in drop_code_languages
            in_code = not in_code
            if not skip_code:
                lines.append(line.strip())
            continue
        if in_code:
            if not skip_code:
                lines.append(line)
            continue
        cleaned = _clean_prose_line(line)
        if cleaned is not None:
            lines.append(cleaned.rstrip())
    return BLANK_RUNS.sub("\n\n", "\n".join(lines)).strip()
