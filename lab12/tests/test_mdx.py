import pytest

from saleor_rag.mdx import clean_body, page_title

DROP = frozenset({"json", "jsom", "mermaid"})


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("---\ntitle: Checkout Lifecycle\n---\nBody", "Checkout Lifecycle"),
        ("---\ntitle: 'Gift Cards'\n---\nBody", "Gift Cards"),
        ('---\nseo:\n  title: SEO title\ntitle: "Orders"\n---\nBody', "Orders"),
        ("---\nseo:\n  title: SEO title\n---\nBody", None),
        ("# Checkout\n\ntitle: not front matter", None),
    ],
    ids=["plain", "quoted", "seo_title_ignored", "only_seo_title", "no_front_matter"],
)
def test_page_title(text, expected):
    assert page_title(text) == expected


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ('<Tabs>\n<Tab value="Mutation">\nText\n</Tab>\n</Tabs>', "Mutation:\nText"),
        (":::note\nKeep the token.\n:::", "Note:\nKeep the token."),
        (":::warning[Careful]\nText\n:::", "Warning: Careful\nText"),
        ("See [the checkout](https://example.com/checkout) docs.", "See the checkout docs."),
        ("Before\n![diagram](/img/a.png)\nAfter", "Before\n\nAfter"),
        ('Before\n<Frame><img src="/img/a.png" /></Frame>\nAfter', "Before\nAfter"),
        ('<Card title="Checkout API" href="/api/checkout">', "Checkout API"),
        ("Before\n{/* a hidden\nnote */}\nAfter", "Before\n\nAfter"),
    ],
    ids=[
        "tab_label",
        "admonition",
        "admonition_with_title",
        "link_text",
        "image",
        "frame_with_img",
        "card_title",
        "jsx_comment",
    ],
)
def test_clean_body_prose(body, expected):
    assert clean_body(body, DROP) == expected


@pytest.mark.parametrize("language", ["json", "JSON", "jsom", "mermaid"])
def test_clean_body_drops_code_languages(language):
    body = f'Response:\n\n```{language}\n{{"id": 1}}\n```\n\nAfter the block.'

    assert clean_body(body, DROP) == "Response:\n\nAfter the block."


@pytest.mark.parametrize("language", ["graphql", ""])
def test_clean_body_keeps_other_code_unchanged(language):
    body = (
        f"```{language}\n"
        "mutation {\n"
        "  # see [the docs](https://example.com)\n"
        ":::note\n"
        '<Tab value="Mutation">\n'
        "}\n"
        "```"
    )

    assert clean_body(body, DROP) == body
