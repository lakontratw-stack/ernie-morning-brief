from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class KnowledgeBundle:
    knowledge_base: str
    tender_playbook: str


BASE_DIR = Path(__file__).resolve().parent
KNOWLEDGE_BASE_PATH = BASE_DIR / "knowledge_base.md"
TENDER_PLAYBOOK_PATH = BASE_DIR / "tender_playbook.md"


def load_knowledge() -> KnowledgeBundle:
    return KnowledgeBundle(
        knowledge_base=_read_markdown(KNOWLEDGE_BASE_PATH),
        tender_playbook=_read_markdown(TENDER_PLAYBOOK_PATH),
    )


def _read_markdown(path: Path) -> str:
    if not path.exists():
        print(f"knowledge file missing: {path}")
        return ""
    return path.read_text(encoding="utf-8")
