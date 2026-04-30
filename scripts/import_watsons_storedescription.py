#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from html import unescape
from pathlib import Path

import requests


DEFAULT_URL = "https://www.watsons.com.tw/openchat"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import Watsons Taiwan store names and addresses into the Lyra store_hours CSV format."
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="Watsons store list URL.")
    parser.add_argument("--input-html", help="Use a saved Watsons HTML file instead of fetching a URL.")
    parser.add_argument(
        "--output",
        default="data/watsons_stores.csv",
        help="Output CSV path.",
    )
    parser.add_argument("--timeout", type=int, default=90, help="HTTP read timeout in seconds.")
    args = parser.parse_args()

    if args.input_html:
        html = Path(args.input_html).read_text(encoding="utf-8", errors="ignore")
    else:
        try:
            html = fetch(args.url, args.timeout)
        except requests.RequestException as exc:
            raise SystemExit(
                f"Could not fetch {args.url}: {exc}\n"
                "If the site times out, open the Watsons page in your browser, save it as HTML, then rerun with --input-html."
            ) from exc
    text = html_to_text(html)
    rows = parse_store_rows(text, args.url)
    if not rows:
        raise SystemExit(
            "No store rows were found. Watsons may have changed the page or the page may require JavaScript."
        )

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    write_csv(output, rows)
    print(f"Wrote {len(rows)} store rows to {output}")
    print("Hours are intentionally blank. Lyra will answer addresses and transfer to staff for unknown hours.")


def fetch(url: str, timeout: int) -> str:
    response = requests.get(url, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding
    return response.text


def html_to_text(html: str) -> str:
    html = re.sub(r"(?i)<(br|/p|/tr|/li|/h[1-6])\b[^>]*>", "\n", html)
    text = re.sub(r"<[^>]+>", " ", html)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def parse_store_rows(text: str, source_url: str) -> list[dict[str, str]]:
    openchat_rows = parse_openchat_rows(text, source_url)
    if openchat_rows:
        return openchat_rows
    return parse_storedescription_rows(text, source_url)


def parse_openchat_rows(text: str, source_url: str) -> list[dict[str, str]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for index, line in enumerate(lines):
        marker = "#### "
        if marker not in line:
            continue
        raw_name = line.split(marker, 1)[1].strip()
        address = next_address_line(lines, index + 1)
        if not raw_name or not address:
            continue
        name = normalize_openchat_store_name(raw_name)
        key = (name, address)
        if key in seen:
            continue
        seen.add(key)
        rows.append(store_row(name, address, raw_name, source_url))
    return rows


def parse_storedescription_rows(text: str, source_url: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    pattern = re.compile(
        r"^(?P<name>[\w\u4e00-\u9fff一-龥A-Za-z0-9~-]+)\s+"
        r"(?P<city>[\u4e00-\u9fff]{2,3}[市縣])\s+"
        r"(?P<district>[\u4e00-\u9fff]{2,4}區|[\u4e00-\u9fff]{2,4}鄉|[\u4e00-\u9fff]{2,4}鎮|[\u4e00-\u9fff]{2,4}市)\s+"
        r"(?P<address>.+)$"
    )
    seen: set[tuple[str, str]] = set()
    for line in text.splitlines():
        match = pattern.match(line)
        if not match:
            continue
        name = match.group("name").strip()
        address = match.group("address").strip()
        key = (name, address)
        if key in seen:
            continue
        seen.add(key)
        rows.append(store_row(name, address, "", source_url))
    return rows


def next_address_line(lines: list[str], start_index: int) -> str:
    city_pattern = re.compile(r"^[\u4e00-\u9fff]{2,3}[市縣][\u4e00-\u9fff]{2,4}[區鄉鎮市].+")
    for line in lines[start_index : start_index + 4]:
        if city_pattern.match(line):
            return line
    return ""


def normalize_openchat_store_name(raw_name: str) -> str:
    name = re.sub(r"^[\u4e00-\u9fff]{2,3}(?=[\u4e00-\u9fff]{2,}店$)", "", raw_name)
    return name or raw_name


def store_row(name: str, address: str, raw_name: str, source_url: str) -> dict[str, str]:
    aliases = [f"屈臣氏{name}店", f"屈臣氏{name}", f"{name}門市", name]
    if raw_name:
        aliases.insert(0, raw_name)
    return {
        "store_name": name,
        "aliases": ";".join(unique(aliases)),
        "address": address,
        "phone": "",
        "days": "每日",
        "open": "",
        "close": "",
        "notes": "由 Watsons 官方門市頁匯入；營業時間需用 Google Sheet、官方門市詳細頁或人工確認後填入。",
        "source_url": source_url,
    }


def unique(items: list[str]) -> list[str]:
    result: list[str] = []
    for item in items:
        item = item.strip()
        if item and item not in result:
            result.append(item)
    return result


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "store_name",
        "aliases",
        "address",
        "phone",
        "days",
        "open",
        "close",
        "notes",
        "source_url",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
