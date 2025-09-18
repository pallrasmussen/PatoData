from __future__ import annotations
import argparse
from pathlib import Path
from typing import List

from rich import print
from rich.table import Table
import xmlschema
from lxml import etree


def summarize_schema(xsd_path: Path) -> str:
    schema = xmlschema.XMLSchema11(xsd_path)
    lines: List[str] = []
    lines.append(f"Schema: {xsd_path}")
    lines.append(f"Version: {getattr(schema, 'version', 'n/a')}")
    lines.append("")
    lines.append("Global elements:")
    for qname, elem in schema.elements.items():
        lines.append(f"- {qname} -> type={elem.type.name if elem.type else 'anyType'}")
    lines.append("")
    lines.append("Global types:")
    for qname, t in schema.types.items():
        base = getattr(getattr(t, 'base_type', None), 'name', None)
        lines.append(f"- {qname} (base={base})")
    return "\n".join(lines)


def generate_example_xml(xsd_path: Path) -> etree._ElementTree:
    schema = xmlschema.XMLSchema11(xsd_path)
    # pick the first global element as root for example
    if not schema.elements:
        raise ValueError("No global elements found in schema; cannot generate example")
    root_qname = next(iter(schema.elements))
    example = schema.elements[root_qname].to_etree()
    return etree.ElementTree(example)


def main():
    parser = argparse.ArgumentParser(description="Analyze an XSD and produce summary and example XML")
    parser.add_argument("--xsd", required=True, help="Path to the .xsd file")
    parser.add_argument("--out", default="out", help="Output folder for artifacts")
    args = parser.parse_args()

    xsd_path = Path(args.xsd)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not xsd_path.exists():
        raise SystemExit(f"XSD not found: {xsd_path}")

    print(f"[bold]Analyzing[/bold] {xsd_path}")

    summary = summarize_schema(xsd_path)
    (out_dir / "schema_summary.txt").write_text(summary, encoding="utf-8")
    print("- Wrote out/schema_summary.txt")

    try:
        tree = generate_example_xml(xsd_path)
        tree.write(str(out_dir / "example.xml"), xml_declaration=True, encoding="utf-8", pretty_print=True)
        print("- Wrote out/example.xml")
    except Exception as e:
        print(f"[yellow]- Skipped example XML generation: {e}")


if __name__ == "__main__":
    main()
