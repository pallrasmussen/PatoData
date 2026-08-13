from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import Any, List

from rich import print
import xmlschema
from lxml import etree

__version__ = "0.1.0"


def _require_xsd(xsd_path: Path) -> Path:
    path = Path(xsd_path)
    if not path.exists():
        raise FileNotFoundError(f"XSD not found: {path}")
    if not path.is_file():
        raise FileNotFoundError(f"XSD path is not a file: {path}")
    return path


def _schema_name(item) -> str:
    local_name = getattr(item, "local_name", None)
    if local_name:
        return str(local_name)
    name = getattr(item, "name", None)
    if name:
        return str(name)
    return "element"


def _sample_value_for_type(type_obj) -> str:
    if type_obj is None:
        return "value"
    type_name = getattr(type_obj, "name", None)
    if type_name:
        return type_name.split(".")[-1]
    return "value"


def _append_example_children(parent: etree._Element, element_or_type) -> None:
    content = getattr(element_or_type, "content", None)
    if content is None:
        return

    for child in content.iter_elements():
        child_el = etree.SubElement(parent, _schema_name(child))
        child_type = getattr(child, "type", None)

        if child_type is not None and getattr(child_type, "is_simple", False):
            child_el.text = _sample_value_for_type(child_type)
            continue

        child_content = getattr(child_type, "content", None)
        if child_content is not None:
            _append_example_children(child_el, child_type)


def summarize_schema(xsd_path: Path) -> str:
    schema_path = _require_xsd(xsd_path)
    schema = xmlschema.XMLSchema11(schema_path)
    lines: List[str] = []
    lines.append(f"Schema: {schema_path}")
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


def summarize_schema_json(xsd_path: Path) -> dict[str, Any]:
    schema_path = _require_xsd(xsd_path)
    schema = xmlschema.XMLSchema11(schema_path)
    global_elements = []
    for qname, elem in schema.elements.items():
        global_elements.append({
            "name": str(qname),
            "type": elem.type.name if elem.type else "anyType",
        })

    global_types = []
    for qname, t in schema.types.items():
        base = getattr(getattr(t, 'base_type', None), 'name', None)
        global_types.append({
            "name": str(qname),
            "base": base,
        })

    return {
        "schema": str(schema_path),
        "tool_version": __version__,
        "schema_version": getattr(schema, "version", "n/a"),
        "global_elements": global_elements,
        "global_types": global_types,
    }


def generate_example_xml(xsd_path: Path) -> etree._ElementTree:
    schema_path = _require_xsd(xsd_path)
    schema = xmlschema.XMLSchema11(schema_path)
    if not schema.elements:
        raise ValueError("No global elements found in schema; cannot generate example")

    root_qname, root_elem = next(iter(schema.elements.items()))
    root = etree.Element(_schema_name(root_elem))

    root_type = getattr(root_elem, "type", None)
    if root_type is None:
        return etree.ElementTree(root)

    if getattr(root_type, "is_simple", False):
        root.text = _sample_value_for_type(root_type)
        return etree.ElementTree(root)

    _append_example_children(root, root_type)
    return etree.ElementTree(root)


def main(argv: List[str] | None = None):
    parser = argparse.ArgumentParser(description="Analyze an XSD and produce summary and example XML")
    parser.add_argument("--xsd", help="Path to the .xsd file")
    parser.add_argument("--out", default="out", help="Output folder for artifacts")
    parser.add_argument("--json", action="store_true", help="Write a JSON summary alongside the text summary")
    parser.add_argument("--version", action="store_true", help="Show the analyzer version and exit")
    args = parser.parse_args(argv)

    if args.version:
        print(__version__)
        return

    if not args.xsd:
        parser.error("the following arguments are required: --xsd")

    xsd_path = Path(args.xsd)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        xsd_path = _require_xsd(xsd_path)
    except FileNotFoundError as exc:
        raise SystemExit(str(exc)) from exc

    print(f"[bold]Analyzing[/bold] {xsd_path}")

    summary = summarize_schema(xsd_path)
    (out_dir / "schema_summary.txt").write_text(summary, encoding="utf-8")
    print("- Wrote out/schema_summary.txt")

    if args.json:
        payload = summarize_schema_json(xsd_path)
        (out_dir / "schema_summary.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print("- Wrote out/schema_summary.json")

    try:
        tree = generate_example_xml(xsd_path)
        tree.write(str(out_dir / "example.xml"), xml_declaration=True, encoding="utf-8", pretty_print=True)
        print("- Wrote out/example.xml")
    except Exception as e:
        print(f"[yellow]- Skipped example XML generation: {e}")


if __name__ == "__main__":
    main()
