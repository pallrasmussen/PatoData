from pathlib import Path

import pytest
from lxml import etree

import src.analyze_xsd as m


def test_summarize_schema_rejects_missing_file(tmp_path: Path):
    missing = tmp_path / "missing.xsd"
    with pytest.raises(FileNotFoundError, match="XSD not found"):
        m.summarize_schema(missing)


def test_summarize_schema_lists_globals(tmp_path: Path):
    xsd = tmp_path / "mini.xsd"
    xsd.write_text(
        """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="Root" type="xs:string"/>
          <xs:simpleType name="CodeType">
            <xs:restriction base="xs:string"/>
          </xs:simpleType>
        </xs:schema>
        """,
        encoding="utf-8",
    )
    summary = m.summarize_schema(xsd)
    assert "Global elements" in summary
    assert "Root" in summary


def test_generate_example_xml_returns_tree(tmp_path: Path):
    xsd = tmp_path / "mini.xsd"
    xsd.write_text(
        """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="Root">
            <xs:complexType>
              <xs:sequence>
                <xs:element name="Child" type="xs:string" />
              </xs:sequence>
            </xs:complexType>
          </xs:element>
        </xs:schema>
        """,
        encoding="utf-8",
    )
    tree = m.generate_example_xml(xsd)
    assert isinstance(tree, etree._ElementTree)
    assert tree.getroot().tag == "Root"


def test_generate_example_xml_raises_for_empty_schema(tmp_path: Path):
    xsd = tmp_path / "empty.xsd"
    xsd.write_text(
        """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" />
        """,
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="No global elements found"):
        m.generate_example_xml(xsd)


def test_main_writes_json_summary_file(tmp_path: Path):
    xsd = tmp_path / "mini.xsd"
    xsd.write_text(
        """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="Root" type="xs:string"/>
          <xs:complexType name="PersonType">
            <xs:sequence>
              <xs:element name="Name" type="xs:string"/>
            </xs:sequence>
          </xs:complexType>
        </xs:schema>
        """,
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"

    m.main(["--xsd", str(xsd), "--out", str(out_dir), "--json"])

    summary_path = out_dir / "schema_summary.json"
    assert summary_path.exists()
    payload = summary_path.read_text(encoding="utf-8")
    assert '"schema"' in payload
    assert '"tool_version"' in payload
    assert '"global_elements"' in payload


def test_main_version_flag_prints_version(capsys):
    m.main(["--version"])
    assert capsys.readouterr().out.strip() == "0.1.0"
