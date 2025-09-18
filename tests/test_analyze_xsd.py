from pathlib import Path
from lxml import etree
import src.analyze_xsd as m

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
