from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
from io import BytesIO, StringIO
from lxml import etree


REMAP_ATTRS = {"t": "type", "v": "value", "u": "unit"}
ATTRS_KEY = "_attrs"

INTERESTING_ATTRS = {
    "type",
    "value",
    "unit",
    "name",
    "index",
    "reference_id",
    "map_type",
    "data_type",
    "orientation",
}


def _local_tag(el: etree._Element) -> str:
    return etree.QName(el).localname


def _try_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(str(x))
    except Exception:
        return None


def _cast_value(type_str: Optional[str], raw: Any) -> Any:
    if raw is None:
        return None
    if not isinstance(raw, str):
        return raw
    if not type_str:
        return raw

    t = type_str.strip().lower()
    if t == "string":
        return raw
    if t in {"int", "integer"}:
        try:
            return int(raw)
        except ValueError:
            return raw
    if t in {"double", "float", "real"}:
        try:
            return float(raw)
        except ValueError:
            return raw
    if t in {"bool", "boolean"}:
        return raw.strip().lower() in {"yes", "true", "1"}
    if t == "date":
        for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                pass
        return raw
    return raw


def _sort_lists_by_index(obj: Any) -> None:
    """Recursively sort any list of dict-nodes by numeric _attrs.index when possible."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == ATTRS_KEY:
                continue
            _sort_lists_by_index(v)
        return

    if isinstance(obj, list):
        idxs = []
        for item in obj:
            if not isinstance(item, dict):
                idxs.append(None)
                continue
            attrs = item.get(ATTRS_KEY, {}) or {}
            idxs.append(_try_int(attrs.get("index")))

        if obj and all(i is not None for i in idxs):
            obj.sort(key=lambda it: _try_int(((it.get(ATTRS_KEY) or {}).get("index"))) or 0)

        for item in obj:
            _sort_lists_by_index(item)


def asix_to_compact_dict(
    root: etree._Element,
    *,
    always_list: bool = False,
    cast_values: bool = False,
    keep_all_attributes: bool = True,
) -> dict[str, Any]:
    """
    Convert ASIX XML root to compact nested dict.

    - always_list=False: singletons are dict; repeats are list
    - always_list=True: children[tag] always list
    - cast_values=True: cast _attrs.value using _attrs.type
    - keep_all_attributes=True: keep all attributes under _attrs (with t/v/u remapped)
    """

    def convert(el: etree._Element, extra_attributes: bool = False) -> dict[str, Any]:
        node: dict[str, Any] = {}

        # Attributes
        attrs_dict: dict[str, Any] = {}
        for k, v in el.attrib.items():
            kk = REMAP_ATTRS.get(k, k)
            if keep_all_attributes or kk in INTERESTING_ATTRS:
                attrs_dict[kk] = v

        if cast_values and "value" in attrs_dict:
            attrs_dict["value"] = _cast_value(attrs_dict.get("type"), attrs_dict.get("value"))

        if attrs_dict:
            if extra_attributes:
                node[ATTRS_KEY] = attrs_dict
            else:
                node.update(attrs_dict)

        # Children grouped by tag
        groups: dict[str, list[dict[str, Any]]] = {}
        for child in el:
            if not isinstance(child.tag, str):  # skip comments/PIs
                continue
            tag = _local_tag(child)
            groups.setdefault(tag, []).append(convert(child))

        for tag, items in groups.items():
            node[tag] = items if always_list else (items[0] if len(items) == 1 else items)

        return node

    out = {_local_tag(root): convert(root)}
    _sort_lists_by_index(out)
    return out


def parse_asix(asix_file, always_list: bool = False, 
               cast_values: bool = False, keep_all_attributes: bool = True) -> dict[str, Any]:
    parser = etree.XMLParser(remove_comments=True, huge_tree=True)
    if hasattr(asix_file, "read"):
        content = asix_file.read()

        if isinstance(content, str):
            tree = etree.parse(StringIO(content), parser)
        else:
            tree = etree.parse(BytesIO(content), parser)
    else:
        tree = etree.parse(str(asix_file), parser)

    root = tree.getroot()
    return asix_to_compact_dict(root, always_list=always_list, 
                                cast_values=cast_values, keep_all_attributes=keep_all_attributes)
