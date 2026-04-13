#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from collections.abc import Iterable
from copy import deepcopy
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yaml

# -----------------------------
# Core FIRE M header parsing
# -----------------------------


def normalize_token(value: str) -> str:
    return value.strip().lower().replace(' ', '_')


def load_mapping_rules(path: str | Path) -> dict:
    with open(path, encoding='utf-8') as f:
        return yaml.safe_load(f)


def split_header(raw_header: str) -> list[str]:
    raw_header = raw_header.replace('(', '').replace(')', '').strip().replace(',', ':')
    return [part.strip().strip('"').strip("'") for part in raw_header.split(':')]


def classify_tokens(tokens: list[str]) -> tuple[list[str], str]:
    if not tokens:
        return [], ''
    return tokens[:-2], tokens[-2]


def apply_token_mappings(context_tokens: list[str], rules: dict) -> dict[str, Any]:
    qualifiers: dict[str, Any] = {}
    token_mappings = rules.get('token_mappings', {})

    for token in context_tokens:
        if token in token_mappings.get('top_level_domains', {}):
            qualifiers.update(token_mappings['top_level_domains'][token])
            qualifiers['raw_top_level_domain'] = token
        elif token in token_mappings.get('namespaces', {}):
            qualifiers.update(token_mappings['namespaces'][token])
            qualifiers['raw_namespace'] = token
        elif token in token_mappings.get('boundaries', {}):
            qualifiers.update(token_mappings['boundaries'][token])
            qualifiers['raw_boundary'] = token
        elif token in token_mappings.get('internal_domains', {}):
            qualifiers.update(token_mappings['internal_domains'][token])
            qualifiers['raw_internal_domain'] = token
        elif token in token_mappings.get('phases', {}):
            qualifiers.update(token_mappings['phases'][token])
        else:
            qualifiers.setdefault('unresolved_tokens', [])
            qualifiers['unresolved_tokens'].append(token)

    return qualifiers


def match_descriptor(descriptor: str, rules: dict) -> tuple[str | None, dict[str, Any]]:
    for entry in rules.get('descriptor_patterns', []):
        match = re.match(entry['pattern'], descriptor)
        if not match:
            continue

        qualifiers = dict(entry.get('fixed_qualifiers', {}))
        for source_group, target_key in entry.get('capture_to_qualifiers', {}).items():
            value = match.group(source_group)
            if value:
                qualifiers[target_key] = normalize_token(value)

        return entry['variable_id'], qualifiers

    return None, {}


def parse_firem_header(raw_header: str, rules: dict) -> dict:
    tokens = split_header(raw_header)
    context_tokens, descriptor = classify_tokens(tokens)

    token_qualifiers = apply_token_mappings(context_tokens, rules)
    variable_id, descriptor_qualifiers = match_descriptor(descriptor, rules)

    qualifiers: dict[str, Any] = {
        'source_software': rules['general_rules']['source_software'],
        'system': rules['general_rules']['system'],
        'acquisition_origin': rules['general_rules']['acquisition_origin'],
        'representation': rules['general_rules']['representation'],
        'units': tokens[-1],  # if tokens[-1][0].islower() else None,
    }
    qualifiers.update(token_qualifiers)
    qualifiers.update(descriptor_qualifiers)

    record = {
        'source_name_raw': raw_header,
        'tokens': tokens,
        'descriptor_raw': descriptor,
        'variable_id': variable_id or 'unmapped',
        'qualifiers': qualifiers,
    }

    if variable_id is None:
        record['unmapped_descriptor'] = descriptor

    return record


# -----------------------------
# Generic ASIX utilities
# -----------------------------


def _walk(
    obj: Any, path: tuple[Any, ...] = ()
) -> Iterable[tuple[tuple[Any, ...], Any]]:
    yield path, obj
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk(v, path + (k,))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _walk(v, path + (i,))


def _extract_name(node: Any) -> str | None:
    if isinstance(node, dict):
        for key in ('name', 'Name', '@name', '_name', 'id', 'ID'):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _extract_material_name(node: Any) -> str | None:
    if not isinstance(node, dict):
        return None

    candidate_keys = [
        'material_name',
        'MaterialName',
        'material',
        'Material',
        'mat_name',
        'material_id',
        'MaterialID',
        'phase_material_name',
    ]
    for key in candidate_keys:
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for k, v in node.items():
        if (
            isinstance(k, str)
            and 'material' in k.lower()
            and isinstance(v, str)
            and v.strip()
        ):
            return v.strip()

    return None


def _extract_aggregate_state(node: Any) -> str | None:
    if not isinstance(node, dict):
        return None

    candidate_keys = ['aggregate_state', 'AggregateState', 'phase_state', 'state']
    for key in candidate_keys:
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()

    for k, v in node.items():
        if isinstance(k, str) and 'aggregate' in k.lower() and 'state' in k.lower():
            if isinstance(v, str) and v.strip():
                return v.strip().lower()

    return None


def build_phase_map_from_asix(asix_dict: dict) -> dict[str, dict[str, dict[str, Any]]]:
    """
    Best-effort extractor.

    Returns
    -------
    {
      "<selection_or_domain_name>": {
          "Phase 1": {"phase_state": "gas", "phase_material_name": "..."},
          "Phase 2": {...}
      }
    }

    This function is intentionally tolerant because ASIX-to-dict conversion
    structures vary a lot. It searches recursively for dict nodes that contain
    entries named Phase 1 / Phase 2 / ... and tries to associate them with the
    nearest parent object carrying a name/id.
    """
    result: dict[str, dict[str, dict[str, Any]]] = {}

    all_nodes = list(_walk(asix_dict))
    named_nodes = {
        path: _extract_name(node) for path, node in all_nodes if _extract_name(node)
    }

    for path, node in all_nodes:
        if not isinstance(node, dict):
            continue

        phase_entries: dict[str, dict[str, Any]] = {}
        for k, v in node.items():
            if isinstance(k, str) and re.fullmatch(r'Phase\s+\d+', k.strip()):
                phase_entries[k.strip()] = {
                    'phase_state': _extract_aggregate_state(v),
                    'phase_material_name': _extract_material_name(v),
                }
            elif isinstance(v, dict):
                name = _extract_name(v)
                if isinstance(name, str) and re.fullmatch(r'Phase\s+\d+', name.strip()):
                    phase_entries[name.strip()] = {
                        'phase_state': _extract_aggregate_state(v),
                        'phase_material_name': _extract_material_name(v),
                    }

        if not phase_entries:
            continue

        parent_name = None
        for i in range(len(path), -1, -1):
            candidate_path = path[:i]
            candidate_name = named_nodes.get(candidate_path)
            if candidate_name and not re.fullmatch(
                r'Phase\s+\d+', candidate_name.strip()
            ):
                parent_name = candidate_name
                break

        if parent_name:
            result.setdefault(parent_name, {}).update(phase_entries)

    return result


def build_domain_lookup_from_asix(asix_dict: dict) -> dict[str, dict[str, Any]]:
    """
    Best-effort domain lookup indexed by any encountered name/id.
    Useful for attaching component/domain hints from the ASIX structure.
    """
    out: dict[str, dict[str, Any]] = {}
    for _, node in _walk(asix_dict):
        if not isinstance(node, dict):
            continue
        name = _extract_name(node)
        if not name:
            continue

        entry: dict[str, Any] = {}
        for key in ('component', 'domain', 'side', 'selection_type', 'region_type'):
            if key in node and isinstance(node[key], str):
                entry[key] = node[key]
        material_name = _extract_material_name(node)
        if material_name:
            entry['material_name'] = material_name
        if entry:
            out[name] = entry
    return out


# -----------------------------
# ASIX enrichment hooks
# -----------------------------


def enrich_with_asix(
    parsed_record: dict,
    asix_dict: dict | None = None,
    phase_map: dict[str, dict[str, dict[str, Any]]] | None = None,
    domain_lookup: dict[str, dict[str, Any]] | None = None,
) -> dict:
    record = deepcopy(parsed_record)
    qualifiers = record['qualifiers']

    if asix_dict is None:
        return record

    if phase_map is None:
        phase_map = build_phase_map_from_asix(asix_dict)
    if domain_lookup is None:
        domain_lookup = build_domain_lookup_from_asix(asix_dict)

    top_domain = qualifiers.get('raw_top_level_domain')
    inner_domain = qualifiers.get('raw_internal_domain')
    phase_label = qualifiers.get('phase_label_raw')

    # Phase enrichment
    if phase_label:
        for domain_key in [inner_domain, top_domain]:
            if (
                domain_key
                and domain_key in phase_map
                and phase_label in phase_map[domain_key]
            ):
                phase_info = phase_map[domain_key][phase_label]
                if phase_info.get('phase_state'):
                    qualifiers['phase_state'] = phase_info['phase_state']
                if phase_info.get('phase_material_name'):
                    qualifiers['phase_material_name'] = phase_info[
                        'phase_material_name'
                    ]
                qualifiers['phase_resolved_from'] = domain_key
                break

    # Domain enrichment
    for domain_key, field_prefix in [
        (top_domain, 'top_domain'),
        (inner_domain, 'internal_domain'),
    ]:
        if domain_key and domain_key in domain_lookup:
            entry = domain_lookup[domain_key]
            for k, v in entry.items():
                qualifiers.setdefault(k, v)
            qualifiers[f'{field_prefix}_resolved'] = domain_key

    return record


# -----------------------------
# Notebook-facing helpers
# -----------------------------


def normalize_2d_results_columns(
    df: pd.DataFrame,
    asix_dict: dict | None,
    rules: yaml.YAMLObject | dict,
) -> pd.DataFrame:
    """
    Parse all FIRE M 2D result column names and return a mapping dataframe.

    Expected usage in a notebook:
        mapping_df = normalize_2d_results_columns(df_2d, asix_dict, "firem_2d_results.yaml")
    """
    # rules = load_mapping_rules(rules_path)
    rows: list[dict[str, Any]] = []

    phase_map = build_phase_map_from_asix(asix_dict) if asix_dict is not None else None
    domain_lookup = (
        build_domain_lookup_from_asix(asix_dict) if asix_dict is not None else None
    )

    for col in df.columns:
        parsed = parse_firem_header(str(col), rules)
        enriched = enrich_with_asix(
            parsed,
            asix_dict=asix_dict,
            phase_map=phase_map,
            domain_lookup=domain_lookup,
        )

        rows.append(
            {
                'raw_column': col,
                'variable_id': enriched['variable_id'],
                'descriptor_raw': enriched['descriptor_raw'],
                'qualifiers_json': json.dumps(
                    enriched['qualifiers'], ensure_ascii=False, sort_keys=True
                ),
                'is_unmapped': enriched['variable_id'] == 'unmapped',
            }
        )

    return pd.DataFrame(rows)


def rename_2d_results_columns(
    df: pd.DataFrame,
    asix_dict: dict | None,
    rules: yaml.YAMLObject | dict,
    sep: str = '__',
) -> pd.DataFrame:
    """
    Return a copy of df with generated canonical column names.
    The raw name is preserved in the mapping dataframe returned separately if needed.

    Generated name pattern:
        <variable_id>__<component>__<side>__<location_role>__<phase_state>

    Only non-empty pieces are kept.
    """
    # rules = load_mapping_rules(rules_path)

    phase_map = build_phase_map_from_asix(asix_dict) if asix_dict is not None else None
    domain_lookup = (
        build_domain_lookup_from_asix(asix_dict) if asix_dict is not None else None
    )

    rename_map: dict[str, str] = {}
    used: dict[str, int] = {}
    df_single_level = df.copy()
    first_column_level = df_single_level.columns.get_level_values(0)
    df_single_level.columns = first_column_level
    first_column_level_list = list(first_column_level)

    for i, col in enumerate(df.columns):
        parsed = parse_firem_header(str(col), rules)
        enriched = enrich_with_asix(
            parsed,
            asix_dict=asix_dict,
            phase_map=phase_map,
            domain_lookup=domain_lookup,
        )
        q = enriched['qualifiers']
        if enriched['variable_id'] == 'unmapped':
            variable_name = f'unmapped{sep}{enriched["tokens"][-2]}'
        else:
            variable_name = enriched['variable_id']
        parts = [
            variable_name,
            q.get('component'),
            q.get('side'),
            q.get('location_role'),
            q.get('phase_state'),
        ]
        parts = [str(p) for p in parts if p and str(p).strip()]
        new_name = sep.join(parts) if parts else str(col)

        used[new_name] = used.get(new_name, 0) + 1
        if used[new_name] > 1:
            new_name = f'{new_name}{sep}{used[new_name]}'

        rename_map[first_column_level_list[i]] = new_name

    return df_single_level.rename(columns=rename_map), rename_map


def normalize_case_bundle(
    cases: dict[str, dict[str, Any]],
    rules_path: str | Path,
) -> dict[str, dict[str, Any]]:
    """
    Notebook-friendly wrapper for multiple cases.

    Expected input shape:
        cases = {
            "case_a": {
                "df_2d": df_case_a,
                "asix_dict": asix_case_a
            },
            ...
        }

    Returns:
        {
            "case_a": {
                "mapping_df": ...,
                "df_renamed": ...,
            },
            ...
        }
    """
    out: dict[str, dict[str, Any]] = {}
    for case_name, bundle in cases.items():
        df = bundle['df_2d']
        asix_dict = bundle.get('asix_dict')
        mapping_df = normalize_2d_results_columns(df, asix_dict, rules_path)
        df_renamed = rename_2d_results_columns(df, asix_dict, rules_path)
        out[case_name] = {
            'mapping_df': mapping_df,
            'df_renamed': df_renamed,
        }
    return out


def load_yaml_from_github():
    url = 'https://raw.githubusercontent.com/ZBT-Tools/rdm-variable-registry/b141be52b96e135b6f0317a544829ce0031b369d/aliases/firem_2d_results.yaml'
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return yaml.safe_load(response.text)


# -----------------------------
# Example notebook snippet
# -----------------------------

NOTEBOOK_SNIPPET = r"""
from firem_name_parser_integration import (
    normalize_2d_results_columns,
    rename_2d_results_columns,
    normalize_case_bundle,
)

rules_path = "firem_2d_results.yaml"

# Single case
mapping_df = normalize_2d_results_columns(df_2d, asix_dict, rules_path)
df_2d_renamed = rename_2d_results_columns(df_2d, asix_dict, rules_path)

# Multiple cases
cases = {
    "case_001": {"df_2d": df_case_001, "asix_dict": asix_case_001},
    "case_002": {"df_2d": df_case_002, "asix_dict": asix_case_002},
}
normalized = normalize_case_bundle(cases, rules_path)

mapping_df_case_001 = normalized["case_001"]["mapping_df"]
df_case_001_renamed = normalized["case_001"]["df_renamed"]
"""


if __name__ == '__main__':
    print(NOTEBOOK_SNIPPET)
