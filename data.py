"""
NutriVault - Production Data Ingestion & ETL Pipeline (v9)
==========================================================
A verification-first, transactional multi-source ETL pipeline and calculation engine
for USDA FoodData Central, ICMR-NIN IFCT 2017, and Open Food Facts.

Key Guarantees in v9:
1. Repaired Fault-Injection Boundary Test (`test_27`):
   - Isolates each batch-boundary failure (`fail_on_batch = 1..5`) in its own temporary SQLite
     database, removes the premature unpatched `main()` call, verifies exact pre-resume record
     and checkpoint counts (`(fail_on_batch - 1) * 2`), and verifies clean resumption to 10/10
     records with full provenance and FTS5 parity.
   - Tests CLI disk-full error handling in a separate isolated database (`cli_disk_full.db`).
2. Mid-Transaction Rollback & Input-Signature Reset Verification (`test_29`):
   - Proves that a failure occurring mid-batch inside `BEGIN IMMEDIATE` rolls back all prior
     inserts from that batch and leaves the checkpoint unchanged.
   - Proves that modifying an input file invalidates the prior checkpoint signature and restarts
     from offset 0.
3. SQLite Concurrency Hardening:
   - Configures `PRAGMA busy_timeout = 5000;` alongside `PRAGMA foreign_keys = ON;` and `WAL` mode.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
from dataclasses import dataclass, field
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import time
import tracemalloc
import unittest
from unittest import mock
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("NutriVault.ETL")

# ============================================================================
# CLI EXIT CODES
# ============================================================================
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_STRICT_REJECTIONS = 2
EXIT_EMPTY_INPUT = 3
EXIT_REPORT_WRITE_ERROR = 4

# ============================================================================
# CONSTANTS & CANONICAL SCHEMA DEFINITIONS
# ============================================================================

NUTRIENT_FIELDS: Tuple[str, ...] = (
    "calories_kcal",
    "protein_g",
    "carbohydrate_g",
    "fat_g",
    "fiber_g",
    "sugar_g",
    "sodium_mg",
    "saturated_fat_g",
    "monounsaturated_fat_g",
    "polyunsaturated_fat_g",
    "cholesterol_mg",
    "potassium_mg",
    "calcium_mg",
    "iron_mg",
    "magnesium_mg",
    "zinc_mg",
    "vitamin_c_mg",
    "vitamin_d_mcg",
    "histidine_mg",
    "isoleucine_mg",
    "leucine_mg",
    "lysine_mg",
    "methionine_mg",
    "phenylalanine_mg",
    "threonine_mg",
    "tryptophan_mg",
    "valine_mg",
)

EXPECTED_NUTRIENT_UNITS: Dict[str, str] = {
    "calories_kcal": "kcal",
    "protein_g": "g",
    "carbohydrate_g": "g",
    "fat_g": "g",
    "fiber_g": "g",
    "sugar_g": "g",
    "sodium_mg": "mg",
    "saturated_fat_g": "g",
    "monounsaturated_fat_g": "g",
    "polyunsaturated_fat_g": "g",
    "cholesterol_mg": "mg",
    "potassium_mg": "mg",
    "calcium_mg": "mg",
    "iron_mg": "mg",
    "magnesium_mg": "mg",
    "zinc_mg": "mg",
    "vitamin_c_mg": "mg",
    "vitamin_d_mcg": "mcg",
    "histidine_mg": "mg",
    "isoleucine_mg": "mg",
    "leucine_mg": "mg",
    "lysine_mg": "mg",
    "methionine_mg": "mg",
    "phenylalanine_mg": "mg",
    "threonine_mg": "mg",
    "tryptophan_mg": "mg",
    "valine_mg": "mg",
}

SOURCE_PRIORITY: Dict[str, int] = {
    "usda": 100,
    "ifct": 90,
    "user": 80,
    "core": 70,
    "openfoodfacts": 50,
}

# USDA Nutrient IDs & Energy Precedence:
# 1008 = Energy (kcal), 2047 = Energy (Atwater General Factors), 2048 = Energy (Atwater Specific Factors), 1062 = Energy (kJ)
USDA_ENERGY_PRECEDENCE: Tuple[int, ...] = (1008, 2047, 2048, 1062)

USDA_NUTRIENT_MAP: Dict[int, str] = {
    1003: "protein_g",
    1005: "carbohydrate_g",
    1004: "fat_g",
    1079: "fiber_g",
    2000: "sugar_g",
    1093: "sodium_mg",
    1258: "saturated_fat_g",
    1292: "monounsaturated_fat_g",
    1293: "polyunsaturated_fat_g",
    1253: "cholesterol_mg",
    1092: "potassium_mg",
    1087: "calcium_mg",
    1089: "iron_mg",
    1090: "magnesium_mg",
    1095: "zinc_mg",
    1162: "vitamin_c_mg",
    1114: "vitamin_d_mcg",
    1221: "histidine_mg",
    1212: "isoleucine_mg",
    1213: "leucine_mg",
    1214: "lysine_mg",
    1215: "methionine_mg",
    1217: "phenylalanine_mg",
    1211: "threonine_mg",
    1210: "tryptophan_mg",
    1219: "valine_mg",
}

KJ_TO_KCAL_DIVISOR = 4.184
SALT_TO_SODIUM_RATIO = 0.40  # 1 g salt (NaCl) = 0.4 g sodium = 400 mg sodium


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class NutrientProvenance:
    nutrient_name: str
    source: str
    source_id: str
    raw_field: str
    raw_value: float
    raw_unit: str
    normalized_value: float
    normalized_unit: str
    basis_unit: str
    conversion_applied: str


@dataclass
class FoodPortionRecord:
    portion_id: str
    food_id: str
    portion_name: str
    amount: float = 1.0
    measure_unit: str = "serving"
    gram_weight: Optional[float] = None
    ml_volume: Optional[float] = None

    def validate(self) -> List[str]:
        errors: List[str] = []
        if not self.portion_name or not self.portion_name.strip():
            errors.append("Portion name cannot be empty.")
        if not is_finite_number(self.amount) or self.amount <= 0:
            errors.append(f"Portion amount must be finite and > 0, got {self.amount}.")
        has_g = self.gram_weight is not None
        has_ml = self.ml_volume is not None
        if not has_g and not has_ml:
            errors.append("Portion must specify at least one of gram_weight or ml_volume.")
        if has_g and (not is_finite_number(self.gram_weight) or self.gram_weight <= 0):
            errors.append(f"Portion gram_weight must be finite and > 0, got {self.gram_weight}.")
        if has_ml and (not is_finite_number(self.ml_volume) or self.ml_volume <= 0):
            errors.append(f"Portion ml_volume must be finite and > 0, got {self.ml_volume}.")
        return errors


@dataclass
class CanonicalFoodRecord:
    food_id: str
    food_name: str
    source: str
    source_id: str
    brand: Optional[str] = None
    barcode: Optional[str] = None
    category: Optional[str] = None
    basis_unit: str = "100g"  # Strictly '100g' or '100ml'
    serving_size_g: Optional[float] = None
    serving_size_ml: Optional[float] = None
    density_g_per_ml: Optional[float] = None

    # 27 Canonical Nutrient Fields (None means unknown/missing, never coerced to 0.0)
    calories_kcal: Optional[float] = None
    protein_g: Optional[float] = None
    carbohydrate_g: Optional[float] = None
    fat_g: Optional[float] = None
    fiber_g: Optional[float] = None
    sugar_g: Optional[float] = None
    sodium_mg: Optional[float] = None
    saturated_fat_g: Optional[float] = None
    monounsaturated_fat_g: Optional[float] = None
    polyunsaturated_fat_g: Optional[float] = None
    cholesterol_mg: Optional[float] = None
    potassium_mg: Optional[float] = None
    calcium_mg: Optional[float] = None
    iron_mg: Optional[float] = None
    magnesium_mg: Optional[float] = None
    zinc_mg: Optional[float] = None
    vitamin_c_mg: Optional[float] = None
    vitamin_d_mcg: Optional[float] = None
    histidine_mg: Optional[float] = None
    isoleucine_mg: Optional[float] = None
    leucine_mg: Optional[float] = None
    lysine_mg: Optional[float] = None
    methionine_mg: Optional[float] = None
    phenylalanine_mg: Optional[float] = None
    threonine_mg: Optional[float] = None
    tryptophan_mg: Optional[float] = None
    valine_mg: Optional[float] = None

    portions: List[FoodPortionRecord] = field(default_factory=list)
    provenance: Dict[str, NutrientProvenance] = field(default_factory=dict)


@dataclass
class ValidationIssue:
    severity: str  # 'CRITICAL' or 'WARNING'
    code: str
    message: str
    field_name: Optional[str] = None


@dataclass
class ValidationResult:
    is_valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)

    @property
    def critical_issues(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "CRITICAL"]

    @property
    def warnings(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "WARNING"]


@dataclass
class RejectedRecord:
    source: str
    source_id: str
    raw_identifier: str
    reasons: List[str]


# ============================================================================
# NORMALIZATION & UNIT CONVERSION UTILITIES
# ============================================================================

def is_finite_number(val: Any) -> bool:
    if isinstance(val, bool) or val is None:
        return False
    try:
        f = float(val)
        return math.isfinite(f)
    except (ValueError, TypeError, OverflowError):
        return False


def parse_optional_float(val: Any) -> Tuple[Optional[float], Optional[str]]:
    """
    Returns (parsed_float_or_None, error_message_or_None).
    Empty string or None -> (None, None).
    Non-finite or unparseable string -> (None, error_reason).
    """
    if val is None:
        return None, None
    if isinstance(val, bool):
        return None, f"Boolean value {val!r} is not a valid numeric nutrient."
    if isinstance(val, (int, float)):
        f = float(val)
        if not math.isfinite(f):
            return None, f"Non-finite numeric value: {val!r}"
        return f, None
    if isinstance(val, str):
        s = val.strip()
        if s == "" or s.lower() in {"null", "none", "na", "n/a", "-", "nd", "tr", "trace"}:
            return None, None
        try:
            f = float(s)
            if not math.isfinite(f):
                return None, f"Non-finite numeric string: {val!r}"
            return f, None
        except ValueError:
            return None, f"Unparseable numeric string: {val!r}"
    return None, f"Unsupported value type {type(val).__name__}"


def normalize_unit_token(unit: Optional[str]) -> Optional[str]:
    if unit is None or not isinstance(unit, str):
        return None
    u = unit.strip().lower()
    if not u:
        return None
    unit_map = {
        "g": "g",
        "gram": "g",
        "grams": "g",
        "gm": "g",
        "mg": "mg",
        "milligram": "mg",
        "milligrams": "mg",
        "ug": "mcg",
        "µg": "mcg",
        "μg": "mcg",
        "mcg": "mcg",
        "microgram": "mcg",
        "micrograms": "mcg",
        "kcal": "kcal",
        "cal": "kcal",
        "kilocalorie": "kcal",
        "kilocalories": "kcal",
        "kj": "kj",
        "kilojoule": "kj",
        "kilojoules": "kj",
        "ml": "ml",
        "milliliter": "ml",
        "milliliters": "ml",
        "millilitre": "ml",
        "millilitres": "ml",
        "l": "l",
        "liter": "l",
        "liters": "l",
        "litre": "l",
        "litres": "l",
    }
    return unit_map.get(u)


def convert_mass_unit(value: float, from_unit: str, to_unit: str) -> float:
    """
    Converts between 'g', 'mg', and 'mcg' preserving full IEEE-754 precision.
    Raises ValueError if units are unknown or incompatible.
    """
    if not is_finite_number(value):
        raise ValueError(f"Cannot convert non-finite value: {value!r}")
    src = normalize_unit_token(from_unit)
    dst = normalize_unit_token(to_unit)
    if src is None or dst is None:
        raise ValueError(f"Unknown mass unit conversion: {from_unit!r} -> {to_unit!r}")
    if src == dst:
        return float(value)
    factors_to_g = {
        "g": 1.0,
        "mg": 1e-3,
        "mcg": 1e-6,
    }
    if src not in factors_to_g or dst not in factors_to_g:
        raise ValueError(f"Incompatible mass conversion: {from_unit!r} -> {to_unit!r}")
    in_grams = float(value) * factors_to_g[src]
    result = in_grams / factors_to_g[dst]
    if not math.isfinite(result):
        raise OverflowError(f"Mass unit conversion overflowed: {value} {from_unit} -> {to_unit}")
    return result


def convert_energy_to_kcal(value: float, from_unit: str) -> Tuple[float, str]:
    if not is_finite_number(value):
        raise ValueError(f"Non-finite energy value: {value!r}")
    u = normalize_unit_token(from_unit)
    if u == "kcal":
        return float(value), "identity_kcal"
    if u == "kj":
        res = float(value) / KJ_TO_KCAL_DIVISOR
        if not math.isfinite(res):
            raise OverflowError("Energy conversion overflowed.")
        return res, "kj_div_4.184"
    raise ValueError(f"Unsupported energy unit: {from_unit!r}")


_SERVING_PAREN_RE = re.compile(
    r"\(\s*([+-]?\d+(?:\.\d+)?)\s*(g|grams?|gm|ml|milliliters?|millilitres?)\s*\)",
    re.IGNORECASE,
)
_SERVING_DIRECT_RE = re.compile(
    r"(?:^|\s)([+-]?\d+(?:\.\d+)?)\s*(g|grams?|gm|ml|milliliters?|millilitres?)\b",
    re.IGNORECASE,
)


def parse_serving_size_strict(
    serving_size_str: Optional[str],
    serving_quantity: Any = None,
    serving_quantity_unit: Optional[str] = None,
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Strictly parses serving size into (serving_size_g, serving_size_ml, error_msg).
    Rejects disagreements between numeric serving_quantity and serving_size_str.
    Never conflates grams and milliliters.
    """
    label_val: Optional[float] = None
    label_unit: Optional[str] = None

    if serving_size_str and isinstance(serving_size_str, str) and serving_size_str.strip():
        m = _SERVING_PAREN_RE.search(serving_size_str)
        if not m:
            m = _SERVING_DIRECT_RE.search(serving_size_str)
        if m:
            parsed_v, err = parse_optional_float(m.group(1))
            if err or parsed_v is None or parsed_v <= 0:
                return None, None, f"Invalid serving_size numeric value in {serving_size_str!r}"
            label_val = parsed_v
            label_unit = normalize_unit_token(m.group(2))

    qty_val, qty_err = parse_optional_float(serving_quantity)
    if qty_err:
        return None, None, f"Invalid serving_quantity: {qty_err}"
    if qty_val is not None and qty_val <= 0:
        return None, None, f"Non-positive serving_quantity: {qty_val}"

    qty_unit = normalize_unit_token(serving_quantity_unit) if serving_quantity_unit else None

    if label_val is not None and qty_val is not None:
        if qty_unit is not None and label_unit is not None and qty_unit != label_unit:
            return (
                None,
                None,
                f"Serving unit mismatch between serving_quantity_unit ({qty_unit}) and serving_size ({label_unit}).",
            )
        rel_diff = abs(label_val - qty_val) / max(label_val, qty_val)
        if rel_diff > 0.05:
            return (
                None,
                None,
                f"Serving quantity ({qty_val}) disagrees with serving_size label ({label_val} {label_unit}).",
            )
        final_val = label_val
        final_unit = label_unit
    elif label_val is not None:
        final_val = label_val
        final_unit = label_unit
    elif qty_val is not None:
        if qty_unit not in {"g", "ml"}:
            return (
                None,
                None,
                "Numeric serving_quantity provided without an explicit unit ('g' or 'ml').",
            )
        final_val = qty_val
        final_unit = qty_unit
    else:
        return None, None, None

    if final_unit == "g":
        return final_val, None, None
    if final_unit == "ml":
        return None, final_val, None
    return None, None, f"Unsupported serving unit: {final_unit!r}"


# ============================================================================
# BASIS-DEPENDENT VALIDATOR
# ============================================================================

class FoodRecordValidator:
    """
    Validates CanonicalFoodRecord instances according to their basis_unit ('100g' or '100ml'),
    physical mass bounds, Atwater calorie consistency, lipid/carbohydrate component sums,
    and portion integrity.
    """

    @staticmethod
    def validate(record: CanonicalFoodRecord) -> ValidationResult:
        issues: List[ValidationIssue] = []

        if not record.food_id or not record.food_id.strip():
            issues.append(ValidationIssue("CRITICAL", "MISSING_ID", "food_id is required.", "food_id"))
        if not record.food_name or not record.food_name.strip():
            issues.append(ValidationIssue("CRITICAL", "MISSING_NAME", "food_name is required.", "food_name"))
        if record.basis_unit not in {"100g", "100ml"}:
            issues.append(
                ValidationIssue(
                    "CRITICAL",
                    "INVALID_BASIS",
                    f"basis_unit must be '100g' or '100ml', got {record.basis_unit!r}.",
                    "basis_unit",
                )
            )

        if record.serving_size_g is not None and (
            not is_finite_number(record.serving_size_g) or record.serving_size_g <= 0
        ):
            issues.append(
                ValidationIssue(
                    "CRITICAL",
                    "INVALID_SERVING_G",
                    f"serving_size_g must be finite and > 0, got {record.serving_size_g!r}.",
                    "serving_size_g",
                )
            )
        if record.serving_size_ml is not None and (
            not is_finite_number(record.serving_size_ml) or record.serving_size_ml <= 0
        ):
            issues.append(
                ValidationIssue(
                    "CRITICAL",
                    "INVALID_SERVING_ML",
                    f"serving_size_ml must be finite and > 0, got {record.serving_size_ml!r}.",
                    "serving_size_ml",
                )
            )
        if record.density_g_per_ml is not None and (
            not is_finite_number(record.density_g_per_ml)
            or record.density_g_per_ml <= 0.05
            or record.density_g_per_ml > 5.0
        ):
            issues.append(
                ValidationIssue(
                    "CRITICAL",
                    "INVALID_DENSITY",
                    f"density_g_per_ml out of physical range (0.05..5.0): {record.density_g_per_ml!r}.",
                    "density_g_per_ml",
                )
            )

        populated_nutrients = 0
        for field_name in NUTRIENT_FIELDS:
            val = getattr(record, field_name)
            if val is not None:
                populated_nutrients += 1
                if not is_finite_number(val):
                    issues.append(
                        ValidationIssue(
                            "CRITICAL",
                            "NON_FINITE_NUTRIENT",
                            f"{field_name} is not finite: {val!r}",
                            field_name,
                        )
                    )
                elif val < 0:
                    issues.append(
                        ValidationIssue(
                            "CRITICAL",
                            "NEGATIVE_NUTRIENT",
                            f"{field_name} cannot be negative: {val}",
                            field_name,
                        )
                    )
                elif field_name not in record.provenance:
                    issues.append(
                        ValidationIssue(
                            "CRITICAL",
                            "MISSING_PROVENANCE",
                            f"Populated nutrient {field_name} is missing a provenance record.",
                            field_name,
                        )
                    )

        if populated_nutrients == 0:
            issues.append(
                ValidationIssue(
                    "CRITICAL",
                    "NO_NUTRIENTS",
                    "Record does not contain any populated nutrient values.",
                )
            )

        macro_upper_bound = 105.0 if record.basis_unit == "100g" else 145.0
        for macro_field in ("protein_g", "carbohydrate_g", "fat_g", "fiber_g", "sugar_g"):
            val = getattr(record, macro_field)
            if val is not None and is_finite_number(val) and val > macro_upper_bound:
                issues.append(
                    ValidationIssue(
                        "CRITICAL",
                        "MACRO_EXCEEDS_BASIS",
                        f"{macro_field}={val}g exceeds {record.basis_unit} ceiling ({macro_upper_bound}g).",
                        macro_field,
                    )
                )

        p = record.protein_g or 0.0
        c = record.carbohydrate_g or 0.0
        f = record.fat_g or 0.0
        if all(is_finite_number(x) for x in (p, c, f)):
            macro_sum = p + c + f
            if macro_sum > macro_upper_bound:
                issues.append(
                    ValidationIssue(
                        "CRITICAL",
                        "MACRO_SUM_EXCEEDS_BASIS",
                        f"Sum of protein+carb+fat ({macro_sum:.2f}g) exceeds {record.basis_unit} limit ({macro_upper_bound}g).",
                    )
                )

        if record.calories_kcal is not None and is_finite_number(record.calories_kcal):
            if record.calories_kcal > 950.0:
                issues.append(
                    ValidationIssue(
                        "CRITICAL",
                        "CALORIES_EXCEED_PHYSICAL_MAX",
                        f"calories_kcal={record.calories_kcal} exceeds physical max (950 kcal per {record.basis_unit}).",
                        "calories_kcal",
                    )
                )

            if (
                record.protein_g is not None
                and record.carbohydrate_g is not None
                and record.fat_g is not None
                and all(is_finite_number(x) for x in (record.protein_g, record.carbohydrate_g, record.fat_g))
            ):
                expected_kcal = 4.0 * record.protein_g + 4.0 * record.carbohydrate_g + 9.0 * record.fat_g
                fiber_deduction = 2.0 * (record.fiber_g or 0.0) if is_finite_number(record.fiber_g or 0.0) else 0.0
                net_expected = max(0.0, expected_kcal - fiber_deduction)
                diff = min(abs(record.calories_kcal - expected_kcal), abs(record.calories_kcal - net_expected))
                tolerance = max(35.0, 0.30 * max(expected_kcal, 1.0))
                if diff > tolerance:
                    issues.append(
                        ValidationIssue(
                            "WARNING",
                            "ATWATER_DEVIATION",
                            f"Reported calories ({record.calories_kcal:.1f}) deviate from Atwater estimate ({expected_kcal:.1f}).",
                            "calories_kcal",
                        )
                    )

        if record.carbohydrate_g is not None and is_finite_number(record.carbohydrate_g):
            if (
                record.fiber_g is not None
                and is_finite_number(record.fiber_g)
                and record.fiber_g > record.carbohydrate_g + 2.0
            ):
                issues.append(
                    ValidationIssue(
                        "WARNING",
                        "FIBER_EXCEEDS_TOTAL_CARB",
                        f"fiber_g ({record.fiber_g}) > carbohydrate_g ({record.carbohydrate_g}); source may report available carbs.",
                        "fiber_g",
                    )
                )
            if (
                record.sugar_g is not None
                and is_finite_number(record.sugar_g)
                and record.sugar_g > record.carbohydrate_g + 2.0
            ):
                issues.append(
                    ValidationIssue(
                        "CRITICAL",
                        "SUGAR_EXCEEDS_CARB",
                        f"sugar_g ({record.sugar_g}) exceeds carbohydrate_g ({record.carbohydrate_g}) beyond tolerance.",
                        "sugar_g",
                    )
                )

        if record.fat_g is not None and is_finite_number(record.fat_g):
            fat_parts = [
                x
                for x in (record.saturated_fat_g, record.monounsaturated_fat_g, record.polyunsaturated_fat_g)
                if x is not None and is_finite_number(x)
            ]
            if fat_parts and sum(fat_parts) > record.fat_g + 2.5:
                issues.append(
                    ValidationIssue(
                        "CRITICAL",
                        "FATTY_ACIDS_EXCEED_TOTAL_FAT",
                        f"Sum of fatty acid fractions ({sum(fat_parts):.2f}g) exceeds total fat ({record.fat_g:.2f}g).",
                        "fat_g",
                    )
                )

        if record.sodium_mg is not None and is_finite_number(record.sodium_mg):
            if record.sodium_mg > 42000.0:
                issues.append(
                    ValidationIssue(
                        "CRITICAL",
                        "SODIUM_EXCEEDS_PHYSICAL_MAX",
                        f"sodium_mg ({record.sodium_mg}) exceeds physical maximum (~40,000 mg/100g).",
                        "sodium_mg",
                    )
                )

        for portion in record.portions:
            p_errors = portion.validate()
            for pe in p_errors:
                issues.append(ValidationIssue("CRITICAL", "INVALID_PORTION", pe, "portions"))

        is_valid = len([i for i in issues if i.severity == "CRITICAL"]) == 0
        return ValidationResult(is_valid=is_valid, issues=issues)


# ============================================================================
# SOURCE ADAPTER 1: OPEN FOOD FACTS (LOCAL JSON/JSONL/CSV + API BARCODES)
# ============================================================================

OFF_NUTRIENT_PREFIX_MAP: Dict[str, Tuple[str, str]] = {
    "protein_g": ("proteins", "g"),
    "carbohydrate_g": ("carbohydrates", "g"),
    "fat_g": ("fat", "g"),
    "fiber_g": ("fiber", "g"),
    "sugar_g": ("sugars", "g"),
    "saturated_fat_g": ("saturated-fat", "g"),
    "monounsaturated_fat_g": ("monounsaturated-fat", "g"),
    "polyunsaturated_fat_g": ("polyunsaturated-fat", "g"),
    "cholesterol_mg": ("cholesterol", "mg"),
    "potassium_mg": ("potassium", "mg"),
    "calcium_mg": ("calcium", "mg"),
    "iron_mg": ("iron", "mg"),
    "magnesium_mg": ("magnesium", "mg"),
    "zinc_mg": ("zinc", "mg"),
    "vitamin_c_mg": ("vitamin-c", "mg"),
    "vitamin_d_mcg": ("vitamin-d", "mcg"),
    "histidine_mg": ("histidine", "mg"),
    "isoleucine_mg": ("isoleucine", "mg"),
    "leucine_mg": ("leucine", "mg"),
    "lysine_mg": ("lysine", "mg"),
    "methionine_mg": ("methionine", "mg"),
    "phenylalanine_mg": ("phenylalanine", "mg"),
    "threonine_mg": ("threonine", "mg"),
    "tryptophan_mg": ("tryptophan", "mg"),
    "valine_mg": ("valine", "mg"),
}


def _iter_json_array_stream(file_obj: Iterable[str], chunk_size: int = 65536) -> Iterator[Dict[str, Any]]:
    """
    Strict 5-state streaming JSON array parser (`START` -> `FIRST_OR_END` -> `ELEMENT`
    -> `AFTER_ELEMENT` -> `AFTER_ARRAY`).
    Rejects empty files, non-array documents, trailing commas (`[1,]`), unclosed arrays (`[1`),
    and trailing non-whitespace tokens after `]`.
    """
    decoder = json.JSONDecoder()
    buf = ""
    pos = 0
    state = "START"

    def compact_buffer() -> None:
        nonlocal buf, pos
        if pos > 0:
            buf = buf[pos:]
            pos = 0

    def skip_ws() -> None:
        nonlocal pos
        n = len(buf)
        while pos < n and buf[pos] in " \t\r\n":
            pos += 1

    def chunk_generator() -> Iterator[str]:
        if hasattr(file_obj, "read"):
            while True:
                data = file_obj.read(chunk_size)  # type: ignore[attr-defined]
                if not data:
                    break
                yield data
        else:
            for line in file_obj:
                yield line

    for chunk in chunk_generator():
        buf += chunk
        while True:
            skip_ws()
            if pos >= len(buf):
                compact_buffer()
                break

            if state == "START":
                if buf[pos] != "[":
                    raise ValueError(f"Expected '[' at start of JSON array stream, got {buf[pos]!r}")
                pos += 1
                state = "FIRST_OR_END"

            elif state == "FIRST_OR_END":
                if buf[pos] == "]":
                    pos += 1
                    state = "AFTER_ARRAY"
                else:
                    state = "ELEMENT"

            elif state == "ELEMENT":
                if buf[pos] == "]":
                    raise ValueError("Trailing comma before ']' is invalid in JSON array.")
                try:
                    obj, end_idx = decoder.raw_decode(buf, pos)
                except json.JSONDecodeError:
                    compact_buffer()
                    break
                if not isinstance(obj, dict):
                    raise ValueError(f"Expected JSON object inside array, got {type(obj).__name__}")
                yield obj
                pos = end_idx
                state = "AFTER_ELEMENT"

            elif state == "AFTER_ELEMENT":
                ch = buf[pos]
                if ch == ",":
                    pos += 1
                    state = "ELEMENT"
                elif ch == "]":
                    pos += 1
                    state = "AFTER_ARRAY"
                else:
                    raise ValueError(f"Expected ',' or ']' after array element, got {ch!r}")

            elif state == "AFTER_ARRAY":
                raise ValueError(f"Unexpected trailing content after JSON array end: {buf[pos:pos+20]!r}")

    skip_ws()
    if state == "START":
        raise ValueError("Empty input when JSON array was expected.")
    if state != "AFTER_ARRAY":
        raise ValueError(
            f"Incomplete or malformed JSON array at EOF (final state={state}, remaining={buf[pos:pos+40]!r})."
        )
    if pos < len(buf):
        raise ValueError(f"Unexpected trailing data at EOF: {buf[pos:pos+40]!r}")


class OpenFoodFactsAdapter:
    """
    Normalizes Open Food Facts JSON/JSONL records or live v2 barcode lookups
    with strict basis separation ('100g' vs '100ml') and explicit unit verification.
    """

    def __init__(
        self,
        file_path: Optional[Path] = None,
        barcodes: Optional[Sequence[str]] = None,
        http_fetcher: Optional[Callable[[str], Dict[str, Any]]] = None,
    ) -> None:
        self.file_path = Path(file_path) if file_path is not None else None
        self.barcodes = list(barcodes) if barcodes else []
        self.http_fetcher = http_fetcher or self._default_http_fetcher

        if self.file_path is None and not self.barcodes:
            raise ValueError(
                "OpenFoodFactsAdapter requires either an existing --input file_path or non-empty --barcodes."
            )
        if self.file_path is not None:
            if not self.file_path.exists():
                raise FileNotFoundError(f"Open Food Facts input file does not exist: {self.file_path}")
            if not self.file_path.is_file():
                raise ValueError(f"Open Food Facts input path is not a regular file: {self.file_path}")

    @staticmethod
    def _default_http_fetcher(barcode: str) -> Dict[str, Any]:
        url = f"https://world.openfoodfacts.org/api/v2/product/{barcode}.json"
        req = urllib.request.Request(
            url, headers={"User-Agent": "NutriVault-ETL/9.0 (Offline-First Nutrition Tracker)"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def iter_records(
        self, start_offset: int = 0
    ) -> Iterator[Tuple[int, Optional[CanonicalFoodRecord], Optional[RejectedRecord]]]:
        idx = 0
        for raw_item in self._stream_raw():
            if idx < start_offset:
                idx += 1
                continue
            rec, rej = self.normalize_record(raw_item, fallback_index=idx)
            yield idx, rec, rej
            idx += 1

    def _stream_raw(self) -> Iterator[Dict[str, Any]]:
        if self.file_path is not None:
            with self.file_path.open("r", encoding="utf-8") as f:
                first_non_ws = ""
                while True:
                    ch = f.read(1)
                    if not ch:
                        break
                    if not ch.isspace():
                        first_non_ws = ch
                        break
                f.seek(0)
                if not first_non_ws:
                    return
                if first_non_ws == "[":
                    yield from _iter_json_array_stream(f)
                else:
                    for line_no, line in enumerate(f, start=1):
                        s = line.strip()
                        if not s:
                            continue
                        try:
                            obj = json.loads(s)
                        except json.JSONDecodeError as exc:
                            yield {"_malformed_jsonl_error": f"Line {line_no}: {exc}", "_raw_line": s[:80]}
                            continue
                        if not isinstance(obj, dict):
                            yield {"_malformed_jsonl_error": f"Line {line_no}: Expected JSON object"}
                            continue
                        yield obj

        for bc in self.barcodes:
            try:
                payload = self.http_fetcher(bc)
                yield payload
            except Exception as exc:
                yield {"code": bc, "_fetch_error": str(exc)}

    @classmethod
    def normalize_record(
        cls, raw: Dict[str, Any], fallback_index: int = 0
    ) -> Tuple[Optional[CanonicalFoodRecord], Optional[RejectedRecord]]:
        if "_malformed_jsonl_error" in raw:
            return None, RejectedRecord(
                source="openfoodfacts",
                source_id=f"line_{fallback_index}",
                raw_identifier=f"line_{fallback_index}",
                reasons=[str(raw["_malformed_jsonl_error"])],
            )
        if "_fetch_error" in raw:
            bc = str(raw.get("code") or f"idx_{fallback_index}")
            return None, RejectedRecord(
                source="openfoodfacts",
                source_id=bc,
                raw_identifier=bc,
                reasons=[f"API fetch failed: {raw['_fetch_error']}"],
            )

        product = raw.get("product") if isinstance(raw.get("product"), dict) else raw
        if not isinstance(product, dict):
            return None, RejectedRecord(
                source="openfoodfacts",
                source_id=f"idx_{fallback_index}",
                raw_identifier=f"idx_{fallback_index}",
                reasons=["Payload does not contain a valid product object."],
            )

        barcode_raw = product.get("code") or product.get("_id") or raw.get("code")
        barcode = str(barcode_raw).strip() if barcode_raw is not None and str(barcode_raw).strip() else None
        source_id = barcode or f"off_idx_{fallback_index}"

        name_raw = (
            product.get("product_name")
            or product.get("product_name_en")
            or product.get("generic_name")
            or ""
        )
        food_name = str(name_raw).strip()
        if not food_name:
            return None, RejectedRecord(
                source="openfoodfacts",
                source_id=source_id,
                raw_identifier=source_id,
                reasons=["Missing product_name / generic_name."],
            )

        brand_raw = product.get("brands")
        brand = str(brand_raw).strip() if brand_raw and str(brand_raw).strip() else None
        category_raw = product.get("categories")
        category = (
            str(category_raw).split(",")[0].strip()
            if category_raw and str(category_raw).strip()
            else "packaged"
        )

        serving_g, serving_ml, serving_err = parse_serving_size_strict(
            serving_size_str=product.get("serving_size"),
            serving_quantity=product.get("serving_quantity"),
            serving_quantity_unit=product.get("serving_quantity_unit"),
        )
        if serving_err:
            return None, RejectedRecord(
                source="openfoodfacts",
                source_id=source_id,
                raw_identifier=food_name,
                reasons=[serving_err],
            )

        nutrition_data_per_raw = str(product.get("nutrition_data_per") or "").strip().lower()
        if nutrition_data_per_raw not in {"", "100g", "100ml", "serving"}:
            return None, RejectedRecord(
                source="openfoodfacts",
                source_id=source_id,
                raw_identifier=food_name,
                reasons=[f"Unsupported nutrition_data_per value: {nutrition_data_per_raw!r}"],
            )

        if nutrition_data_per_raw == "100ml":
            basis_unit = "100ml"
        elif nutrition_data_per_raw == "100g":
            basis_unit = "100g"
        elif nutrition_data_per_raw == "serving":
            if serving_ml is not None and serving_g is None:
                basis_unit = "100ml"
            elif serving_g is not None and serving_ml is None:
                basis_unit = "100g"
            else:
                return None, RejectedRecord(
                    source="openfoodfacts",
                    source_id=source_id,
                    raw_identifier=food_name,
                    reasons=[
                        "nutrition_data_per='serving' requires a valid unambiguous serving_size in g or ml."
                    ],
                )
        else:
            basis_unit = "100ml" if (serving_ml is not None and serving_g is None) else "100g"

        nutriments = product.get("nutriments")
        if not isinstance(nutriments, dict):
            return None, RejectedRecord(
                source="openfoodfacts",
                source_id=source_id,
                raw_identifier=food_name,
                reasons=["Missing or invalid 'nutriments' dictionary."],
            )

        record = CanonicalFoodRecord(
            food_id=f"off:{source_id}",
            food_name=food_name,
            source="openfoodfacts",
            source_id=source_id,
            brand=brand,
            barcode=barcode,
            category=category,
            basis_unit=basis_unit,
            serving_size_g=serving_g,
            serving_size_ml=serving_ml,
        )

        extraction_errors: List[str] = []

        kcal_val, kcal_prov, kcal_err = cls._extract_energy(
            nutriments, basis_unit, source_id, serving_g, serving_ml
        )
        if kcal_err:
            extraction_errors.append(kcal_err)
        elif kcal_val is not None and kcal_prov is not None:
            record.calories_kcal = kcal_val
            record.provenance["calories_kcal"] = kcal_prov

        na_val, na_prov, na_err = cls._extract_sodium(
            nutriments, basis_unit, source_id, serving_g, serving_ml
        )
        if na_err:
            extraction_errors.append(na_err)
        elif na_val is not None and na_prov is not None:
            record.sodium_mg = na_val
            record.provenance["sodium_mg"] = na_prov

        for canonical_field, (off_prefix, target_unit) in OFF_NUTRIENT_PREFIX_MAP.items():
            val, prov, err = cls._extract_nutrient(
                nutriments=nutriments,
                canonical_field=canonical_field,
                off_prefix=off_prefix,
                target_unit=target_unit,
                basis_unit=basis_unit,
                source_id=source_id,
                serving_g=serving_g,
                serving_ml=serving_ml,
            )
            if err:
                extraction_errors.append(err)
            elif val is not None and prov is not None:
                setattr(record, canonical_field, val)
                record.provenance[canonical_field] = prov

        if extraction_errors:
            return None, RejectedRecord(
                source="openfoodfacts",
                source_id=source_id,
                raw_identifier=food_name,
                reasons=extraction_errors,
            )

        if serving_g is not None or serving_ml is not None:
            portion_label = str(product.get("serving_size") or "").strip() or (
                f"{serving_g:g} g" if serving_g is not None else f"{serving_ml:g} ml"
            )
            record.portions.append(
                FoodPortionRecord(
                    portion_id=f"off:{source_id}:serving",
                    food_id=record.food_id,
                    portion_name=portion_label,
                    amount=1.0,
                    measure_unit="serving",
                    gram_weight=serving_g,
                    ml_volume=serving_ml,
                )
            )

        return record, None

    @classmethod
    def _extract_basis_or_serving_raw(
        cls,
        nutriments: Dict[str, Any],
        prefix: str,
        basis_unit: str,
        serving_g: Optional[float],
        serving_ml: Optional[float],
    ) -> Tuple[Optional[float], Optional[str], Optional[str], Optional[str]]:
        basis_key = f"{prefix}_{basis_unit}"
        if (
            basis_key in nutriments
            and nutriments[basis_key] is not None
            and str(nutriments[basis_key]).strip() != ""
        ):
            val, err = parse_optional_float(nutriments[basis_key])
            if err:
                return None, basis_key, None, f"Invalid value for {basis_key}: {err}"
            if val is not None:
                return val, basis_key, f"direct_{basis_unit}", None

        serving_key = f"{prefix}_serving"
        if (
            serving_key in nutriments
            and nutriments[serving_key] is not None
            and str(nutriments[serving_key]).strip() != ""
        ):
            s_val, s_err = parse_optional_float(nutriments[serving_key])
            if s_err:
                return None, serving_key, None, f"Invalid value for {serving_key}: {s_err}"
            if s_val is not None:
                if basis_unit == "100g" and serving_g is not None and serving_g > 0:
                    scaled = (s_val * 100.0) / serving_g
                    if not math.isfinite(scaled):
                        return None, serving_key, None, f"Scaling overflow for {serving_key}"
                    return scaled, serving_key, f"scaled_from_{serving_g}g_serving", None
                if basis_unit == "100ml" and serving_ml is not None and serving_ml > 0:
                    scaled = (s_val * 100.0) / serving_ml
                    if not math.isfinite(scaled):
                        return None, serving_key, None, f"Scaling overflow for {serving_key}"
                    return scaled, serving_key, f"scaled_from_{serving_ml}ml_serving", None
                return (
                    None,
                    serving_key,
                    None,
                    f"Cannot scale {serving_key} to {basis_unit}: compatible serving size is missing.",
                )

        return None, None, None, None

    @classmethod
    def _extract_energy(
        cls,
        nutriments: Dict[str, Any],
        basis_unit: str,
        source_id: str,
        serving_g: Optional[float],
        serving_ml: Optional[float],
    ) -> Tuple[Optional[float], Optional[NutrientProvenance], Optional[str]]:
        val, raw_field, scale_note, err = cls._extract_basis_or_serving_raw(
            nutriments, "energy-kcal", basis_unit, serving_g, serving_ml
        )
        if err:
            return None, None, err
        if val is not None and raw_field is not None:
            raw_unit_str = str(nutriments.get("energy-kcal_unit") or "kcal").strip()
            norm_u = normalize_unit_token(raw_unit_str)
            if norm_u != "kcal":
                return None, None, f"Unexpected unit for energy-kcal: {raw_unit_str!r}"
            return (
                val,
                NutrientProvenance(
                    nutrient_name="calories_kcal",
                    source="openfoodfacts",
                    source_id=source_id,
                    raw_field=raw_field,
                    raw_value=float(nutriments[raw_field]),
                    raw_unit=raw_unit_str,
                    normalized_value=val,
                    normalized_unit="kcal",
                    basis_unit=basis_unit,
                    conversion_applied=f"{scale_note}:identity_kcal",
                ),
                None,
            )

        val_kj, raw_field_kj, scale_note_kj, err_kj = cls._extract_basis_or_serving_raw(
            nutriments, "energy-kj", basis_unit, serving_g, serving_ml
        )
        if err_kj:
            return None, None, err_kj
        if val_kj is not None and raw_field_kj is not None:
            kcal_converted, conv_name = convert_energy_to_kcal(val_kj, "kj")
            return (
                kcal_converted,
                NutrientProvenance(
                    nutrient_name="calories_kcal",
                    source="openfoodfacts",
                    source_id=source_id,
                    raw_field=raw_field_kj,
                    raw_value=float(nutriments[raw_field_kj]),
                    raw_unit="kJ",
                    normalized_value=kcal_converted,
                    normalized_unit="kcal",
                    basis_unit=basis_unit,
                    conversion_applied=f"{scale_note_kj}:{conv_name}",
                ),
                None,
            )

        val_e, raw_field_e, scale_note_e, err_e = cls._extract_basis_or_serving_raw(
            nutriments, "energy", basis_unit, serving_g, serving_ml
        )
        if err_e:
            return None, None, err_e
        if val_e is not None and raw_field_e is not None:
            raw_unit_e = nutriments.get("energy_unit")
            norm_u_e = normalize_unit_token(raw_unit_e) if isinstance(raw_unit_e, str) else None
            if norm_u_e not in {"kcal", "kj"}:
                return (
                    None,
                    None,
                    f"Ambiguous or missing energy_unit ({raw_unit_e!r}) for {raw_field_e}.",
                )
            kcal_converted, conv_name = convert_energy_to_kcal(val_e, norm_u_e)
            return (
                kcal_converted,
                NutrientProvenance(
                    nutrient_name="calories_kcal",
                    source="openfoodfacts",
                    source_id=source_id,
                    raw_field=raw_field_e,
                    raw_value=float(nutriments[raw_field_e]),
                    raw_unit=str(raw_unit_e),
                    normalized_value=kcal_converted,
                    normalized_unit="kcal",
                    basis_unit=basis_unit,
                    conversion_applied=f"{scale_note_e}:{conv_name}",
                ),
                None,
            )

        return None, None, None

    @classmethod
    def _extract_sodium(
        cls,
        nutriments: Dict[str, Any],
        basis_unit: str,
        source_id: str,
        serving_g: Optional[float],
        serving_ml: Optional[float],
    ) -> Tuple[Optional[float], Optional[NutrientProvenance], Optional[str]]:
        val_na, field_na, scale_na, err_na = cls._extract_basis_or_serving_raw(
            nutriments, "sodium", basis_unit, serving_g, serving_ml
        )
        if err_na:
            return None, None, err_na
        if val_na is not None and field_na is not None:
            raw_unit = nutriments.get("sodium_unit")
            norm_unit = normalize_unit_token(raw_unit) if isinstance(raw_unit, str) else None
            if norm_unit not in {"g", "mg", "mcg"}:
                return (
                    None,
                    None,
                    f"Missing or invalid sodium_unit ({raw_unit!r}) for {field_na}; refusing to guess unit.",
                )
            try:
                na_mg = convert_mass_unit(val_na, norm_unit, "mg")
            except (ValueError, OverflowError) as exc:
                return None, None, str(exc)
            return (
                na_mg,
                NutrientProvenance(
                    nutrient_name="sodium_mg",
                    source="openfoodfacts",
                    source_id=source_id,
                    raw_field=field_na,
                    raw_value=float(nutriments[field_na]),
                    raw_unit=str(raw_unit),
                    normalized_value=na_mg,
                    normalized_unit="mg",
                    basis_unit=basis_unit,
                    conversion_applied=f"{scale_na}:mass_{norm_unit}_to_mg",
                ),
                None,
            )

        val_salt, field_salt, scale_salt, err_salt = cls._extract_basis_or_serving_raw(
            nutriments, "salt", basis_unit, serving_g, serving_ml
        )
        if err_salt:
            return None, None, err_salt
        if val_salt is not None and field_salt is not None:
            raw_unit = nutriments.get("salt_unit")
            norm_unit = normalize_unit_token(raw_unit) if isinstance(raw_unit, str) else None
            if norm_unit not in {"g", "mg", "mcg"}:
                return (
                    None,
                    None,
                    f"Missing or invalid salt_unit ({raw_unit!r}) for {field_salt}; refusing to guess unit.",
                )
            try:
                salt_mg = convert_mass_unit(val_salt, norm_unit, "mg")
                na_mg = salt_mg * SALT_TO_SODIUM_RATIO
            except (ValueError, OverflowError) as exc:
                return None, None, str(exc)
            return (
                na_mg,
                NutrientProvenance(
                    nutrient_name="sodium_mg",
                    source="openfoodfacts",
                    source_id=source_id,
                    raw_field=field_salt,
                    raw_value=float(nutriments[field_salt]),
                    raw_unit=str(raw_unit),
                    normalized_value=na_mg,
                    normalized_unit="mg",
                    basis_unit=basis_unit,
                    conversion_applied=f"{scale_salt}:salt_{norm_unit}_to_sodium_mg_x0.40",
                ),
                None,
            )

        return None, None, None

    @classmethod
    def _extract_nutrient(
        cls,
        nutriments: Dict[str, Any],
        canonical_field: str,
        off_prefix: str,
        target_unit: str,
        basis_unit: str,
        source_id: str,
        serving_g: Optional[float],
        serving_ml: Optional[float],
    ) -> Tuple[Optional[float], Optional[NutrientProvenance], Optional[str]]:
        val, raw_field, scale_note, err = cls._extract_basis_or_serving_raw(
            nutriments, off_prefix, basis_unit, serving_g, serving_ml
        )
        if err:
            return None, None, err
        if val is None or raw_field is None:
            return None, None, None

        unit_key = f"{off_prefix}_unit"
        raw_unit = nutriments.get(unit_key)
        norm_unit = normalize_unit_token(raw_unit) if isinstance(raw_unit, str) else None
        if norm_unit not in {"g", "mg", "mcg"}:
            return (
                None,
                None,
                f"Missing or unsupported {unit_key}={raw_unit!r} for {raw_field}; explicit unit required.",
            )

        try:
            converted = convert_mass_unit(val, norm_unit, target_unit)
        except (ValueError, OverflowError) as exc:
            return None, None, f"{canonical_field}: {exc}"

        return (
            converted,
            NutrientProvenance(
                nutrient_name=canonical_field,
                source="openfoodfacts",
                source_id=source_id,
                raw_field=raw_field,
                raw_value=float(nutriments[raw_field]),
                raw_unit=str(raw_unit),
                normalized_value=converted,
                normalized_unit=target_unit,
                basis_unit=basis_unit,
                conversion_applied=f"{scale_note}:mass_{norm_unit}_to_{target_unit}",
            ),
            None,
        )


# ============================================================================
# SOURCE ADAPTER 2: USDA FOODDATA CENTRAL (DISK-BACKED STAGING)
# ============================================================================

class USDAAdapter:
    """
    Streams USDA FoodData Central CSV directories (`food.csv`, `nutrient.csv`,
    `food_nutrient.csv`, `food_portion.csv`, `food_category.csv`, `measure_unit.csv`)
    into a temporary on-disk SQLite staging database (`usda_stage.db`) so full exports
    never exceed O(batch_size) RAM.
    """

    def __init__(self, directory_path: Path) -> None:
        self.directory_path = Path(directory_path)
        if not self.directory_path.exists() or not self.directory_path.is_dir():
            raise FileNotFoundError(f"USDA directory not found: {self.directory_path}")
        self.food_csv = self.directory_path / "food.csv"
        self.nutrient_csv = self.directory_path / "nutrient.csv"
        self.food_nutrient_csv = self.directory_path / "food_nutrient.csv"
        if not self.food_csv.exists():
            raise FileNotFoundError(f"Required USDA file missing: {self.food_csv}")
        if not self.nutrient_csv.exists():
            raise FileNotFoundError(f"Required USDA file missing: {self.nutrient_csv}")
        if not self.food_nutrient_csv.exists():
            raise FileNotFoundError(f"Required USDA file missing: {self.food_nutrient_csv}")

    def iter_records(
        self, start_offset: int = 0
    ) -> Iterator[Tuple[int, Optional[CanonicalFoodRecord], Optional[RejectedRecord]]]:
        with tempfile.TemporaryDirectory(prefix="nutrivault_usda_stage_") as tmp_dir:
            stage_db = Path(tmp_dir) / "usda_stage.db"
            conn = sqlite3.connect(str(stage_db))
            try:
                self._build_staging_db(conn)
                yield from self._iter_from_staging(conn, start_offset=start_offset)
            finally:
                conn.close()

    def _build_staging_db(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute("PRAGMA synchronous = OFF;")
        cur.execute("PRAGMA journal_mode = MEMORY;")
        cur.execute(
            """
            CREATE TABLE stage_categories (
                id TEXT PRIMARY KEY,
                description TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE stage_measure_units (
                id TEXT PRIMARY KEY,
                name TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE stage_nutrients (
                id INTEGER PRIMARY KEY,
                name TEXT,
                unit_name TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE stage_foods (
                seq_idx INTEGER PRIMARY KEY AUTOINCREMENT,
                fdc_id TEXT UNIQUE,
                description TEXT,
                data_type TEXT,
                category_id TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE stage_food_nutrients (
                fdc_id TEXT,
                nutrient_id INTEGER,
                amount_raw TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE stage_portions (
                id TEXT,
                fdc_id TEXT,
                amount_raw TEXT,
                measure_unit_id TEXT,
                portion_description TEXT,
                modifier TEXT,
                gram_weight_raw TEXT
            );
            """
        )

        cat_csv = self.directory_path / "food_category.csv"
        if cat_csv.exists():
            with cat_csv.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                cur.executemany(
                    "INSERT OR REPLACE INTO stage_categories (id, description) VALUES (?, ?)",
                    ((r.get("id", "").strip(), r.get("description", "").strip()) for r in reader),
                )

        mu_csv = self.directory_path / "measure_unit.csv"
        if mu_csv.exists():
            with mu_csv.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                cur.executemany(
                    "INSERT OR REPLACE INTO stage_measure_units (id, name) VALUES (?, ?)",
                    ((r.get("id", "").strip(), r.get("name", "").strip()) for r in reader),
                )

        tracked_nutrient_ids = set(USDA_NUTRIENT_MAP.keys()) | set(USDA_ENERGY_PRECEDENCE)
        with self.nutrient_csv.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            rows = []
            for r in reader:
                try:
                    nid = int(str(r.get("id", "")).strip())
                except ValueError:
                    continue
                if nid in tracked_nutrient_ids:
                    rows.append((nid, str(r.get("name", "")).strip(), str(r.get("unit_name", "")).strip()))
            cur.executemany(
                "INSERT OR REPLACE INTO stage_nutrients (id, name, unit_name) VALUES (?, ?, ?)", rows
            )

        with self.food_csv.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            batch: List[Tuple[str, str, str, str]] = []
            for r in reader:
                batch.append(
                    (
                        str(r.get("fdc_id", "")).strip(),
                        str(r.get("description", "")).strip(),
                        str(r.get("data_type", "")).strip(),
                        str(r.get("food_category_id", "")).strip(),
                    )
                )
                if len(batch) >= 5000:
                    cur.executemany(
                        "INSERT OR IGNORE INTO stage_foods (fdc_id, description, data_type, category_id) VALUES (?, ?, ?, ?)",
                        batch,
                    )
                    batch.clear()
            if batch:
                cur.executemany(
                    "INSERT OR IGNORE INTO stage_foods (fdc_id, description, data_type, category_id) VALUES (?, ?, ?, ?)",
                    batch,
                )

        with self.food_nutrient_csv.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fn_batch: List[Tuple[str, int, str]] = []
            for r in reader:
                try:
                    nid = int(str(r.get("nutrient_id", "")).strip())
                except ValueError:
                    continue
                if nid not in tracked_nutrient_ids:
                    continue
                fn_batch.append(
                    (
                        str(r.get("fdc_id", "")).strip(),
                        nid,
                        str(r.get("amount", "")).strip(),
                    )
                )
                if len(fn_batch) >= 10000:
                    cur.executemany(
                        "INSERT INTO stage_food_nutrients (fdc_id, nutrient_id, amount_raw) VALUES (?, ?, ?)",
                        fn_batch,
                    )
                    fn_batch.clear()
            if fn_batch:
                cur.executemany(
                    "INSERT INTO stage_food_nutrients (fdc_id, nutrient_id, amount_raw) VALUES (?, ?, ?)",
                    fn_batch,
                )

        portion_csv = self.directory_path / "food_portion.csv"
        if portion_csv.exists():
            with portion_csv.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                p_batch: List[Tuple[str, str, str, str, str, str, str]] = []
                for r in reader:
                    p_batch.append(
                        (
                            str(r.get("id", "")).strip(),
                            str(r.get("fdc_id", "")).strip(),
                            str(r.get("amount", "")).strip(),
                            str(r.get("measure_unit_id", "")).strip(),
                            str(r.get("portion_description", "")).strip(),
                            str(r.get("modifier", "")).strip(),
                            str(r.get("gram_weight", "")).strip(),
                        )
                    )
                    if len(p_batch) >= 5000:
                        cur.executemany(
                            "INSERT INTO stage_portions VALUES (?, ?, ?, ?, ?, ?, ?)",
                            p_batch,
                        )
                        p_batch.clear()
                if p_batch:
                    cur.executemany("INSERT INTO stage_portions VALUES (?, ?, ?, ?, ?, ?, ?)", p_batch)

        cur.execute("CREATE INDEX idx_stage_fn_fdc ON stage_food_nutrients(fdc_id);")
        cur.execute("CREATE INDEX idx_stage_p_fdc ON stage_portions(fdc_id);")
        conn.commit()

    def _iter_from_staging(
        self, conn: sqlite3.Connection, start_offset: int = 0
    ) -> Iterator[Tuple[int, Optional[CanonicalFoodRecord], Optional[RejectedRecord]]]:
        cur = conn.cursor()
        nutrient_meta: Dict[int, Tuple[str, str]] = {}
        for nid, nname, u_name in cur.execute("SELECT id, name, unit_name FROM stage_nutrients"):
            nutrient_meta[int(nid)] = (str(nname), str(u_name))

        categories: Dict[str, str] = {
            str(cid): str(desc) for cid, desc in cur.execute("SELECT id, description FROM stage_categories")
        }
        measure_units: Dict[str, str] = {
            str(mid): str(mname) for mid, mname in cur.execute("SELECT id, name FROM stage_measure_units")
        }

        food_cur = conn.cursor()
        food_cur.execute(
            "SELECT seq_idx - 1 AS idx, fdc_id, description, category_id FROM stage_foods WHERE seq_idx - 1 >= ? ORDER BY seq_idx ASC",
            (start_offset,),
        )

        detail_cur = conn.cursor()
        for idx, fdc_id, description, category_id in food_cur:
            fdc_id_str = str(fdc_id).strip()
            name_str = str(description).strip()
            if not fdc_id_str or not name_str:
                yield idx, None, RejectedRecord(
                    source="usda",
                    source_id=fdc_id_str or f"idx_{idx}",
                    raw_identifier=name_str or f"idx_{idx}",
                    reasons=["Missing fdc_id or food description in food.csv."],
                )
                continue

            cat_name = categories.get(str(category_id), None) or (
                str(category_id) if category_id and not str(category_id).isdigit() else "whole_food"
            )

            record = CanonicalFoodRecord(
                food_id=f"usda:{fdc_id_str}",
                food_name=name_str,
                source="usda",
                source_id=fdc_id_str,
                category=cat_name,
                basis_unit="100g",
                serving_size_g=100.0,
            )

            errors: List[str] = []
            energy_candidates: Dict[int, Tuple[float, str]] = {}

            for nid, amount_raw in detail_cur.execute(
                "SELECT nutrient_id, amount_raw FROM stage_food_nutrients WHERE fdc_id = ?", (fdc_id_str,)
            ):
                nid_int = int(nid)
                val, err = parse_optional_float(amount_raw)
                if err:
                    errors.append(f"Nutrient {nid_int}: {err}")
                    continue
                if val is None:
                    continue

                if nid_int not in nutrient_meta:
                    errors.append(f"Nutrient ID {nid_int} missing from nutrient.csv metadata.")
                    continue
                _, raw_unit = nutrient_meta[nid_int]

                if nid_int in USDA_ENERGY_PRECEDENCE:
                    energy_candidates[nid_int] = (val, raw_unit)
                    continue

                canonical_field = USDA_NUTRIENT_MAP.get(nid_int)
                if not canonical_field:
                    continue

                target_unit = EXPECTED_NUTRIENT_UNITS[canonical_field]
                norm_u = normalize_unit_token(raw_unit)
                if norm_u not in {"g", "mg", "mcg"}:
                    errors.append(
                        f"Unsupported unit {raw_unit!r} for USDA nutrient {nid_int} ({canonical_field})."
                    )
                    continue

                try:
                    converted = convert_mass_unit(val, norm_u, target_unit)
                except (ValueError, OverflowError) as exc:
                    errors.append(str(exc))
                    continue

                setattr(record, canonical_field, converted)
                record.provenance[canonical_field] = NutrientProvenance(
                    nutrient_name=canonical_field,
                    source="usda",
                    source_id=fdc_id_str,
                    raw_field=f"usda_nutrient_{nid_int}",
                    raw_value=val,
                    raw_unit=raw_unit,
                    normalized_value=converted,
                    normalized_unit=target_unit,
                    basis_unit="100g",
                    conversion_applied=f"usda_mass_{norm_u}_to_{target_unit}",
                )

            for en_id in USDA_ENERGY_PRECEDENCE:
                if en_id in energy_candidates:
                    raw_e_val, raw_e_unit = energy_candidates[en_id]
                    try:
                        kcal_val, conv_desc = convert_energy_to_kcal(raw_e_val, raw_e_unit)
                        record.calories_kcal = kcal_val
                        record.provenance["calories_kcal"] = NutrientProvenance(
                            nutrient_name="calories_kcal",
                            source="usda",
                            source_id=fdc_id_str,
                            raw_field=f"usda_nutrient_{en_id}",
                            raw_value=raw_e_val,
                            raw_unit=raw_e_unit,
                            normalized_value=kcal_val,
                            normalized_unit="kcal",
                            basis_unit="100g",
                            conversion_applied=f"usda_energy_{en_id}:{conv_desc}",
                        )
                    except (ValueError, OverflowError) as exc:
                        errors.append(f"Energy nutrient {en_id}: {exc}")
                    break

            for pid, p_amt_raw, mu_id, p_desc, modifier, gw_raw in detail_cur.execute(
                "SELECT id, amount_raw, measure_unit_id, portion_description, modifier, gram_weight_raw FROM stage_portions WHERE fdc_id = ?",
                (fdc_id_str,),
            ):
                gw_val, gw_err = parse_optional_float(gw_raw)
                if gw_err or gw_val is None or gw_val <= 0:
                    continue
                amt_val, amt_err = parse_optional_float(p_amt_raw)
                if amt_err or amt_val is None or amt_val <= 0:
                    amt_val = 1.0
                mu_name = measure_units.get(str(mu_id), "")
                if mu_name.lower() in {"undetermined", "none", ""}:
                    mu_name = ""
                label_parts = [p for p in (p_desc, mu_name, modifier) if p and p.strip()]
                portion_name = " ".join(label_parts).strip() or f"{amt_val:g} serving"
                record.portions.append(
                    FoodPortionRecord(
                        portion_id=f"usda:{fdc_id_str}:p_{pid or len(record.portions)}",
                        food_id=record.food_id,
                        portion_name=portion_name,
                        amount=amt_val,
                        measure_unit=mu_name or "serving",
                        gram_weight=gw_val,
                    )
                )

            if errors:
                yield idx, None, RejectedRecord(
                    source="usda",
                    source_id=fdc_id_str,
                    raw_identifier=name_str,
                    reasons=errors,
                )
            else:
                yield idx, record, None


# ============================================================================
# SOURCE ADAPTER 3: ICMR-NIN IFCT 2017 (CSV / JSON)
# ============================================================================

IFCT_COLUMN_SPECS: Dict[str, Tuple[Tuple[str, ...], str, str]] = {
    "calories_kcal": (
        ("enerc_kcal", "energy_kcal", "calories_kcal", "enerc_kj", "energy_kj", "enerc"),
        "kcal",
        "kcal",
    ),
    "protein_g": (("protcnt", "protein", "protein_g"), "g", "g"),
    "carbohydrate_g": (("choavldf", "carbohydrate", "carbohydrate_g", "carbs_g"), "g", "g"),
    "fat_g": (("fatce", "fat", "fat_g"), "g", "g"),
    "fiber_g": (("fibtg", "fiber", "fiber_g"), "g", "g"),
    "sugar_g": (("sugar", "sugars", "sugar_g"), "g", "g"),
    "sodium_mg": (("na", "sodium", "sodium_mg"), "mg", "mg"),
    "saturated_fat_g": (("fasat", "saturated_fat", "saturated_fat_g"), "g", "g"),
    "monounsaturated_fat_g": (("fams", "monounsaturated_fat", "monounsaturated_fat_g"), "g", "g"),
    "polyunsaturated_fat_g": (("fapu", "polyunsaturated_fat", "polyunsaturated_fat_g"), "g", "g"),
    "cholesterol_mg": (("chole", "cholesterol", "cholesterol_mg"), "mg", "mg"),
    "potassium_mg": (("k", "potassium", "potassium_mg"), "mg", "mg"),
    "calcium_mg": (("ca", "calcium", "calcium_mg"), "mg", "mg"),
    "iron_mg": (("fe", "iron", "iron_mg"), "mg", "mg"),
    "magnesium_mg": (("mg", "magnesium", "magnesium_mg"), "mg", "mg"),
    "zinc_mg": (("zn", "zinc", "zinc_mg"), "mg", "mg"),
    "vitamin_c_mg": (("vitc", "vitamin_c", "vitamin_c_mg"), "mg", "mg"),
    "vitamin_d_mcg": (("vitd", "vitamin_d", "vitamin_d_mcg"), "mcg", "mcg"),
    "histidine_mg": (("his", "histidine", "histidine_mg", "histidine_g"), "mg", "mg"),
    "isoleucine_mg": (("ile", "isoleucine", "isoleucine_mg", "isoleucine_g"), "mg", "mg"),
    "leucine_mg": (("leu", "leucine", "leucine_mg", "leucine_g"), "mg", "mg"),
    "lysine_mg": (("lys", "lysine", "lysine_mg", "lysine_g"), "mg", "mg"),
    "methionine_mg": (("met", "methionine", "methionine_mg", "methionine_g"), "mg", "mg"),
    "phenylalanine_mg": (("phe", "phenylalanine", "phenylalanine_mg", "phenylalanine_g"), "mg", "mg"),
    "threonine_mg": (("thr", "threonine", "threonine_mg", "threonine_g"), "mg", "mg"),
    "tryptophan_mg": (("trp", "tryptophan", "tryptophan_mg", "tryptophan_g"), "mg", "mg"),
    "valine_mg": (("val", "valine", "valine_mg", "valine_g"), "mg", "mg"),
}

_HEADER_UNIT_SUFFIX_RE = re.compile(
    r"(?:[_\s\-\(\[/]+)(kcal|kj|g|gm|grams?|mg|ug|µg|μg|mcg)\)?\]?$",
    re.IGNORECASE,
)


class IFCTAdapter:
    """
    Ingests ICMR-NIN Indian Food Composition Tables (IFCT 2017) from CSV or JSON.
    Detects explicit unit suffixes in column headers (e.g., `enerc_kj`, `his_g`, `na_g`)
    or sibling `<col>_unit` keys before falling back to IFCT standard column units.
    """

    def __init__(self, file_path: Path) -> None:
        self.file_path = Path(file_path)
        if not self.file_path.exists() or not self.file_path.is_file():
            raise FileNotFoundError(f"IFCT input file not found: {self.file_path}")

    def iter_records(
        self, start_offset: int = 0
    ) -> Iterator[Tuple[int, Optional[CanonicalFoodRecord], Optional[RejectedRecord]]]:
        idx = 0
        for raw_row in self._stream_rows():
            if idx < start_offset:
                idx += 1
                continue
            rec, rej = self.normalize_row(raw_row, fallback_index=idx)
            yield idx, rec, rej
            idx += 1

    def _stream_rows(self) -> Iterator[Dict[str, Any]]:
        suffix = self.file_path.suffix.lower()
        with self.file_path.open("r", encoding="utf-8-sig", newline="") as f:
            if suffix in {".json", ".jsonl"}:
                first_ch = ""
                while True:
                    c = f.read(1)
                    if not c:
                        break
                    if not c.isspace():
                        first_ch = c
                        break
                f.seek(0)
                if not first_ch:
                    return
                if first_ch == "[":
                    yield from _iter_json_array_stream(f)
                else:
                    for line_no, line in enumerate(f, start=1):
                        s = line.strip()
                        if not s:
                            continue
                        try:
                            obj = json.loads(s)
                            if isinstance(obj, dict):
                                yield obj
                            else:
                                yield {"_error": f"Line {line_no}: expected JSON object"}
                        except json.JSONDecodeError as exc:
                            yield {"_error": f"Line {line_no}: {exc}"}
            else:
                reader = csv.DictReader(f)
                for row in reader:
                    yield dict(row)

    @classmethod
    def _detect_unit_from_key(
        cls, raw_key: str, row_lower: Dict[str, Tuple[str, Any]], default_unit: str
    ) -> str:
        unit_col = f"{raw_key.lower()}_unit"
        if unit_col in row_lower and row_lower[unit_col][1] is not None:
            u_str = str(row_lower[unit_col][1]).strip()
            norm = normalize_unit_token(u_str)
            if norm:
                return norm

        m = _HEADER_UNIT_SUFFIX_RE.search(raw_key.strip())
        if m:
            norm = normalize_unit_token(m.group(1))
            if norm:
                return norm

        return default_unit

    @classmethod
    def normalize_row(
        cls, row: Dict[str, Any], fallback_index: int = 0
    ) -> Tuple[Optional[CanonicalFoodRecord], Optional[RejectedRecord]]:
        if "_error" in row:
            return None, RejectedRecord(
                source="ifct",
                source_id=f"row_{fallback_index}",
                raw_identifier=f"row_{fallback_index}",
                reasons=[str(row["_error"])],
            )

        row_lower: Dict[str, Tuple[str, Any]] = {
            str(k).strip().lower(): (str(k).strip(), v) for k, v in row.items()
        }

        code = None
        for k in ("code", "food_code", "ifct_code", "id"):
            if k in row_lower and row_lower[k][1] is not None and str(row_lower[k][1]).strip():
                code = str(row_lower[k][1]).strip()
                break
        if not code:
            return None, RejectedRecord(
                source="ifct",
                source_id=f"row_{fallback_index}",
                raw_identifier=f"row_{fallback_index}",
                reasons=["Missing IFCT food code ('code' or 'food_code')."],
            )

        name = None
        for k in ("name", "food_name", "description", "food"):
            if k in row_lower and row_lower[k][1] is not None and str(row_lower[k][1]).strip():
                name = str(row_lower[k][1]).strip()
                break
        if not name:
            return None, RejectedRecord(
                source="ifct",
                source_id=code,
                raw_identifier=code,
                reasons=["Missing IFCT food name."],
            )

        cat = "indian_staple"
        for k in ("grup", "group", "category", "food_group"):
            if k in row_lower and row_lower[k][1] is not None and str(row_lower[k][1]).strip():
                cat = str(row_lower[k][1]).strip()
                break

        record = CanonicalFoodRecord(
            food_id=f"ifct:{code}",
            food_name=name,
            source="ifct",
            source_id=code,
            category=cat,
            basis_unit="100g",
            serving_size_g=100.0,
        )

        errors: List[str] = []

        for canonical_field, (prefixes, default_unit, target_unit) in IFCT_COLUMN_SPECS.items():
            matched_orig_key: Optional[str] = None
            matched_raw_val: Any = None

            for cand in prefixes:
                if cand in row_lower:
                    matched_orig_key, matched_raw_val = row_lower[cand]
                    break
                for rk_lower, (orig_k, rv) in row_lower.items():
                    if rk_lower.endswith("_unit"):
                        continue
                    if (
                        rk_lower.startswith(cand + "_")
                        or rk_lower.startswith(cand + " ")
                        or rk_lower.startswith(cand + "(")
                    ):
                        matched_orig_key, matched_raw_val = orig_k, rv
                        break
                if matched_orig_key is not None:
                    break

            if matched_orig_key is None:
                continue

            val, err = parse_optional_float(matched_raw_val)
            if err:
                errors.append(f"{matched_orig_key}: {err}")
                continue
            if val is None:
                continue

            detected_unit = cls._detect_unit_from_key(matched_orig_key, row_lower, default_unit)
            try:
                if canonical_field == "calories_kcal":
                    converted, conv_note = convert_energy_to_kcal(val, detected_unit)
                else:
                    converted = convert_mass_unit(val, detected_unit, target_unit)
                    conv_note = f"ifct_mass_{detected_unit}_to_{target_unit}"
            except (ValueError, OverflowError) as exc:
                errors.append(f"{canonical_field} ({matched_orig_key}): {exc}")
                continue

            setattr(record, canonical_field, converted)
            record.provenance[canonical_field] = NutrientProvenance(
                nutrient_name=canonical_field,
                source="ifct",
                source_id=code,
                raw_field=matched_orig_key,
                raw_value=val,
                raw_unit=detected_unit,
                normalized_value=converted,
                normalized_unit=target_unit,
                basis_unit="100g",
                conversion_applied=conv_note,
            )

        if errors:
            return None, RejectedRecord(
                source="ifct",
                source_id=code,
                raw_identifier=name,
                reasons=errors,
            )

        return record, None


# ============================================================================
# SQLITE REPOSITORY, SCHEMA MIGRATION & PROVENANCE CONFLICT RESOLUTION
# ============================================================================

V4_DDL = """
CREATE TABLE IF NOT EXISTS foods (
    food_id TEXT PRIMARY KEY,
    food_name TEXT NOT NULL CHECK(length(trim(food_name)) > 0),
    brand TEXT,
    barcode TEXT UNIQUE,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    category TEXT,
    basis_unit TEXT NOT NULL DEFAULT '100g' CHECK(basis_unit IN ('100g', '100ml')),
    serving_size_g REAL CHECK(serving_size_g IS NULL OR serving_size_g > 0),
    serving_size_ml REAL CHECK(serving_size_ml IS NULL OR serving_size_ml > 0),
    density_g_per_ml REAL CHECK(density_g_per_ml IS NULL OR (density_g_per_ml > 0.05 AND density_g_per_ml <= 5.0)),
    calories_kcal REAL CHECK(calories_kcal IS NULL OR (calories_kcal >= 0 AND calories_kcal <= 950)),
    protein_g REAL CHECK(protein_g IS NULL OR protein_g >= 0),
    carbohydrate_g REAL CHECK(carbohydrate_g IS NULL OR carbohydrate_g >= 0),
    fat_g REAL CHECK(fat_g IS NULL OR fat_g >= 0),
    fiber_g REAL CHECK(fiber_g IS NULL OR fiber_g >= 0),
    sugar_g REAL CHECK(sugar_g IS NULL OR sugar_g >= 0),
    sodium_mg REAL CHECK(sodium_mg IS NULL OR sodium_mg >= 0),
    saturated_fat_g REAL CHECK(saturated_fat_g IS NULL OR saturated_fat_g >= 0),
    monounsaturated_fat_g REAL CHECK(monounsaturated_fat_g IS NULL OR monounsaturated_fat_g >= 0),
    polyunsaturated_fat_g REAL CHECK(polyunsaturated_fat_g IS NULL OR polyunsaturated_fat_g >= 0),
    cholesterol_mg REAL CHECK(cholesterol_mg IS NULL OR cholesterol_mg >= 0),
    potassium_mg REAL CHECK(potassium_mg IS NULL OR potassium_mg >= 0),
    calcium_mg REAL CHECK(calcium_mg IS NULL OR calcium_mg >= 0),
    iron_mg REAL CHECK(iron_mg IS NULL OR iron_mg >= 0),
    magnesium_mg REAL CHECK(magnesium_mg IS NULL OR magnesium_mg >= 0),
    zinc_mg REAL CHECK(zinc_mg IS NULL OR zinc_mg >= 0),
    vitamin_c_mg REAL CHECK(vitamin_c_mg IS NULL OR vitamin_c_mg >= 0),
    vitamin_d_mcg REAL CHECK(vitamin_d_mcg IS NULL OR vitamin_d_mcg >= 0),
    histidine_mg REAL CHECK(histidine_mg IS NULL OR histidine_mg >= 0),
    isoleucine_mg REAL CHECK(isoleucine_mg IS NULL OR isoleucine_mg >= 0),
    leucine_mg REAL CHECK(leucine_mg IS NULL OR leucine_mg >= 0),
    lysine_mg REAL CHECK(lysine_mg IS NULL OR lysine_mg >= 0),
    methionine_mg REAL CHECK(methionine_mg IS NULL OR methionine_mg >= 0),
    phenylalanine_mg REAL CHECK(phenylalanine_mg IS NULL OR phenylalanine_mg >= 0),
    threonine_mg REAL CHECK(threonine_mg IS NULL OR threonine_mg >= 0),
    tryptophan_mg REAL CHECK(tryptophan_mg IS NULL OR tryptophan_mg >= 0),
    valine_mg REAL CHECK(valine_mg IS NULL OR valine_mg >= 0),
    is_favorite INTEGER NOT NULL DEFAULT 0 CHECK(is_favorite IN (0, 1)),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_foods_barcode ON foods(barcode) WHERE barcode IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_foods_source ON foods(source, source_id);

CREATE TABLE IF NOT EXISTS food_portions (
    portion_id TEXT PRIMARY KEY,
    food_id TEXT NOT NULL,
    portion_name TEXT NOT NULL,
    amount REAL NOT NULL DEFAULT 1.0 CHECK(amount > 0),
    measure_unit TEXT NOT NULL DEFAULT 'serving',
    gram_weight REAL CHECK(gram_weight IS NULL OR gram_weight > 0),
    ml_volume REAL CHECK(ml_volume IS NULL OR ml_volume > 0),
    CHECK(gram_weight IS NOT NULL OR ml_volume IS NOT NULL),
    FOREIGN KEY(food_id) REFERENCES foods(food_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_portions_food_id ON food_portions(food_id);

CREATE TABLE IF NOT EXISTS nutrient_provenance (
    food_id TEXT NOT NULL,
    nutrient_name TEXT NOT NULL,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    raw_field TEXT NOT NULL,
    raw_value REAL NOT NULL,
    raw_unit TEXT NOT NULL,
    normalized_value REAL NOT NULL,
    normalized_unit TEXT NOT NULL,
    basis_unit TEXT NOT NULL,
    conversion_applied TEXT NOT NULL,
    is_selected INTEGER NOT NULL DEFAULT 0 CHECK(is_selected IN (0, 1)),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(food_id, nutrient_name, source),
    FOREIGN KEY(food_id) REFERENCES foods(food_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_prov_food_selected ON nutrient_provenance(food_id, is_selected);

CREATE TABLE IF NOT EXISTS import_checkpoints (
    job_key TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    input_signature TEXT NOT NULL,
    last_offset INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS foods_fts USING fts5(
    food_name,
    brand,
    category,
    content='foods',
    content_rowid='rowid',
    tokenize='unicode61 remove_diacritics 1'
);

CREATE TRIGGER IF NOT EXISTS foods_ai AFTER INSERT ON foods BEGIN
    INSERT INTO foods_fts(rowid, food_name, brand, category)
    VALUES (new.rowid, new.food_name, new.brand, new.category);
END;

CREATE TRIGGER IF NOT EXISTS foods_ad AFTER DELETE ON foods BEGIN
    INSERT INTO foods_fts(foods_fts, rowid, food_name, brand, category)
    VALUES ('delete', old.rowid, old.food_name, old.brand, old.category);
END;

CREATE TRIGGER IF NOT EXISTS foods_au AFTER UPDATE ON foods BEGIN
    INSERT INTO foods_fts(foods_fts, rowid, food_name, brand, category)
    VALUES ('delete', old.rowid, old.food_name, old.brand, old.category);
    INSERT INTO foods_fts(rowid, food_name, brand, category)
    VALUES (new.rowid, new.food_name, new.brand, new.category);
END;
"""


def execute_sql_script_transactionally(cur: sqlite3.Cursor, sql_script: str) -> None:
    """
    Executes a multi-statement SQL script on `cur` without implicitly committing the
    active transaction (unlike `cur.executescript()`) and without breaking multi-line
    `CREATE TRIGGER ... BEGIN ...; END;` blocks on internal semicolons.
    """
    statement_lines: List[str] = []
    for line in sql_script.splitlines():
        stripped = line.strip()
        if not statement_lines and (not stripped or stripped.startswith("--")):
            continue
        statement_lines.append(line)
        candidate = "\n".join(statement_lines)
        if sqlite3.complete_statement(candidate):
            stmt = candidate.strip()
            if stmt:
                cur.execute(stmt)
            statement_lines.clear()

    if statement_lines:
        rem = "\n".join(statement_lines).strip()
        if rem:
            cur.execute(rem)


class NutritionDatabaseRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        if self.db_path.exists() and self.db_path.is_dir():
            raise IsADirectoryError(f"Database path points to a directory, not a file: {self.db_path}")
        if self.db_path.parent and not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), timeout=5.0, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout = 5000;")
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.initialize_schema()

    def close(self) -> None:
        if getattr(self, "conn", None) is not None:
            self.conn.close()

    def initialize_schema(self) -> None:
        cur = self.conn.cursor()
        existing_tables = {
            r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "foods" in existing_tables:
            cols = {r[1]: r for r in cur.execute("PRAGMA table_info(foods)").fetchall()}
            needs_rebuild = (
                "id" in cols
                or "carbs_g" in cols
                or "food_id" not in cols
                or "carbohydrate_g" not in cols
                or "basis_unit" not in cols
            )
            if needs_rebuild:
                self._rebuild_legacy_schema_in_tx()
                return

        cur.execute("BEGIN IMMEDIATE;")
        try:
            execute_sql_script_transactionally(cur, V4_DDL)
            cur.execute("COMMIT;")
        except Exception:
            cur.execute("ROLLBACK;")
            raise

    def _rebuild_legacy_schema_in_tx(self) -> None:
        """
        Upgrades legacy v1/v2 schemas (`id`, `name`, `carbs_g`, NOT NULL macro constraints,
        or dangling foreign keys) inside a single atomic transaction.
        Rebuilds child tables (`food_portions`, `nutrient_provenance`) in the same transaction
        so foreign keys never point to `_foods_legacy_mig`.
        """
        cur = self.conn.cursor()
        cur.execute("PRAGMA foreign_keys = OFF;")
        cur.execute("BEGIN IMMEDIATE;")
        try:
            for trig in ("foods_ai", "foods_ad", "foods_au"):
                cur.execute(f"DROP TRIGGER IF EXISTS {trig};")
            cur.execute("DROP TABLE IF EXISTS foods_fts;")

            legacy_cols = {r[1] for r in cur.execute("PRAGMA table_info(foods)").fetchall()}
            cur.execute("ALTER TABLE foods RENAME TO _foods_legacy_mig;")

            existing_tables = {
                r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            has_legacy_portions = "food_portions" in existing_tables
            if has_legacy_portions:
                cur.execute("ALTER TABLE food_portions RENAME TO _portions_legacy_mig;")

            has_legacy_prov = "nutrient_provenance" in existing_tables
            if has_legacy_prov:
                cur.execute("ALTER TABLE nutrient_provenance RENAME TO _prov_legacy_mig;")

            execute_sql_script_transactionally(cur, V4_DDL)

            id_expr = "food_id" if "food_id" in legacy_cols else "id"
            name_expr = "food_name" if "food_name" in legacy_cols else "name"
            carb_expr = (
                "carbohydrate_g"
                if "carbohydrate_g" in legacy_cols
                else ("carbs_g" if "carbs_g" in legacy_cols else "NULL")
            )
            source_id_expr = "source_id" if "source_id" in legacy_cols else id_expr
            basis_expr = "basis_unit" if "basis_unit" in legacy_cols else "'100g'"
            now_ts = int(time.time())

            cur.execute(
                f"""
                INSERT INTO foods (
                    food_id, food_name, brand, barcode, source, source_id, category,
                    basis_unit, serving_size_g, calories_kcal, protein_g, carbohydrate_g,
                    fat_g, fiber_g, sugar_g, sodium_mg, created_at, updated_at
                )
                SELECT
                    {id_expr},
                    {name_expr},
                    brand,
                    barcode,
                    COALESCE(source, 'core'),
                    {source_id_expr},
                    category,
                    {basis_expr},
                    CASE WHEN serving_size_g IS NOT NULL AND serving_size_g > 0 THEN serving_size_g ELSE 100.0 END,
                    calories_kcal,
                    protein_g,
                    {carb_expr},
                    fat_g,
                    fiber_g,
                    sugar_g,
                    sodium_mg,
                    COALESCE(created_at, {now_ts}),
                    COALESCE(updated_at, {now_ts})
                FROM _foods_legacy_mig;
                """
            )

            if not has_legacy_prov:
                for nf in ("calories_kcal", "protein_g", "carbohydrate_g", "fat_g", "fiber_g", "sugar_g", "sodium_mg"):
                    unit_str = EXPECTED_NUTRIENT_UNITS[nf]
                    cur.execute(
                        f"""
                        INSERT OR IGNORE INTO nutrient_provenance (
                            food_id, nutrient_name, source, source_id, raw_field,
                            raw_value, raw_unit, normalized_value, normalized_unit,
                            basis_unit, conversion_applied, is_selected, updated_at
                        )
                        SELECT
                            food_id, '{nf}', source, source_id, 'legacy_{nf}',
                            {nf}, '{unit_str}', {nf}, '{unit_str}',
                            basis_unit, 'legacy_migration', 1, updated_at
                        FROM foods
                        WHERE {nf} IS NOT NULL;
                        """
                    )

            if has_legacy_portions:
                p_cols = {r[1] for r in cur.execute("PRAGMA table_info(_portions_legacy_mig)").fetchall()}
                pid_expr = "portion_id" if "portion_id" in p_cols else "id"
                amt_expr = "amount" if "amount" in p_cols else "1.0"
                mu_expr = "measure_unit" if "measure_unit" in p_cols else "'serving'"
                ml_expr = "ml_volume" if "ml_volume" in p_cols else "NULL"
                cur.execute(
                    f"""
                    INSERT INTO food_portions (portion_id, food_id, portion_name, amount, measure_unit, gram_weight, ml_volume)
                    SELECT {pid_expr}, food_id, portion_name, {amt_expr}, {mu_expr}, gram_weight, {ml_expr}
                    FROM _portions_legacy_mig
                    WHERE food_id IN (SELECT food_id FROM foods);
                    """
                )
                cur.execute("DROP TABLE _portions_legacy_mig;")

            if has_legacy_prov:
                prov_cols = {r[1] for r in cur.execute("PRAGMA table_info(_prov_legacy_mig)").fetchall()}
                sel_expr = "is_selected" if "is_selected" in prov_cols else "1"
                cur.execute(
                    f"""
                    INSERT INTO nutrient_provenance (
                        food_id, nutrient_name, source, source_id, raw_field, raw_value,
                        raw_unit, normalized_value, normalized_unit, basis_unit,
                        conversion_applied, is_selected, updated_at
                    )
                    SELECT
                        food_id, nutrient_name, source, source_id, raw_field, raw_value,
                        raw_unit, normalized_value, normalized_unit, basis_unit,
                        conversion_applied, {sel_expr}, updated_at
                    FROM _prov_legacy_mig
                    WHERE food_id IN (SELECT food_id FROM foods);
                    """
                )
                cur.execute("DROP TABLE _prov_legacy_mig;")

            cur.execute("DROP TABLE _foods_legacy_mig;")

            for child_tbl in ("food_portions", "nutrient_provenance"):
                fk_rows = cur.execute(f"PRAGMA foreign_key_list({child_tbl});").fetchall()
                for fk in fk_rows:
                    if fk[2] != "foods":
                        raise sqlite3.IntegrityError(
                            f"Foreign key on {child_tbl} references {fk[2]!r} instead of 'foods'."
                        )

            fk_violations = cur.execute("PRAGMA foreign_key_check;").fetchall()
            if fk_violations:
                raise sqlite3.IntegrityError(f"Foreign key violations after migration: {fk_violations}")

            cur.execute("COMMIT;")
        except Exception:
            cur.execute("ROLLBACK;")
            raise
        finally:
            cur.execute("PRAGMA foreign_keys = ON;")

    def upsert_batch(self, records: Sequence[CanonicalFoodRecord]) -> int:
        if not records:
            return 0
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        try:
            count = 0
            for rec in records:
                self._upsert_single_record_in_tx(cur, rec)
                count += 1
            cur.execute("COMMIT;")
            return count
        except Exception:
            cur.execute("ROLLBACK;")
            raise

    def _upsert_single_record_in_tx(self, cur: sqlite3.Cursor, rec: CanonicalFoodRecord) -> None:
        now = int(time.time())
        existing = None
        if rec.barcode:
            existing = cur.execute(
                "SELECT * FROM foods WHERE barcode = ? OR food_id = ?",
                (rec.barcode, rec.food_id),
            ).fetchone()
        if existing is None:
            existing = cur.execute("SELECT * FROM foods WHERE food_id = ?", (rec.food_id,)).fetchone()

        if existing is None:
            cols = [
                "food_id",
                "food_name",
                "brand",
                "barcode",
                "source",
                "source_id",
                "category",
                "basis_unit",
                "serving_size_g",
                "serving_size_ml",
                "density_g_per_ml",
                *NUTRIENT_FIELDS,
                "created_at",
                "updated_at",
            ]
            vals = [
                rec.food_id,
                rec.food_name,
                rec.brand,
                rec.barcode,
                rec.source,
                rec.source_id,
                rec.category,
                rec.basis_unit,
                rec.serving_size_g,
                rec.serving_size_ml,
                rec.density_g_per_ml,
                *[getattr(rec, nf) for nf in NUTRIENT_FIELDS],
                now,
                now,
            ]
            placeholders = ", ".join(["?"] * len(cols))
            cur.execute(
                f"INSERT INTO foods ({', '.join(cols)}) VALUES ({placeholders})",
                vals,
            )
            target_food_id = rec.food_id
            for nf, prov in rec.provenance.items():
                cur.execute(
                    """
                    INSERT OR REPLACE INTO nutrient_provenance (
                        food_id, nutrient_name, source, source_id, raw_field,
                        raw_value, raw_unit, normalized_value, normalized_unit,
                        basis_unit, conversion_applied, is_selected, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    """,
                    (
                        target_food_id,
                        nf,
                        prov.source,
                        prov.source_id,
                        prov.raw_field,
                        prov.raw_value,
                        prov.raw_unit,
                        prov.normalized_value,
                        prov.normalized_unit,
                        prov.basis_unit,
                        prov.conversion_applied,
                        now,
                    ),
                )
        else:
            target_food_id = str(existing["food_id"])
            old_source = str(existing["source"])
            old_prio = SOURCE_PRIORITY.get(old_source, 0)
            new_prio = SOURCE_PRIORITY.get(rec.source, 0)

            existing_basis = str(existing["basis_unit"])
            if existing_basis != rec.basis_unit and new_prio < old_prio:
                return

            win_header = new_prio >= old_prio
            food_name = rec.food_name if win_header else str(existing["food_name"])
            brand = (rec.brand or existing["brand"]) if win_header else (existing["brand"] or rec.brand)
            barcode = rec.barcode or existing["barcode"]
            source = rec.source if win_header else old_source
            source_id = rec.source_id if win_header else str(existing["source_id"])
            category = (
                (rec.category or existing["category"]) if win_header else (existing["category"] or rec.category)
            )
            basis_unit = rec.basis_unit if win_header else existing_basis
            serving_g = (
                rec.serving_size_g
                if (win_header and rec.serving_size_g is not None)
                else (existing["serving_size_g"] or rec.serving_size_g)
            )
            serving_ml = (
                rec.serving_size_ml
                if (win_header and rec.serving_size_ml is not None)
                else (existing["serving_size_ml"] or rec.serving_size_ml)
            )
            density = (
                rec.density_g_per_ml
                if (win_header and rec.density_g_per_ml is not None)
                else (existing["density_g_per_ml"] or rec.density_g_per_ml)
            )

            selected_prov_rows = cur.execute(
                "SELECT nutrient_name, source FROM nutrient_provenance WHERE food_id = ? AND is_selected = 1",
                (target_food_id,),
            ).fetchall()
            active_nutrient_source: Dict[str, str] = {
                str(r["nutrient_name"]): str(r["source"]) for r in selected_prov_rows
            }

            merged_nutrients: Dict[str, Optional[float]] = {}
            for nf in NUTRIENT_FIELDS:
                old_val = existing[nf]
                new_val = getattr(rec, nf)
                if new_val is None:
                    merged_nutrients[nf] = old_val
                    continue

                if nf in rec.provenance:
                    prov = rec.provenance[nf]
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO nutrient_provenance (
                            food_id, nutrient_name, source, source_id, raw_field,
                            raw_value, raw_unit, normalized_value, normalized_unit,
                            basis_unit, conversion_applied, is_selected, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                        """,
                        (
                            target_food_id,
                            nf,
                            prov.source,
                            prov.source_id,
                            prov.raw_field,
                            prov.raw_value,
                            prov.raw_unit,
                            prov.normalized_value,
                            prov.normalized_unit,
                            prov.basis_unit,
                            prov.conversion_applied,
                            now,
                        ),
                    )

                curr_nut_source = active_nutrient_source.get(nf, old_source)
                curr_nut_prio = SOURCE_PRIORITY.get(curr_nut_source, 0)

                if old_val is None or new_prio >= curr_nut_prio:
                    merged_nutrients[nf] = new_val
                    cur.execute(
                        "UPDATE nutrient_provenance SET is_selected = 0 WHERE food_id = ? AND nutrient_name = ?",
                        (target_food_id, nf),
                    )
                    cur.execute(
                        "UPDATE nutrient_provenance SET is_selected = 1 WHERE food_id = ? AND nutrient_name = ? AND source = ?",
                        (target_food_id, nf, rec.source),
                    )
                else:
                    merged_nutrients[nf] = old_val

            set_fragments = [
                "food_name = ?",
                "brand = ?",
                "barcode = ?",
                "source = ?",
                "source_id = ?",
                "category = ?",
                "basis_unit = ?",
                "serving_size_g = ?",
                "serving_size_ml = ?",
                "density_g_per_ml = ?",
                *[f"{nf} = ?" for nf in NUTRIENT_FIELDS],
                "updated_at = ?",
            ]
            update_vals = [
                food_name,
                brand,
                barcode,
                source,
                source_id,
                category,
                basis_unit,
                serving_g,
                serving_ml,
                density,
                *[merged_nutrients[nf] for nf in NUTRIENT_FIELDS],
                now,
                target_food_id,
            ]
            cur.execute(
                f"UPDATE foods SET {', '.join(set_fragments)} WHERE food_id = ?",
                update_vals,
            )

        for p in rec.portions:
            cur.execute(
                """
                INSERT OR REPLACE INTO food_portions (
                    portion_id, food_id, portion_name, amount, measure_unit, gram_weight, ml_volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    p.portion_id,
                    target_food_id,
                    p.portion_name,
                    p.amount,
                    p.measure_unit,
                    p.gram_weight,
                    p.ml_volume,
                ),
            )

    def get_checkpoint(self, job_key: str, input_signature: str) -> int:
        row = self.conn.execute(
            "SELECT last_offset, input_signature FROM import_checkpoints WHERE job_key = ?",
            (job_key,),
        ).fetchone()
        if not row:
            return 0
        if str(row["input_signature"]) != input_signature:
            logger.warning("Checkpoint signature mismatch for %s; restarting from offset 0.", job_key)
            return 0
        return int(row["last_offset"])

    def save_checkpoint_and_batch(
        self,
        records: Sequence[CanonicalFoodRecord],
        job_key: str,
        source: str,
        input_signature: str,
        next_offset: int,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        try:
            for rec in records:
                self._upsert_single_record_in_tx(cur, rec)
            cur.execute(
                """
                INSERT OR REPLACE INTO import_checkpoints (job_key, source, input_signature, last_offset, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_key, source, input_signature, next_offset, int(time.time())),
            )
            cur.execute("COMMIT;")
            return len(records)
        except Exception:
            cur.execute("ROLLBACK;")
            raise


# ============================================================================
# DATABASE INTEGRITY VERIFIER & SCALE BENCHMARK ENGINE
# ============================================================================

def verify_database_integrity(db_path: Path) -> Dict[str, Any]:
    """
    Performs a comprehensive post-import verification audit on `db_path`:
    1. SQLite `PRAGMA foreign_key_check` & `PRAGMA quick_check`.
    2. FTS5 index parity (`SELECT COUNT(*) FROM foods` vs `foods_fts`).
    3. Complete Provenance & Value Parity Audit:
       - No `(food_id, nutrient_name)` has > 1 `is_selected = 1` row.
       - Every non-NULL nutrient column in `foods` has an active `is_selected = 1`
         row in `nutrient_provenance` whose `normalized_value` matches `foods.<nutrient>`.
    4. Orphan portion & provenance detection.
    5. Source breakdown, portion counts, and nutrient population coverage.
    """
    if not db_path.exists() or not db_path.is_file():
        raise FileNotFoundError(f"Database file not found for verification: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        issues: List[str] = []

        fk_violations = cur.execute("PRAGMA foreign_key_check;").fetchall()
        if fk_violations:
            issues.append(f"Foreign key violations detected: {len(fk_violations)}")

        quick_check = [r[0] for r in cur.execute("PRAGMA quick_check;").fetchall()]
        if quick_check != ["ok"]:
            issues.append(f"SQLite quick_check failed: {quick_check}")

        total_foods = int(cur.execute("SELECT COUNT(*) FROM foods").fetchone()[0])
        fts_count = int(cur.execute("SELECT COUNT(*) FROM foods_fts").fetchone()[0])
        if total_foods != fts_count:
            issues.append(f"FTS5 index count ({fts_count}) does not match foods count ({total_foods}).")

        total_portions = int(cur.execute("SELECT COUNT(*) FROM food_portions").fetchone()[0])
        orphan_portions = int(
            cur.execute(
                "SELECT COUNT(*) FROM food_portions WHERE food_id NOT IN (SELECT food_id FROM foods)"
            ).fetchone()[0]
        )
        if orphan_portions > 0:
            issues.append(f"Found {orphan_portions} orphan rows in food_portions.")

        duplicate_selected = cur.execute(
            """
            SELECT food_id, nutrient_name, COUNT(*) AS c
            FROM nutrient_provenance
            WHERE is_selected = 1
            GROUP BY food_id, nutrient_name
            HAVING c > 1
            """
        ).fetchall()
        if duplicate_selected:
            issues.append(f"Found {len(duplicate_selected)} nutrients with multiple is_selected=1 provenance rows.")

        nutrient_coverage: Dict[str, int] = {}
        missing_prov_total = 0
        mismatched_prov_total = 0

        for nf in NUTRIENT_FIELDS:
            cnt = int(cur.execute(f"SELECT COUNT(*) FROM foods WHERE {nf} IS NOT NULL").fetchone()[0])
            nutrient_coverage[nf] = cnt
            if cnt > 0:
                missing_prov = int(
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM foods f
                        LEFT JOIN nutrient_provenance p
                          ON p.food_id = f.food_id AND p.nutrient_name = ? AND p.is_selected = 1
                        WHERE f.{nf} IS NOT NULL AND p.food_id IS NULL
                        """,
                        (nf,),
                    ).fetchone()[0]
                )
                missing_prov_total += missing_prov

                mismatched_val = int(
                    cur.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM foods f
                        JOIN nutrient_provenance p
                          ON p.food_id = f.food_id AND p.nutrient_name = ? AND p.is_selected = 1
                        WHERE f.{nf} IS NOT NULL AND ABS(f.{nf} - p.normalized_value) > 1e-6
                        """,
                        (nf,),
                    ).fetchone()[0]
                )
                mismatched_prov_total += mismatched_val

        if missing_prov_total > 0:
            issues.append(f"Found {missing_prov_total} populated nutrient values missing active provenance.")
        if mismatched_prov_total > 0:
            issues.append(
                f"Found {mismatched_prov_total} populated nutrient values disagreeing with selected provenance."
            )

        source_counts = {
            str(r["source"]): int(r["cnt"])
            for r in cur.execute("SELECT source, COUNT(*) AS cnt FROM foods GROUP BY source").fetchall()
        }
        basis_counts = {
            str(r["basis_unit"]): int(r["cnt"])
            for r in cur.execute("SELECT basis_unit, COUNT(*) AS cnt FROM foods GROUP BY basis_unit").fetchall()
        }

        return {
            "status": "PASS" if not issues else "FAIL",
            "db_path": str(db_path),
            "db_size_bytes": db_path.stat().st_size,
            "total_foods": total_foods,
            "fts_row_count": fts_count,
            "total_portions": total_portions,
            "source_counts": source_counts,
            "basis_counts": basis_counts,
            "nutrient_coverage": nutrient_coverage,
            "missing_provenance_count": missing_prov_total,
            "mismatched_provenance_count": mismatched_prov_total,
            "issues": issues,
        }
    finally:
        conn.close()


# ============================================================================
# NUTRITION CALCULATION & SEARCH SERVICE (STAGE 1 DATA API)
# ============================================================================

WHO_EAA_REFERENCE_MG_PER_G_PROTEIN: Dict[str, float] = {
    "histidine_mg": 15.0,
    "isoleucine_mg": 30.0,
    "leucine_mg": 59.0,
    "lysine_mg": 45.0,
    "methionine_mg": 22.0,
    "phenylalanine_mg": 38.0,
    "threonine_mg": 23.0,
    "tryptophan_mg": 6.0,
    "valine_mg": 39.0,
}


class NutritionAppService:
    """
    Deterministic SQLite-backed calculation and search engine.
    Enforces strict input validation, overflow protection, mass/volume density rules,
    and honest `None` propagation when summing meals with incomplete nutrient data.
    """

    def __init__(self, repo: NutritionDatabaseRepository) -> None:
        self.repo = repo

    def search_foods_fts(self, query: str, limit: int = 25) -> List[Dict[str, Any]]:
        if not isinstance(query, str) or not query.strip():
            return []
        tokens = [re.sub(r"[^\w]+", "", t) for t in query.strip().split()]
        tokens = [t for t in tokens if t]
        if not tokens:
            return []
        fts_query = " AND ".join(f'"{t}"*' for t in tokens)
        rows = self.repo.conn.execute(
            """
            SELECT f.*
            FROM foods f
            JOIN foods_fts fts ON f.rowid = fts.rowid
            WHERE foods_fts MATCH ?
            ORDER BY f.is_favorite DESC, fts.rank
            LIMIT ?
            """,
            (fts_query, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_food_by_barcode(self, barcode: str) -> Optional[Dict[str, Any]]:
        if not barcode or not str(barcode).strip():
            return None
        row = self.repo.conn.execute(
            "SELECT * FROM foods WHERE barcode = ?", (str(barcode).strip(),)
        ).fetchone()
        return dict(row) if row else None

    def calculate_custom_quantity(
        self,
        food_id: str,
        amount_g: Optional[float] = None,
        amount_ml: Optional[float] = None,
        portion_id: Optional[str] = None,
        portion_multiplier: float = 1.0,
    ) -> Dict[str, Any]:
        provided = [x is not None for x in (amount_g, amount_ml, portion_id)]
        if sum(provided) != 1:
            raise ValueError("Specify exactly one of amount_g, amount_ml, or portion_id.")

        food = self.repo.conn.execute("SELECT * FROM foods WHERE food_id = ?", (food_id,)).fetchone()
        if not food:
            raise KeyError(f"Food not found: {food_id}")

        basis = str(food["basis_unit"])
        density = food["density_g_per_ml"]

        if portion_id is not None:
            if not is_finite_number(portion_multiplier) or portion_multiplier <= 0:
                raise ValueError(f"portion_multiplier must be finite and > 0, got {portion_multiplier!r}")
            portion = self.repo.conn.execute(
                "SELECT * FROM food_portions WHERE portion_id = ? AND food_id = ?",
                (portion_id, food_id),
            ).fetchone()
            if not portion:
                raise KeyError(f"Portion {portion_id!r} not found for food {food_id!r}")
            if portion["gram_weight"] is not None:
                amount_g = float(portion["gram_weight"]) * float(portion_multiplier)
                if not math.isfinite(amount_g):
                    raise OverflowError("Portion gram weight multiplication overflowed.")
            elif portion["ml_volume"] is not None:
                amount_ml = float(portion["ml_volume"]) * float(portion_multiplier)
                if not math.isfinite(amount_ml):
                    raise OverflowError("Portion ml volume multiplication overflowed.")

        if amount_g is not None:
            if not is_finite_number(amount_g) or amount_g < 0:
                raise ValueError(f"amount_g must be a finite non-negative number, got {amount_g!r}")
            if basis == "100g":
                scale = float(amount_g) / 100.0
            else:
                if density is None or density <= 0:
                    raise ValueError(
                        f"Cannot scale 100ml-basis food {food_id!r} by gram weight without density_g_per_ml."
                    )
                equivalent_ml = float(amount_g) / float(density)
                scale = equivalent_ml / 100.0
        else:
            assert amount_ml is not None
            if not is_finite_number(amount_ml) or amount_ml < 0:
                raise ValueError(f"amount_ml must be a finite non-negative number, got {amount_ml!r}")
            if basis == "100ml":
                scale = float(amount_ml) / 100.0
            else:
                if density is None or density <= 0:
                    raise ValueError(
                        f"Cannot scale 100g-basis food {food_id!r} by volume (ml) without density_g_per_ml."
                    )
                equivalent_g = float(amount_ml) * float(density)
                scale = equivalent_g / 100.0

        if not math.isfinite(scale):
            raise OverflowError("Calculated scaling factor is not finite.")

        scaled_nutrients: Dict[str, Optional[float]] = {}
        for nf in NUTRIENT_FIELDS:
            base_val = food[nf]
            if base_val is None:
                scaled_nutrients[nf] = None
            else:
                prod = float(base_val) * scale
                if not math.isfinite(prod):
                    raise OverflowError(f"Nutrient scaling overflowed for {nf}.")
                scaled_nutrients[nf] = prod

        return {
            "food_id": food_id,
            "food_name": food["food_name"],
            "basis_unit": basis,
            "scale_factor": scale,
            "nutrients": scaled_nutrients,
        }

    def calculate_meal_totals(self, items: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregates multiple scaled food items into a meal summary.
        CRITICAL GUARANTEE: If ANY constituent item has `None` (unknown) for a nutrient,
        `totals[nutrient]` is `None` (never silently undercounted as 0), while
        `partial_totals[nutrient]` exposes the sum of known items alongside `missing_counts`.
        """
        if not items:
            return {
                "item_count": 0,
                "totals": {nf: 0.0 for nf in NUTRIENT_FIELDS},
                "partial_totals": {nf: 0.0 for nf in NUTRIENT_FIELDS},
                "missing_counts": {nf: 0 for nf in NUTRIENT_FIELDS},
                "incomplete_nutrients": [],
            }

        scaled_items = [self.calculate_custom_quantity(**spec) for spec in items]
        totals: Dict[str, Optional[float]] = {}
        partial_totals: Dict[str, float] = {}
        missing_counts: Dict[str, int] = {}
        incomplete_nutrients: List[str] = []

        for nf in NUTRIENT_FIELDS:
            known_vals: List[float] = []
            missing = 0
            for si in scaled_items:
                v = si["nutrients"].get(nf)
                if v is None:
                    missing += 1
                else:
                    known_vals.append(float(v))
            part_sum = sum(known_vals)
            if not math.isfinite(part_sum):
                raise OverflowError(f"Meal total overflowed while summing {nf}.")
            partial_totals[nf] = part_sum
            missing_counts[nf] = missing
            if missing > 0:
                totals[nf] = None
                incomplete_nutrients.append(nf)
            else:
                totals[nf] = part_sum

        return {
            "item_count": len(scaled_items),
            "totals": totals,
            "partial_totals": partial_totals,
            "missing_counts": missing_counts,
            "incomplete_nutrients": incomplete_nutrients,
        }


# ============================================================================
# ETL PIPELINE ORCHESTRATOR & CHECKPOINT RECOVERY
# ============================================================================

@dataclass
class ETLConfig:
    db_path: Path
    source: str
    input_path: Optional[Path] = None
    barcodes: List[str] = field(default_factory=list)
    batch_size: int = 500
    resume: bool = True
    strict: bool = False
    job_key: Optional[str] = None


class ETLPipeline:
    def __init__(self, config: ETLConfig) -> None:
        self.config = config
        self.repo = NutritionDatabaseRepository(config.db_path)

    def close(self) -> None:
        self.repo.close()

    @staticmethod
    def compute_input_signature(
        source: str, input_path: Optional[Path], barcodes: Sequence[str]
    ) -> str:
        h = hashlib.sha256()
        h.update(source.encode("utf-8"))
        if input_path is not None:
            p = Path(input_path).resolve()
            h.update(str(p).encode("utf-8"))
            if p.exists():
                st = p.stat()
                h.update(f"{st.st_size}:{st.st_mtime_ns}".encode("utf-8"))
        if barcodes:
            h.update(",".join(sorted(barcodes)).encode("utf-8"))
        return h.hexdigest()[:32]

    def _build_adapter(self) -> Any:
        src = self.config.source.lower().strip()
        if src == "openfoodfacts":
            return OpenFoodFactsAdapter(
                file_path=self.config.input_path,
                barcodes=self.config.barcodes,
            )
        if src == "usda":
            if self.config.input_path is None:
                raise ValueError("USDA source requires --input pointing to the USDA CSV directory.")
            return USDAAdapter(self.config.input_path)
        if src == "ifct":
            if self.config.input_path is None:
                raise ValueError("IFCT source requires --input pointing to the IFCT CSV/JSON file.")
            return IFCTAdapter(self.config.input_path)
        raise ValueError(f"Unsupported ETL source: {self.config.source!r}")

    def run(self) -> Dict[str, Any]:
        adapter = self._build_adapter()
        sig = self.compute_input_signature(self.config.source, self.config.input_path, self.config.barcodes)
        job_key = self.config.job_key or f"{self.config.source}:{sig[:12]}"

        start_offset = self.repo.get_checkpoint(job_key, sig) if self.config.resume else 0
        accepted_batch: List[CanonicalFoodRecord] = []
        rejected_records: List[RejectedRecord] = []
        warnings_count = 0
        accepted_count = 0
        seen_in_run = 0
        last_idx = start_offset - 1

        t0 = time.time()

        for idx, record, rejection in adapter.iter_records(start_offset=start_offset):
            seen_in_run += 1
            last_idx = idx
            if rejection is not None:
                rejected_records.append(rejection)
            elif record is not None:
                val_res = FoodRecordValidator.validate(record)
                if not val_res.is_valid:
                    rejected_records.append(
                        RejectedRecord(
                            source=record.source,
                            source_id=record.source_id,
                            raw_identifier=record.food_name,
                            reasons=[f"{i.code}: {i.message}" for i in val_res.critical_issues],
                        )
                    )
                else:
                    warnings_count += len(val_res.warnings)
                    accepted_batch.append(record)

            if len(accepted_batch) >= self.config.batch_size:
                self.repo.save_checkpoint_and_batch(
                    records=accepted_batch,
                    job_key=job_key,
                    source=self.config.source,
                    input_signature=sig,
                    next_offset=idx + 1,
                )
                accepted_count += len(accepted_batch)
                accepted_batch.clear()

        if accepted_batch or seen_in_run > 0:
            self.repo.save_checkpoint_and_batch(
                records=accepted_batch,
                job_key=job_key,
                source=self.config.source,
                input_signature=sig,
                next_offset=last_idx + 1,
            )
            accepted_count += len(accepted_batch)
            accepted_batch.clear()

        elapsed = round(time.time() - t0, 4)
        rejected_count = len(rejected_records)

        if seen_in_run == 0 and start_offset == 0:
            status = "EMPTY_INPUT"
        elif accepted_count == 0 and rejected_count > 0:
            status = "FAILED"
        elif rejected_count > 0:
            status = "PARTIAL_WITH_REJECTIONS"
        else:
            status = "COMPLETE"

        return {
            "status": status,
            "source": self.config.source,
            "job_key": job_key,
            "resumed_from_offset": start_offset,
            "records_seen": seen_in_run,
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "warnings_count": warnings_count,
            "elapsed_seconds": elapsed,
            "rejected_samples": [dataclasses.asdict(r) for r in rejected_records[:25]],
        }


# ============================================================================
# SCALE BENCHMARK RUNNER (`--benchmark`)
# ============================================================================

def run_scale_benchmark(num_records: int = 2500, batch_size: int = 500) -> Dict[str, Any]:
    """
    Generates a synthetic dataset of `num_records` items, runs a full transactional
    ETL import + FTS5 search + post-import integrity verification inside a temporary
    directory, and measures throughput, peak memory (`tracemalloc`), and DB size.
    """
    num_records = max(1, int(num_records))
    target_probe_idx = num_records // 2
    probe_query = f"Benchmark Whole Food Item {target_probe_idx}"

    with tempfile.TemporaryDirectory(prefix="nutrivault_bench_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        jsonl_path = tmp_path / "bench_off.jsonl"
        db_path = tmp_path / "bench_core_foods.db"

        with jsonl_path.open("w", encoding="utf-8") as f:
            for i in range(num_records):
                item = {
                    "code": f"{8900000000000 + i}",
                    "product_name": f"Benchmark Whole Food Item {i}",
                    "brands": "NutriVault Labs",
                    "categories": "staples",
                    "nutrition_data_per": "100g",
                    "serving_size": "1 cup (100 g)",
                    "serving_quantity": 100,
                    "serving_quantity_unit": "g",
                    "nutriments": {
                        "energy-kcal_100g": 220.0,
                        "energy-kcal_unit": "kcal",
                        "proteins_100g": 12.5,
                        "proteins_unit": "g",
                        "carbohydrates_100g": 30.0,
                        "carbohydrates_unit": "g",
                        "fat_100g": 5.0,
                        "fat_unit": "g",
                        "fiber_100g": 4.0,
                        "fiber_unit": "g",
                        "sugars_100g": 2.0,
                        "sugars_unit": "g",
                        "sodium_100g": 120.0,
                        "sodium_unit": "mg",
                    },
                }
                f.write(json.dumps(item) + "\n")

        tracemalloc.start()
        t0 = time.perf_counter()
        pipeline = ETLPipeline(
            ETLConfig(
                db_path=db_path,
                source="openfoodfacts",
                input_path=jsonl_path,
                batch_size=batch_size,
            )
        )
        try:
            report = pipeline.run()
            svc = NutritionAppService(pipeline.repo)
            t_search_0 = time.perf_counter()
            hits = svc.search_foods_fts(probe_query, limit=10)
            search_ms = (time.perf_counter() - t_search_0) * 1000.0
        finally:
            pipeline.close()

        elapsed_s = time.perf_counter() - t0
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        audit = verify_database_integrity(db_path)
        benchmark_passed = (
            report["status"] == "COMPLETE"
            and report["accepted_count"] == num_records
            and audit["status"] == "PASS"
            and len(hits) > 0
        )
        return {
            "status": "PASS" if benchmark_passed else "FAIL",
            "records_imported": report["accepted_count"],
            "elapsed_seconds": round(elapsed_s, 4),
            "records_per_second": round(num_records / max(elapsed_s, 1e-6), 1),
            "peak_memory_mb": round(peak_bytes / (1024 * 1024), 3),
            "db_size_mb": round(audit["db_size_bytes"] / (1024 * 1024), 3),
            "fts5_probe_query": probe_query,
            "fts5_search_latency_ms": round(search_ms, 3),
            "fts5_hits_returned": len(hits),
            "integrity_audit_status": audit["status"],
        }


# ============================================================================
# CLI ENTRY POINT (HARDENED ERROR HANDLING FOR DB INIT & REPORT WRITING)
# ============================================================================

def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="NutriVault Production Data Ingestion & Verification CLI (v9)"
    )
    parser.add_argument("--db", type=Path, default=Path("core_foods.db"), help="Path to SQLite database")
    parser.add_argument(
        "--source",
        choices=["usda", "ifct", "openfoodfacts"],
        help="Dataset source to ingest",
    )
    parser.add_argument("--input", type=Path, help="Input file (IFCT/OFF) or directory (USDA)")
    parser.add_argument("--barcodes", nargs="*", default=[], help="Open Food Facts barcodes to fetch via API")
    parser.add_argument("--batch-size", type=int, default=500, help="Transactional batch size")
    parser.add_argument("--no-resume", action="store_true", help="Ignore existing import checkpoints")
    parser.add_argument("--strict", action="store_true", help="Return exit code 2 if any record is rejected")
    parser.add_argument("--report-out", type=Path, help="Optional path to write JSON import summary report")
    parser.add_argument("--verify-db", action="store_true", help="Run post-import integrity audit on --db")
    parser.add_argument(
        "--benchmark",
        type=int,
        nargs="?",
        const=2500,
        metavar="N_RECORDS",
        help="Run synthetic scale & memory benchmark with N_RECORDS (default: 2500)",
    )
    parser.add_argument("--run-tests", action="store_true", help="Run built-in verification test suite")

    args = parser.parse_args(argv)

    if args.run_tests:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestNutriVaultETL)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        return EXIT_OK if result.wasSuccessful() else EXIT_ERROR

    if args.benchmark is not None:
        try:
            bench_report = run_scale_benchmark(
                num_records=max(1, int(args.benchmark)), batch_size=args.batch_size
            )
            print(json.dumps(bench_report, indent=2))
            return EXIT_OK if bench_report["status"] == "PASS" else EXIT_ERROR
        except Exception as exc:
            logger.error("Benchmark failed: %s", exc)
            return EXIT_ERROR

    if args.verify_db and not args.source:
        try:
            audit_report = verify_database_integrity(args.db)
            print(json.dumps(audit_report, indent=2))
            return EXIT_OK if audit_report["status"] == "PASS" else EXIT_ERROR
        except (FileNotFoundError, sqlite3.Error, OSError) as exc:
            logger.error("Database verification failed: %s", exc)
            return EXIT_ERROR

    if not args.source:
        parser.error("Must specify --source (usda, ifct, openfoodfacts), --verify-db, --benchmark, or --run-tests.")

    if args.source in {"usda", "ifct"} and args.input is None:
        logger.error("Source %r requires --input path.", args.source)
        return EXIT_ERROR
    if args.source == "openfoodfacts" and args.input is None and not args.barcodes:
        logger.error("Source 'openfoodfacts' requires either --input <file> or --barcodes <codes...>.")
        return EXIT_ERROR
    if args.input is not None and not args.input.exists():
        logger.error("Input path does not exist: %s", args.input)
        return EXIT_ERROR

    cfg = ETLConfig(
        db_path=args.db,
        source=args.source,
        input_path=args.input,
        barcodes=args.barcodes,
        batch_size=args.batch_size,
        resume=not args.no_resume,
        strict=args.strict,
    )

    pipeline: Optional[ETLPipeline] = None
    try:
        pipeline = ETLPipeline(cfg)
        report = pipeline.run()
        if args.verify_db:
            report["post_import_verification"] = verify_database_integrity(args.db)
    except (FileNotFoundError, ValueError, sqlite3.Error, OSError) as exc:
        logger.error("ETL execution failed: %s", exc)
        return EXIT_ERROR
    finally:
        if pipeline is not None:
            try:
                pipeline.close()
            except Exception as close_exc:
                logger.warning("Failed to close database cleanly: %s", close_exc)

    print(json.dumps(report, indent=2))

    status = report.get("status")
    if status == "FAILED":
        import_exit_code = EXIT_ERROR
    elif status == "EMPTY_INPUT":
        import_exit_code = EXIT_EMPTY_INPUT
    elif status == "PARTIAL_WITH_REJECTIONS" and args.strict:
        import_exit_code = EXIT_STRICT_REJECTIONS
    elif args.verify_db and report.get("post_import_verification", {}).get("status") != "PASS":
        import_exit_code = EXIT_ERROR
    else:
        import_exit_code = EXIT_OK

    if args.report_out is not None:
        try:
            if args.report_out.exists() and args.report_out.is_dir():
                raise IsADirectoryError(f"Report output path is a directory: {args.report_out}")
            if args.report_out.parent and not args.report_out.parent.exists():
                args.report_out.parent.mkdir(parents=True, exist_ok=True)
            args.report_out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        except OSError as report_exc:
            logger.error(
                "Database import finished with status=%s, but writing report file %s failed: %s",
                status,
                args.report_out,
                report_exc,
            )
            return EXIT_REPORT_WRITE_ERROR if import_exit_code == EXIT_OK else import_exit_code

    return import_exit_code


# ============================================================================
# BUILT-IN VERIFICATION & REGRESSION TEST SUITE (29 TESTS)
# ============================================================================

class TestNutriVaultETL(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory(prefix="nutrivault_test_")
        self.tmp_path = Path(self.tmp_dir.name)
        self.db_path = self.tmp_path / "test_nutrivault.db"

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_01_off_sodium_and_salt_unit_respect(self) -> None:
        raw_mg = {
            "code": "1001",
            "product_name": "Soup With Sodium MG",
            "nutriments": {
                "energy-kcal_100g": 50,
                "energy-kcal_unit": "kcal",
                "sodium_100g": 450,
                "sodium_unit": "mg",
            },
        }
        rec1, rej1 = OpenFoodFactsAdapter.normalize_record(raw_mg)
        self.assertIsNone(rej1)
        assert rec1 is not None
        self.assertAlmostEqual(rec1.sodium_mg or 0.0, 450.0, places=5)

        raw_salt_g = {
            "code": "1002",
            "product_name": "Crackers With Salt G",
            "nutriments": {
                "energy-kcal_100g": 400,
                "energy-kcal_unit": "kcal",
                "salt_100g": 1.25,
                "salt_unit": "g",
            },
        }
        rec2, rej2 = OpenFoodFactsAdapter.normalize_record(raw_salt_g)
        self.assertIsNone(rej2)
        assert rec2 is not None
        self.assertAlmostEqual(rec2.sodium_mg or 0.0, 500.0, places=5)

    def test_02_off_missing_unit_rejected_never_guessed(self) -> None:
        raw_missing_unit = {
            "code": "1003",
            "product_name": "Ambiguous Sodium Product",
            "nutriments": {
                "energy-kcal_100g": 100,
                "energy-kcal_unit": "kcal",
                "sodium_100g": 0.5,
            },
        }
        rec, rej = OpenFoodFactsAdapter.normalize_record(raw_missing_unit)
        self.assertIsNone(rec)
        self.assertIsNotNone(rej)

    def test_03_serving_size_regex_and_disagreement_rejection(self) -> None:
        g_val, ml_val, err = parse_serving_size_strict("2 tbsp (30 g)", 30, "g")
        self.assertIsNone(err)
        self.assertEqual(g_val, 30.0)
        self.assertIsNone(ml_val)

        g_bad, ml_bad, err_bad = parse_serving_size_strict("2 tbsp (30 g)", 250, "g")
        self.assertIsNotNone(err_bad)
        self.assertIsNone(g_bad)
        self.assertIsNone(ml_bad)

    def test_04_basis_100ml_never_falls_back_to_100g(self) -> None:
        raw_beverage = {
            "code": "2001",
            "product_name": "Oat Drink",
            "nutrition_data_per": "100ml",
            "serving_size": "250 ml",
            "nutriments": {
                "energy-kcal_100g": 60,
                "energy-kcal_unit": "kcal",
                "proteins_100ml": 1.2,
                "proteins_unit": "g",
            },
        }
        rec, rej = OpenFoodFactsAdapter.normalize_record(raw_beverage)
        self.assertIsNone(rej)
        assert rec is not None
        self.assertEqual(rec.basis_unit, "100ml")
        self.assertIsNone(rec.calories_kcal)
        self.assertAlmostEqual(rec.protein_g or 0.0, 1.2)

    def test_05_missing_nutrients_stay_none_not_zero(self) -> None:
        raw = {
            "code": "3001",
            "product_name": "Pure Olive Oil",
            "nutriments": {
                "energy-kcal_100g": 884,
                "energy-kcal_unit": "kcal",
                "fat_100g": 100.0,
                "fat_unit": "g",
            },
        }
        rec, rej = OpenFoodFactsAdapter.normalize_record(raw)
        self.assertIsNone(rej)
        assert rec is not None
        self.assertIsNone(rec.protein_g)
        self.assertIsNone(rec.carbohydrate_g)
        self.assertIsNone(rec.fiber_g)
        self.assertIsNone(rec.sodium_mg)
        self.assertIsNone(rec.leucine_mg)

    def test_06_basis_dependent_validation_100g_vs_100ml(self) -> None:
        rec_100g = CanonicalFoodRecord(
            food_id="test:hyper_100g",
            food_name="Impossible Solid",
            source="user",
            source_id="1",
            basis_unit="100g",
            carbohydrate_g=120.0,
            sugar_g=120.0,
        )
        for nf in ("carbohydrate_g", "sugar_g"):
            rec_100g.provenance[nf] = NutrientProvenance(nf, "user", "1", nf, 120.0, "g", 120.0, "g", "100g", "id")
        res_100g = FoodRecordValidator.validate(rec_100g)
        self.assertFalse(res_100g.is_valid)

        rec_100ml = CanonicalFoodRecord(
            food_id="test:syrup_100ml",
            food_name="Dense Agave Syrup",
            source="user",
            source_id="2",
            basis_unit="100ml",
            density_g_per_ml=1.35,
            carbohydrate_g=120.0,
            sugar_g=115.0,
        )
        for nf, v in (("carbohydrate_g", 120.0), ("sugar_g", 115.0)):
            rec_100ml.provenance[nf] = NutrientProvenance(nf, "user", "2", nf, v, "g", v, "g", "100ml", "id")
        res_100ml = FoodRecordValidator.validate(rec_100ml)
        self.assertTrue(res_100ml.is_valid)

    def test_07_usda_staging_energy_precedence_and_amino_units(self) -> None:
        usda_dir = self.tmp_path / "usda_sample"
        usda_dir.mkdir()
        (usda_dir / "food.csv").write_text(
            "fdc_id,data_type,description,food_category_id\n170001,foundation_food,Raw Oats,Cereal Grains\n",
            encoding="utf-8",
        )
        (usda_dir / "nutrient.csv").write_text(
            "id,name,unit_name\n1062,Energy kJ,kJ\n2047,Energy Atwater,KCAL\n1003,Protein,G\n1213,Leucine,G\n",
            encoding="utf-8",
        )
        (usda_dir / "food_nutrient.csv").write_text(
            "id,fdc_id,nutrient_id,amount\n1,170001,1062,1600\n2,170001,2047,389\n3,170001,1003,16.9\n4,170001,1213,1.284\n",
            encoding="utf-8",
        )
        (usda_dir / "food_portion.csv").write_text(
            "id,fdc_id,amount,measure_unit_id,portion_description,modifier,gram_weight\n10,170001,1,1000,1 cup,rolled,81.0\n",
            encoding="utf-8",
        )

        adapter = USDAAdapter(usda_dir)
        items = list(adapter.iter_records())
        self.assertEqual(len(items), 1)
        _, rec, rej = items[0]
        self.assertIsNone(rej)
        assert rec is not None
        self.assertAlmostEqual(rec.calories_kcal or 0.0, 389.0)
        self.assertAlmostEqual(rec.leucine_mg or 0.0, 1284.0)
        self.assertEqual(len(rec.portions), 1)
        self.assertAlmostEqual(rec.portions[0].gram_weight or 0.0, 81.0)

    def test_08_ifct_unit_detection_from_headers_and_sibling_columns(self) -> None:
        ifct_csv = self.tmp_path / "ifct_sample.csv"
        ifct_csv.write_text(
            "code,name,grup,enerc_kj,protcnt_g,his_g,na,na_unit\nA001,Bajra (Pearl Millet),Cereals,1456,10.96,0.25,0.012,g\n",
            encoding="utf-8",
        )
        adapter = IFCTAdapter(ifct_csv)
        items = list(adapter.iter_records())
        self.assertEqual(len(items), 1)
        _, rec, rej = items[0]
        self.assertIsNone(rej)
        assert rec is not None
        self.assertAlmostEqual(rec.calories_kcal or 0.0, 1456.0 / 4.184, places=3)
        self.assertAlmostEqual(rec.histidine_mg or 0.0, 250.0, places=3)
        self.assertAlmostEqual(rec.sodium_mg or 0.0, 12.0, places=3)

    def test_09_per_nutrient_provenance_conflict_resolution(self) -> None:
        repo = NutritionDatabaseRepository(self.db_path)
        try:
            off_rec, _ = OpenFoodFactsAdapter.normalize_record(
                {
                    "code": "8901234567890",
                    "product_name": "Toor Dal",
                    "nutriments": {
                        "energy-kcal_100g": 330,
                        "energy-kcal_unit": "kcal",
                        "proteins_100g": 20.0,
                        "proteins_unit": "g",
                        "sodium_100g": 45.0,
                        "sodium_unit": "mg",
                    },
                }
            )
            assert off_rec is not None
            repo.upsert_batch([off_rec])

            ifct_rec, _ = IFCTAdapter.normalize_row(
                {
                    "code": "B001",
                    "name": "Red Gram Dal (Toor)",
                    "enerc_kcal": "335",
                    "protcnt_g": "22.3",
                }
            )
            assert ifct_rec is not None
            ifct_rec.barcode = "8901234567890"
            repo.upsert_batch([ifct_rec])

            row = repo.conn.execute("SELECT * FROM foods WHERE barcode = '8901234567890'").fetchone()
            self.assertAlmostEqual(float(row["protein_g"]), 22.3)
            self.assertAlmostEqual(float(row["sodium_mg"]), 45.0)

            sel_prov = {
                str(r["nutrient_name"]): str(r["source"])
                for r in repo.conn.execute(
                    "SELECT nutrient_name, source FROM nutrient_provenance WHERE food_id = ? AND is_selected = 1",
                    (row["food_id"],),
                ).fetchall()
            }
            self.assertEqual(sel_prov["protein_g"], "ifct")
            self.assertEqual(sel_prov["sodium_mg"], "openfoodfacts")
        finally:
            repo.close()

    def test_10_meal_totals_null_propagation_and_partial_totals(self) -> None:
        repo = NutritionDatabaseRepository(self.db_path)
        try:
            r1, _ = OpenFoodFactsAdapter.normalize_record(
                {
                    "code": "5001",
                    "product_name": "Salted Almonds",
                    "nutriments": {
                        "energy-kcal_100g": 580,
                        "energy-kcal_unit": "kcal",
                        "proteins_100g": 21.0,
                        "proteins_unit": "g",
                        "sodium_100g": 300.0,
                        "sodium_unit": "mg",
                    },
                }
            )
            r2, _ = OpenFoodFactsAdapter.normalize_record(
                {
                    "code": "5002",
                    "product_name": "Unsalted Banana (Sodium Unknown)",
                    "nutriments": {
                        "energy-kcal_100g": 89,
                        "energy-kcal_unit": "kcal",
                        "proteins_100g": 1.1,
                        "proteins_unit": "g",
                    },
                }
            )
            assert r1 is not None and r2 is not None
            repo.upsert_batch([r1, r2])

            svc = NutritionAppService(repo)
            meal = svc.calculate_meal_totals(
                [
                    {"food_id": r1.food_id, "amount_g": 50.0},
                    {"food_id": r2.food_id, "amount_g": 100.0},
                ]
            )
            self.assertAlmostEqual(meal["totals"]["calories_kcal"], 290.0 + 89.0)
            self.assertAlmostEqual(meal["totals"]["protein_g"], 10.5 + 1.1)
            self.assertIsNone(meal["totals"]["sodium_mg"])
            self.assertAlmostEqual(meal["partial_totals"]["sodium_mg"], 150.0)
            self.assertEqual(meal["missing_counts"]["sodium_mg"], 1)
        finally:
            repo.close()

    def test_11_legacy_schema_migration_preserves_child_foreign_keys(self) -> None:
        conn = sqlite3.connect(str(self.db_path))
        conn.execute(
            """
            CREATE TABLE foods (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                brand TEXT,
                barcode TEXT UNIQUE,
                source TEXT NOT NULL,
                category TEXT,
                serving_size_g REAL DEFAULT 100.0,
                calories_kcal REAL NOT NULL,
                protein_g REAL NOT NULL,
                carbs_g REAL NOT NULL,
                fat_g REAL NOT NULL,
                fiber_g REAL DEFAULT 0.0,
                sugar_g REAL DEFAULT 0.0,
                sodium_mg REAL DEFAULT 0.0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE food_portions (
                id TEXT PRIMARY KEY,
                food_id TEXT NOT NULL,
                portion_name TEXT NOT NULL,
                gram_weight REAL NOT NULL,
                FOREIGN KEY(food_id) REFERENCES foods(id) ON DELETE CASCADE
            );
            """
        )
        conn.execute(
            """
            INSERT INTO foods (id, name, source, calories_kcal, protein_g, carbs_g, fat_g, created_at, updated_at)
            VALUES ('usda:100', 'Legacy Apple', 'usda', 52.0, 0.3, 13.8, 0.2, 1700000000, 1700000000);
            """
        )
        conn.execute(
            """
            INSERT INTO food_portions (id, food_id, portion_name, gram_weight)
            VALUES ('p1', 'usda:100', '1 medium apple', 182.0);
            """
        )
        conn.commit()
        conn.close()

        repo = NutritionDatabaseRepository(self.db_path)
        try:
            row = repo.conn.execute(
                "SELECT food_id, food_name, carbohydrate_g FROM foods WHERE food_id = 'usda:100'"
            ).fetchone()
            self.assertEqual(row["food_name"], "Legacy Apple")
            self.assertAlmostEqual(float(row["carbohydrate_g"]), 13.8)

            fk_list = repo.conn.execute("PRAGMA foreign_key_list(food_portions);").fetchall()
            self.assertEqual(fk_list[0][2], "foods")
        finally:
            repo.close()

        audit = verify_database_integrity(self.db_path)
        self.assertEqual(audit["status"], "PASS")

    def test_12_json_array_streamer_rejects_malformed_inputs(self) -> None:
        for bad_json in ("", "   ", "[1,]", '[{"a": 1}', '[{"a": 1}]trailing', '{"not": "array"}'):
            with self.assertRaises(ValueError):
                list(_iter_json_array_stream([bad_json]))

    def test_13_full_precision_microgram_conversion(self) -> None:
        in_g = convert_mass_unit(0.25, "mcg", "g")
        self.assertAlmostEqual(in_g, 2.5e-7, places=12)
        back_mcg = convert_mass_unit(in_g, "g", "mcg")
        self.assertAlmostEqual(back_mcg, 0.25, places=10)

    def test_14_quantity_scaling_rejects_ambiguous_or_overflow_inputs(self) -> None:
        repo = NutritionDatabaseRepository(self.db_path)
        try:
            rec, _ = OpenFoodFactsAdapter.normalize_record(
                {
                    "code": "7001",
                    "product_name": "Plain Yogurt",
                    "nutriments": {"energy-kcal_100g": 61, "energy-kcal_unit": "kcal"},
                }
            )
            assert rec is not None
            repo.upsert_batch([rec])
            svc = NutritionAppService(repo)

            with self.assertRaises(ValueError):
                svc.calculate_custom_quantity(rec.food_id, amount_g=100.0, amount_ml=100.0)

            with self.assertRaises((ValueError, OverflowError)):
                svc.calculate_custom_quantity(rec.food_id, amount_g=1e308 * 2.0)

            with self.assertRaises(ValueError):
                svc.calculate_custom_quantity(rec.food_id, amount_ml=250.0)
        finally:
            repo.close()

    def test_15_fts5_synchronization_on_insert_update_delete(self) -> None:
        repo = NutritionDatabaseRepository(self.db_path)
        try:
            rec, _ = OpenFoodFactsAdapter.normalize_record(
                {
                    "code": "8001",
                    "product_name": "Alphonso Mango Pulp",
                    "brands": "Ratnagiri Naturals",
                    "nutriments": {"energy-kcal_100g": 95, "energy-kcal_unit": "kcal"},
                }
            )
            assert rec is not None
            repo.upsert_batch([rec])

            svc = NutritionAppService(repo)
            self.assertEqual(len(svc.search_foods_fts("Alphonso")), 1)

            repo.conn.execute(
                "UPDATE foods SET food_name = 'Kesar Mango Pulp' WHERE food_id = ?", (rec.food_id,)
            )
            self.assertEqual(len(svc.search_foods_fts("Alphonso")), 0)
            self.assertEqual(len(svc.search_foods_fts("Kesar")), 1)

            repo.conn.execute("DELETE FROM foods WHERE food_id = ?", (rec.food_id,))
            self.assertEqual(len(svc.search_foods_fts("Kesar")), 0)
        finally:
            repo.close()

    def test_16_duplicate_reimport_idempotency(self) -> None:
        off_file = self.tmp_path / "dup.jsonl"
        off_file.write_text(
            json.dumps(
                {
                    "code": "9001",
                    "product_name": "Idempotent Oats",
                    "nutriments": {"energy-kcal_100g": 375, "energy-kcal_unit": "kcal"},
                }
            )
            + "\n",
            encoding="utf-8",
        )
        for _ in range(2):
            p = ETLPipeline(
                ETLConfig(db_path=self.db_path, source="openfoodfacts", input_path=off_file, resume=False)
            )
            try:
                p.run()
            finally:
                p.close()

        repo = NutritionDatabaseRepository(self.db_path)
        try:
            cnt = repo.conn.execute("SELECT COUNT(*) FROM foods").fetchone()[0]
            self.assertEqual(cnt, 1)
        finally:
            repo.close()

    def test_17_strict_mode_exit_code_on_partial_rejections(self) -> None:
        mixed_file = self.tmp_path / "mixed.jsonl"
        mixed_file.write_text(
            json.dumps(
                {
                    "code": "9101",
                    "product_name": "Valid Food",
                    "nutriments": {"energy-kcal_100g": 120, "energy-kcal_unit": "kcal"},
                }
            )
            + "\n"
            + json.dumps({"code": "9102", "product_name": "", "nutriments": {}})
            + "\n",
            encoding="utf-8",
        )
        code_non_strict = main(
            ["--db", str(self.db_path), "--source", "openfoodfacts", "--input", str(mixed_file), "--no-resume"]
        )
        self.assertEqual(code_non_strict, EXIT_OK)

        code_strict = main(
            [
                "--db",
                str(self.db_path),
                "--source",
                "openfoodfacts",
                "--input",
                str(mixed_file),
                "--no-resume",
                "--strict",
            ]
        )
        self.assertEqual(code_strict, EXIT_STRICT_REJECTIONS)

    def test_18_online_barcode_fetching_with_custom_fetcher(self) -> None:
        def fake_fetcher(bc: str) -> Dict[str, Any]:
            return {
                "code": bc,
                "product": {
                    "code": bc,
                    "product_name": "Fetched Greek Yogurt",
                    "nutriments": {
                        "energy-kcal_100g": 97,
                        "energy-kcal_unit": "kcal",
                        "proteins_100g": 9.0,
                        "proteins_unit": "g",
                    },
                },
            }

        adapter = OpenFoodFactsAdapter(barcodes=["737628064502"], http_fetcher=fake_fetcher)
        rows = list(adapter.iter_records())
        self.assertEqual(len(rows), 1)
        _, rec, rej = rows[0]
        self.assertIsNone(rej)
        assert rec is not None
        self.assertEqual(rec.food_name, "Fetched Greek Yogurt")

    def test_19_missing_input_file_raises_and_cli_returns_nonzero(self) -> None:
        nonexistent = self.tmp_path / "does_not_exist.jsonl"
        with self.assertRaises(FileNotFoundError):
            OpenFoodFactsAdapter(file_path=nonexistent)

        exit_code = main(["--db", str(self.db_path), "--source", "openfoodfacts", "--input", str(nonexistent)])
        self.assertEqual(exit_code, EXIT_ERROR)

    def test_20_empty_input_vs_failed_import_outcomes(self) -> None:
        empty_file = self.tmp_path / "empty.jsonl"
        empty_file.write_text("", encoding="utf-8")
        code_empty = main(["--db", str(self.db_path), "--source", "openfoodfacts", "--input", str(empty_file)])
        self.assertEqual(code_empty, EXIT_EMPTY_INPUT)

        all_bad_file = self.tmp_path / "all_bad.jsonl"
        all_bad_file.write_text(
            json.dumps({"code": "000", "product_name": "", "nutriments": {}}) + "\n", encoding="utf-8"
        )
        code_failed = main(
            ["--db", str(self.db_path), "--source", "openfoodfacts", "--input", str(all_bad_file), "--no-resume"]
        )
        self.assertEqual(code_failed, EXIT_ERROR)

    def test_21_cli_db_init_failure_controlled_exit(self) -> None:
        valid_input = self.tmp_path / "valid.jsonl"
        valid_input.write_text(
            json.dumps(
                {
                    "code": "2101",
                    "product_name": "Test Item",
                    "nutriments": {"energy-kcal_100g": 100, "energy-kcal_unit": "kcal"},
                }
            )
            + "\n",
            encoding="utf-8",
        )
        db_as_dir = self.tmp_path / "dir_not_a_db_file"
        db_as_dir.mkdir()

        exit_code = main(["--db", str(db_as_dir), "--source", "openfoodfacts", "--input", str(valid_input)])
        self.assertEqual(exit_code, EXIT_ERROR)

        with mock.patch.object(
            NutritionDatabaseRepository,
            "initialize_schema",
            side_effect=sqlite3.OperationalError("simulated read-only database failure"),
        ):
            exit_code_sqlite = main(
                ["--db", str(self.db_path), "--source", "openfoodfacts", "--input", str(valid_input)]
            )
            self.assertEqual(exit_code_sqlite, EXIT_ERROR)

    def test_22_cli_report_write_failure_distinct_exit_code(self) -> None:
        valid_input = self.tmp_path / "valid_report_test.jsonl"
        valid_input.write_text(
            json.dumps(
                {
                    "code": "2201",
                    "product_name": "Committed Despite Report Failure",
                    "nutriments": {"energy-kcal_100g": 150, "energy-kcal_unit": "kcal"},
                }
            )
            + "\n",
            encoding="utf-8",
        )
        report_dir_conflict = self.tmp_path / "report_is_a_dir"
        report_dir_conflict.mkdir()

        exit_code = main(
            [
                "--db",
                str(self.db_path),
                "--source",
                "openfoodfacts",
                "--input",
                str(valid_input),
                "--report-out",
                str(report_dir_conflict),
            ]
        )
        self.assertEqual(exit_code, EXIT_REPORT_WRITE_ERROR)

        repo = NutritionDatabaseRepository(self.db_path)
        try:
            row = repo.conn.execute("SELECT food_name FROM foods WHERE barcode = '2201'").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["food_name"], "Committed Despite Report Failure")
        finally:
            repo.close()

    def test_23_interrupted_import_and_checkpoint_resume(self) -> None:
        jsonl_file = self.tmp_path / "interrupt_resume.jsonl"
        with jsonl_file.open("w", encoding="utf-8") as f:
            for i in range(4):
                f.write(
                    json.dumps(
                        {
                            "code": f"230{i}",
                            "product_name": f"Resume Food {i}",
                            "nutriments": {"energy-kcal_100g": 100 + i * 10, "energy-kcal_unit": "kcal"},
                        }
                    )
                    + "\n"
                )

        pipeline1 = ETLPipeline(
            ETLConfig(
                db_path=self.db_path,
                source="openfoodfacts",
                input_path=jsonl_file,
                batch_size=2,
                resume=True,
            )
        )
        orig_save = pipeline1.repo.save_checkpoint_and_batch
        call_counter = {"n": 0}

        def interrupting_save(*args: Any, **kwargs: Any) -> int:
            call_counter["n"] += 1
            if call_counter["n"] == 2:
                raise sqlite3.OperationalError("Simulated disk/power interruption on batch 2")
            return orig_save(*args, **kwargs)

        try:
            with mock.patch.object(pipeline1.repo, "save_checkpoint_and_batch", side_effect=interrupting_save):
                with self.assertRaises(sqlite3.OperationalError):
                    pipeline1.run()
        finally:
            pipeline1.close()

        repo_check = NutritionDatabaseRepository(self.db_path)
        try:
            count_after_crash = repo_check.conn.execute("SELECT COUNT(*) FROM foods").fetchone()[0]
            self.assertEqual(count_after_crash, 2)
        finally:
            repo_check.close()

        resume_code = main(
            [
                "--db",
                str(self.db_path),
                "--source",
                "openfoodfacts",
                "--input",
                str(jsonl_file),
                "--batch-size",
                "2",
            ]
        )
        self.assertEqual(resume_code, EXIT_OK)

        repo_final = NutritionDatabaseRepository(self.db_path)
        try:
            final_count = repo_final.conn.execute("SELECT COUNT(*) FROM foods").fetchone()[0]
            self.assertEqual(final_count, 4)
        finally:
            repo_final.close()

    def test_24_multi_source_smoke_test_and_integrity_audit(self) -> None:
        usda_dir = self.tmp_path / "usda_smoke"
        usda_dir.mkdir()
        (usda_dir / "food.csv").write_text(
            "fdc_id,data_type,description,food_category_id\n171287,foundation_food,Egg Whole Raw,Dairy and Egg Products\n",
            encoding="utf-8",
        )
        (usda_dir / "nutrient.csv").write_text(
            "id,name,unit_name\n1008,Energy,KCAL\n1003,Protein,G\n1004,Total lipid (fat),G\n1005,Carbohydrate,G\n1093,Sodium,MG\n1213,Leucine,G\n",
            encoding="utf-8",
        )
        (usda_dir / "food_nutrient.csv").write_text(
            "id,fdc_id,nutrient_id,amount\n1,171287,1008,143\n2,171287,1003,12.56\n3,171287,1004,9.51\n4,171287,1005,0.72\n5,171287,1093,142\n6,171287,1213,1.086\n",
            encoding="utf-8",
        )
        (usda_dir / "food_portion.csv").write_text(
            "id,fdc_id,amount,measure_unit_id,portion_description,modifier,gram_weight\n1,171287,1,1000,1 large,egg,50.0\n",
            encoding="utf-8",
        )

        ifct_file = self.tmp_path / "ifct_smoke.csv"
        ifct_file.write_text(
            "code,name,grup,enerc_kcal,protcnt,fatce,choavldf,fibtg,na\nA002,Brown Rice Raw,Cereals,353,9.16,1.24,74.8,4.43,3.62\n",
            encoding="utf-8",
        )

        off_file = self.tmp_path / "off_smoke.jsonl"
        off_file.write_text(
            json.dumps(
                {
                    "code": "8901058000290",
                    "product_name": "Amul Pasteurised Butter",
                    "brands": "Amul",
                    "serving_size": "10 g",
                    "nutriments": {
                        "energy-kcal_100g": 722,
                        "energy-kcal_unit": "kcal",
                        "proteins_100g": 0.5,
                        "proteins_unit": "g",
                        "carbohydrates_100g": 0.1,
                        "carbohydrates_unit": "g",
                        "fat_100g": 80.0,
                        "fat_unit": "g",
                        "saturated-fat_100g": 51.0,
                        "saturated-fat_unit": "g",
                        "sodium_100g": 836.0,
                        "sodium_unit": "mg",
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )

        self.assertEqual(
            main(["--db", str(self.db_path), "--source", "usda", "--input", str(usda_dir)]), EXIT_OK
        )
        self.assertEqual(
            main(["--db", str(self.db_path), "--source", "ifct", "--input", str(ifct_file)]), EXIT_OK
        )
        self.assertEqual(
            main(["--db", str(self.db_path), "--source", "openfoodfacts", "--input", str(off_file)]), EXIT_OK
        )

        audit = verify_database_integrity(self.db_path)
        self.assertEqual(audit["status"], "PASS")
        self.assertEqual(audit["total_foods"], 3)
        self.assertEqual(audit["fts_row_count"], 3)
        self.assertEqual(audit["missing_provenance_count"], 0)
        self.assertEqual(audit["mismatched_provenance_count"], 0)
        self.assertEqual(audit["issues"], [])

    def test_25_benchmark_passes_and_fails_when_fts5_returns_zero_hits(self) -> None:
        bench_report = run_scale_benchmark(num_records=50, batch_size=25)
        self.assertEqual(bench_report["status"], "PASS")
        self.assertGreaterEqual(bench_report["fts5_hits_returned"], 1)

        bench_code = main(["--benchmark", "50", "--batch-size", "25"])
        self.assertEqual(bench_code, EXIT_OK)

        with mock.patch.object(NutritionAppService, "search_foods_fts", return_value=[]):
            failed_bench = run_scale_benchmark(num_records=20, batch_size=10)
            self.assertEqual(failed_bench["fts5_hits_returned"], 0)
            self.assertEqual(failed_bench["status"], "FAIL")

            failed_cli_code = main(["--benchmark", "20", "--batch-size", "10"])
            self.assertEqual(failed_cli_code, EXIT_ERROR)

    def test_26_verify_db_catches_missing_or_mismatched_provenance(self) -> None:
        repo = NutritionDatabaseRepository(self.db_path)
        try:
            rec, _ = OpenFoodFactsAdapter.normalize_record(
                {
                    "code": "2601",
                    "product_name": "Provenance Audit Item",
                    "nutriments": {"energy-kcal_100g": 200, "energy-kcal_unit": "kcal"},
                }
            )
            assert rec is not None
            repo.upsert_batch([rec])
            repo.conn.execute("DELETE FROM nutrient_provenance WHERE food_id = ?", (rec.food_id,))
        finally:
            repo.close()

        audit = verify_database_integrity(self.db_path)
        self.assertEqual(audit["status"], "FAIL")
        self.assertGreater(audit["missing_provenance_count"], 0)
        self.assertEqual(main(["--db", str(self.db_path), "--verify-db"]), EXIT_ERROR)

    def test_27_fault_injection_every_transaction_boundary_and_disk_full(self) -> None:
        """
        Phase 1 Fix:
        - Uses a dedicated isolated database (`fault_boundary_{fail_on_batch}.db`) for each
          of the 5 transaction boundaries (10 records, batch_size=2 -> batches 1, 2, 3, 4, 5).
        - Injects `sqlite3.OperationalError("database or disk is full")` on batch `fail_on_batch`
          without calling `main()` beforehand.
        - Verifies exact pre-resume committed count `(fail_on_batch - 1) * 2`, checkpoint offset,
          and database integrity after the simulated disk-full failure.
        - Resumes without fault injection and verifies completion (`10` records, `0` duplicates,
          checkpoint `10`, and `verify_database_integrity() == PASS`).
        - Tests CLI error handling separately on its own isolated database (`cli_disk_full.db`).
        """
        jsonl_file = self.tmp_path / "fault_boundaries.jsonl"
        with jsonl_file.open("w", encoding="utf-8") as f:
            for i in range(10):
                f.write(
                    json.dumps(
                        {
                            "code": f"{27000 + i}",
                            "product_name": f"Boundary Food {i}",
                            "nutriments": {
                                "energy-kcal_100g": 100.0 + i,
                                "energy-kcal_unit": "kcal",
                                "proteins_100g": 5.0,
                                "proteins_unit": "g",
                            },
                        }
                    )
                    + "\n"
                )

        # 1. Exercise each of the 5 batch boundaries in its own isolated database
        for fail_on_batch in range(1, 6):
            boundary_db = self.tmp_path / f"fault_boundary_{fail_on_batch}.db"
            cfg = ETLConfig(
                db_path=boundary_db,
                source="openfoodfacts",
                input_path=jsonl_file,
                batch_size=2,
                resume=True,
            )

            p_crash = ETLPipeline(cfg)
            orig_save = p_crash.repo.save_checkpoint_and_batch
            call_tracker = {"batch_calls": 0}

            def fail_at_target_boundary(*args: Any, **kwargs: Any) -> int:
                call_tracker["batch_calls"] += 1
                if call_tracker["batch_calls"] == fail_on_batch:
                    raise sqlite3.OperationalError("database or disk is full")
                return orig_save(*args, **kwargs)

            try:
                with mock.patch.object(
                    p_crash.repo, "save_checkpoint_and_batch", side_effect=fail_at_target_boundary
                ):
                    with self.assertRaises(sqlite3.OperationalError):
                        p_crash.run()
            finally:
                p_crash.close()

            self.assertEqual(call_tracker["batch_calls"], fail_on_batch)
            expected_committed_before_resume = (fail_on_batch - 1) * 2

            # Verify pre-resume state: no partial batch committed, exact checkpoint offset, and valid DB
            pre_audit = verify_database_integrity(boundary_db)
            self.assertEqual(pre_audit["status"], "PASS")
            self.assertEqual(pre_audit["total_foods"], expected_committed_before_resume)

            repo_check = NutritionDatabaseRepository(boundary_db)
            try:
                sig = ETLPipeline.compute_input_signature("openfoodfacts", jsonl_file, [])
                job_key = f"openfoodfacts:{sig[:12]}"
                saved_offset = repo_check.get_checkpoint(job_key, sig)
                self.assertEqual(saved_offset, expected_committed_before_resume)
            finally:
                repo_check.close()

            # Resume without fault injection and verify exact completion
            p_resume = ETLPipeline(cfg)
            try:
                resume_report = p_resume.run()
                self.assertEqual(resume_report["status"], "COMPLETE")
                self.assertEqual(resume_report["resumed_from_offset"], expected_committed_before_resume)
                self.assertEqual(resume_report["accepted_count"], 10 - expected_committed_before_resume)
            finally:
                p_resume.close()

            post_audit = verify_database_integrity(boundary_db)
            self.assertEqual(post_audit["status"], "PASS")
            self.assertEqual(post_audit["total_foods"], 10)
            self.assertEqual(post_audit["fts_row_count"], 10)
            self.assertEqual(post_audit["missing_provenance_count"], 0)

        # 2. Test CLI disk-full error handling separately using its own isolated database
        cli_fault_db = self.tmp_path / "cli_disk_full.db"
        with mock.patch.object(
            NutritionDatabaseRepository,
            "save_checkpoint_and_batch",
            side_effect=sqlite3.OperationalError("database or disk is full"),
        ):
            cli_exit = main(
                [
                    "--db",
                    str(cli_fault_db),
                    "--source",
                    "openfoodfacts",
                    "--input",
                    str(jsonl_file),
                    "--batch-size",
                    "2",
                ]
            )
            self.assertEqual(cli_exit, EXIT_ERROR)

        cli_audit = verify_database_integrity(cli_fault_db)
        self.assertEqual(cli_audit["status"], "PASS")
        self.assertEqual(cli_audit["total_foods"], 0)

    def test_28_large_dataset_end_to_end_disposable_db_validation(self) -> None:
        large_json = self.tmp_path / "large_1000.json"
        records = [
            {
                "code": f"{8909000000000 + i}",
                "product_name": f"Large Scale Staple Food {i}",
                "brands": "NutriVault Core",
                "serving_size": "50 g",
                "nutriments": {
                    "energy-kcal_100g": 250.0,
                    "energy-kcal_unit": "kcal",
                    "proteins_100g": 10.0,
                    "proteins_unit": "g",
                    "carbohydrates_100g": 40.0,
                    "carbohydrates_unit": "g",
                    "fat_100g": 5.0,
                    "fat_unit": "g",
                },
            }
            for i in range(1000)
        ]
        large_json.write_text(json.dumps(records), encoding="utf-8")

        code = main(
            [
                "--db",
                str(self.db_path),
                "--source",
                "openfoodfacts",
                "--input",
                str(large_json),
                "--batch-size",
                "250",
                "--verify-db",
            ]
        )
        self.assertEqual(code, EXIT_OK)

        audit = verify_database_integrity(self.db_path)
        self.assertEqual(audit["status"], "PASS")
        self.assertEqual(audit["total_foods"], 1000)
        self.assertEqual(audit["fts_row_count"], 1000)
        self.assertEqual(audit["missing_provenance_count"], 0)

    def test_29_mid_transaction_rollback_and_changed_input_checkpoint_reset(self) -> None:
        """
        Verifies:
        1. If a failure occurs halfway through `save_checkpoint_and_batch` (after record 1 is
           inserted into `foods` inside `BEGIN IMMEDIATE`, before `COMMIT`), the entire batch
           rolls back atomically (`total_foods == 0`, checkpoint not advanced).
        2. If the input file is modified after a checkpoint is saved, `get_checkpoint()` detects
           the signature change and resets the offset to 0.
        """
        r1, _ = OpenFoodFactsAdapter.normalize_record(
            {
                "code": "2901",
                "product_name": "First In Batch",
                "nutriments": {"energy-kcal_100g": 100.0, "energy-kcal_unit": "kcal"},
            }
        )
        r2, _ = OpenFoodFactsAdapter.normalize_record(
            {
                "code": "2902",
                "product_name": "Second In Batch Triggers Failure",
                "nutriments": {"energy-kcal_100g": 200.0, "energy-kcal_unit": "kcal"},
            }
        )
        assert r1 is not None and r2 is not None

        repo = NutritionDatabaseRepository(self.db_path)
        orig_upsert_single = repo._upsert_single_record_in_tx
        upsert_calls = {"n": 0}

        def fail_on_second_record(cur: sqlite3.Cursor, rec: CanonicalFoodRecord) -> None:
            upsert_calls["n"] += 1
            if upsert_calls["n"] == 1:
                orig_upsert_single(cur, rec)
                return
            raise sqlite3.OperationalError("Simulated disk failure mid-transaction after record 1 inserted")

        try:
            with mock.patch.object(repo, "_upsert_single_record_in_tx", side_effect=fail_on_second_record):
                with self.assertRaises(sqlite3.OperationalError):
                    repo.save_checkpoint_and_batch(
                        records=[r1, r2],
                        job_key="test_job",
                        source="openfoodfacts",
                        input_signature="sig_v1",
                        next_offset=2,
                    )

            # Record 1 MUST have been rolled back; checkpoint MUST remain 0
            cnt = repo.conn.execute("SELECT COUNT(*) FROM foods").fetchone()[0]
            self.assertEqual(cnt, 0)
            self.assertEqual(repo.get_checkpoint("test_job", "sig_v1"), 0)

            # Now commit normally with signature "sig_v1" and verify offset is 2,
            # but querying with modified file signature "sig_v2" resets to 0.
            repo.save_checkpoint_and_batch(
                records=[r1, r2],
                job_key="test_job",
                source="openfoodfacts",
                input_signature="sig_v1",
                next_offset=2,
            )
            self.assertEqual(repo.get_checkpoint("test_job", "sig_v1"), 2)
            self.assertEqual(repo.get_checkpoint("test_job", "sig_v2_modified_input"), 0)
        finally:
            repo.close()


if __name__ == "__main__":
    sys.exit(main())
