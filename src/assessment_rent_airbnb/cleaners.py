"""
Cleaning and parsing functions for Airbnb and Kamernet rental data.

Pure Python — no Spark dependency — so these work as Spark UDFs,
in unit tests, and in local debugging without a cluster.
"""

import re
from typing import Optional, Tuple


def parse_rent_string(rent_str: str) -> Tuple[Optional[float], bool]:
    """
    Extract numeric rent and utilities flag from Kamernet rent strings.

    Examples:
        "€ 500,-"                  -> (500.0, False)
        "€ 950,-  Utilities incl." -> (950.0, True)
        "€ 1.250,-"                -> (1250.0, False)
    """
    if not rent_str or not isinstance(rent_str, str):
        return None, False

    utilities_included = "utilities incl" in rent_str.lower()
    cleaned = rent_str.replace("€", "").replace("\u20ac", "").strip()
    cleaned = re.sub(r"\s*,-.*", "", cleaned).strip()
    cleaned = cleaned.replace(".", "")

    try:
        return float(cleaned), utilities_included
    except (ValueError, TypeError):
        return None, utilities_included


def parse_rent_amount(rent_str: str) -> Optional[float]:
    """UDF-friendly wrapper — returns just the numeric rent."""
    amount, _ = parse_rent_string(rent_str)
    return amount


def parse_rent_utilities(rent_str: str) -> bool:
    """UDF-friendly wrapper — returns just the utilities flag."""
    _, utils = parse_rent_string(rent_str)
    return utils


def normalize_postal_code_to_pc4(postal_code: str) -> Optional[str]:
    """
    Normalize any Dutch postal code to PC4 (4-digit) format.

    "1053"    -> "1053"
    "1016 AM" -> "1016"
    "1013HE"  -> "1013"
    """
    if not postal_code or not isinstance(postal_code, str):
        return None
    match = re.match(r"^(\d{4})", postal_code.strip())
    if match:
        pc4 = match.group(1)
        if 1000 <= int(pc4) <= 9999:
            return pc4
    return None


def parse_area_sqm(area_str: str) -> Optional[float]:
    """Extract numeric area from strings like '14 m2'."""
    if not area_str or not isinstance(area_str, str):
        return None
    match = re.search(r"(\d+(?:\.\d+)?)", area_str)
    return float(match.group(1)) if match else None


def parse_match_capacity(capacity_str: str) -> Optional[int]:
    """Extract number of persons from strings like '1 person'."""
    if not capacity_str or not isinstance(capacity_str, str):
        return None
    match = re.search(r"(\d+)", capacity_str)
    return int(match.group(1)) if match else None


def parse_deposit_string(deposit_str: str) -> Optional[float]:
    """Extract numeric deposit from strings like '\\n  € 500\\n'."""
    if not deposit_str or not isinstance(deposit_str, str):
        return None
    cleaned = deposit_str.replace("€", "").replace("\u20ac", "").strip()
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def detect_price_outliers(
    price: float,
    lower_bound: float = 10.0,
    upper_bound: float = 2000.0,
) -> bool:
    """Flag Airbnb nightly prices outside a reasonable range."""
    if price is None:
        return True
    return price < lower_bound or price > upper_bound