"""Unit tests for cleaning and parsing functions.

These run locally without Spark — cleaners are pure Python.
"""

import pytest
from assessment_rent_airbnb.cleaners import (
    parse_rent_string,
    parse_rent_amount,
    parse_rent_utilities,
    normalize_postal_code_to_pc4,
    parse_area_sqm,
    parse_match_capacity,
    parse_deposit_string,
    detect_price_outliers,
)


class TestParseRentString:
    def test_simple_rent(self):
        amount, utils = parse_rent_string("€ 500,-")
        assert amount == 500.0
        assert utils is False

    def test_rent_with_utilities(self):
        amount, utils = parse_rent_string("€ 950,-  Utilities incl.")
        assert amount == 950.0
        assert utils is True

    def test_rent_with_thousands_separator(self):
        amount, utils = parse_rent_string("€ 1.250,-")
        assert amount == 1250.0
        assert utils is False

    def test_empty_string(self):
        amount, utils = parse_rent_string("")
        assert amount is None
        assert utils is False

    def test_none_input(self):
        amount, utils = parse_rent_string(None)
        assert amount is None
        assert utils is False


class TestUDFWrappers:
    def test_parse_rent_amount(self):
        assert parse_rent_amount("€ 500,-") == 500.0
        assert parse_rent_amount(None) is None

    def test_parse_rent_utilities(self):
        assert parse_rent_utilities("€ 500,-") is False
        assert parse_rent_utilities("€ 950,-  Utilities incl.") is True


class TestNormalizePostalCode:
    def test_four_digit(self):
        assert normalize_postal_code_to_pc4("1053") == "1053"

    def test_with_space(self):
        assert normalize_postal_code_to_pc4("1016 AM") == "1016"

    def test_no_space(self):
        assert normalize_postal_code_to_pc4("1013HE") == "1013"

    def test_full_format(self):
        assert normalize_postal_code_to_pc4("1018AS") == "1018"

    def test_empty(self):
        assert normalize_postal_code_to_pc4("") is None

    def test_none(self):
        assert normalize_postal_code_to_pc4(None) is None

    def test_invalid(self):
        assert normalize_postal_code_to_pc4("0999") is None


class TestParseAreaSqm:
    def test_standard(self):
        assert parse_area_sqm("14 m2") == 14.0

    def test_none(self):
        assert parse_area_sqm(None) is None


class TestParseMatchCapacity:
    def test_single(self):
        assert parse_match_capacity("1 person") == 1

    def test_plural(self):
        assert parse_match_capacity("2 persons") == 2

    def test_none(self):
        assert parse_match_capacity(None) is None


class TestParseDepositString:
    def test_standard(self):
        assert parse_deposit_string("\n  € 500\n") == 500.0

    def test_none(self):
        assert parse_deposit_string(None) is None


class TestDetectPriceOutliers:
    def test_normal_price(self):
        assert detect_price_outliers(125.0) is False

    def test_too_low(self):
        assert detect_price_outliers(5.0) is True

    def test_too_high(self):
        assert detect_price_outliers(9000.0) is True

    def test_none(self):
        assert detect_price_outliers(None) is True