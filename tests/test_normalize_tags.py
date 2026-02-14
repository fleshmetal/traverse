from __future__ import annotations

import pandas as pd

from traverse.processing.normalize import split_tags, pretty_label


class TestSplitTags:
    def test_pipe_delimited(self) -> None:
        assert split_tags("rock|pop|jazz") == ["rock", "pop", "jazz"]

    def test_comma_delimited(self) -> None:
        assert split_tags("rock,pop,jazz") == ["rock", "pop", "jazz"]

    def test_semicolon_delimited(self) -> None:
        assert split_tags("rock;pop;jazz") == ["rock", "pop", "jazz"]

    def test_json_array(self) -> None:
        assert split_tags('["Electronic","IDM"]') == ["electronic", "idm"]

    def test_sentinel_nan(self) -> None:
        assert split_tags("nan") == []

    def test_sentinel_none(self) -> None:
        assert split_tags("None") == []

    def test_sentinel_null(self) -> None:
        assert split_tags("null") == []

    def test_sentinel_bracket_empty(self) -> None:
        assert split_tags("[]") == []

    def test_sentinel_na(self) -> None:
        assert split_tags("na") == []
        assert split_tags("<NA>") == []
        assert split_tags("n/a") == []

    def test_none_input(self) -> None:
        assert split_tags(None) == []

    def test_pandas_na(self) -> None:
        assert split_tags(pd.NA) == []

    def test_float_nan(self) -> None:
        assert split_tags(float("nan")) == []

    def test_whitespace_collapse(self) -> None:
        assert split_tags("  post   punk  |  new  wave  ") == ["post punk", "new wave"]

    def test_dedupe(self) -> None:
        assert split_tags("rock|rock|pop") == ["rock", "pop"]

    def test_empty_string(self) -> None:
        assert split_tags("") == []

    def test_single_tag(self) -> None:
        assert split_tags("electronic") == ["electronic"]


class TestPrettyLabel:
    def test_basic(self) -> None:
        assert pretty_label("electronic") == "Electronic"

    def test_idm(self) -> None:
        assert pretty_label("idm") == "IDM"

    def test_edm(self) -> None:
        assert pretty_label("edm") == "EDM"

    def test_dnb(self) -> None:
        assert pretty_label("dnb") == "DnB"

    def test_uk_prefix(self) -> None:
        assert pretty_label("uk garage") == "UK Garage"

    def test_multiword(self) -> None:
        assert pretty_label("post punk") == "Post Punk"
