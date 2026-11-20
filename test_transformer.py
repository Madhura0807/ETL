import numpy as np
import pandas as pd

from src.transformations.transformer import Transformer, column_name_map, standardize_column_name


def run(df, **cfg):
    return Transformer(cfg).transform(df)


def test_standardize_column_names():
    assert standardize_column_name("Customer ID") == "customer_id"
    assert standardize_column_name("customerId") == "customer_id"
    assert standardize_column_name("  E-mail ") == "e_mail"
    assert standardize_column_name("1st value") == "col_1st_value"
    assert standardize_column_name("###") == "column"


def test_standardize_collisions_never_overwrite():
    m = column_name_map(["ID", "Id", "id"])
    assert list(m.values()) == ["id", "id_2", "id_3"]
    res = run(pd.DataFrame({"ID": [1], "Id": [2]}), standardize_columns=True)
    assert list(res.data.columns) == ["id", "id_2"] and res.column_map == {"ID": "id", "Id": "id_2"}


def test_trim_strings_and_blank_to_null():
    df = pd.DataFrame({"a": ["  x ", "   ", "y"], "n": [1, 2, 3]})
    out = run(df, trim_strings=True).data
    assert out["a"].iloc[0] == "x" and pd.isna(out["a"].iloc[1]) and out["n"].tolist() == [1, 2, 3]


def test_integer_conversion_records_failures_with_row_index():
    df = pd.DataFrame({"n": ["1", "2.0", "abc", "3.5", None]})
    res = run(df, type_conversions={"n": "integer"})
    assert str(res.data["n"].dtype) == "Int64"
    assert res.data["n"].isna().tolist() == [False, False, True, True, True]
    assert set(res.conversion_failures) == {2, 3}
    assert "abc" in res.conversion_failures[2][0]


def test_float_boolean_string_conversions():
    df = pd.DataFrame({"f": ["1.5", "x"], "b": ["yes", "maybe"], "s": [1, 2]})
    res = run(df, type_conversions={"f": "float", "b": "boolean", "s": "string"})
    assert res.data["f"].iloc[0] == 1.5 and bool(res.data["b"].iloc[0]) is True
    assert res.data["s"].tolist() == ["1", "2"]
    assert set(res.conversion_failures) == {1}  # row 1 failed float AND boolean
    assert len(res.conversion_failures[1]) == 2


def test_date_standardisation_mixed_formats_and_dayfirst():
    df = pd.DataFrame({"d": ["2024-03-05", "05/03/2024", "2024-03-05 14:30:00", "garbage", None]})
    res = run(df, date_columns=["d"], date_dayfirst=True)
    assert res.data["d"].iloc[0] == pd.Timestamp("2024-03-05")
    assert res.data["d"].iloc[1] == pd.Timestamp("2024-03-05")        # dayfirst: 5 March
    assert res.data["d"].iloc[2] == pd.Timestamp("2024-03-05")        # normalised to midnight
    assert list(res.conversion_failures) == [3]
    res2 = run(pd.DataFrame({"d": ["05/03/2024"]}), date_columns=["d"], date_dayfirst=False)
    assert res2.data["d"].iloc[0] == pd.Timestamp("2024-05-03")


def test_missing_value_fill_and_strategies():
    df = pd.DataFrame({"tier": ["a", None], "n": [1, None], "m": [10.0, np.nan], "c": ["x", None]})
    res = run(df, type_conversions={"n": "integer"},
              missing_values={"fill": {"tier": "std"}, "strategies": {"n": "median", "m": "mean", "c": "mode"}})
    assert res.data["tier"].tolist() == ["a", "std"]
    assert res.data["n"].tolist() == [1, 1] and res.data["m"].tolist() == [10.0, 10.0]
    assert res.data["c"].tolist() == ["x", "x"]


def test_normalize_case():
    df = pd.DataFrame({"e": ["A@X.com"], "c": ["in"], "n": ["jane doe"]})
    out = run(df, normalize={"lowercase": ["e"], "uppercase": ["c"], "titlecase": ["n"]}).data
    assert out.iloc[0].tolist() == ["a@x.com", "IN", "Jane Doe"]


def test_duplicates_removed_and_counted_after_normalisation():
    df = pd.DataFrame({"e": ["A@x.com", "a@x.com ", "b@x.com"]})
    res = run(df, trim_strings=True, normalize={"lowercase": ["e"]}, remove_duplicates=True)
    assert len(res.data) == 2 and res.stats["duplicates_removed"] == 1


def test_duplicates_subset_keep_last_and_failures_follow_removed_rows():
    df = pd.DataFrame({"k": [1, 1, 2], "n": ["a", "bad", "c"]})
    res = run(df, remove_duplicates={"subset": ["k"], "keep": "last"}, type_conversions={"k": "integer"})
    assert res.data["n"].tolist() == ["bad", "c"]
    df2 = pd.DataFrame({"k": [1, 1], "v": ["x", "oops"]})
    res2 = run(df2, remove_duplicates={"subset": ["k"], "keep": "first"}, type_conversions={"v": "integer"})
    assert list(res2.conversion_failures) == [0]   # idx 1 ('oops') was a removed duplicate, so only the kept row's failure remains


def test_empty_dataframe_and_missing_config_columns_do_not_crash():
    res = run(pd.DataFrame({"a": pd.Series([], dtype=object)}), trim_strings=True, remove_duplicates=True,
              type_conversions={"ghost": "integer"}, normalize={"lowercase": ["ghost"]})
    assert res.data.empty and res.stats["output_rows"] == 0


def test_original_dataframe_is_not_mutated():
    df = pd.DataFrame({"A B": [" x "]})
    run(df, standardize_columns=True, trim_strings=True)
    assert list(df.columns) == ["A B"] and df.iloc[0, 0] == " x "
