from __future__ import annotations

import json
import runpy
import shutil
import textwrap
import urllib.request
from datetime import date
from pathlib import Path
from typing import Iterable
from uuid import uuid4

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = PROJECT_ROOT / "Source Data"
RAW_DIR = SOURCE_DIR / "Raw"
CURATED_DIR = SOURCE_DIR / "Curated"
SEMANTIC_DIR = PROJECT_ROOT / "School Attendance Dashboard.SemanticModel"
SEMANTIC_DEF_DIR = SEMANTIC_DIR / "definition"
TABLES_DIR = SEMANTIC_DEF_DIR / "tables"
REPORT_DIR = PROJECT_ROOT / "School Attendance Dashboard.Report"
REPORT_PAGES_DIR = REPORT_DIR / "definition" / "pages"


DATASETS = {
    "absence_rates_by_geographic_level": {
        "title": "Absence rates by geographic level",
        "url": "https://explore-education-statistics.service.gov.uk/data-catalogue/data-set/d37f27c4-cca2-4274-97e9-1cdcb4ecad18/csv",
        "raw_file": "absence_rates_by_geographic_level.csv",
        "coverage": "Annual local authority, regional and national absence data, 2006/07 to 2024/25.",
    },
    "absence_by_geographic_level_termly": {
        "title": "Absence by geographic level - termly",
        "url": "https://explore-education-statistics.service.gov.uk/data-catalogue/data-set/9bcb4217-4dba-4039-a754-a80ca681aedb/csv",
        "raw_file": "absence_by_geographic_level_termly.csv",
        "coverage": "Termly local authority, regional and national absence data, 2018/19 autumn term to 2024/25 summer term.",
    },
    "absence_by_pupil_characteristics": {
        "title": "Absence by pupil characteristics",
        "url": "https://explore-education-statistics.service.gov.uk/data-catalogue/data-set/e2eb493a-dc95-41f4-96ef-b886c5cd2019/csv",
        "raw_file": "absence_by_pupil_characteristics.csv",
        "coverage": "Annual local authority, regional and national absence by pupil characteristic, 2018/19 to 2024/25.",
    },
    "absence_by_ethnicity_and_fsm": {
        "title": "Absence by pupil ethnicity and FSM eligibility",
        "url": "https://explore-education-statistics.service.gov.uk/data-catalogue/data-set/26adfbe5-cfb5-40d9-bfa8-bdfe85995b34/csv",
        "raw_file": "absence_by_ethnicity_and_fsm.csv",
        "coverage": "National annual absence by ethnicity minor group and FSM eligibility, 2023/24 to 2024/25.",
    },
}


NUMERIC_COLUMNS = {
    "num_schools",
    "enrolments",
    "sess_possible",
    "sess_overall",
    "sess_authorised",
    "sess_unauthorised",
    "sess_overall_percent",
    "sess_authorised_percent",
    "sess_unauthorised_percent",
    "enrolments_pa_10_exact",
    "enrolments_pa_10_exact_percent",
    "sess_possible_pa_10_exact",
    "sess_overall_pa_10_exact",
    "sess_authorised_pa_10_exact",
    "sess_unauthorised_pa_10_exact",
    "sess_overall_percent_pa_10_exact",
    "sess_authorised_percent_pa_10_exact",
    "sess_unauthorised_percent_pa_10_exact",
    "enrolments_pa_50_exact",
    "enrolments_pa_50_exact_percent",
    "sess_auth_illness",
    "sess_auth_appointments",
    "sess_auth_religious",
    "sess_auth_study",
    "sess_auth_traveller",
    "sess_auth_holiday",
    "sess_auth_ext_holiday",
    "sess_auth_excluded",
    "sess_auth_interview",
    "sess_auth_reg_performance",
    "sess_auth_temp_reduced_timetable",
    "sess_auth_other",
    "sess_auth_totalreasons",
    "sess_unauth_holiday",
    "sess_unauth_late",
    "sess_unauth_other",
    "sess_unauth_noyet",
    "sess_unauth_totalreasons",
    "sess_overall_totalreasons",
    "sess_x_covid",
    "sess_y_exceptional",
    "sess_q_la_arrangements",
    "sess_auth_illness_rate",
    "sess_auth_appointments_rate",
    "sess_auth_religious_rate",
    "sess_auth_study_rate",
    "sess_auth_traveller_rate",
    "sess_auth_holiday_rate",
    "sess_auth_ext_holiday_rate",
    "sess_auth_excluded_rate",
    "sess_auth_interview_rate",
    "sess_auth_reg_performance_rate",
    "sess_auth_temp_reduced_timetable_rate",
    "sess_auth_other_rate",
    "sess_auth_totalreasons_rate",
    "sess_unauth_holiday_rate",
    "sess_unauth_late_rate",
    "sess_unauth_other_rate",
    "sess_unauth_noyet_rate",
    "sess_unauth_totalreasons_rate",
    "sess_overall_totalreasons_rate",
    "sess_x_covid_rate",
}


def ensure_dirs() -> None:
    for path in [RAW_DIR, CURATED_DIR, TABLES_DIR, REPORT_PAGES_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def download_sources(force: bool = False) -> None:
    headers = {"User-Agent": "school-attendance-powerbi-portfolio/1.0"}
    for key, meta in DATASETS.items():
        out_path = RAW_DIR / meta["raw_file"]
        if out_path.exists() and not force:
            print(f"Using existing raw file: {out_path.name}")
            continue
        print(f"Downloading {meta['title']}...")
        request = urllib.request.Request(meta["url"], headers=headers)
        with urllib.request.urlopen(request, timeout=180) as response:
            out_path.write_bytes(response.read())
        print(f"Saved {out_path.name}")


def read_raw(name: str) -> pd.DataFrame:
    path = RAW_DIR / DATASETS[name]["raw_file"]
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    return df


def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.replace({"": None, "x": None, "z": None, "c": None}), errors="coerce")


def clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if col in NUMERIC_COLUMNS:
            df[col] = to_number(df[col])
    return df


def academic_year_label(value: str) -> str:
    text = str(value).strip()
    if "/" in text:
        return text
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}/{text[4:]}"
    if len(text) == 4 and text.isdigit():
        start = int(text)
        return f"{start}/{str(start + 1)[-2:]}"
    return text


def academic_year_sort(value: str) -> int | None:
    text = academic_year_label(value)
    try:
        return int(text[:4])
    except ValueError:
        return None


def geokey(row: pd.Series) -> str:
    level = str(row.get("geographic_level", "")).strip()
    if level == "National":
        return f"NAT|{row.get('country_code', 'E92000001') or 'E92000001'}"
    if level == "Regional":
        return f"REG|{row.get('region_code', '')}"
    if level == "Local authority":
        code = row.get("new_la_code") or row.get("old_la_code")
        return f"LA|{code}"
    return f"{level}|{row.get('country_code', '')}|{row.get('region_code', '')}|{row.get('new_la_code', '')}"


def geography_name(row: pd.Series) -> str:
    level = str(row.get("geographic_level", "")).strip()
    if level == "National":
        return row.get("country_name") or "England"
    if level == "Regional":
        return row.get("region_name") or ""
    if level == "Local authority":
        return row.get("la_name") or ""
    return ""


def school_type_key(value: str) -> str:
    return str(value or "Unknown").strip()


def add_common_fields(df: pd.DataFrame, school_col: str = "education_phase") -> pd.DataFrame:
    df = clean_numeric(df)
    df["AcademicYearKey"] = df["time_period"].map(academic_year_label)
    df["AcademicYearSort"] = df["time_period"].map(academic_year_sort)
    df["GeographyKey"] = df.apply(geokey, axis=1)
    df["GeographyName"] = df.apply(geography_name, axis=1)
    df["SchoolTypeKey"] = df[school_col].map(school_type_key)
    return df


def fact_annual(df: pd.DataFrame) -> pd.DataFrame:
    df = add_common_fields(df)
    out = pd.DataFrame(
        {
            "AcademicYearKey": df["AcademicYearKey"],
            "AcademicYearSort": df["AcademicYearSort"],
            "YearBreakdown": df.get("year_breakdown", ""),
            "GeographyKey": df["GeographyKey"],
            "SchoolTypeKey": df["SchoolTypeKey"],
            "EducationPhase": df["education_phase"],
            "NumSchools": df.get("num_schools"),
            "Enrolments": df.get("enrolments"),
            "SessionsPossible": df.get("sess_possible"),
            "OverallAbsenceSessions": df.get("sess_overall"),
            "AuthorisedAbsenceSessions": df.get("sess_authorised"),
            "UnauthorisedAbsenceSessions": df.get("sess_unauthorised"),
            "OverallAbsenceRateSource": df.get("sess_overall_percent") / 100,
            "AuthorisedAbsenceRateSource": df.get("sess_authorised_percent") / 100,
            "UnauthorisedAbsenceRateSource": df.get("sess_unauthorised_percent") / 100,
            "AttendanceRateSource": 1 - (df.get("sess_overall_percent") / 100),
            "PersistentAbsentees": df.get("enrolments_pa_10_exact"),
            "PersistentAbsenceRateSource": df.get("enrolments_pa_10_exact_percent") / 100,
            "SevereAbsentees": df.get("enrolments_pa_50_exact"),
            "SevereAbsenceRateSource": df.get("enrolments_pa_50_exact_percent") / 100,
            "IllnessAbsenceRateSource": df.get("sess_auth_illness_rate") / 100,
            "AppointmentsAbsenceRateSource": df.get("sess_auth_appointments_rate") / 100,
            "UnauthorisedHolidayAbsenceRateSource": df.get("sess_unauth_holiday_rate") / 100,
        }
    )
    return out


def fact_termly(df: pd.DataFrame) -> pd.DataFrame:
    df = add_common_fields(df)
    term_order = {"Autumn term": 1, "Spring term": 2, "Summer term": 3}
    term = df["time_identifier"].fillna("")
    out = pd.DataFrame(
        {
            "TermKey": df["AcademicYearKey"] + "|" + term,
            "AcademicYearKey": df["AcademicYearKey"],
            "AcademicYearSort": df["AcademicYearSort"],
            "TermName": term,
            "TermSort": term.map(term_order).fillna(0).astype("int64"),
            "GeographyKey": df["GeographyKey"],
            "SchoolTypeKey": df["SchoolTypeKey"],
            "EducationPhase": df["education_phase"],
            "Enrolments": df.get("enrolments"),
            "SessionsPossible": df.get("sess_possible"),
            "OverallAbsenceSessions": df.get("sess_overall"),
            "AuthorisedAbsenceSessions": df.get("sess_authorised"),
            "UnauthorisedAbsenceSessions": df.get("sess_unauthorised"),
            "OverallAbsenceRateSource": df.get("sess_overall_percent") / 100,
            "AttendanceRateSource": 1 - (df.get("sess_overall_percent") / 100),
            "PersistentAbsentees": df.get("enrolments_pa_10_exact"),
            "PersistentAbsenceRateSource": df.get("enrolments_pa_10_exact_percent") / 100,
            "SevereAbsentees": df.get("enrolments_pa_50_exact"),
            "SevereAbsenceRateSource": df.get("enrolments_pa_50_exact_percent") / 100,
        }
    )
    return out


def fact_characteristics(df: pd.DataFrame) -> pd.DataFrame:
    df = add_common_fields(df)
    characteristic_key = df["breakdown_topic"].fillna("") + "|" + df["breakdown"].fillna("")
    out = pd.DataFrame(
        {
            "AcademicYearKey": df["AcademicYearKey"],
            "AcademicYearSort": df["AcademicYearSort"],
            "GeographyKey": df["GeographyKey"],
            "SchoolTypeKey": df["SchoolTypeKey"],
            "CharacteristicKey": characteristic_key,
            "CharacteristicGroup": df["breakdown_topic"],
            "Characteristic": df["breakdown"],
            "EducationPhase": df["education_phase"],
            "Enrolments": df.get("enrolments"),
            "SessionsPossible": df.get("sess_possible"),
            "OverallAbsenceSessions": df.get("sess_overall"),
            "AuthorisedAbsenceSessions": df.get("sess_authorised"),
            "UnauthorisedAbsenceSessions": df.get("sess_unauthorised"),
            "OverallAbsenceRateSource": df.get("sess_overall_percent") / 100,
            "AttendanceRateSource": 1 - (df.get("sess_overall_percent") / 100),
            "PersistentAbsentees": df.get("enrolments_pa_10_exact"),
            "PersistentAbsenceRateSource": df.get("enrolments_pa_10_exact_percent") / 100,
            "SevereAbsentees": df.get("enrolments_pa_50_exact"),
            "SevereAbsenceRateSource": df.get("enrolments_pa_50_exact_percent") / 100,
        }
    )
    return out


def fact_ethnicity_fsm(df: pd.DataFrame) -> pd.DataFrame:
    df = add_common_fields(df)
    out = pd.DataFrame(
        {
            "AcademicYearKey": df["AcademicYearKey"],
            "AcademicYearSort": df["AcademicYearSort"],
            "SchoolTypeKey": df["SchoolTypeKey"],
            "EducationPhase": df["education_phase"],
            "FsmStatus": df["fsm"],
            "EthnicityMinor": df["ethnicity_minor"],
            "Enrolments": df.get("enrolments"),
            "SessionsPossible": df.get("sess_possible"),
            "OverallAbsenceSessions": df.get("sess_overall"),
            "OverallAbsenceRateSource": df.get("sess_overall_percent") / 100,
            "AttendanceRateSource": 1 - (df.get("sess_overall_percent") / 100),
            "PersistentAbsentees": df.get("enrolments_pa_10_exact"),
            "PersistentAbsenceRateSource": df.get("enrolments_pa_10_exact_percent") / 100,
            "SevereAbsentees": df.get("enrolments_pa_50_exact"),
            "SevereAbsenceRateSource": df.get("enrolments_pa_50_exact_percent") / 100,
        }
    )
    return out


def dim_date(*frames: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for df in frames:
        for key, sort_value in df[["AcademicYearKey", "AcademicYearSort"]].drop_duplicates().itertuples(index=False):
            if pd.isna(sort_value):
                continue
            start = int(sort_value)
            rows.append(
                {
                    "AcademicYearKey": key,
                    "AcademicYearSort": start,
                    "AcademicYearStart": start,
                    "AcademicYearEnd": start + 1,
                    "AcademicYearLabel": key,
                    "PostPandemicFlag": "Post-pandemic" if start >= 2021 else "Pre/during pandemic",
                }
            )
    out = pd.DataFrame(rows).drop_duplicates().sort_values("AcademicYearSort")
    latest = out["AcademicYearSort"].max()
    out["IsLatestAcademicYear"] = out["AcademicYearSort"].eq(latest)
    return out


def dim_term(term_fact: pd.DataFrame) -> pd.DataFrame:
    out = term_fact[["TermKey", "AcademicYearKey", "AcademicYearSort", "TermName", "TermSort"]].drop_duplicates()
    out["TermLabel"] = out["AcademicYearKey"] + " " + out["TermName"]
    out["TermSequence"] = out["AcademicYearSort"] * 10 + out["TermSort"]
    return out.sort_values(["AcademicYearSort", "TermSort"])


def dim_geography(source_frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for df in source_frames:
        geo_cols = [
            "geographic_level",
            "country_code",
            "country_name",
            "region_code",
            "region_name",
            "old_la_code",
            "new_la_code",
            "la_name",
        ]
        available = [c for c in geo_cols if c in df.columns]
        for _, row in df[available].drop_duplicates().iterrows():
            key = geokey(row)
            level = row.get("geographic_level", "")
            name = geography_name(row)
            if not key or key.endswith("|") or not name:
                continue
            rows.append(
                {
                    "GeographyKey": key,
                    "GeographyLevel": level,
                    "GeographyName": name,
                    "CountryCode": row.get("country_code", ""),
                    "CountryName": row.get("country_name", "England") or "England",
                    "RegionCode": row.get("region_code", ""),
                    "RegionName": row.get("region_name", ""),
                    "OldLocalAuthorityCode": row.get("old_la_code", ""),
                    "LocalAuthorityCode": row.get("new_la_code", ""),
                    "LocalAuthorityName": row.get("la_name", ""),
                }
            )
    out = pd.DataFrame(rows).drop_duplicates("GeographyKey")
    level_sort = {"National": 1, "Regional": 2, "Local authority": 3}
    out["GeographyLevelSort"] = out["GeographyLevel"].map(level_sort).fillna(9).astype("int64")
    out["MapLocation"] = out.apply(
        lambda r: f"{r['GeographyName']}, England" if r["GeographyLevel"] != "National" else "England",
        axis=1,
    )
    out["AreaLabel"] = out.apply(
        lambda r: r["GeographyName"] if r["GeographyLevel"] != "Local authority" else f"{r['GeographyName']} ({r['RegionName']})",
        axis=1,
    )
    return out.sort_values(["GeographyLevelSort", "RegionName", "GeographyName"])


def dim_school_type(*frames: pd.DataFrame) -> pd.DataFrame:
    values = set()
    for df in frames:
        if "SchoolTypeKey" in df.columns:
            values.update(df["SchoolTypeKey"].dropna().unique().tolist())
    out = pd.DataFrame({"SchoolTypeKey": sorted(values)})
    out["SchoolType"] = out["SchoolTypeKey"]
    sort_map = {
        "Total": 0,
        "State-funded primary": 1,
        "State-funded secondary": 2,
        "Special": 3,
    }
    out["SchoolTypeSort"] = out["SchoolType"].map(sort_map).fillna(9).astype("int64")
    return out.sort_values(["SchoolTypeSort", "SchoolType"])


def dim_characteristic(characteristic_fact: pd.DataFrame) -> pd.DataFrame:
    out = characteristic_fact[["CharacteristicKey", "CharacteristicGroup", "Characteristic"]].drop_duplicates()
    group_sort = {
        "FSM eligibility": 1,
        "SEN provision": 2,
        "SEN primary need": 3,
        "Sex": 4,
        "Ethnicity": 5,
        "Year group": 6,
        "Language": 7,
    }
    out["CharacteristicGroupSort"] = out["CharacteristicGroup"].map(group_sort).fillna(99).astype("int64")
    return out.sort_values(["CharacteristicGroupSort", "CharacteristicGroup", "Characteristic"])


def write_curated_csvs() -> dict[str, pd.DataFrame]:
    annual_raw = read_raw("absence_rates_by_geographic_level")
    termly_raw = read_raw("absence_by_geographic_level_termly")
    characteristics_raw = read_raw("absence_by_pupil_characteristics")
    eth_fsm_raw = read_raw("absence_by_ethnicity_and_fsm")

    annual = fact_annual(annual_raw)
    termly = fact_termly(termly_raw)
    characteristics = fact_characteristics(characteristics_raw)
    eth_fsm = fact_ethnicity_fsm(eth_fsm_raw)
    dates = dim_date(annual, termly, characteristics, eth_fsm)
    terms = dim_term(termly)
    geographies = dim_geography([annual_raw, termly_raw, characteristics_raw])
    school_types = dim_school_type(annual, termly, characteristics, eth_fsm)
    pupil_characteristics = dim_characteristic(characteristics)

    outputs = {
        "dim_date": dates,
        "dim_term": terms,
        "dim_geography": geographies,
        "dim_school_type": school_types,
        "dim_pupil_characteristic": pupil_characteristics,
        "fact_attendance_annual": annual,
        "fact_attendance_termly": termly,
        "fact_pupil_characteristic_absence": characteristics,
        "fact_ethnicity_fsm_absence": eth_fsm,
    }

    for name, df in outputs.items():
        path = CURATED_DIR / f"{name}.csv"
        df.to_csv(path, index=False, encoding="utf-8", na_rep="")
        print(f"Wrote {path.relative_to(PROJECT_ROOT)} ({len(df):,} rows)")

    source_manifest = {
        "generated_on": date.today().isoformat(),
        "source_release": "DfE Explore Education Statistics: Pupil absence in schools in England, Academic year 2024/25",
        "datasets": DATASETS,
        "curated_outputs": {name: f"Source Data/Curated/{name}.csv" for name in outputs},
    }
    (SOURCE_DIR / "data_sources.json").write_text(json.dumps(source_manifest, indent=2), encoding="utf-8")
    return outputs


def tmdl_type(dtype: str) -> str:
    return {"text": "string", "int": "int64", "number": "double", "bool": "boolean"}.get(dtype, "string")


def m_type(dtype: str) -> str:
    return {"text": "type text", "int": "Int64.Type", "number": "type number", "bool": "type logical"}.get(dtype, "type text")


TABLE_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "Dim Date": [
        ("AcademicYearKey", "text"),
        ("AcademicYearSort", "int"),
        ("AcademicYearStart", "int"),
        ("AcademicYearEnd", "int"),
        ("AcademicYearLabel", "text"),
        ("PostPandemicFlag", "text"),
        ("IsLatestAcademicYear", "bool"),
    ],
    "Dim Term": [
        ("TermKey", "text"),
        ("AcademicYearKey", "text"),
        ("AcademicYearSort", "int"),
        ("TermName", "text"),
        ("TermSort", "int"),
        ("TermLabel", "text"),
        ("TermSequence", "int"),
    ],
    "Dim Geography": [
        ("GeographyKey", "text"),
        ("GeographyLevel", "text"),
        ("GeographyName", "text"),
        ("CountryCode", "text"),
        ("CountryName", "text"),
        ("RegionCode", "text"),
        ("RegionName", "text"),
        ("OldLocalAuthorityCode", "text"),
        ("LocalAuthorityCode", "text"),
        ("LocalAuthorityName", "text"),
        ("GeographyLevelSort", "int"),
        ("MapLocation", "text"),
        ("AreaLabel", "text"),
    ],
    "Dim School Type": [
        ("SchoolTypeKey", "text"),
        ("SchoolType", "text"),
        ("SchoolTypeSort", "int"),
    ],
    "Dim Pupil Characteristic": [
        ("CharacteristicKey", "text"),
        ("CharacteristicGroup", "text"),
        ("Characteristic", "text"),
        ("CharacteristicGroupSort", "int"),
    ],
    "Fact Attendance Annual": [
        ("AcademicYearKey", "text"),
        ("AcademicYearSort", "int"),
        ("YearBreakdown", "text"),
        ("GeographyKey", "text"),
        ("SchoolTypeKey", "text"),
        ("EducationPhase", "text"),
        ("NumSchools", "number"),
        ("Enrolments", "number"),
        ("SessionsPossible", "number"),
        ("OverallAbsenceSessions", "number"),
        ("AuthorisedAbsenceSessions", "number"),
        ("UnauthorisedAbsenceSessions", "number"),
        ("OverallAbsenceRateSource", "number"),
        ("AuthorisedAbsenceRateSource", "number"),
        ("UnauthorisedAbsenceRateSource", "number"),
        ("AttendanceRateSource", "number"),
        ("PersistentAbsentees", "number"),
        ("PersistentAbsenceRateSource", "number"),
        ("SevereAbsentees", "number"),
        ("SevereAbsenceRateSource", "number"),
        ("IllnessAbsenceRateSource", "number"),
        ("AppointmentsAbsenceRateSource", "number"),
        ("UnauthorisedHolidayAbsenceRateSource", "number"),
    ],
    "Fact Attendance Termly": [
        ("TermKey", "text"),
        ("AcademicYearKey", "text"),
        ("AcademicYearSort", "int"),
        ("TermName", "text"),
        ("TermSort", "int"),
        ("GeographyKey", "text"),
        ("SchoolTypeKey", "text"),
        ("EducationPhase", "text"),
        ("Enrolments", "number"),
        ("SessionsPossible", "number"),
        ("OverallAbsenceSessions", "number"),
        ("AuthorisedAbsenceSessions", "number"),
        ("UnauthorisedAbsenceSessions", "number"),
        ("OverallAbsenceRateSource", "number"),
        ("AttendanceRateSource", "number"),
        ("PersistentAbsentees", "number"),
        ("PersistentAbsenceRateSource", "number"),
        ("SevereAbsentees", "number"),
        ("SevereAbsenceRateSource", "number"),
    ],
    "Fact Pupil Characteristic Absence": [
        ("AcademicYearKey", "text"),
        ("AcademicYearSort", "int"),
        ("GeographyKey", "text"),
        ("SchoolTypeKey", "text"),
        ("CharacteristicKey", "text"),
        ("CharacteristicGroup", "text"),
        ("Characteristic", "text"),
        ("EducationPhase", "text"),
        ("Enrolments", "number"),
        ("SessionsPossible", "number"),
        ("OverallAbsenceSessions", "number"),
        ("AuthorisedAbsenceSessions", "number"),
        ("UnauthorisedAbsenceSessions", "number"),
        ("OverallAbsenceRateSource", "number"),
        ("AttendanceRateSource", "number"),
        ("PersistentAbsentees", "number"),
        ("PersistentAbsenceRateSource", "number"),
        ("SevereAbsentees", "number"),
        ("SevereAbsenceRateSource", "number"),
    ],
    "Fact Ethnicity FSM Absence": [
        ("AcademicYearKey", "text"),
        ("AcademicYearSort", "int"),
        ("SchoolTypeKey", "text"),
        ("EducationPhase", "text"),
        ("FsmStatus", "text"),
        ("EthnicityMinor", "text"),
        ("Enrolments", "number"),
        ("SessionsPossible", "number"),
        ("OverallAbsenceSessions", "number"),
        ("OverallAbsenceRateSource", "number"),
        ("AttendanceRateSource", "number"),
        ("PersistentAbsentees", "number"),
        ("PersistentAbsenceRateSource", "number"),
        ("SevereAbsentees", "number"),
        ("SevereAbsenceRateSource", "number"),
    ],
}


TABLE_TO_CSV = {
    "Dim Date": "dim_date.csv",
    "Dim Term": "dim_term.csv",
    "Dim Geography": "dim_geography.csv",
    "Dim School Type": "dim_school_type.csv",
    "Dim Pupil Characteristic": "dim_pupil_characteristic.csv",
    "Fact Attendance Annual": "fact_attendance_annual.csv",
    "Fact Attendance Termly": "fact_attendance_termly.csv",
    "Fact Pupil Characteristic Absence": "fact_pupil_characteristic_absence.csv",
    "Fact Ethnicity FSM Absence": "fact_ethnicity_fsm_absence.csv",
}


KEY_COLUMNS = {
    "Dim Date": {"AcademicYearKey"},
    "Dim Term": {"TermKey"},
    "Dim Geography": {"GeographyKey"},
    "Dim School Type": {"SchoolTypeKey"},
    "Dim Pupil Characteristic": {"CharacteristicKey"},
}


HIDDEN_FACT_COLUMNS = {
    "AcademicYearKey",
    "AcademicYearSort",
    "GeographyKey",
    "SchoolTypeKey",
    "TermKey",
    "CharacteristicKey",
    "EducationPhase",
    "YearBreakdown",
    "TermName",
    "TermSort",
    "CharacteristicGroup",
    "Characteristic",
}


def lineagetag() -> str:
    return str(uuid4())


def tmdl_table(table_name: str, schema: list[tuple[str, str]]) -> str:
    csv_path = str(CURATED_DIR / TABLE_TO_CSV[table_name])
    type_rows = ",\n".join([f'\t\t\t\t\t\t{{"{col}", {m_type(dtype)}}}' for col, dtype in schema])
    lines = [f"table '{table_name}'", f"\tlineageTag: {lineagetag()}", ""]
    lines.extend(measures_for_table(table_name))
    for col, dtype in schema:
        lines.append(f"\tcolumn {col}")
        lines.append(f"\t\tdataType: {tmdl_type(dtype)}")
        if col in KEY_COLUMNS.get(table_name, set()):
            lines.append("\t\tisKey")
        if table_name.startswith("Fact ") and col in HIDDEN_FACT_COLUMNS:
            lines.append("\t\tisHidden")
        lines.append(f"\t\tlineageTag: {lineagetag()}")
        lines.append("\t\tsummarizeBy: none")
        lines.append(f"\t\tsourceColumn: {col}")
        lines.append("")
    lines.append(f"\tpartition '{table_name}' = m")
    lines.append("\t\tmode: import")
    lines.append("\t\tsource =")
    lines.append("\t\t\t\tlet")
    lines.append(
        f'\t\t\t\t\tSource = Csv.Document(File.Contents("{csv_path}"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),'
    )
    lines.append("\t\t\t\t\tHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),")
    lines.append("\t\t\t\t\tTyped = Table.TransformColumnTypes(Headers, {")
    lines.append(type_rows)
    lines.append('\t\t\t\t\t}, "en-GB")')
    lines.append("\t\t\t\tin")
    lines.append("\t\t\t\t\tTyped")
    lines.append("")
    return "\n".join(lines)


def measures_for_table(table_name: str) -> list[str]:
    if table_name == "Fact Attendance Annual":
        return annual_measures()
    if table_name == "Fact Attendance Termly":
        return termly_measures()
    if table_name == "Fact Pupil Characteristic Absence":
        return characteristic_measures()
    if table_name == "Fact Ethnicity FSM Absence":
        return ethnicity_fsm_measures()
    return []


def measure_block(name: str, expression: str, fmt: str | None = None) -> list[str]:
    expr = textwrap.indent(textwrap.dedent(expression).strip(), "\t\t\t")
    lines = [f"\tmeasure '{name}' =", expr, f"\t\tlineageTag: {lineagetag()}"]
    if fmt:
        lines.insert(2, f"\t\tformatString: {fmt}")
    lines.append("")
    return lines


def annual_measures() -> list[str]:
    measures: list[str] = []
    entries = [
        (
            "Pupil Enrolments",
            "SUM('Fact Attendance Annual'[Enrolments])",
            "#,##0",
        ),
        (
            "Schools",
            "SUM('Fact Attendance Annual'[NumSchools])",
            "#,##0",
        ),
        (
            "Absence Rate",
            "DIVIDE(SUM('Fact Attendance Annual'[OverallAbsenceSessions]), SUM('Fact Attendance Annual'[SessionsPossible]))",
            "0.0%",
        ),
        ("Attendance Rate", "1 - [Absence Rate]", "0.0%"),
        (
            "Authorised Absence Rate",
            "DIVIDE(SUM('Fact Attendance Annual'[AuthorisedAbsenceSessions]), SUM('Fact Attendance Annual'[SessionsPossible]))",
            "0.0%",
        ),
        (
            "Unauthorised Absence Rate",
            "DIVIDE(SUM('Fact Attendance Annual'[UnauthorisedAbsenceSessions]), SUM('Fact Attendance Annual'[SessionsPossible]))",
            "0.0%",
        ),
        (
            "Persistent Absentees",
            "SUM('Fact Attendance Annual'[PersistentAbsentees])",
            "#,##0",
        ),
        (
            "Persistent Absence Rate",
            "DIVIDE(SUM('Fact Attendance Annual'[PersistentAbsentees]), SUM('Fact Attendance Annual'[Enrolments]))",
            "0.0%",
        ),
        (
            "Severe Absentees",
            "SUM('Fact Attendance Annual'[SevereAbsentees])",
            "#,##0",
        ),
        (
            "Severe Absence Rate",
            "DIVIDE(SUM('Fact Attendance Annual'[SevereAbsentees]), SUM('Fact Attendance Annual'[Enrolments]))",
            "0.0%",
        ),
        (
            "Illness Absence Rate",
            "AVERAGE('Fact Attendance Annual'[IllnessAbsenceRateSource])",
            "0.0%",
        ),
        (
            "Unauthorised Holiday Absence Rate",
            "AVERAGE('Fact Attendance Annual'[UnauthorisedHolidayAbsenceRateSource])",
            "0.0%",
        ),
        (
            "Latest Academic Year Sort",
            "CALCULATE(MAX('Dim Date'[AcademicYearSort]), REMOVEFILTERS('Dim Date'))",
            "0",
        ),
        (
            "Latest Attendance Rate",
            """
            VAR LatestYear = [Latest Academic Year Sort]
            RETURN
                CALCULATE(
                    [Attendance Rate],
                    REMOVEFILTERS('Dim Date'),
                    'Dim Date'[AcademicYearSort] = LatestYear
                )
            """,
            "0.0%",
        ),
        (
            "Latest Absence Rate",
            """
            VAR LatestYear = [Latest Academic Year Sort]
            RETURN
                CALCULATE(
                    [Absence Rate],
                    REMOVEFILTERS('Dim Date'),
                    'Dim Date'[AcademicYearSort] = LatestYear
                )
            """,
            "0.0%",
        ),
        (
            "Latest Persistent Absence Rate",
            """
            VAR LatestYear = [Latest Academic Year Sort]
            RETURN
                CALCULATE(
                    [Persistent Absence Rate],
                    REMOVEFILTERS('Dim Date'),
                    'Dim Date'[AcademicYearSort] = LatestYear
                )
            """,
            "0.0%",
        ),
        (
            "Latest Severe Absence Rate",
            """
            VAR LatestYear = [Latest Academic Year Sort]
            RETURN
                CALCULATE(
                    [Severe Absence Rate],
                    REMOVEFILTERS('Dim Date'),
                    'Dim Date'[AcademicYearSort] = LatestYear
                )
            """,
            "0.0%",
        ),
        (
            "Absence Rate YoY Change",
            """
            VAR CurrentYear = MAX('Dim Date'[AcademicYearSort])
            VAR CurrentValue = [Absence Rate]
            VAR PreviousValue =
                CALCULATE(
                    [Absence Rate],
                    REMOVEFILTERS('Dim Date'),
                    'Dim Date'[AcademicYearSort] = CurrentYear - 1
                )
            RETURN
                (CurrentValue - PreviousValue) * 100
            """,
            "+0.0;-0.0;0.0",
        ),
        (
            "Persistent Absence YoY Change",
            """
            VAR CurrentYear = MAX('Dim Date'[AcademicYearSort])
            VAR CurrentValue = [Persistent Absence Rate]
            VAR PreviousValue =
                CALCULATE(
                    [Persistent Absence Rate],
                    REMOVEFILTERS('Dim Date'),
                    'Dim Date'[AcademicYearSort] = CurrentYear - 1
                )
            RETURN
                (CurrentValue - PreviousValue) * 100
            """,
            "+0.0;-0.0;0.0",
        ),
        (
            "Latest Absence Rate YoY Change",
            """
            VAR LatestYear = [Latest Academic Year Sort]
            VAR CurrentValue =
                CALCULATE([Absence Rate], REMOVEFILTERS('Dim Date'), 'Dim Date'[AcademicYearSort] = LatestYear)
            VAR PreviousValue =
                CALCULATE([Absence Rate], REMOVEFILTERS('Dim Date'), 'Dim Date'[AcademicYearSort] = LatestYear - 1)
            RETURN
                (CurrentValue - PreviousValue) * 100
            """,
            "+0.0;-0.0;0.0",
        ),
        (
            "Latest Persistent Absence YoY Change",
            """
            VAR LatestYear = [Latest Academic Year Sort]
            VAR CurrentValue =
                CALCULATE([Persistent Absence Rate], REMOVEFILTERS('Dim Date'), 'Dim Date'[AcademicYearSort] = LatestYear)
            VAR PreviousValue =
                CALCULATE([Persistent Absence Rate], REMOVEFILTERS('Dim Date'), 'Dim Date'[AcademicYearSort] = LatestYear - 1)
            RETURN
                (CurrentValue - PreviousValue) * 100
            """,
            "+0.0;-0.0;0.0",
        ),
        (
            "Trend Flag",
            """
            VAR Change = [Latest Absence Rate YoY Change]
            RETURN
                SWITCH(
                    TRUE(),
                    ISBLANK(Change), "No comparison",
                    Change > 0.25, "Worsening",
                    Change < -0.25, "Improving",
                    "Stabilising"
                )
            """,
            None,
        ),
        (
            "Persistent Absence Pressure Flag",
            """
            VAR Rate = [Latest Persistent Absence Rate]
            RETURN
                SWITCH(
                    TRUE(),
                    ISBLANK(Rate), "No data",
                    Rate >= 0.25, "Serious risk",
                    Rate >= 0.20, "High pressure",
                    Rate >= 0.15, "Elevated",
                    "Lower pressure"
                )
            """,
            None,
        ),
        (
            "Local Authority Persistent Absence Rank",
            """
            VAR LatestYear = [Latest Academic Year Sort]
            RETURN
                IF(
                    SELECTEDVALUE('Dim Geography'[GeographyLevel]) = "Local authority",
                    RANKX(
                        FILTER(
                            ALLSELECTED('Dim Geography'[GeographyKey], 'Dim Geography'[GeographyName], 'Dim Geography'[GeographyLevel]),
                            'Dim Geography'[GeographyLevel] = "Local authority"
                        ),
                        CALCULATE([Persistent Absence Rate], REMOVEFILTERS('Dim Date'), 'Dim Date'[AcademicYearSort] = LatestYear),
                        ,
                        DESC,
                        Dense
                    )
                )
            """,
            "0",
        ),
        (
            "Decision Maker Takeaway",
            """
            VAR Trend = [Trend Flag]
            VAR PersistentRate = [Latest Persistent Absence Rate]
            VAR SevereRate = [Latest Severe Absence Rate]
            RETURN
                "Attendance is " & LOWER(Trend) &
                "; latest persistent absence is " & FORMAT(PersistentRate, "0.0%") &
                " and severe absence is " & FORMAT(SevereRate, "0.0%") & "."
            """,
            None,
        ),
    ]
    for name, expr, fmt in entries:
        measures.extend(measure_block(name, expr, fmt))
    return measures


def termly_measures() -> list[str]:
    measures: list[str] = []
    entries = [
        (
            "Termly Absence Rate",
            "DIVIDE(SUM('Fact Attendance Termly'[OverallAbsenceSessions]), SUM('Fact Attendance Termly'[SessionsPossible]))",
            "0.0%",
        ),
        ("Termly Attendance Rate", "1 - [Termly Absence Rate]", "0.0%"),
        (
            "Termly Persistent Absence Rate",
            "DIVIDE(SUM('Fact Attendance Termly'[PersistentAbsentees]), SUM('Fact Attendance Termly'[Enrolments]))",
            "0.0%",
        ),
        (
            "Termly Severe Absence Rate",
            "DIVIDE(SUM('Fact Attendance Termly'[SevereAbsentees]), SUM('Fact Attendance Termly'[Enrolments]))",
            "0.0%",
        ),
    ]
    for name, expr, fmt in entries:
        measures.extend(measure_block(name, expr, fmt))
    return measures


def characteristic_measures() -> list[str]:
    measures: list[str] = []
    entries = [
        (
            "Characteristic Absence Rate",
            "DIVIDE(SUM('Fact Pupil Characteristic Absence'[OverallAbsenceSessions]), SUM('Fact Pupil Characteristic Absence'[SessionsPossible]))",
            "0.0%",
        ),
        ("Characteristic Attendance Rate", "1 - [Characteristic Absence Rate]", "0.0%"),
        (
            "Characteristic Persistent Absence Rate",
            "DIVIDE(SUM('Fact Pupil Characteristic Absence'[PersistentAbsentees]), SUM('Fact Pupil Characteristic Absence'[Enrolments]))",
            "0.0%",
        ),
        (
            "Characteristic Severe Absence Rate",
            "DIVIDE(SUM('Fact Pupil Characteristic Absence'[SevereAbsentees]), SUM('Fact Pupil Characteristic Absence'[Enrolments]))",
            "0.0%",
        ),
        (
            "Characteristic Persistent Risk Index",
            """
            DIVIDE(
                [Characteristic Persistent Absence Rate],
                CALCULATE(
                    [Characteristic Persistent Absence Rate],
                    REMOVEFILTERS('Dim Pupil Characteristic')
                )
            )
            """,
            "0.00x",
        ),
    ]
    for name, expr, fmt in entries:
        measures.extend(measure_block(name, expr, fmt))
    return measures


def ethnicity_fsm_measures() -> list[str]:
    measures: list[str] = []
    entries = [
        (
            "Ethnicity FSM Absence Rate",
            "DIVIDE(SUM('Fact Ethnicity FSM Absence'[OverallAbsenceSessions]), SUM('Fact Ethnicity FSM Absence'[SessionsPossible]))",
            "0.0%",
        ),
        ("Ethnicity FSM Attendance Rate", "1 - [Ethnicity FSM Absence Rate]", "0.0%"),
        (
            "Ethnicity FSM Persistent Absence Rate",
            "DIVIDE(SUM('Fact Ethnicity FSM Absence'[PersistentAbsentees]), SUM('Fact Ethnicity FSM Absence'[Enrolments]))",
            "0.0%",
        ),
        (
            "Ethnicity FSM Severe Absence Rate",
            "DIVIDE(SUM('Fact Ethnicity FSM Absence'[SevereAbsentees]), SUM('Fact Ethnicity FSM Absence'[Enrolments]))",
            "0.0%",
        ),
    ]
    for name, expr, fmt in entries:
        measures.extend(measure_block(name, expr, fmt))
    return measures


def write_model_files() -> None:
    if TABLES_DIR.exists():
        for path in TABLES_DIR.glob("*.tmdl"):
            path.unlink()
    else:
        TABLES_DIR.mkdir(parents=True)

    for table_name, schema in TABLE_SCHEMAS.items():
        (TABLES_DIR / f"{table_name}.tmdl").write_text(tmdl_table(table_name, schema), encoding="utf-8")

    model = """model Model
\tculture: en-GB
\tdefaultPowerBIDataSourceVersion: powerBI_V3
\tdiscourageImplicitMeasures
\tsourceQueryCulture: en-GB
\tdataAccessOptions
\t\tlegacyRedirects
\t\treturnErrorValuesAsNull

annotation __PBI_TimeIntelligenceEnabled = 0

annotation PBI_ProTooling = ["DevMode"]

annotation PBI_QueryOrder = ["Dim Date","Dim Term","Dim Geography","Dim School Type","Dim Pupil Characteristic","Fact Attendance Annual","Fact Attendance Termly","Fact Pupil Characteristic Absence","Fact Ethnicity FSM Absence"]

ref table 'Dim Date'
ref table 'Dim Term'
ref table 'Dim Geography'
ref table 'Dim School Type'
ref table 'Dim Pupil Characteristic'
ref table 'Fact Attendance Annual'
ref table 'Fact Attendance Termly'
ref table 'Fact Pupil Characteristic Absence'
ref table 'Fact Ethnicity FSM Absence'

ref cultureInfo en-GB
"""
    (SEMANTIC_DEF_DIR / "model.tmdl").write_text(model, encoding="utf-8")

    relationships = """relationship 'Fact Attendance Annual to Dim Date'
\tfromColumn: 'Fact Attendance Annual'.AcademicYearKey
\ttoColumn: 'Dim Date'.AcademicYearKey

relationship 'Fact Attendance Annual to Dim Geography'
\tfromColumn: 'Fact Attendance Annual'.GeographyKey
\ttoColumn: 'Dim Geography'.GeographyKey

relationship 'Fact Attendance Annual to Dim School Type'
\tfromColumn: 'Fact Attendance Annual'.SchoolTypeKey
\ttoColumn: 'Dim School Type'.SchoolTypeKey

relationship 'Fact Attendance Termly to Dim Term'
\tfromColumn: 'Fact Attendance Termly'.TermKey
\ttoColumn: 'Dim Term'.TermKey

relationship 'Dim Term to Dim Date'
\tfromColumn: 'Dim Term'.AcademicYearKey
\ttoColumn: 'Dim Date'.AcademicYearKey

relationship 'Fact Attendance Termly to Dim Geography'
\tfromColumn: 'Fact Attendance Termly'.GeographyKey
\ttoColumn: 'Dim Geography'.GeographyKey

relationship 'Fact Attendance Termly to Dim School Type'
\tfromColumn: 'Fact Attendance Termly'.SchoolTypeKey
\ttoColumn: 'Dim School Type'.SchoolTypeKey

relationship 'Fact Pupil Characteristic Absence to Dim Date'
\tfromColumn: 'Fact Pupil Characteristic Absence'.AcademicYearKey
\ttoColumn: 'Dim Date'.AcademicYearKey

relationship 'Fact Pupil Characteristic Absence to Dim Geography'
\tfromColumn: 'Fact Pupil Characteristic Absence'.GeographyKey
\ttoColumn: 'Dim Geography'.GeographyKey

relationship 'Fact Pupil Characteristic Absence to Dim School Type'
\tfromColumn: 'Fact Pupil Characteristic Absence'.SchoolTypeKey
\ttoColumn: 'Dim School Type'.SchoolTypeKey

relationship 'Fact Pupil Characteristic Absence to Dim Pupil Characteristic'
\tfromColumn: 'Fact Pupil Characteristic Absence'.CharacteristicKey
\ttoColumn: 'Dim Pupil Characteristic'.CharacteristicKey

relationship 'Fact Ethnicity FSM Absence to Dim Date'
\tfromColumn: 'Fact Ethnicity FSM Absence'.AcademicYearKey
\ttoColumn: 'Dim Date'.AcademicYearKey

relationship 'Fact Ethnicity FSM Absence to Dim School Type'
\tfromColumn: 'Fact Ethnicity FSM Absence'.SchoolTypeKey
\ttoColumn: 'Dim School Type'.SchoolTypeKey
"""
    (SEMANTIC_DEF_DIR / "relationships.tmdl").write_text(relationships, encoding="utf-8")
    print("Updated semantic model TMDL files.")


def write_report_scoped_measures() -> None:
    """Restore measures referenced by the PBIP report visuals."""
    script_path = PROJECT_ROOT / ".build" / "add_measures.py"
    if script_path.exists():
        runpy.run_path(str(script_path), run_name="__main__")


def write_report_pages() -> None:
    pages = [
        ("executive-summary", "Executive Summary"),
        ("national-regional-trends", "National And Regional Trends"),
        ("local-authority-explorer", "Local Authority Explorer"),
        ("demographic-pupil-group-deep-dive", "Demographic And Pupil Group Deep Dive"),
    ]
    for old in REPORT_PAGES_DIR.iterdir():
        if old.is_dir() and old.name not in {p[0] for p in pages}:
            shutil.rmtree(old)
    for name, display in pages:
        page_dir = REPORT_PAGES_DIR / name
        page_dir.mkdir(parents=True, exist_ok=True)
        page_json = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json",
            "name": name,
            "displayName": display,
            "displayOption": "FitToPage",
            "height": 720,
            "width": 1280,
        }
        (page_dir / "page.json").write_text(json.dumps(page_json, indent=2), encoding="utf-8")
    pages_json = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [p[0] for p in pages],
        "activePageName": "executive-summary",
    }
    (REPORT_PAGES_DIR / "pages.json").write_text(json.dumps(pages_json, indent=2), encoding="utf-8")
    print("Updated report page scaffold.")


def write_manual_guide() -> None:
    guide = """# Manual Power BI Build Guide

This project has already prepared the source data, curated CSV layer, semantic model tables, relationships, DAX measures, and four report pages in the PBIP. The remaining work is visual placement and final formatting in Power BI Desktop.

## 1. Open and Refresh

1. Open `School Attendance Dashboard.pbip` in Power BI Desktop.
2. If prompted, allow local file access to the CSV files under `Source Data/Curated`.
3. Refresh the model.
4. Confirm these dimension tables are visible: `Dim Date`, `Dim Geography`, `Dim School Type`, `Dim Pupil Characteristic`.
5. Confirm these fact tables are visible: `Fact Attendance Annual`, `Fact Attendance Termly`, `Fact Pupil Characteristic Absence`, `Fact Ethnicity FSM Absence`.

## 2. Executive Summary

Recommended visuals:

- KPI cards: `Latest Attendance Rate`, `Latest Absence Rate`, `Latest Persistent Absence Rate`, `Latest Severe Absence Rate`.
- Card or smart narrative: `Decision Maker Takeaway`.
- Small cards: `Trend Flag`, `Persistent Absence Pressure Flag`.
- Line chart: `Dim Date[AcademicYearLabel]` by `Attendance Rate` and `Persistent Absence Rate`, filtered to `Dim Geography[GeographyLevel] = National` and `Dim School Type[SchoolType] = Total`.
- Bar chart: latest `Persistent Absence Rate` by `Dim School Type[SchoolType]`.

Decision-maker story: show that attendance is the inverse of absence, then focus attention on persistent and severe absence because those are the long-term risk indicators.

## 3. National And Regional Trends

Recommended visuals:

- Line chart: `Dim Date[AcademicYearLabel]` by `Absence Rate`, `Authorised Absence Rate`, and `Unauthorised Absence Rate`.
- Line chart: `Dim Date[AcademicYearLabel]` by `Persistent Absence Rate` and `Severe Absence Rate`.
- Regional small multiples or clustered bar: `Dim Geography[RegionName]` by `Latest Persistent Absence Rate`.
- Termly line chart: `Dim Term[TermLabel]` by `Termly Absence Rate`, using `Fact Attendance Termly`.

Useful filters: `Dim Geography[GeographyLevel]`, `Dim School Type[SchoolType]`, and `Dim Date[AcademicYearLabel]`.

## 4. Local Authority Explorer

Recommended visuals:

- Table or matrix: `Dim Geography[LocalAuthorityName]`, `Dim Geography[RegionName]`, `Latest Attendance Rate`, `Latest Persistent Absence Rate`, `Latest Severe Absence Rate`, `Local Authority Persistent Absence Rank`.
- Filled map or Azure map: location field `Dim Geography[MapLocation]`, color by `Latest Persistent Absence Rate`, tooltip with attendance, persistent absence, severe absence, and enrolments.
- Scatter chart: `Latest Absence Rate` versus `Latest Persistent Absence Rate`, size by `Pupil Enrolments`, details by `Dim Geography[LocalAuthorityName]`.
- Slicer: `Dim Geography[RegionName]`.

Filter the page or relevant visuals to `Dim Geography[GeographyLevel] = Local authority`.

## 5. Demographic And Pupil Group Deep Dive

Recommended visuals:

- Matrix: `Dim Pupil Characteristic[CharacteristicGroup]` and `Dim Pupil Characteristic[Characteristic]` by `Characteristic Persistent Absence Rate`, `Characteristic Severe Absence Rate`, and `Characteristic Persistent Risk Index`.
- Bar chart: top pupil groups by `Characteristic Persistent Absence Rate`.
- Heatmap-style matrix: `Fact Ethnicity FSM Absence[EthnicityMinor]` by `Fact Ethnicity FSM Absence[FsmStatus]`, value `Ethnicity FSM Persistent Absence Rate`.
- Slicers: `Dim Date[AcademicYearLabel]`, `Dim School Type[SchoolType]`, `Dim Geography[RegionName]`.

Use the risk index to explain which groups are materially above the current comparison average.

Analytical caution: categories such as `Unclassified`, `FSM unclassified`, and `Year not followed or missing` can be useful data-quality signals, but should not be presented as substantive pupil groups without a note.

## 6. Portfolio Finishing Notes

- Add a source note on each page: Department for Education, Explore Education Statistics, Pupil absence in schools in England, academic year 2024/25 release.
- Use consistent percentage formatting and label changes as percentage-point changes.
- Avoid mixing annual and termly measures on the same visual unless the axis clearly distinguishes term from academic year.
- For local authority visuals, use the latest academic year unless a trend visual explicitly needs multiple years.
"""
    (PROJECT_ROOT / "manual_report_build_guide.md").write_text(guide, encoding="utf-8")


def write_project_map(outputs: dict[str, pd.DataFrame]) -> None:
    rows = "\n".join(
        f"<tr><td>{name}</td><td>{len(df):,}</td><td>Source Data/Curated/{name}.csv</td></tr>"
        for name, df in outputs.items()
    )
    source_rows = "\n".join(
        f"<tr><td>{meta['title']}</td><td><a href=\"{meta['url']}\">{meta['url']}</a></td><td>{meta['coverage']}</td></tr>"
        for meta in DATASETS.values()
    )
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>School Attendance Dashboard Project Map</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 32px; line-height: 1.45; color: #1f2933; }}
    h1, h2 {{ color: #0b3d5c; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; }}
    th, td {{ border: 1px solid #ccd6dd; padding: 8px; vertical-align: top; }}
    th {{ background: #e8f1f5; text-align: left; }}
    code {{ background: #f3f5f7; padding: 2px 4px; border-radius: 3px; }}
  </style>
</head>
<body>
  <h1>School Attendance Dashboard Project Map</h1>
  <p><strong>Last updated:</strong> {date.today().isoformat()}</p>
  <p>This PBIP portfolio project analyses school attendance and absence in England using Department for Education Explore Education Statistics data. The dashboard story focuses on whether attendance is improving or worsening, where persistent absence is most severe, and which areas and pupil groups face the highest pressure.</p>

  <h2>Build Workflow</h2>
  <ol>
    <li>Run <code>python "Source Data/build_school_attendance_project.py"</code> from the project root.</li>
    <li>The script downloads raw official CSV files into <code>Source Data/Raw</code>.</li>
    <li>It creates model-ready CSV files in <code>Source Data/Curated</code>.</li>
    <li>It regenerates TMDL semantic model tables, relationships, and DAX measures in <code>School Attendance Dashboard.SemanticModel/definition</code>.</li>
    <li>It restores report-scoped measures from <code>.build/add_measures.py</code>; these are required by the existing visual bindings.</li>
    <li>It scaffolds four report pages in <code>School Attendance Dashboard.Report/definition/pages</code>.</li>
    <li>Open <code>School Attendance Dashboard.pbip</code> in Power BI Desktop, refresh, then place visuals using <code>manual_report_build_guide.md</code>.</li>
  </ol>

  <h2>Official Sources</h2>
  <table>
    <thead><tr><th>Dataset</th><th>CSV URL</th><th>Use</th></tr></thead>
    <tbody>{source_rows}</tbody>
  </table>

  <h2>Curated Tables</h2>
  <table>
    <thead><tr><th>Table</th><th>Rows</th><th>File</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>

  <h2>Semantic Model</h2>
  <p>Dimensions: <code>Dim Date</code>, <code>Dim Term</code>, <code>Dim Geography</code>, <code>Dim School Type</code>, <code>Dim Pupil Characteristic</code>.</p>
  <p>Facts: <code>Fact Attendance Annual</code>, <code>Fact Attendance Termly</code>, <code>Fact Pupil Characteristic Absence</code>, <code>Fact Ethnicity FSM Absence</code>.</p>
  <p>Core DAX measures include attendance rate, absence rate, authorised and unauthorised absence rates, persistent absence rate, severe absence rate, latest-year measures, year-on-year changes, trend flags, local authority ranking, pupil-group risk index, and a decision-maker takeaway sentence. Additional national, regional, local authority, FSM, and pupil-characteristic measures are restored from <code>.build/add_measures.py</code> because the report visuals bind to those exact measure names.</p>

  <h2>Validation</h2>
  <p>Run <code>python .build/check_refs.py</code> to confirm every visual field reference resolves to a model column or measure. Run <code>python .build/validate.py</code> for PBIP JSON and TMDL structure checks.</p>

  <h2>Report Pages</h2>
  <ul>
    <li>Executive Summary</li>
    <li>National And Regional Trends</li>
    <li>Local Authority Explorer</li>
    <li>Demographic And Pupil Group Deep Dive</li>
  </ul>

  <h2>Manual Work Remaining</h2>
  <p>Power BI visual placement, visual formatting, slicer layout, map visual selection, and final portfolio styling remain manual. Use <code>manual_report_build_guide.md</code> for beginner-friendly steps and field recommendations.</p>
</body>
</html>
"""
    (PROJECT_ROOT / "project_map.html").write_text(html, encoding="utf-8")


def main() -> None:
    ensure_dirs()
    download_sources(force=False)
    outputs = write_curated_csvs()
    write_model_files()
    write_report_scoped_measures()
    write_report_pages()
    write_manual_guide()
    write_project_map(outputs)
    print("School attendance Power BI project build complete.")


if __name__ == "__main__":
    main()
