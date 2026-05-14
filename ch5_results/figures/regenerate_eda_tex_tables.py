"""Regenerate the LaTeX field-dictionary tables with proper column-width sizing
so the descriptions don't overflow the printable text width.

Each table is emitted as a `longtable` (allows page breaks) with explicit
\\p{…cm} column widths summing to \\textwidth, and \\footnotesize font so
prose columns wrap cleanly.
"""
import pandas as pd
from pathlib import Path

TBL_DIR = Path("/mnt/d/thesis/main/tables/eda")

# Per-table column widths in cm (must sum to <= ~14.5cm for typical \textwidth).
# >{\raggedright\arraybackslash} keeps text left-aligned and lets it wrap naturally.
# Thesis textwidth = 373.44pt ≈ 13.13cm (typearea KOMA-script setup).
# Content budget (after \tabcolsep=3pt × 2 × ncol):
#   7 cols → ~11.7cm available for p{} widths
#   6 cols → ~11.8cm
#   5 cols → ~11.9cm
TABLES = {
    "data_fields_ais": dict(  # 5 cols: DB column / Type / Unit / Valid range / Description
        caption="NOAA AIS broadcast points field dictionary (MarineCadastre.gov, "
                "September~2025 revision).",
        label="tab:eda_fields_ais",
        widths_cm=[2.5, 1.5, 1.5, 2.4, 4.0],
    ),
    "data_fields_icoads": dict(  # 4 cols: DB column / Storage / Convert / Description
        caption="NOAA ICOADS Release~3 field dictionary for the columns used "
                "in this thesis "
                "\\autocite{smith2016_imma1, freeman2017_icoads}.",
        label="tab:eda_fields_icoads",
        widths_cm=[3.0, 2.5, 1.6, 4.9],
    ),
    "data_fields_opensky": dict(  # 4 cols: DB column / Type / Unit / Description
        caption="OpenSky Network state-vector field dictionary "
                "(\\texttt{state\\_vectors\\_data4} schema).",
        label="tab:eda_fields_opensky",
        widths_cm=[2.7, 2.0, 2.0, 5.3],
    ),
    "data_fields_ourairports": dict(  # 4 cols: DB column / Type / Valid values / Description
        caption="OurAirports \\texttt{airports.csv} field dictionary (CC-BY).",
        label="tab:eda_fields_ourairports",
        widths_cm=[2.5, 1.5, 3.0, 5.0],
    ),
    "data_fields_osm": dict(  # 4 cols: Table / DB column / Type / Description
        caption="OpenStreetMap tags and thesis-curated columns used in the analysis.",
        label="tab:eda_fields_osm",
        widths_cm=[2.5, 2.5, 1.5, 5.5],
    ),
    "data_units_reference": dict(  # 5 cols: Table / Column / Storage / Convert / Verified by
        caption="Storage encoding and SI conversion for every numeric column "
                "referenced in the analysis.",
        label="tab:eda_data_units",
        widths_cm=[2.7, 2.7, 1.9, 1.7, 2.9],   # widened Table + Column to fit
    ),                                         # ``weather_observations'' / ``wind_speed_indicator''
}


# LaTeX-safe substitutions for Unicode characters that the thesis class doesn't
# render natively. Applied to every string cell before to_latex().
UNICODE_SUBS = {
    "−": "-",      # U+2212 minus sign
    "–": "--",     # en dash
    "—": "---",    # em dash
    "…": "...",    # horizontal ellipsis
    "≤": "<=",
    "≥": ">=",
    "°": " deg",   # degree sign (e.g., "tenths of deg C")
    "±": "+/-",
    "×": "x",
    "÷": "/",
    "½": "1/2",
    "“": "\"",
    "”": "\"",
    "‘": "'",
    "’": "'",
    " ": " ",      # non-breaking space
    "µ": "u",
    "²": "^2",
    "³": "^3",
    "→": "to",        # cleaner than "->" in tabular prose; collapse_spaces() below
    "←": "from",
    "≈": "~=",
    "®": "(R)",
}


def latex_safe(s):
    """Replace LaTeX-unfriendly Unicode characters with safe equivalents,
    then collapse any resulting consecutive spaces."""
    if not isinstance(s, str):
        return s
    for u, replacement in UNICODE_SUBS.items():
        s = s.replace(u, replacement)
    # Collapse runs of spaces produced by ASCII substitution near existing spaces
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()


def emit(name: str, spec: dict) -> None:
    csv_path = TBL_DIR / f"{name}.csv"
    tex_path = TBL_DIR / f"{name}.tex"
    df = pd.read_csv(csv_path)
    # Sanitise every string cell (and column names) for LaTeX
    df.columns = [latex_safe(c) for c in df.columns]
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(latex_safe)
    if len(spec["widths_cm"]) != df.shape[1]:
        raise ValueError(f"{name}: widths_cm length {len(spec['widths_cm'])} "
                         f"!= column count {df.shape[1]}")
    column_format = "".join(
        f">{{\\raggedright\\arraybackslash}}p{{{w}cm}}" for w in spec["widths_cm"]
    )
    body = df.to_latex(
        index=False,
        escape=True,
        longtable=True,
        column_format=column_format,
        caption=spec["caption"],
        label=spec["label"],
        na_rep="—",
    )
    # pandas emits caption + label inside longtable. Tighten \tabcolsep so all
    # the column-separator gaps fit inside \textwidth, use \footnotesize for
    # prose-heavy tables, enable slash-hyphenation + emergency stretch, AND
    # redefine \_ to permit soft line breaks at every underscore so long
    # identifiers like `stage2_helicopter_tracks` wrap inside narrow columns
    # instead of overflowing into the next column.
    tex_path.write_text(
        "{\\footnotesize\\setlength{\\tabcolsep}{3pt}\n"
        "\\setlength{\\emergencystretch}{2em}\n"
        "\\hyphenpenalty=1000\\exhyphenpenalty=0\n"
        # Make `\_` breakable: keep the underscore glyph but follow it with
        # a zero-penalty zero-width break opportunity. Scoped by surrounding
        # braces — outside the table, \_ reverts to default.
        "\\let\\OldUnderscore\\_\n"
        "\\renewcommand{\\_}{\\OldUnderscore\\penalty0\\hskip0pt\\relax}\n"
        + body + "}\n"
    )
    print(f"  {name}.tex  cols={df.shape[1]}  widths={sum(spec['widths_cm']):.1f}cm")


def main():
    for name, spec in TABLES.items():
        emit(name, spec)


if __name__ == "__main__":
    main()
