"""Run inside Docker: pip install fpdf2 && python gen_sample_pdf.py /out/sample.pdf"""
import sys
from pathlib import Path

from fpdf import FPDF


def main() -> None:
    out = Path(sys.argv[1])
    out.parent.mkdir(parents=True, exist_ok=True)
    p = FPDF()
    p.add_page()
    p.set_font("Helvetica", size=20)
    p.text(40, 100, "Hello from test PDF")
    p.output(str(out))
    print("Wrote", out)


if __name__ == "__main__":
    main()
