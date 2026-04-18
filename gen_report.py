"""
Hawala v2 — Daily Pre-Market PDF Report Generator
Mirrors the premarketpulse.com layout + extended analysis pages.
Run daily at 7:30 AM IST by the cron/scheduled task.
Data is passed in via --data data.json (fetched live) or uses SAMPLE_DATA.

PDF Pages:
  1  Fear & Greed gauge + quote
  2  Overview: GIFT Nifty, Asian/Europe/US/India markets
  3  Commodities (spot + MCX), Crypto, Currencies
  4  Snapshot: Gainers/Losers, Volume Shockers, 52W High, L/S Buildup
  5  Sectoral Indices (1D + 7D)
  6  India Market Bulletin (top news from prev close)
  7  Nifty 50 Analysis: Option Chain, PCR, Pivots, FII/DII
  8  Bank Nifty Analysis: Option Chain, PCR, Pivots, Greeks
  9  Hawala v2 Signal: macro filters + gap signal + trade params

Signal JSON (machine-readable, written alongside PDF):
  market_signal_YYYY-MM-DD.json
"""

import math
import sys
import os
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

# ── Palette ────────────────────────────────────────────────────────────────
C_GREEN  = colors.HexColor("#00A86B")
C_RED    = colors.HexColor("#E03535")
C_DARK   = colors.HexColor("#0D1B2A")
C_ACCENT = colors.HexColor("#1DB954")
C_TEAL   = colors.HexColor("#17C3B2")
C_GRAY   = colors.HexColor("#6B7280")
C_LGRAY  = colors.HexColor("#F3F4F6")
C_WHITE  = colors.white
C_BG     = colors.HexColor("#F8FAFC")
C_BLUE   = colors.HexColor("#2563EB")

W, H = A4  # 595 x 842 pts

def pct_color(val):
    """Return green/red color based on sign of value."""
    try:
        return C_GREEN if float(str(val).replace('%','').replace('+','')) >= 0 else C_RED
    except:
        return C_GRAY

def fmt_pct(val, plus=True):
    try:
        f = float(str(val).replace('%','').replace('+',''))
        s = f"+{f:.2f}%" if f >= 0 else f"{f:.2f}%"
        return s
    except:
        return str(val)

# ═══════════════════════════════════════════════════════════════════════════
# PAGE 1 — Fear & Greed + Header
# ═══════════════════════════════════════════════════════════════════════════
def draw_page1(c, data):
    c.setFillColor(C_WHITE)
    c.rect(0, 0, W, H, fill=1, stroke=0)

    # Top teal stripe
    c.setFillColor(C_TEAL)
    c.rect(0, H-8, W, 8, fill=1, stroke=0)

    # Left teal sidebar
    c.setFillColor(C_TEAL)
    c.rect(0, 0, 8, H, fill=1, stroke=0)

    # Right teal sidebar
    c.setFillColor(C_TEAL)
    c.rect(W-8, 0, 8, H, fill=1, stroke=0)

    # Bottom teal stripe
    c.setFillColor(C_TEAL)
    c.rect(0, 0, W, 8, fill=1, stroke=0)

    # Header text
    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 9)
    c.drawString(30, H-30, "premarketpulse.com  |  Consolidated Daily Market News")

    # Date
    date_str = data.get("date_str", datetime.now().strftime("%a, %-d %b"))
    year_str = data.get("year_str", datetime.now().strftime("%Y"))

    c.setFillColor(C_DARK)
    c.setFont("Helvetica-Bold", 54)
    c.drawString(30, H-100, date_str)

    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 30)
    c.drawRightString(W-30, H-78, year_str)

    # Divider
    c.setStrokeColor(C_GRAY)
    c.setLineWidth(0.5)
    c.line(30, H-115, W-30, H-115)

    # Fear & Greed label
    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 10)
    c.drawString(30, H-140, "Fear & Greed Index")

    # Draw gauge (semicircle)
    fg_val = float(data.get("fear_greed_val", 50))
    _draw_gauge(c, cx=W/2, cy=H-320, radius=130, value=fg_val)

    # Value below gauge
    c.setFillColor(C_DARK)
    c.setFont("Helvetica-Bold", 36)
    c.drawCentredString(W/2, H-385, f"{fg_val:.2f}")

    # Label
    label = data.get("fear_greed_label", "Neutral")
    c.setFont("Helvetica", 18)
    c.setFillColor(C_DARK)
    c.drawCentredString(W/2, H-412, label)

    # Definition box
    box_x, box_y = 30, H-540
    box_w, box_h = 340, 120
    c.setFillColor(C_LGRAY)
    c.roundRect(box_x, box_y, box_w, box_h, 6, fill=1, stroke=0)
    c.setFont("Helvetica", 9)
    c.setFillColor(C_GRAY)
    c.drawString(box_x+10, box_y+box_h-18, "Definition")
    defs = [
        (C_GREEN, "Extreme Fear (<30):  Good time to open positions"),
        (colors.HexColor("#A3C349"), "Fear (30-50):  Wait for market direction"),
        (colors.HexColor("#F59E0B"), "Greed (50-70):  Be cautious with new positions"),
        (C_RED, "Extreme Greed (>70):  Avoid opening positions"),
    ]
    for i, (col, txt) in enumerate(defs):
        y_pos = box_y + box_h - 38 - i*20
        c.setFillColor(col)
        c.circle(box_x+18, y_pos+4, 5, fill=1, stroke=0)
        c.setFillColor(C_DARK)
        c.setFont("Helvetica", 8.5)
        c.drawString(box_x+28, y_pos, txt)

    # Weekly change box
    wc_x, wc_y = 385, H-490
    wc_w, wc_h = 170, 70
    c.setFillColor(C_LGRAY)
    c.roundRect(wc_x, wc_y, wc_w, wc_h, 6, fill=1, stroke=0)
    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 9)
    c.drawCentredString(wc_x + wc_w/2, wc_y + wc_h - 18, "Weekly Change")
    c.setFillColor(C_DARK)
    c.setFont("Helvetica-Bold", 16)
    prev_fg = data.get("fear_greed_prev", "26.21")
    c.drawString(wc_x+15, wc_y+20, str(prev_fg))
    c.drawString(wc_x+80, wc_y+20, "→")
    c.drawRightString(wc_x+wc_w-15, wc_y+20, f"{fg_val:.2f}")

    # Quote
    quote = data.get("quote", '"The stock market is a device for transferring money from the impatient to the patient."')
    author = data.get("quote_author", "@ Warren Buffett")

    c.setFillColor(C_DARK)
    c.setFont("Helvetica-Bold", 13)
    # Simple word-wrap for quote
    words = quote.split()
    lines = []
    line = ""
    for w in words:
        test = (line + " " + w).strip()
        if c.stringWidth(test, "Helvetica-Bold", 13) < W - 80:
            line = test
        else:
            lines.append(line)
            line = w
    if line:
        lines.append(line)

    y_q = 130
    for ln in lines:
        c.drawString(30, y_q, ln)
        y_q -= 18

    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 10)
    c.drawString(30, y_q - 8, author)

    # Footer
    _draw_footer(c, 1)
    c.showPage()


def _draw_gauge(c, cx, cy, radius, value):
    """Draw semicircular Fear & Greed gauge."""
    import math
    # Background arc segments: extreme fear → fear → neutral → greed → extreme greed
    segments = [
        (180, 144, colors.HexColor("#2ECC71")),   # extreme fear (green)
        (144, 108, colors.HexColor("#A3C349")),   # fear
        (108,  72, colors.HexColor("#F59E0B")),   # neutral
        (72,   36, colors.HexColor("#FF6B35")),   # greed
        (36,    0, colors.HexColor("#E03535")),   # extreme greed
    ]
    for start_deg, end_deg, col in segments:
        c.setStrokeColor(col)
        c.setLineWidth(18)
        c.arc(cx-radius, cy-radius, cx+radius, cy+radius,
              startAng=start_deg, extent=-(start_deg - end_deg))

    # Needle
    needle_angle = 180 - (value / 100) * 180
    angle_rad = math.radians(needle_angle)
    needle_len = radius - 12
    nx = cx + needle_len * math.cos(angle_rad)
    ny = cy + needle_len * math.sin(angle_rad)
    c.setStrokeColor(C_DARK)
    c.setLineWidth(3)
    c.line(cx, cy, nx, ny)

    # Center circle
    c.setFillColor(colors.HexColor("#CCCCCC"))
    c.setStrokeColor(C_WHITE)
    c.setLineWidth(2)
    c.circle(cx, cy, 12, fill=1, stroke=1)

    # Scale labels
    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 7)
    labels = [(0, "0"), (30, "30"), (70, "70"), (100, "100")]
    for v, lbl in labels:
        ang = math.radians(180 - (v/100)*180)
        lx = cx + (radius+14) * math.cos(ang)
        ly = cy + (radius+14) * math.sin(ang)
        c.drawCentredString(lx, ly-3, lbl)

    # Rotated arc labels
    arc_labels = [(10, "EXTREME\nFEAR"), (35, "FEAR"), (65, "GREED"), (90, "EXTREME\nGREED")]
    for v, lbl in arc_labels:
        ang_deg = 180 - (v/100)*180
        ang_rad = math.radians(ang_deg)
        lx = cx + (radius - 40) * math.cos(ang_rad)
        ly = cy + (radius - 40) * math.sin(ang_rad)
        c.saveState()
        c.translate(lx, ly)
        c.rotate(ang_deg - 90)
        c.setFont("Helvetica", 6)
        c.setFillColor(C_DARK)
        for j, part in enumerate(lbl.split('\n')):
            c.drawCentredString(0, -j*7, part)
        c.restoreState()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 2 — Overview
# ═══════════════════════════════════════════════════════════════════════════
def draw_page2(c, data):
    c.setFillColor(C_WHITE)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    _draw_sidebar(c)

    y = H - 40

    # Title
    c.setFillColor(C_DARK)
    c.setFont("Helvetica-Bold", 36)
    c.drawString(30, y, "Overview")
    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 9)
    c.drawRightString(W-30, y+8, f"As of today at  {data.get('overview_time','7:08 AM')}")
    y -= 30

    # GIFT Nifty highlight row
    gift = data.get("gift_nifty", {})
    gift_val = gift.get("val", "23,778.0")
    gift_chg = gift.get("chg", "-313.5")
    gift_pct = gift.get("pct", "-1.30%")
    gift_up = not str(gift_chg).startswith("-")
    bg_col = colors.HexColor("#E8F8F0") if gift_up else colors.HexColor("#FEE2E2")
    dot_col = C_GREEN if gift_up else C_RED
    c.setFillColor(bg_col)
    c.roundRect(20, y-18, W-40, 26, 4, fill=1, stroke=0)
    c.setFillColor(dot_col)
    c.circle(38, y-5, 6, fill=1, stroke=0)
    c.setFillColor(C_DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(52, y-9, "GIFT Nifty")
    c.setFont("Helvetica-Bold", 11)
    c.drawRightString(W-160, y-9, gift_val)
    c.setFillColor(dot_col)
    c.setFont("Helvetica-Bold", 11)
    c.drawRightString(W-90, y-9, str(gift_chg))
    c.drawRightString(W-30, y-9, str(gift_pct))
    y -= 42

    # Market sections
    sections = [
        ("Asian Market", data.get("asian_markets", [])),
        ("Europe Market", data.get("europe_markets", [])),
        ("US Market", data.get("us_markets", [])),
        ("Indian Market", data.get("india_markets", [])),
    ]

    for section_name, rows in sections:
        c.setFillColor(C_DARK)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(30, y, section_name)
        y -= 20

        for row in rows:
            name    = row.get("name", "")
            region  = row.get("region", "")
            val     = row.get("val", "")
            chg     = row.get("chg", "")
            pct     = row.get("pct", "")
            up      = not str(chg).startswith("-")
            dot_c   = C_GREEN if up else C_RED

            c.setFillColor(dot_c)
            c.circle(38, y+4, 5, fill=1, stroke=0)
            c.setFillColor(C_DARK)
            c.setFont("Helvetica-Bold", 11)
            c.drawString(52, y, name)
            if region:
                c.setFillColor(C_GRAY)
                c.setFont("Helvetica", 10)
                c.drawString(160, y, region)
            c.setFillColor(C_DARK)
            c.setFont("Helvetica", 11)
            c.drawRightString(W-160, y, str(val))
            c.setFillColor(dot_c)
            c.setFont("Helvetica-Bold", 11)
            c.drawRightString(W-90, y, str(chg))
            c.drawRightString(W-30, y, str(pct))

            # Divider
            c.setStrokeColor(colors.HexColor("#E5E7EB"))
            c.setLineWidth(0.4)
            c.line(30, y-6, W-30, y-6)
            y -= 22

        y -= 8

    _draw_footer(c, 2)
    c.showPage()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 3 — Commodities, Crypto, Currencies
# ═══════════════════════════════════════════════════════════════════════════
def draw_page3(c, data):
    c.setFillColor(C_WHITE)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    _draw_sidebar(c)

    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 9)
    c.drawRightString(W-30, H-30, f"As of today at  {data.get('commodity_time','7:00 AM')}")

    y = H - 55

    def _draw_section(title, subtitle, rows, y_pos):
        c.setFillColor(C_DARK)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(30, y_pos, title)
        if subtitle:
            c.setFillColor(C_GRAY)
            c.setFont("Helvetica", 13)
            c.drawString(30 + c.stringWidth(title, "Helvetica-Bold", 16) + 6, y_pos, subtitle)
        y_pos -= 22
        for row in rows:
            name = row.get("name","")
            val  = row.get("val","")
            chg  = row.get("chg","-")
            pct  = row.get("pct","")
            up   = not str(chg).lstrip("+").startswith("-") if chg != "-" else (not str(pct).startswith("-"))
            dot_c = C_GREEN if up else C_RED

            c.setFillColor(dot_c)
            c.circle(38, y_pos+4, 5, fill=1, stroke=0)
            c.setFillColor(C_DARK)
            c.setFont("Helvetica-Bold", 11)
            c.drawString(52, y_pos, name)
            c.setFont("Helvetica", 11)
            c.drawRightString(W-160, y_pos, str(val))
            c.setFillColor(dot_c)
            c.setFont("Helvetica-Bold", 11)
            c.drawRightString(W-90, y_pos, str(chg))
            c.drawRightString(W-30, y_pos, str(pct))

            c.setStrokeColor(colors.HexColor("#E5E7EB"))
            c.setLineWidth(0.4)
            c.line(30, y_pos-6, W-30, y_pos-6)
            y_pos -= 22
        return y_pos - 12

    y = _draw_section("Commodity", " (Global Spot)", data.get("commodities_spot", []), y)
    y = _draw_section("MCX Futures", " (Current Expiry)", data.get("mcx_futures", []), y)
    y = _draw_section("Crypto", "", data.get("crypto", []), y)
    y = _draw_section("Currencies", "", data.get("currencies", []), y)

    _draw_footer(c, 3)
    c.showPage()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 4 — Snapshot (Top Gainers/Losers, Volume, 52W, Long/Short)
# ═══════════════════════════════════════════════════════════════════════════
def draw_page4(c, data):
    c.setFillColor(C_WHITE)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    _draw_sidebar(c)

    c.setFillColor(C_DARK)
    c.setFont("Helvetica-Bold", 36)
    c.drawString(30, H-50, "Snapshot")

    y = H - 85

    # Top Gainers & Losers — side by side
    half = (W - 60) / 2
    def _draw_gainers_losers(gainers, losers, y_start):
        # Gainers header
        c.setFillColor(C_GREEN)
        c.circle(38, y_start+14, 10, fill=1, stroke=0)
        c.setFillColor(C_WHITE)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(38, y_start+11, "▲")
        c.setFillColor(C_DARK)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(56, y_start+8, "Top Gainers")
        c.setFillColor(C_GRAY)
        c.setFont("Helvetica", 9)
        c.drawString(56, y_start-4, "Nifty 500")

        # Losers header
        lx = 30 + half + 10
        c.setFillColor(C_RED)
        c.circle(lx+8, y_start+14, 10, fill=1, stroke=0)
        c.setFillColor(C_WHITE)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(lx+8, y_start+11, "▼")
        c.setFillColor(C_DARK)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(lx+24, y_start+8, "Top Losers")
        c.setFillColor(C_GRAY)
        c.setFont("Helvetica", 9)
        c.drawString(lx+24, y_start-4, "Nifty 500")

        yr = y_start - 24
        for i in range(max(len(gainers), len(losers))):
            if i < len(gainers):
                g = gainers[i]
                c.setFillColor(C_DARK)
                c.setFont("Helvetica", 10)
                c.drawString(30, yr, g.get("name",""))
                c.setFillColor(C_GREEN)
                c.setFont("Helvetica-Bold", 10)
                c.drawRightString(30+half-5, yr, g.get("pct",""))
            if i < len(losers):
                l = losers[i]
                c.setFillColor(C_DARK)
                c.setFont("Helvetica", 10)
                c.drawString(lx, yr, l.get("name",""))
                c.setFillColor(C_RED)
                c.setFont("Helvetica-Bold", 10)
                c.drawRightString(W-30, yr, l.get("pct",""))
            yr -= 18
        # Divider
        c.setStrokeColor(colors.HexColor("#E5E7EB"))
        c.setLineWidth(0.4)
        c.line(30, yr-4, W-30, yr-4)
        return yr - 18

    y = _draw_gainers_losers(data.get("top_gainers",[]), data.get("top_losers",[]), y)

    # Volume Shockers & 52 Week High
    def _draw_two_tables(left_title, left_rows, right_title, right_rows, y_start):
        half_x = 30 + (W-60)/2 + 10
        # Headers
        c.setFillColor(C_BLUE)
        c.circle(38, y_start+12, 10, fill=1, stroke=0)
        c.setFillColor(C_WHITE)
        c.setFont("Helvetica-Bold", 9)
        c.drawCentredString(38, y_start+9, "▮▮")
        c.setFillColor(C_DARK)
        c.setFont("Helvetica-Bold", 13)
        c.drawString(56, y_start+6, left_title)

        c.setFillColor(C_BLUE)
        c.circle(half_x+8, y_start+12, 10, fill=1, stroke=0)
        c.setFillColor(C_WHITE)
        c.drawCentredString(half_x+8, y_start+9, "↑")
        c.setFillColor(C_DARK)
        c.setFont("Helvetica-Bold", 13)
        c.drawString(half_x+24, y_start+6, right_title)

        yr = y_start - 10
        c.setFillColor(C_GRAY)
        c.setFont("Helvetica", 7.5)
        c.drawString(30, yr, "Name")
        c.drawRightString(30+(W-60)/2, yr, "CMP")
        c.drawString(half_x, yr, "Name")
        c.drawRightString(W-30, yr, "Change %")
        yr -= 14

        for i in range(max(len(left_rows), len(right_rows))):
            if i < len(left_rows):
                r = left_rows[i]
                c.setFillColor(C_DARK)
                c.setFont("Helvetica", 9.5)
                c.drawString(30, yr, r.get("name",""))
                c.drawRightString(30+(W-60)/2-5, yr, str(r.get("val","")))
            if i < len(right_rows):
                r = right_rows[i]
                c.setFillColor(C_DARK)
                c.setFont("Helvetica", 9.5)
                c.drawString(half_x, yr, r.get("name",""))
                pct_val = r.get("pct","")
                up = not str(pct_val).startswith("-")
                c.setFillColor(C_GREEN if up else C_RED)
                c.setFont("Helvetica-Bold", 9.5)
                c.drawRightString(W-30, yr, str(pct_val))
            yr -= 16
        c.setStrokeColor(colors.HexColor("#E5E7EB"))
        c.line(30, yr-4, W-30, yr-4)
        return yr - 16

    y = _draw_two_tables(
        "Volume Shockers", data.get("volume_shockers",[]),
        "52 Week High", data.get("week52_highs",[]),
        y
    )

    # Long & Short Buildup
    c.setFillColor(C_DARK)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(30, y, "Long & Short Buildup")
    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 9)
    c.drawString(30 + c.stringWidth("Long & Short Buildup","Helvetica-Bold",14)+6, y, " (Stock Futures - Current Expiry)")
    y -= 14

    c.setFont("Helvetica", 7.5)
    c.drawString(30, y, "Long build-up: Open interest and volumes rise, and the price goes up.")
    y -= 10
    c.drawString(30, y, "Short build-up: Open interest and volumes rise, but the price goes down.")
    y -= 18

    # Column headers
    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 8)
    half_x = 30 + (W-60)/2 + 10
    c.drawString(30, y, "Long Buildup")
    c.drawRightString(30+(W-60)/2, y, "OI Change %")
    c.drawString(half_x, y, "Short Buildup")
    c.drawRightString(W-30, y, "OI Change %")
    y -= 14

    longs  = data.get("long_buildup",[])
    shorts = data.get("short_buildup",[])
    for i in range(max(len(longs), len(shorts))):
        if i < len(longs):
            r = longs[i]
            c.setFillColor(C_GREEN)
            c.setFont("Helvetica-Bold", 9.5)
            c.drawString(30, y, r.get("name",""))
            c.drawRightString(30+(W-60)/2-5, y, str(r.get("pct","")))
        if i < len(shorts):
            r = shorts[i]
            c.setFillColor(C_RED)
            c.setFont("Helvetica-Bold", 9.5)
            c.drawString(half_x, y, r.get("name",""))
            c.drawRightString(W-30, y, str(r.get("pct","")))
        y -= 16

    _draw_footer(c, 4)
    c.showPage()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 5 — Sectoral Indices
# ═══════════════════════════════════════════════════════════════════════════
def draw_page5(c, data):
    c.setFillColor(C_WHITE)
    c.rect(0, 0, W, H, fill=1, stroke=0)
    _draw_sidebar(c)

    c.setFillColor(C_DARK)
    c.setFont("Helvetica-Bold", 36)
    c.drawString(30, H-50, "Sectoral Indices")

    y = H - 85
    bar_max_w = W - 200

    def _draw_sector_section(num_label, title, rows, y_pos):
        # Circle with number
        c.setFillColor(C_DARK)
        c.circle(44, y_pos+8, 12, fill=1, stroke=0)
        c.setFillColor(C_WHITE)
        c.setFont("Helvetica-Bold", 10)
        c.drawCentredString(44, y_pos+5, str(num_label))
        c.setFillColor(C_DARK)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(64, y_pos+2, title)
        y_pos -= 22

        sorted_rows = sorted(rows, key=lambda x: float(str(x.get("pct","0")).replace('%','').replace('+','')), reverse=True)
        for row in sorted_rows:
            name = row.get("name","")
            pct_str = str(row.get("pct","0%"))
            try:
                pct_f = float(pct_str.replace('%','').replace('+',''))
            except:
                pct_f = 0
            bar_w = abs(pct_f) / 3.0 * bar_max_w * 0.6
            bar_w = min(bar_w, bar_max_w * 0.55)
            col = C_GREEN if pct_f >= 0 else C_RED

            c.setFillColor(C_DARK)
            c.setFont("Helvetica", 9)
            c.drawString(30, y_pos, name)

            bar_x = 185
            if pct_f >= 0:
                c.setFillColor(col)
                c.rect(bar_x, y_pos-1, bar_w, 10, fill=1, stroke=0)
                c.setFillColor(C_DARK)
                c.setFont("Helvetica-Bold", 8.5)
                c.drawString(bar_x + bar_w + 4, y_pos+1, pct_str if pct_str.startswith("+") else f"+{pct_str}")
            else:
                bar_end = bar_x
                c.setFillColor(col)
                c.rect(bar_end - bar_w, y_pos-1, bar_w, 10, fill=1, stroke=0)
                c.setFillColor(C_DARK)
                c.setFont("Helvetica-Bold", 8.5)
                c.drawRightString(bar_end - bar_w - 3, y_pos+1, pct_str)

            y_pos -= 17
        return y_pos - 10

    y = _draw_sector_section(1, "Day Change", data.get("sectoral_1d",[]), y)
    y = _draw_sector_section(7, "Day Change", data.get("sectoral_7d",[]), y)

    _draw_footer(c, 5)
    c.showPage()


# ── Helpers ─────────────────────────────────────────────────────────────────
def _draw_sidebar(c):
    c.setFillColor(C_TEAL)
    c.rect(0, H-8, W, 8, fill=1, stroke=0)
    c.rect(0, 0, W, 8, fill=1, stroke=0)
    c.rect(0, 0, 8, H, fill=1, stroke=0)
    c.rect(W-8, 0, 8, H, fill=1, stroke=0)


def _draw_footer(c, page_num):
    c.setFillColor(C_TEAL)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(30, 18, "Hawala v2")
    c.setFillColor(C_GRAY)
    c.setFont("Helvetica", 8)
    c.drawCentredString(W/2, 18, "hawala-v2.local")
    c.setFillColor(C_DARK)
    c.setFont("Helvetica-Bold", 9)
    c.drawRightString(W-30, 18, f"{page_num:02d}")


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 6 — India Market Bulletin
# ═══════════════════════════════════════════════════════════════════════════
def draw_page6(c, data):
    c.setFillColor(C_WHITE); c.rect(0,0,W,H,fill=1,stroke=0)
    _draw_sidebar(c)

    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 36)
    c.drawString(30, H-50, "Market Bulletin")
    c.setFillColor(C_GRAY); c.setFont("Helvetica", 9)
    c.drawRightString(W-30, H-30, f"Previous close highlights — {data.get('date_str','')}")

    y = H - 85
    items = data.get("market_bulletin", [])
    for i, item in enumerate(items, 1):
        # Number badge
        c.setFillColor(C_TEAL)
        c.circle(40, y+4, 9, fill=1, stroke=0)
        c.setFillColor(C_WHITE); c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(40, y+1, str(i))

        # Text — word wrap to fit width
        c.setFillColor(C_DARK); c.setFont("Helvetica", 10)
        words = item.split()
        line, lines = "", []
        for w in words:
            test = (line+" "+w).strip()
            if c.stringWidth(test,"Helvetica",10) < W-100:
                line = test
            else:
                lines.append(line); line = w
        if line: lines.append(line)

        for j, ln in enumerate(lines):
            c.drawString(58, y - j*13, ln)

        y -= max(len(lines)*13 + 10, 26)
        # Divider
        c.setStrokeColor(colors.HexColor("#E5E7EB")); c.setLineWidth(0.4)
        c.line(30, y, W-30, y); y -= 10
        if y < 40: break

    _draw_footer(c, 6); c.showPage()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 7 — Nifty 50 Analysis (Option Chain, PCR, Pivots, FII/DII)
# ═══════════════════════════════════════════════════════════════════════════
def draw_page7(c, data):
    c.setFillColor(C_WHITE); c.rect(0,0,W,H,fill=1,stroke=0)
    _draw_sidebar(c)

    nf = data.get("nifty_analysis", {})
    prev_close = nf.get("prev_close", "—")
    pct        = nf.get("prev_pct", "+1.16%")
    pct_col    = C_GREEN if not str(pct).startswith("-") else C_RED

    # Header
    c.setFillColor(C_DARK); c.circle(46, H-38, 14, fill=1, stroke=0)
    c.setFillColor(C_WHITE); c.setFont("Helvetica-Bold", 11); c.drawCentredString(46, H-41, "50")
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 28); c.drawString(68, H-50, "Nifty 50")
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 18); c.drawRightString(W-120, H-44, str(prev_close))
    c.setFillColor(pct_col); c.setFont("Helvetica-Bold", 14); c.drawRightString(W-30, H-44, str(pct))
    c.setFillColor(pct_col)
    c.circle(W-115, H-40, 8, fill=1, stroke=0)

    y = H - 70

    # ── Option Chain Summary ──
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 14); c.drawString(30, y, "Option Chain Highlights")
    y -= 18
    oc = nf.get("option_chain", {})
    oi_rows = [
        ("Max Pain Strike",       oc.get("max_pain", "—"),       C_DARK),
        ("Highest Call OI Strike",oc.get("call_resistance","—"),  C_RED),
        ("Highest Put OI Strike", oc.get("put_support","—"),      C_GREEN),
        ("Key Call Writing",      oc.get("call_writing_strikes","—"), C_RED),
        ("Key Put Writing",       oc.get("put_writing_strikes","—"),  C_GREEN),
    ]
    for label, val, col in oi_rows:
        c.setFillColor(C_GRAY); c.setFont("Helvetica", 9); c.drawString(30, y, label)
        c.setFillColor(col); c.setFont("Helvetica-Bold", 11); c.drawRightString(W/2-10, y, str(val))
        y -= 16

    # Advance / Decline
    ad = nf.get("advance_decline", {})
    adv, dec = ad.get("advance", 0), ad.get("decline", 0)
    total = adv + dec if (adv+dec) > 0 else 1
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 12); c.drawString(W/2+10, H-88, "Advance / Decline")
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 22); c.drawString(W/2+10, H-112, str(adv))
    bar_x, bar_y, bar_w, bar_h = W/2+50, H-118, W/2-60, 12
    c.setFillColor(C_GREEN); c.rect(bar_x, bar_y, bar_w*(adv/total), bar_h, fill=1, stroke=0)
    c.setFillColor(C_RED);   c.rect(bar_x+bar_w*(adv/total), bar_y, bar_w*(dec/total), bar_h, fill=1, stroke=0)
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 22); c.drawRightString(W-30, H-112, str(dec))

    # ── PCR Weekly Trend ──
    y = H - 175
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 14); c.drawString(30, y, "Weekly PCR Trend")
    y -= 6
    pcr_trend = nf.get("pcr_weekly_trend", [])
    bar_area_w = W - 220
    bar_max_h  = 60
    if pcr_trend:
        max_pcr = max(float(p.get("pcr",1)) for p in pcr_trend)
        bar_slot = bar_area_w / len(pcr_trend)
        for i, p in enumerate(pcr_trend):
            pcr_val = float(p.get("pcr", 1))
            bh = int((pcr_val / max(max_pcr, 1.5)) * bar_max_h)
            bx = 30 + i * bar_slot + 5
            by = y - bar_max_h - 24
            c.setFillColor(C_BLUE); c.rect(bx, by, bar_slot-10, bh, fill=1, stroke=0)
            c.setFillColor(C_DARK); c.setFont("Helvetica", 7)
            c.drawCentredString(bx+(bar_slot-10)/2, by-10, p.get("day",""))
            c.setFont("Helvetica-Bold", 8)
            c.drawCentredString(bx+(bar_slot-10)/2, by-20, f"({pcr_val})")
        y -= bar_max_h + 40

    # ── Pivot Levels ──
    y -= 10
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 14); c.drawString(30, y, "Pivot Levels")
    y -= 18
    _draw_pivot_table(c, nf.get("pivot_classic",{}), nf.get("pivot_fibonacci",{}), 30, y)
    y -= 56

    # ── FII/DII summary ──
    fii = data.get("fii_dii", {})
    y -= 10
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 14); c.drawString(30, y, "FII / DII Flow (Cash Market)")
    y -= 18
    rows = [
        ("FII Net",   fii.get("fii_net_crore","—"),  "Cr"),
        ("DII Net",   fii.get("dii_net_crore","—"),  "Cr"),
        ("FII Buy",   fii.get("fii_buy_crore","—"),  "Cr"),
        ("FII Sell",  fii.get("fii_sell_crore","—"), "Cr"),
        ("DII Buy",   fii.get("dii_buy_crore","—"),  "Cr"),
        ("DII Sell",  fii.get("dii_sell_crore","—"), "Cr"),
    ]
    half = (W-60)//2
    for i, (label, val, unit) in enumerate(rows):
        col_x = 30 if i%2==0 else 30+half+10
        col_y = y - (i//2)*18
        try:
            fval = float(str(val).replace(",",""))
            col  = C_GREEN if fval >= 0 else C_RED
        except:
            col  = C_DARK
        c.setFillColor(C_GRAY); c.setFont("Helvetica",9); c.drawString(col_x, col_y, label)
        c.setFillColor(col); c.setFont("Helvetica-Bold",11)
        c.drawString(col_x+80, col_y, f"₹{val} {unit}")

    y -= (len(rows)//2 + 1)*18 + 10

    # ── FII F&O ──
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 12); c.drawString(30, y, "FII in F&O (Index Futures)")
    y -= 16
    fno_rows = [
        ("Long Contracts",  fii.get("fii_fno_long","—")),
        ("Short Contracts", fii.get("fii_fno_short","—")),
        ("Long/Short Ratio",fii.get("fii_fno_ls_ratio","—")),
        ("Net Position",    fii.get("fii_fno_net","—")),
    ]
    for label, val in fno_rows:
        c.setFillColor(C_GRAY); c.setFont("Helvetica",9); c.drawString(30, y, label)
        c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",10); c.drawString(200, y, str(val))
        y -= 15

    _draw_footer(c, 7); c.showPage()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 8 — Bank Nifty Analysis (Option Chain, Greeks, PCR, Pivots)
# ═══════════════════════════════════════════════════════════════════════════
def draw_page8(c, data):
    c.setFillColor(C_WHITE); c.rect(0,0,W,H,fill=1,stroke=0)
    _draw_sidebar(c)

    bn = data.get("banknifty_analysis", {})
    prev_close = bn.get("prev_close","—")
    pct        = bn.get("prev_pct","+1.99%")
    pct_col    = C_GREEN if not str(pct).startswith("-") else C_RED

    # Header
    c.setFillColor(C_TEAL); c.circle(46, H-38, 14, fill=1, stroke=0)
    c.setFillColor(C_WHITE); c.setFont("Helvetica-Bold", 8); c.drawCentredString(46, H-41, "BN")
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 28); c.drawString(68, H-50, "Bank Nifty")
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 18); c.drawRightString(W-120, H-44, str(prev_close))
    c.setFillColor(pct_col); c.setFont("Helvetica-Bold", 14); c.drawRightString(W-30, H-44, str(pct))
    c.setFillColor(pct_col); c.circle(W-115, H-40, 8, fill=1, stroke=0)

    y = H - 72

    # ── Option Chain Summary ──
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 14); c.drawString(30, y, "Option Chain Highlights")
    y -= 18
    oc = bn.get("option_chain", {})
    oi_rows = [
        ("Max Pain Strike",        oc.get("max_pain","—"),            C_DARK),
        ("Highest Call OI",        oc.get("call_resistance","—"),     C_RED),
        ("Highest Put OI",         oc.get("put_support","—"),         C_GREEN),
        ("Max Call Unwinding",     oc.get("call_unwinding","—"),      C_GREEN),
        ("Max Put Unwinding",      oc.get("put_unwinding","—"),       C_RED),
        ("Key Call Writing",       oc.get("call_writing_strikes","—"),C_RED),
        ("Key Put Writing",        oc.get("put_writing_strikes","—"), C_GREEN),
    ]
    for label, val, col in oi_rows:
        c.setFillColor(C_GRAY); c.setFont("Helvetica",9); c.drawString(30,y,label)
        c.setFillColor(col); c.setFont("Helvetica-Bold",11); c.drawRightString(W/2-10,y,str(val))
        y -= 16

    # ── Greeks (ATM options) ──
    g = bn.get("atm_greeks", {})
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",14); c.drawString(W/2+10, H-88, "ATM Greeks")
    c.setFillColor(C_GRAY);  c.setFont("Helvetica",8)
    c.drawString(W/2+10, H-100, f"Strike: {g.get('atm_strike','—')}  Expiry: {g.get('expiry','—')}")
    greek_rows = [
        ("IV (Call / Put)", f"{g.get('call_iv','—')} / {g.get('put_iv','—')}%"),
        ("Delta (C/P)",     f"{g.get('call_delta','—')} / {g.get('put_delta','—')}"),
        ("Gamma",           str(g.get("gamma","—"))),
        ("Theta/day (C/P)", f"{g.get('call_theta','—')} / {g.get('put_theta','—')}"),
        ("Vega",            str(g.get("vega","—"))),
        ("PCR (weekly)",    str(bn.get("option_chain",{}).get("pcr","—"))),
    ]
    gy = H - 115
    for label, val in greek_rows:
        c.setFillColor(C_GRAY); c.setFont("Helvetica",9); c.drawString(W/2+10, gy, label)
        c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",10); c.drawRightString(W-30, gy, val)
        gy -= 15

    # ── OI Bar Chart (simplified horizontal bars) ──
    y -= 12
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",13); c.drawString(30, y, "Key Strikes — OI View")
    y -= 16
    oi_strikes = bn.get("oi_strikes", [])
    bar_max_w = 160
    max_oi = max((s.get("call_oi",0) + s.get("put_oi",0)) for s in oi_strikes) if oi_strikes else 1
    for s in oi_strikes:
        strike    = s.get("strike","")
        call_oi   = s.get("call_oi", 0)
        put_oi    = s.get("put_oi", 0)
        is_atm    = s.get("is_atm", False)
        c.setFillColor(C_DARK if not is_atm else C_TEAL)
        c.setFont("Helvetica-Bold" if is_atm else "Helvetica", 9)
        c.drawString(30, y, str(strike) + ("  ◄ ATM" if is_atm else ""))
        # Call bar (right side, red)
        cbw = int((call_oi / max_oi) * bar_max_w) if max_oi else 0
        c.setFillColor(C_RED); c.rect(W/2, y-1, cbw, 9, fill=1, stroke=0)
        c.setFillColor(C_DARK); c.setFont("Helvetica",7); c.drawString(W/2+cbw+3, y+1, f"{call_oi:,}")
        # Put bar (left side, green — drawn right-to-left)
        pbw = int((put_oi / max_oi) * bar_max_w) if max_oi else 0
        c.setFillColor(C_GREEN); c.rect(W/2-pbw, y-1, pbw, 9, fill=1, stroke=0)
        c.setFillColor(C_DARK); c.setFont("Helvetica",7); c.drawRightString(W/2-pbw-3, y+1, f"{put_oi:,}")
        y -= 15
        if y < 200: break

    # Legend
    c.setFillColor(C_GREEN); c.rect(30, y, 10, 8, fill=1, stroke=0)
    c.setFillColor(C_DARK); c.setFont("Helvetica",8); c.drawString(44, y+1, "Put OI")
    c.setFillColor(C_RED); c.rect(100, y, 10, 8, fill=1, stroke=0)
    c.drawString(114, y+1, "Call OI")
    y -= 24

    # ── PCR Trend ──
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",14); c.drawString(30, y, "Weekly PCR Trend (Bank Nifty)")
    y -= 8
    pcr_trend = bn.get("pcr_weekly_trend", [])
    if pcr_trend:
        max_pcr = max(float(p.get("pcr",1)) for p in pcr_trend)
        bar_area_w = W - 200; bar_max_h = 50
        bar_slot = bar_area_w / len(pcr_trend)
        for i, p in enumerate(pcr_trend):
            pcr_val = float(p.get("pcr",1))
            bh = int((pcr_val / max(max_pcr,1.5)) * bar_max_h)
            bx = 30 + i*bar_slot + 5
            by = y - bar_max_h - 20
            c.setFillColor(C_BLUE); c.rect(bx, by, bar_slot-10, bh, fill=1, stroke=0)
            c.setFillColor(C_DARK); c.setFont("Helvetica",7)
            c.drawCentredString(bx+(bar_slot-10)/2, by-10, p.get("day",""))
            c.setFont("Helvetica-Bold",8)
            c.drawCentredString(bx+(bar_slot-10)/2, by-20, f"({pcr_val})")
        y -= bar_max_h + 38

    # ── Pivots ──
    y -= 6
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",14); c.drawString(30, y, "Pivot Levels")
    y -= 18
    _draw_pivot_table(c, bn.get("pivot_classic",{}), bn.get("pivot_fibonacci",{}), 30, y)
    y -= 56

    # ── Advance/Decline ──
    ad = bn.get("advance_decline", {})
    adv, dec = ad.get("advance",0), ad.get("decline",0)
    total = adv+dec if (adv+dec) > 0 else 1
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",12); c.drawString(30, y, "Advance / Decline (Bank Nifty constituents)")
    y -= 18
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",20); c.drawString(30, y, str(adv))
    bx2, bw2 = 70, W-140
    c.setFillColor(C_GREEN); c.rect(bx2, y-2, bw2*(adv/total), 14, fill=1, stroke=0)
    c.setFillColor(C_RED);   c.rect(bx2+bw2*(adv/total), y-2, bw2*(dec/total), 14, fill=1, stroke=0)
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",20); c.drawRightString(W-30, y, str(dec))

    _draw_footer(c, 8); c.showPage()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE 9 — Hawala v2 Signal
# ═══════════════════════════════════════════════════════════════════════════
def draw_page9(c, data):
    c.setFillColor(C_WHITE); c.rect(0,0,W,H,fill=1,stroke=0)
    _draw_sidebar(c)

    sig = data.get("hawala_signal", {})
    macro = sig.get("macro_filters", {})
    gap   = sig.get("gap", {})
    trade = sig.get("trade_params", {})
    all_pass = macro.get("all_pass", False)
    signal_dir = gap.get("signal", "NO TRADE")
    trade_enabled = gap.get("trade_enabled", False) and all_pass

    # Header
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold", 28); c.drawString(30, H-50, "Hawala v2 — Trade Signal")
    c.setFillColor(C_GRAY); c.setFont("Helvetica", 9)
    c.drawRightString(W-30, H-30, f"Auto-generated {data.get('date_str','')}  07:30 AM IST")

    # ── Big signal box ──
    if trade_enabled:
        sig_col = C_GREEN if signal_dir == "LONG" else C_RED
        sig_bg  = colors.HexColor("#E8F8F0") if signal_dir == "LONG" else colors.HexColor("#FEE2E2")
    else:
        sig_col = C_GRAY
        sig_bg  = colors.HexColor("#F3F4F6")

    c.setFillColor(sig_bg)
    c.roundRect(30, H-150, W-60, 85, 8, fill=1, stroke=0)
    c.setStrokeColor(sig_col); c.setLineWidth(2)
    c.roundRect(30, H-150, W-60, 85, 8, fill=0, stroke=1)

    c.setFillColor(sig_col); c.setFont("Helvetica-Bold", 42)
    label = ("✓ " if trade_enabled else "✗ ") + signal_dir
    c.drawCentredString(W/2, H-110, label)
    c.setFillColor(C_DARK); c.setFont("Helvetica", 11)
    gap_pct = gap.get("gap_pct","—")
    gap_dir = gap.get("gap_direction","—")
    c.drawCentredString(W/2, H-130, f"GIFT Nifty gap: {gap_dir} {gap_pct}  |  BankNifty prev close: {gap.get('prev_close_banknifty','—')}")
    c.drawCentredString(W/2, H-144, trade.get("entry_type",""))

    y = H - 168

    # ── Macro Filter Checklist ──
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",14); c.drawString(30, y, "Macro Filter Status"); y -= 20
    filters = [
        ("India VIX",         macro.get("india_vix","—"),  macro.get("vix_threshold","<19"),  macro.get("vix_pass", False)),
        ("S&P 500 Overnight", f"{macro.get('sp500_overnight_pct','—')}%", f">{macro.get('sp500_threshold','-1.5')}%", macro.get("sp500_pass",False)),
        ("FII Net (Cash)",    f"₹{macro.get('fii_net_crore','—')} Cr",   f">₹{macro.get('fii_threshold','-3000')} Cr", macro.get("fii_pass",False)),
        ("Brent Crude",       f"${macro.get('brent_crude','—')}/bbl",    "Informational",        None),
        ("India VIX Trend",   macro.get("vix_trend","—"),               "Declining = better",   None),
    ]
    for label, val, threshold, passed in filters:
        if passed is True:
            icon, icol = "✓", C_GREEN
        elif passed is False:
            icon, icol = "✗", C_RED
        else:
            icon, icol = "◎", C_GRAY

        c.setFillColor(icol); c.setFont("Helvetica-Bold",14); c.drawString(30, y, icon)
        c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",10); c.drawString(52, y, label)
        c.setFillColor(C_GRAY); c.setFont("Helvetica",9)
        c.drawString(200, y, f"Value: {val}")
        c.drawString(340, y, f"Threshold: {threshold}")
        y -= 18
    y -= 8

    # ── Trade Parameters ──
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",14); c.drawString(30, y, "Trade Parameters"); y -= 20
    param_rows = [
        ("Instrument",        trade.get("instrument","BANKNIFTY")),
        ("Direction",         trade.get("direction","—")),
        ("Entry",             trade.get("entry_type","9:15 AM open or 9:20 AM 5-min candle")),
        ("Trailing Stop",     f"{trade.get('trailing_stop_pts','75')} pts"),
        ("Expected Target",   f"Gap fill: {gap.get('gap_fill_target','—')} pts"),
        ("Risk:Reward",       trade.get("risk_reward","—")),
        ("Position Size",     trade.get("position_size","1 lot")),
        ("Force Exit",        trade.get("force_exit","15:25 IST")),
    ]
    for label, val in param_rows:
        c.setFillColor(C_GRAY); c.setFont("Helvetica",9); c.drawString(30, y, label)
        c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",10); c.drawString(200, y, str(val))
        y -= 16
    y -= 10

    # ── Macro Context ──
    c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",14); c.drawString(30, y, "Macro Context Summary"); y -= 18
    ctx = sig.get("macro_context", {})
    ctx_rows = [
        ("Fear & Greed",    f"{ctx.get('fear_greed','—')} ({ctx.get('fear_greed_label','—')})"),
        ("Brent Crude",     f"${ctx.get('brent_crude','—')} ({ctx.get('crude_pct_change','—')}%)"),
        ("Gold",            f"${ctx.get('gold','—')}/oz"),
        ("USD/INR",         str(ctx.get("usd_inr","—"))),
        ("BTC",             f"${ctx.get('btc','—')}"),
        ("GIFT Nifty %",    f"{ctx.get('gift_nifty_pct','—')}%"),
        ("Nikkei",          f"{ctx.get('nikkei_pct','—')}%"),
        ("S&P 500",         f"{ctx.get('sp500_pct','—')}%"),
    ]
    half = (W-60)//2
    for i, (label, val) in enumerate(ctx_rows):
        col_x = 30 if i%2==0 else 30+half+10
        col_y = y - (i//2)*16
        c.setFillColor(C_GRAY); c.setFont("Helvetica",9); c.drawString(col_x, col_y, label)
        c.setFillColor(C_DARK); c.setFont("Helvetica-Bold",10); c.drawString(col_x+130, col_y, val)
    y -= ((len(ctx_rows)//2)+1)*16 + 10

    # ── Paper Trade Log Reminder ──
    c.setFillColor(colors.HexColor("#EEF2FF")); c.roundRect(30, y-36, W-60, 40, 6, fill=1, stroke=0)
    c.setStrokeColor(C_BLUE); c.setLineWidth(1); c.roundRect(30, y-36, W-60, 40, 6, fill=0, stroke=1)
    c.setFillColor(C_BLUE); c.setFont("Helvetica-Bold",10)
    c.drawString(44, y-8, "📝  After market close (3:30 PM) — run Cell 12 in Hawala v2 notebook to log today's paper trade result")
    c.setFillColor(C_GRAY); c.setFont("Helvetica",9)
    c.drawString(44, y-22, f"paper_trades.csv  |  Signal today: {signal_dir}  |  Gap: {gap_pct}")

    _draw_footer(c, 9); c.showPage()


# ── Shared helper: pivot table ───────────────────────────────────────────
def _draw_pivot_table(c, classic, fibo, x, y):
    headers  = ["Type", "R3", "R2", "R1", "PP", "S1", "S2", "S3"]
    col_w    = (W - x - 30) / len(headers)
    # Header row
    c.setFillColor(colors.HexColor("#F1F5F9"))
    c.rect(x, y, W-x-30, 14, fill=1, stroke=0)
    for i, h in enumerate(headers):
        c.setFillColor(C_GRAY); c.setFont("Helvetica-Bold",8)
        c.drawCentredString(x + i*col_w + col_w/2, y+3, h)
    y -= 16
    for row_label, row_data in [("Classic", classic), ("Fibonacci", fibo)]:
        keys = ["R3","R2","R1","PP","S1","S2","S3"]
        c.setFillColor(C_DARK); c.setFont("Helvetica",8)
        c.drawString(x+4, y+1, row_label)
        for i, k in enumerate(keys):
            val = str(row_data.get(k,"—"))
            col = C_RED if k.startswith("R") else (C_DARK if k=="PP" else C_GREEN)
            c.setFillColor(col); c.setFont("Helvetica-Bold",8)
            c.drawCentredString(x + (i+1)*col_w + col_w/2, y+1, val)
        c.setStrokeColor(colors.HexColor("#E5E7EB")); c.setLineWidth(0.3)
        c.line(x, y-3, W-30, y-3)
        y -= 16


# ═══════════════════════════════════════════════════════════════════════════
# MAIN — build PDF from data dict
# ═══════════════════════════════════════════════════════════════════════════
def build_pdf(data, output_path):
    c = canvas.Canvas(output_path, pagesize=A4)
    c.setTitle(f"Hawala v2 Pre-Market Report — {data.get('date_str','')}")
    draw_page1(c, data)
    draw_page2(c, data)
    draw_page3(c, data)
    draw_page4(c, data)
    draw_page5(c, data)
    draw_page6(c, data)
    draw_page7(c, data)
    draw_page8(c, data)
    draw_page9(c, data)
    c.save()
    print(f"PDF saved: {output_path}")


# ═══════════════════════════════════════════════════════════════════════════
# SAMPLE DATA — April 13, 2026 (from premarketpulse reference)
# ═══════════════════════════════════════════════════════════════════════════
SAMPLE_DATA_APR13 = {
    "date_str": "Mon, 13 Apr",
    "year_str": "2026",
    "overview_time": "7:08 AM",
    "commodity_time": "7:00 AM",

    # Fear & Greed
    "fear_greed_val": 54.92,
    "fear_greed_label": "Greed \U0001f60f",
    "fear_greed_prev": "26.21",
    "quote": '"Don\'t look for the needle in the haystack. Just buy the haystack!"',
    "quote_author": "@ John Bogle",

    # GIFT Nifty
    "gift_nifty": {"val": "23,778.0", "chg": "-313.5", "pct": "-1.30%"},

    # Asian Markets
    "asian_markets": [
        {"name": "Hang Seng",   "region": "Hong Kong",   "val": "25,623.76", "chg": "-269.78", "pct": "-1.04%"},
        {"name": "Nikkei 225",  "region": "Japan",        "val": "56,528.58", "chg": "-395.53", "pct": "-0.69%"},
        {"name": "KOSPI",       "region": "South Korea",  "val": "5,809.85",  "chg": "-49.02",  "pct": "-0.84%"},
        {"name": "ASX 200",     "region": "Australia",    "val": "8,927.9",   "chg": "-32.7",   "pct": "-0.36%"},
    ],

    # Europe Markets
    "europe_markets": [
        {"name": "DAX",     "region": "Germany",        "val": "23,803.95", "chg": "-3.04",  "pct": "-0.01%"},
        {"name": "FTSE 100","region": "United Kingdom", "val": "10,600.53", "chg": "-2.95",  "pct": "-0.03%"},
        {"name": "CAC 40",  "region": "France",         "val": "8,259.6",   "chg": "+13.81", "pct": "+0.17%"},
    ],

    # US Markets
    "us_markets": [
        {"name": "Dow Jones", "region": "", "val": "47,916.57", "chg": "-269.23", "pct": "-0.56%"},
        {"name": "Nasdaq",    "region": "", "val": "22,902.9",  "chg": "+80.48",  "pct": "+0.35%"},
        {"name": "S&P",       "region": "", "val": "6,816.89",  "chg": "-7.77",   "pct": "-0.11%"},
        {"name": "VIX",       "region": "", "val": "19.23",     "chg": "-0.26",   "pct": "-1.33%"},
    ],

    # India Markets (prev close)
    "india_markets": [
        {"name": "Sensex",    "region": "", "val": "77,550.25", "chg": "+918.60",  "pct": "+1.20%"},
        {"name": "Nifty 50",  "region": "", "val": "24,050.6",  "chg": "+275.50",  "pct": "+1.16%"},
        {"name": "Bank Nifty","region": "", "val": "55,912.75", "chg": "+1,091.05","pct": "+1.99%"},
        {"name": "India VIX", "region": "", "val": "18.85",     "chg": "-1.58",    "pct": "-7.72%"},
    ],

    # Commodities global spot
    "commodities_spot": [
        {"name": "Gold",           "val": "4,717.03", "chg": "-32.66", "pct": "-0.69%"},
        {"name": "Silver",         "val": "74.38",    "chg": "-1.55",  "pct": "-2.04%"},
        {"name": "Brent Crude Oil","val": "101.68",   "chg": "+7.43",  "pct": "+7.88%"},
    ],

    # MCX Futures
    "mcx_futures": [
        {"name": "Gold",      "val": "1,52,690", "chg": "-", "pct": "+0.02%"},
        {"name": "Silver",    "val": "2,43,300", "chg": "-", "pct": "+0.01%"},
        {"name": "Crude Oil", "val": "9,122",    "chg": "-", "pct": "-0.34%"},
    ],

    # Crypto
    "crypto": [
        {"name": "Bitcoin",  "val": "71,112.00", "chg": "+356.65", "pct": "+0.50%"},
        {"name": "Ethereum", "val": "2,203.49",  "chg": "+11.69",  "pct": "+0.53%"},
        {"name": "Solana",   "val": "82.08",     "chg": "+0.54",   "pct": "+0.66%"},
    ],

    # Currencies
    "currencies": [
        {"name": "USD/INR", "val": "93.05",  "chg": "+0.00", "pct": "+0.00%"},
        {"name": "EUR/USD", "val": "1.17",   "chg": "-0.00", "pct": "-0.33%"},
        {"name": "USD/JPY", "val": "159.74", "chg": "+0.50", "pct": "+0.31%"},
    ],

    # Snapshot
    "top_gainers": [
        {"name": "NIACL",              "pct": "+19.83%"},
        {"name": "Cohance Lifesciences","pct": "+19.27%"},
        {"name": "Ola Electric",        "pct": "+12.56%"},
        {"name": "Blue Jet Healthcare", "pct": "+9.99%"},
        {"name": "Allied Blenders",     "pct": "+9.18%"},
    ],
    "top_losers": [
        {"name": "Coal India",         "pct": "-4.40%"},
        {"name": "Sun Pharmaceutical", "pct": "-3.62%"},
        {"name": "Coforge",            "pct": "-3.21%"},
        {"name": "Data Patterns",      "pct": "-2.96%"},
        {"name": "Infosys",            "pct": "-2.94%"},
    ],
    "volume_shockers": [
        {"name": "Ola Electric",       "val": "40.88"},
        {"name": "NIACL",              "val": "155.71"},
        {"name": "Billionbrains Garage","val": "193.70"},
        {"name": "HFCL",               "val": "84.40"},
        {"name": "Wipro",              "val": "204.88"},
    ],
    "week52_highs": [
        {"name": "Natco Pharma",        "pct": "-1.74"},
        {"name": "Billionbrains Garage","pct": "+3.77"},
        {"name": "BSE",                 "pct": "+0.73"},
        {"name": "Multi Commodity Exch.","pct": "+0.45"},
        {"name": "Ather Energy",        "pct": "+5.22"},
    ],
    "long_buildup": [
        {"name": "Godfrey Phillips",   "pct": "64.19%"},
        {"name": "Motilal Oswal Fin.", "pct": "26.63%"},
        {"name": "Nippon Life India",  "pct": "17.15%"},
        {"name": "Cochin Shipyard",    "pct": "12.15%"},
        {"name": "Sona BLW Precision", "pct": "7.86%"},
    ],
    "short_buildup": [
        {"name": "Oil India Ltd",       "pct": "-3.84%"},
        {"name": "Persistent Systems",  "pct": "-1.91%"},
        {"name": "HCL Technologies",    "pct": "-1.82%"},
        {"name": "Avenue Supermarts",   "pct": "-1.40%"},
        {"name": "Petronet LNG",        "pct": "-1.28%"},
    ],

    # Sectoral 1D
    "sectoral_1d": [
        {"name": "Nifty Auto",              "pct": "+2.85%"},
        {"name": "Nifty Realty",            "pct": "+2.08%"},
        {"name": "Nifty Financial Services","pct": "+2.06%"},
        {"name": "Nifty PSU Bank",          "pct": "+2.01%"},
        {"name": "Nifty Bank",              "pct": "+1.99%"},
        {"name": "Nifty Private Bank",      "pct": "+1.98%"},
        {"name": "Nifty Media",             "pct": "+1.96%"},
        {"name": "Nifty India Consumption", "pct": "+1.55%"},
        {"name": "Nifty India Defence",     "pct": "+1.50%"},
        {"name": "Nifty Infrastructure",    "pct": "+1.31%"},
        {"name": "Nifty FMCG",             "pct": "+1.16%"},
        {"name": "Nifty Services Sector",  "pct": "+1.16%"},
        {"name": "Nifty Energy",            "pct": "+1.11%"},
        {"name": "Nifty Metal",             "pct": "+1.04%"},
        {"name": "Nifty Oil & Gas",         "pct": "+0.91%"},
        {"name": "Nifty Pharma",            "pct": "+0.13%"},
        {"name": "Nifty IT",                "pct": "-1.91%"},
    ],

    # ── FII / DII ──────────────────────────────────────────────────────────
    "fii_dii": {
        "fii_net_crore":   "-1,246",
        "fii_buy_crore":   "8,432",
        "fii_sell_crore":  "9,678",
        "dii_net_crore":   "+2,108",
        "dii_buy_crore":   "6,540",
        "dii_sell_crore":  "4,432",
        "fii_fno_long":    "1,82,340",
        "fii_fno_short":   "2,14,560",
        "fii_fno_ls_ratio":"0.85",
        "fii_fno_net":     "Short-heavy",
    },

    # ── Market Bulletin ─────────────────────────────────────────────────────
    "market_bulletin": [
        "GIFT Nifty fell 1.30% to 23,778, indicating early weakness for the Nifty 50.",
        "NSE Nifty 50 and BSE Sensex surged nearly 6% for the week — best since February 2021 — with both indices gaining 1.2% on Friday.",
        "Donald Trump announced a U.S. naval blockade on Iranian-linked ships in the Strait of Hormuz after failed ceasefire talks.",
        "Defence stocks surged up to 16%, with the Nifty India Defence Index rising 1.4% as most constituents traded in the green.",
        "Israel put forces on high alert for possible conflict with Iran after ceasefire talks collapsed.",
        "The S&P 500 slipped 0.11% to 6,816.89 on Friday but logged its best week since November, while the Nasdaq Composite rose 0.35% to 22,902.89.",
        "Foreign banks are reclassifying trades as hedges to bypass the Reserve Bank of India's $100 million limit, drawing scrutiny.",
        "Oil prices surged 8.6%, with Brent crude at $103.16 and WTI at $104.83 per barrel amid tensions around the Strait of Hormuz.",
        "The rupee erased early gains to settle 17 paise lower at 92.68 against the US dollar on Friday.",
        "Tata Capital Ltd. shares will be in focus as over ₹90,000 crore worth of stock becomes tradable following the end of the six-month lock-in period after its 2025 IPO.",
        "FIIs sold Indian equities for 27 straight sessions, offloading ₹1.6 lakh crore amid geopolitical tensions.",
        "Gold fell 0.63% on mounting inflation concerns after US-Iran talks failed.",
        "Inventurus Knowledge Solutions (backed by Rakesh Jhunjhunwala family) in advanced talks to acquire TruBridge for ~$600 million.",
    ],

    # ── Nifty 50 Analysis ────────────────────────────────────────────────────
    "nifty_analysis": {
        "prev_close": "24,050.6",
        "prev_pct":   "+1.16%",
        "option_chain": {
            "max_pain":             "23,994",
            "call_resistance":      "24,500 (1.11 Cr OI)",
            "put_support":          "24,000 (61.65 L OI)",
            "call_writing_strikes": "24,500 / 24,700 / 24,600",
            "put_writing_strikes":  "24,000 / 23,900 / 23,950",
            "pcr":                  "1.18",
        },
        "pcr_weekly_trend": [
            {"day": "Mon Apr 6",  "pcr": 1.43},
            {"day": "Tue Apr 7",  "pcr": 1.25},
            {"day": "Wed Apr 8",  "pcr": 1.20},
            {"day": "Thu Apr 9",  "pcr": 0.92},
            {"day": "Fri Apr 10", "pcr": 1.18},
        ],
        "pivot_classic":   {"R3":24349,"R2":24211,"R1":24131,"PP":23994,"S1":23913,"S2":23776,"S3":23696},
        "pivot_fibonacci": {"R3":24211,"R2":24128,"R1":24077,"PP":23994,"S1":23911,"S2":23859,"S3":23776},
        "advance_decline": {"advance": 43, "decline": 8},
    },

    # ── Bank Nifty Analysis ──────────────────────────────────────────────────
    "banknifty_analysis": {
        "prev_close": "55,912.75",
        "prev_pct":   "+1.99%",
        "option_chain": {
            "max_pain":             "55,679",
            "call_resistance":      "57,000 (6.26 L OI)",
            "put_support":          "55,000 (6.8 L OI)",
            "call_writing_strikes": "57,500 / 55,800 / 55,900",
            "put_writing_strikes":  "56,000 / 55,000 / 55,900",
            "call_unwinding":       "55,000 strike (1.6 L)",
            "put_unwinding":        "54,800 strike (17,670)",
            "pcr":                  "1.15",
        },
        "atm_greeks": {
            "atm_strike":  "55,900",
            "expiry":      "Apr 16, 2026",
            "call_iv":     "14.2",
            "put_iv":      "14.5",
            "call_delta":  "0.502",
            "put_delta":   "-0.498",
            "gamma":       "0.00018",
            "call_theta":  "-45.2",
            "put_theta":   "-44.8",
            "vega":        "28.4",
        },
        "oi_strikes": [
            {"strike": 57000, "call_oi": 626000, "put_oi": 120000, "is_atm": False},
            {"strike": 56500, "call_oi": 210000, "put_oi": 180000, "is_atm": False},
            {"strike": 56000, "call_oi": 320000, "put_oi": 280000, "is_atm": False},
            {"strike": 55900, "call_oi": 185000, "put_oi": 174000, "is_atm": True},
            {"strike": 55500, "call_oi": 145000, "put_oi": 310000, "is_atm": False},
            {"strike": 55000, "call_oi": 180000, "put_oi": 680000, "is_atm": False},
            {"strike": 54800, "call_oi": 90000,  "put_oi": 200000, "is_atm": False},
        ],
        "pcr_weekly_trend": [
            {"day": "Mon Apr 6",  "pcr": 1.38},
            {"day": "Tue Apr 7",  "pcr": 1.22},
            {"day": "Wed Apr 8",  "pcr": 1.18},
            {"day": "Thu Apr 9",  "pcr": 0.96},
            {"day": "Fri Apr 10", "pcr": 1.15},
        ],
        "pivot_classic":   {"R3":57046,"R2":56512,"R1":56212,"PP":55679,"S1":55379,"S2":54846,"S3":54546},
        "pivot_fibonacci": {"R3":56512,"R2":56194,"R1":55997,"PP":55679,"S1":55361,"S2":55164,"S3":54846},
        "advance_decline": {"advance": 14, "decline": 0},
    },

    # ── Hawala v2 Signal ─────────────────────────────────────────────────────
    "hawala_signal": {
        "macro_filters": {
            "india_vix":           18.85,
            "vix_threshold":       19.0,
            "vix_pass":            True,
            "vix_trend":           "Declining (20.4 → 19.6 → 18.85)",
            "sp500_overnight_pct": -0.11,
            "sp500_threshold":     -1.5,
            "sp500_pass":          True,
            "fii_net_crore":       -1246,
            "fii_threshold":       -3000,
            "fii_pass":            True,
            "brent_crude":         101.68,
            "all_pass":            True,
        },
        "gap": {
            "prev_close_banknifty": "55,912.75",
            "gift_nifty_pct":       -1.30,
            "gap_direction":        "DOWN",
            "gap_pct":              "-1.30%",
            "gap_fill_target":      "200-400",
            "signal":               "LONG",
            "trade_enabled":        True,
        },
        "trade_params": {
            "instrument":      "BANKNIFTY",
            "direction":       "LONG (fade the gap-down)",
            "entry_type":      "After 9:20 AM 5-min candle close confirms gap-down",
            "trailing_stop_pts": 75,
            "risk_reward":     "1:3 to 1:5 (gap fill)",
            "position_size":   "1 lot",
            "force_exit":      "15:25 IST",
        },
        "macro_context": {
            "fear_greed":       54.92,
            "fear_greed_label": "Greed",
            "brent_crude":      101.68,
            "crude_pct_change": 7.88,
            "gold":             4717.03,
            "usd_inr":          93.05,
            "btc":              71112,
            "gift_nifty_pct":   -1.30,
            "nikkei_pct":       -0.69,
            "sp500_pct":        -0.11,
        },
    },

    # Sectoral 7D
    "sectoral_7d": [
        {"name": "Nifty Realty",            "pct": "+12.97%"},
        {"name": "Nifty Auto",              "pct": "+10.59%"},
        {"name": "Nifty India Defence",     "pct": "+9.20%"},
        {"name": "Nifty Financial Services","pct": "+9.04%"},
        {"name": "Nifty Private Bank",      "pct": "+8.57%"},
        {"name": "Nifty Bank",              "pct": "+8.47%"},
        {"name": "Nifty PSU Bank",          "pct": "+7.92%"},
        {"name": "Nifty Metal",             "pct": "+7.85%"},
        {"name": "Nifty India Consumption", "pct": "+6.57%"},
        {"name": "Nifty Services Sector",   "pct": "+6.48%"},
        {"name": "Nifty Infrastructure",    "pct": "+5.64%"},
        {"name": "Nifty Energy",            "pct": "+5.31%"},
        {"name": "Nifty Media",             "pct": "+4.75%"},
        {"name": "Nifty FMCG",             "pct": "+4.24%"},
        {"name": "Nifty Oil & Gas",         "pct": "+3.24%"},
        {"name": "Nifty IT",                "pct": "+1.94%"},
        {"name": "Nifty Pharma",            "pct": "+1.63%"},
    ],
}

if __name__ == "__main__":
    import json as _json
    # Usage:
    #   python gen_report.py output.pdf                        <- uses SAMPLE_DATA
    #   python gen_report.py --data data.json output.pdf      <- uses JSON file
    if len(sys.argv) >= 3 and sys.argv[1] == "--data":
        with open(sys.argv[2]) as f:
            data = _json.load(f)
        out = sys.argv[3] if len(sys.argv) > 3 else "/tmp/market_report.pdf"
    else:
        data = SAMPLE_DATA_APR13
        out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/market_report.pdf"
    build_pdf(data, out)
