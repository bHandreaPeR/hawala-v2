"""
gen_report.py — Hawala v2 Pre-Market PDF  (dark theme, matches market_report HTML)

3-page A4 PDF:
  Page 1: Header · Alert · Quick Snapshot · India/Global Markets · Commodities/Currency
  Page 2: Sentiment · Hawala Signal · Expected Scenario · Key News · Events Calendar
  Page 3: BankNifty Deep Dive · FII/DII · Nifty Deep Dive

Usage:
    from gen_report import build_pdf
    build_pdf(data_dict, "market_report_2026-04-20.pdf")
"""

import os
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas as rl_canvas

# ── Register Arial Unicode for ₹ and other symbols ────────────────────────
_FONT_PATHS = [
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    "/Library/Fonts/Arial Unicode MS.ttf",
]
_BOLD_PATHS = [
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial.ttf",
]

_UNI   = "ArialUni"
_UNIB  = "ArialUni-Bold"
_REG   = "Helvetica"
_BOLD  = "Helvetica-Bold"

for _fp in _FONT_PATHS:
    if os.path.exists(_fp):
        try:
            pdfmetrics.registerFont(TTFont(_UNI, _fp))
            _REG = _UNI
        except Exception:
            pass
        break

for _fp in _BOLD_PATHS:
    if os.path.exists(_fp):
        try:
            pdfmetrics.registerFont(TTFont(_UNIB, _fp))
            _BOLD = _UNIB
        except Exception:
            pass
        break

RUPEE = "\u20b9" if _REG == _UNI else "Rs."

# ── Palette ────────────────────────────────────────────────────────────────
C_BG    = colors.HexColor("#0d0f14")
C_SURF  = colors.HexColor("#141720")
C_SURF2 = colors.HexColor("#1c2030")
C_BORD  = colors.HexColor("#2a2f42")
C_ACC   = colors.HexColor("#6366f1")
C_GREEN = colors.HexColor("#22c55e")
C_RED   = colors.HexColor("#ef4444")
C_YELL  = colors.HexColor("#f59e0b")
C_BLUE  = colors.HexColor("#3b82f6")
C_PURP  = colors.HexColor("#a855f7")
C_TEXT  = colors.HexColor("#e4e8f0")
C_MUTED = colors.HexColor("#8890a8")
C_WHITE = colors.white

W, H = A4

# ── Layout constants ───────────────────────────────────────────────────────
ML      = 28          # left margin
MR      = 28          # right margin
CW      = W - ML - MR  # content width ≈ 539
HDR_H   = 56          # header height
FOOT_H  = 26          # footer height
# container: y from (H - HDR_H - 12) down to FOOT_H + 6
C_TOP   = H - HDR_H - 14
C_BOT   = FOOT_H + 8

COL2_W  = (CW - 12) / 2   # ~263.5
COL2_G  = 12
COL3_W  = (CW - 16) / 3   # ~174.3
COL3_G  = 8
COL4_W  = (CW - 9) / 4    # ~132.5
COL4_G  = 3

ROW_H   = 13          # table body row height
HDR_ROW = 18          # table header section height
CARD_PX = 12          # card horizontal inner padding
CARD_PY = 10          # card vertical inner padding
SEC_H   = 14          # section title height
ELEM_G  = 12          # gap between elements


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _chg_color(val):
    try:
        v = float(str(val).replace("%", "").replace("+", ""))
        return C_GREEN if v >= 0 else C_RED
    except:
        return C_MUTED


def _fmt_num(val, dec=2, comma=True):
    try:
        v = float(val)
        if comma:
            return f"{v:,.{dec}f}"
        return f"{v:.{dec}f}"
    except:
        return str(val)


def _fmt_pct(val):
    try:
        v = float(str(val).replace("%", "").replace("+", ""))
        return f"+{v:.2f}%" if v >= 0 else f"{v:.2f}%"
    except:
        return str(val)


def _sign_pct(val):
    try:
        v = float(str(val).replace("%", "").replace("+", ""))
        return f"+{v:.2f}%" if v >= 0 else f"{v:.2f}%"
    except:
        return str(val)


def _wrap(c, text, font, size, max_w):
    """Split text into lines fitting within max_w."""
    words = str(text).split()
    lines, cur = [], ""
    for w in words:
        t = (cur + " " + w).strip()
        if c.stringWidth(t, font, size) <= max_w:
            cur = t
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines or [""]


# ─────────────────────────────────────────────────────────────────────────────
# Drawing primitives
# ─────────────────────────────────────────────────────────────────────────────

def _bg(c):
    c.setFillColor(C_BG)
    c.rect(0, 0, W, H, fill=1, stroke=0)


def _card(c, x, top_y, w, h, accent=None):
    """Card with top at top_y, bottom at top_y-h."""
    bot = top_y - h
    c.setFillColor(C_SURF)
    c.setStrokeColor(C_BORD)
    c.setLineWidth(0.5)
    c.roundRect(x, bot, w, h, 5, fill=1, stroke=1)
    if accent:
        c.setFillColor(accent)
        c.setStrokeColor(accent)
        c.roundRect(x, top_y - 3, w, 3, 2, fill=1, stroke=0)


def _sec_title(c, x, y, text):
    """Accent-bar section heading. Returns new y after title."""
    c.setFillColor(C_ACC)
    c.rect(x, y - 9, 3, 10, fill=1, stroke=0)
    c.setFillColor(C_MUTED)
    c.setFont(_BOLD, 7.5)
    c.drawString(x + 7, y - 8, text.upper())
    return y - SEC_H


def _tbl_header(c, x, y, cols, widths, aligns=None):
    """Draw table header row. Returns new y."""
    c.setFillColor(C_MUTED)
    c.setFont(_BOLD, 6.5)
    cx = x
    for i, (col, w) in enumerate(zip(cols, widths)):
        align = (aligns[i] if aligns else "L")
        if align == "R":
            c.drawRightString(cx + w - 2, y, col.upper())
        else:
            c.drawString(cx + 2, y, col.upper())
        cx += w
    c.setStrokeColor(C_BORD)
    c.setLineWidth(0.4)
    c.line(x, y - 4, x + sum(widths), y - 4)
    return y - HDR_ROW


def _tbl_row(c, x, y, cells, widths, colors_=None, bold_=None, aligns=None):
    """Draw one table row. Returns new y."""
    cx = x
    for i, (cell, w) in enumerate(zip(cells, widths)):
        col  = colors_[i] if colors_ else C_TEXT
        bold = bold_[i]   if bold_   else False
        aln  = aligns[i]  if aligns  else "L"
        c.setFillColor(col)
        c.setFont(_BOLD if bold else _REG, 8.5)
        txt = str(cell)
        if aln == "R":
            c.drawRightString(cx + w - 2, y, txt)
        else:
            c.drawString(cx + 2, y, txt)
        cx += w
    c.setStrokeColor(C_BORD)
    c.setLineWidth(0.3)
    c.line(x, y - 4, x + sum(widths), y - 4)
    return y - ROW_H


def _alert_box(c, x, top_y, w, title, body_lines, kind="warning"):
    """Draw alert box (warning=yellow, error=red, info=blue). Returns new y."""
    if kind == "error":
        bg  = colors.HexColor("#ef444412")
        brd = colors.HexColor("#ef444460")
        tc  = C_RED
    elif kind == "info":
        bg  = colors.HexColor("#3b82f612")
        brd = colors.HexColor("#3b82f660")
        tc  = C_BLUE
    else:   # warning
        bg  = colors.HexColor("#f59e0b12")
        brd = colors.HexColor("#f59e0b60")
        tc  = C_YELL

    line_h = 11
    h = CARD_PY + 14 + 4 + len(body_lines) * line_h + CARD_PY
    bot = top_y - h

    c.setFillColor(bg)
    c.setStrokeColor(brd)
    c.setLineWidth(0.8)
    c.roundRect(x, bot, w, h, 5, fill=1, stroke=1)
    # left accent bar
    c.setFillColor(tc)
    c.rect(x, bot, 3, h, fill=1, stroke=0)

    # title
    c.setFillColor(tc)
    c.setFont(_BOLD, 8)
    c.drawString(x + CARD_PX + 4, top_y - CARD_PY - 8, title)
    # body
    c.setFillColor(C_TEXT)
    c.setFont(_REG, 8)
    by = top_y - CARD_PY - 8 - 14
    for line in body_lines:
        c.drawString(x + CARD_PX + 4, by, line)
        by -= line_h
    return top_y - h


def _gauge_bar(c, x, y, w, score):
    """Draw fear & greed gradient bar with needle."""
    steps = 30
    sw = w / steps
    for i in range(steps):
        t = i / steps
        r = int(239 * (1-t) + 34 * t)
        g = int(68  * (1-t) + 197 * t)
        b = int(68  * (1-t) + 94 * t)
        c.setFillColor(colors.Color(r/255, g/255, b/255))
        c.rect(x + i*sw, y, sw + 0.3, 6, fill=1, stroke=0)
    # gauge labels
    c.setFillColor(C_MUTED)
    c.setFont(_REG, 6)
    c.drawString(x, y - 8, "Extreme Fear")
    c.drawCentredString(x + w/2, y - 8, "Neutral")
    c.drawRightString(x + w, y - 8, "Extreme Greed")
    # needle
    try:
        nx = x + w * (float(score) / 100)
        c.setFillColor(C_WHITE)
        c.setStrokeColor(C_BG)
        c.setLineWidth(1.2)
        c.circle(nx, y + 3, 4, fill=1, stroke=1)
    except:
        pass


def _footer(c, page_num):
    c.setStrokeColor(C_BORD)
    c.setLineWidth(0.5)
    c.line(ML, FOOT_H - 2, W - MR, FOOT_H - 2)
    c.setFillColor(C_ACC)
    c.setFont(_BOLD, 7.5)
    c.drawString(ML, FOOT_H - 14, "Hawala v2")
    c.setFillColor(C_MUTED)
    c.setFont(_REG, 7)
    c.drawString(ML + 52, FOOT_H - 14, "— BankNifty Gap Fill Strategy · Not financial advice")
    c.drawCentredString(W/2, FOOT_H - 14, "Data: NSE · Yahoo Finance · CoinGecko · CNN")
    c.drawRightString(W - MR, FOOT_H - 14, f"Page {page_num} / 3")


# ─────────────────────────────────────────────────────────────────────────────
# Page 1 — Header + Alert + Snapshot + Markets + Commodity/Currency
# ─────────────────────────────────────────────────────────────────────────────

def _draw_header(c, data):
    # dark gradient header band
    c.setFillColor(colors.HexColor("#1a1d2e"))
    c.rect(0, H - HDR_H, W, HDR_H, fill=1, stroke=0)
    c.setStrokeColor(C_BORD)
    c.setLineWidth(0.5)
    c.line(0, H - HDR_H, W, H - HDR_H)

    # title
    c.setFillColor(C_ACC)
    c.setFont(_BOLD, 17)
    c.drawString(ML, H - 26, "Hawala")
    c.setFillColor(C_WHITE)
    c.setFont(_BOLD, 17)
    c.drawString(ML + 67, H - 26, "v2 — Pre-Market Intelligence")
    c.setFillColor(C_MUTED)
    c.setFont(_REG, 8)
    c.drawString(ML, H - 42, "BankNifty Gap Fill Strategy  ·  Automated Daily Briefing")

    # right side date badge + gen time
    date_str = str(data.get("date_str", ""))
    gen_at   = str(data.get("generated_at", ""))
    bw = c.stringWidth(date_str, _BOLD, 8) + 22
    bx = W - MR - bw
    c.setFillColor(C_ACC)
    c.roundRect(bx, H - 36, bw, 18, 9, fill=1, stroke=0)
    c.setFillColor(C_WHITE)
    c.setFont(_BOLD, 8)
    c.drawCentredString(bx + bw/2, H - 25, date_str)
    c.setFillColor(C_MUTED)
    c.setFont(_REG, 7)
    c.drawRightString(W - MR, H - 46, f"Generated at {gen_at}  ·  Data cutoff {gen_at}")


def _draw_snap_cards(c, y, data):
    """Draw 4 quick-snapshot cards: BankNifty, Nifty, India VIX, S&P500."""
    bn     = data.get("banknifty_analysis", {})
    nf_row = next((r for r in data.get("india_markets", []) if "Nifty 50" in r.get("name","")), {})
    vix_row= data.get("india_vix", {})
    sp_row = next((r for r in data.get("us_markets", []) if "S&P" in r.get("name","")), {})

    bn_close = bn.get("prev_close", "—")
    gap_pts  = bn.get("gap_pts", "—")

    try:
        gap_f = float(gap_pts)
        gap_str = f"+{gap_f:,.0f} pts" if gap_f >= 0 else f"{gap_f:,.0f} pts"
        gap_sub = f"Gap {'up' if gap_f >= 0 else 'down'} estimate"
        bn_accent = C_GREEN if gap_f >= 0 else C_RED
    except:
        gap_str = "—"
        gap_sub = "Gap estimate"
        bn_accent = C_YELL

    try:
        vix_f = float(vix_row.get("price", 99))
        vix_accent = C_GREEN if vix_f < 19 else C_RED
        vix_sub = "Below 19 — Trades ALLOWED" if vix_f < 19 else "Above 19 — Caution"
    except:
        vix_accent = C_YELL
        vix_sub = "—"

    sp_chg = sp_row.get("chg_pct", "—")
    try:
        sp_f = float(str(sp_chg).replace("%","").replace("+",""))
        sp_accent = C_GREEN if sp_f >= 0 else C_RED
    except:
        sp_accent = C_YELL

    cards = [
        {
            "label":  "Bank Nifty (Prev Close)",
            "value":  f"{bn_close:,.0f}" if bn_close != "—" else "—",
            "change": gap_str,
            "sub":    gap_sub,
            "accent": bn_accent,
            "chg_col": C_GREEN if bn_accent == C_GREEN else C_RED,
        },
        {
            "label":  "Nifty 50 (Prev Close)",
            "value":  f"{float(nf_row['price']):,.2f}" if nf_row.get("price") not in ("—", None) else "—",
            "change": _sign_pct(nf_row.get("chg_pct","—")),
            "sub":    (f"Chg {'+' if float(nf_row['chg_pts'])>=0 else ''}{float(nf_row['chg_pts']):,.1f} pts"
                       if nf_row.get("chg_pts") not in ("—", None) else "—"),
            "accent": _chg_color(nf_row.get("chg_pct","—")),
            "chg_col": _chg_color(nf_row.get("chg_pct","—")),
        },
        {
            "label":  "India VIX (Prev Close)",
            "value":  f"{vix_row.get('price','—')}",
            "change": _sign_pct(vix_row.get("chg_pct","—")),
            "sub":    vix_sub,
            "accent": vix_accent,
            "chg_col": _chg_color(vix_row.get("chg_pct","—")),
        },
        {
            "label":  "S&P 500 (US, prev session)",
            "value":  f"{sp_row.get('price','—'):,.2f}" if sp_row.get("price") not in ("—", None) else "—",
            "change": _sign_pct(sp_chg),
            "sub":    "Overnight move",
            "accent": sp_accent,
            "chg_col": sp_accent,
        },
    ]

    ch = 72
    for i, card in enumerate(cards):
        cx = ML + i * (COL4_W + COL4_G)
        _card(c, cx, y, COL4_W, ch, accent=card["accent"])
        c.setFillColor(C_MUTED)
        c.setFont(_BOLD, 6.5)
        c.drawString(cx + CARD_PX, y - CARD_PY - 6, card["label"].upper())
        c.setFillColor(C_WHITE)
        c.setFont(_BOLD, 15)
        c.drawString(cx + CARD_PX, y - CARD_PY - 24, str(card["value"])[:13])
        c.setFillColor(card["chg_col"])
        c.setFont(_BOLD, 8.5)
        c.drawString(cx + CARD_PX, y - CARD_PY - 38, str(card["change"])[:22])
        c.setFillColor(C_MUTED)
        c.setFont(_REG, 7)
        c.drawString(cx + CARD_PX, y - CARD_PY - 51, str(card["sub"])[:26])

    return y - ch


def _draw_markets_2col(c, y, data):
    """India Indices (left) + Global Markets (right). Returns new y."""
    prev_label = data.get("prev_day_label", "Prev")
    # section title spanning full width
    _sec_title(c, ML, y, f"India Indices — Previous Close ({prev_label})")
    _sec_title(c, ML + COL2_W + COL2_G, y, "Global Markets")
    y -= SEC_H + 2

    india  = data.get("india_markets", [])[:7]
    global_mkts = (data.get("us_markets", []) +
                   data.get("asian_markets", []) +
                   data.get("europe_markets", []))[:11]

    india_rows  = len(india)
    global_rows = len(global_mkts)
    max_rows    = max(india_rows, global_rows)
    card_h      = HDR_ROW + max_rows * ROW_H + 2 * CARD_PY

    lx = ML
    rx = ML + COL2_W + COL2_G

    _card(c, lx, y, COL2_W, card_h)
    _card(c, rx, y, COL2_W, card_h)

    # India table
    iy = y - CARD_PY
    iy = _tbl_header(c, lx + CARD_PX, iy, ["Index","Close","Chg","Chg %"],
                     [100, 58, 44, 44])
    for row in india:
        price = row.get("price", "—")
        chg   = row.get("chg_pts", "—")
        chgp  = row.get("chg_pct", "—")
        cc    = _chg_color(chgp)
        iy = _tbl_row(c, lx + CARD_PX, iy,
                      [row["name"],
                       _fmt_num(price, 2) if price != "—" else "—",
                       (_sign_pct(chg).replace("%","")) if chg != "—" else "—",
                       _sign_pct(chgp)],
                      [100, 58, 44, 44],
                      colors_=[C_TEXT, C_TEXT, cc, cc],
                      bold_=[True, False, True, True],
                      aligns=["L","R","R","R"])

    # Global table
    gy = y - CARD_PY
    gy = _tbl_header(c, rx + CARD_PX, gy, ["Market","Level","Chg %"],
                     [110, 72, 56])
    for row in global_mkts:
        price = row.get("price", "—")
        chgp  = row.get("chg_pct", "—")
        cc    = _chg_color(chgp)
        gy = _tbl_row(c, rx + CARD_PX, gy,
                      [row["name"],
                       _fmt_num(price, 2) if price != "—" else "—",
                       _sign_pct(chgp)],
                      [110, 72, 56],
                      colors_=[C_TEXT, C_TEXT, cc],
                      bold_=[False, False, True],
                      aligns=["L","R","R"])

    return y - card_h


def _draw_commodity_currency(c, y, data):
    """Commodities (left) + Currency & Crypto (right). Returns new y."""
    commod  = data.get("commodities_spot", [])
    curr    = data.get("currencies", [])
    crypto  = data.get("crypto", [])

    # Combine currency + crypto for right column
    right_rows = list(curr)
    for cr in crypto:
        sym   = cr.get("symbol","?")
        price = cr.get("price_usd", "—")
        chg24 = cr.get("chg_pct_24h","—")
        right_rows.append({
            "pair": f"{sym} / USD (24h)",
            "rate": f"${price:,.0f}" if price != "—" else "—",
            "chg_pct": chg24,
        })

    max_rows = max(len(commod), len(right_rows))
    card_h   = HDR_ROW + max_rows * ROW_H + 2 * CARD_PY

    lx = ML
    rx = ML + COL2_W + COL2_G

    _sec_title(c, lx, y, "Commodities (Pre-Market)")
    _sec_title(c, rx, y, "Currency & Crypto (Pre-Market)")
    y -= SEC_H + 2

    _card(c, lx, y, COL2_W, card_h)
    _card(c, rx, y, COL2_W, card_h)

    # Commodity table
    cy2 = y - CARD_PY
    cy2 = _tbl_header(c, lx + CARD_PX, cy2, ["Commodity","Price","Chg %"],
                      [100, 90, 48])
    for row in commod:
        chgp = row.get("chg_pct","—")
        cc   = _chg_color(chgp)
        cy2 = _tbl_row(c, lx + CARD_PX, cy2,
                       [row.get("name",""), row.get("price","—"), _sign_pct(chgp)],
                       [100, 90, 48],
                       colors_=[C_TEXT, C_TEXT, cc],
                       bold_=[False, False, True],
                       aligns=["L","R","R"])

    # Currency+Crypto table
    ry = y - CARD_PY
    ry = _tbl_header(c, rx + CARD_PX, ry, ["Pair / Asset","Rate","Chg %"],
                     [100, 90, 48])
    for row in right_rows:
        chgp = row.get("chg_pct","—")
        cc   = _chg_color(chgp)
        ry = _tbl_row(c, rx + CARD_PX, ry,
                      [row.get("pair",""), str(row.get("rate","—")), _sign_pct(chgp)],
                      [100, 90, 48],
                      colors_=[C_TEXT, C_TEXT, cc],
                      bold_=[False, False, True],
                      aligns=["L","R","R"])

    return y - card_h


def _draw_page1(c, data):
    _bg(c)
    _draw_header(c, data)

    y = C_TOP

    # ── Macro / Expected Scenario Alert ──────────────────────────────────
    sig     = data.get("hawala_signal", {})
    bn      = data.get("banknifty_analysis", {})
    overall = sig.get("overall", "—")

    alert_title = f"\u26a0  Expected Opening Scenario — Signal: {overall}"
    scenario    = data.get("scenario_text", "")
    body_lines  = _wrap(c, scenario, _REG, 8, CW - CARD_PX*2 - 20)
    y = _alert_box(c, ML, y, CW, alert_title, body_lines, kind="warning")
    y -= ELEM_G

    # ── Quick Snapshot ────────────────────────────────────────────────────
    _sec_title(c, ML, y, "Quick Snapshot")
    y -= SEC_H + 2
    y = _draw_snap_cards(c, y, data)
    y -= ELEM_G

    # ── India + Global ────────────────────────────────────────────────────
    y = _draw_markets_2col(c, y, data)
    y -= ELEM_G

    # ── Commodities + Currency ────────────────────────────────────────────
    y = _draw_commodity_currency(c, y, data)

    _footer(c, 1)


# ─────────────────────────────────────────────────────────────────────────────
# Page 2 — Sentiment · Signal · Scenario · News · Events
# ─────────────────────────────────────────────────────────────────────────────

def _draw_sentiment_row(c, y, data):
    """3 sentiment cards. Returns new y."""
    fg_score   = data.get("fear_greed_val", "—")
    fg_label   = data.get("fear_greed_label", "—")
    cfg_score  = data.get("crypto_fg_score", "—")
    cfg_label  = data.get("crypto_fg_label", "—")
    vix_row    = data.get("india_vix", {})
    vix_val    = vix_row.get("price", "—")

    try:
        vv    = float(vix_val)
        vc    = C_GREEN if vv < 19 else C_RED
        v_sub = "Below 19 — Trades ENABLED" if vv < 19 else "Above 19 — Caution"
    except:
        vc    = C_MUTED
        v_sub = "—"

    def _fg_color(score):
        try:
            s = float(score)
            if s <= 25: return C_RED
            if s <= 44: return C_YELL
            if s <= 55: return C_MUTED
            return C_GREEN
        except:
            return C_MUTED

    card_h = 82
    cards = [
        {"title": "CNN Fear & Greed (Equities)", "score": fg_score,  "label": fg_label,  "col": _fg_color(fg_score)},
        {"title": "Crypto Fear & Greed",          "score": cfg_score, "label": cfg_label, "col": _fg_color(cfg_score)},
        {"title": "India VIX Trend",              "score": vix_val,   "label": v_sub,     "col": vc, "no_gauge": True},
    ]

    for i, card in enumerate(cards):
        cx = ML + i * (COL3_W + COL3_G)
        _card(c, cx, y, COL3_W, card_h)
        # inner section title
        c.setFillColor(C_MUTED)
        c.setFont(_BOLD, 7)
        c.drawString(cx + CARD_PX, y - CARD_PY - 6, card["title"].upper())
        # big number
        c.setFillColor(card["col"])
        c.setFont(_BOLD, 22)
        c.drawString(cx + CARD_PX, y - CARD_PY - 28, str(card["score"]))
        c.setFont(_BOLD, 8)
        c.drawString(cx + CARD_PX, y - CARD_PY - 40, str(card["label"])[:32])
        if not card.get("no_gauge"):
            _gauge_bar(c, cx + CARD_PX, y - CARD_PY - 62, COL3_W - 2*CARD_PX, card["score"])
        else:
            c.setFillColor(C_MUTED)
            c.setFont(_REG, 7.5)
            c.drawString(cx + CARD_PX, y - CARD_PY - 58, f"VIX threshold: 19.0  |  Alert if > 19")

    return y - card_h


def _draw_signal_grid(c, y, data):
    """6 signal check cards (3×2). Returns new y."""
    sig = data.get("hawala_signal", {})

    def _pf(ok):
        if ok is None: return "warn"
        return "pass" if ok else "fail"

    def _icon(ok):
        if ok is None: return "\u26a0"
        return "\u2713" if ok else "\u2717"

    gap_pts = sig.get("gap_pts", "—")
    gap_str = f"{'+' if (gap_pts not in ('—',None) and float(gap_pts)>=0) else ''}{float(gap_pts):,.0f} pts" if gap_pts not in ("—",None) else "—"
    gap_strat = sig.get("gap_strategy","—")

    chips = [
        {"label": "India VIX Filter",
         "val":   f"{sig.get('vix_val','—')}  (threshold > {sig.get('vix_thresh',19)})",
         "status": _pf(sig.get("vix_pass")),
         "icon":  _icon(sig.get("vix_pass"))},
        {"label": "S&P Overnight Move",
         "val":   f"{_sign_pct(sig.get('sp_chg','—'))}  (threshold < {sig.get('sp_thresh',-1.5)}%)",
         "status": _pf(sig.get("sp_pass")),
         "icon":  _icon(sig.get("sp_pass"))},
        {"label": "FPI Net Flow (Prev Day)",
         "val":   (f"{RUPEE}{float(sig.get('fii_net','0')):,.0f} Cr"
                   if sig.get("fii_net") not in ("—",None) else "Data pending"),
         "status": _pf(sig.get("fii_pass")),
         "icon":  _icon(sig.get("fii_pass"))},
        {"label": "GIFT Nifty Gap",
         "val":   f"BN gap est. {gap_str} → {gap_strat}",
         "status": "warn",
         "icon":  "\u2197" if sig.get("gap_dir","") == "GAP UP" else ("\u2198" if sig.get("gap_dir","") == "GAP DOWN" else "\u2192")},
        {"label": "DOW Filter",
         "val":   f"{sig.get('dow_name','—')} — {'EXCLUDED' if sig.get('dow_blocked') else 'ALLOWED'}",
         "status": "fail" if sig.get("dow_blocked") else "pass",
         "icon":  _icon(not sig.get("dow_blocked"))},
        {"label": "Overall Signal",
         "val":   str(sig.get("overall","—")),
         "status": "fail" if sig.get("overall") == "NO TRADE" else "pass",
         "icon":  _icon(sig.get("overall") != "NO TRADE")},
    ]

    chip_h = 46
    ICON_COLORS = {"pass": C_GREEN, "fail": C_RED, "warn": C_YELL}
    BG_COLORS   = {"pass": "#22c55e18", "fail": "#ef444418", "warn": "#f59e0b18"}

    for row in range(2):
        for col in range(3):
            idx = row * 3 + col
            if idx >= len(chips): break
            chip = chips[idx]
            cx   = ML + col * (COL3_W + COL3_G)
            cy   = y - row * (chip_h + 5)
            _card(c, cx, cy, COL3_W, chip_h)
            # icon circle
            ic = ICON_COLORS[chip["status"]]
            c.setFillColor(colors.HexColor(BG_COLORS[chip["status"]]))
            c.circle(cx + 20, cy - chip_h/2, 11, fill=1, stroke=0)
            c.setFillColor(ic)
            c.setFont(_BOLD, 10)
            c.drawCentredString(cx + 20, cy - chip_h/2 - 4, chip["icon"])
            # text
            c.setFillColor(C_MUTED)
            c.setFont(_BOLD, 6.5)
            c.drawString(cx + 36, cy - 12, chip["label"].upper())
            c.setFillColor(ic)
            c.setFont(_BOLD, 8)
            c.drawString(cx + 36, cy - 23, str(chip["val"])[:34])
            c.setFillColor(C_MUTED)
            c.setFont(_REG, 7)
            c.drawString(cx + 36, cy - 33, f"{'PASS' if chip['status']=='pass' else ('FAIL' if chip['status']=='fail' else 'PENDING')}")

    return y - 2 * (chip_h + 5) + 5


def _draw_news(c, y, data):
    """Key news list with tag pills. Returns new y."""
    news = data.get("news_items", [])
    if not news:
        return y

    TAG = {
        "macro":  (colors.HexColor("#ef444420"), colors.HexColor("#f87171")),
        "energy": (colors.HexColor("#f59e0b20"), colors.HexColor("#fbbf24")),
        "india":  (colors.HexColor("#6366f120"), colors.HexColor("#818cf8")),
        "crypto": (colors.HexColor("#a855f720"), colors.HexColor("#c084fc")),
        "global": (colors.HexColor("#3b82f620"), colors.HexColor("#60a5fa")),
    }

    LINE_H = 11
    item_heights = []
    for item in news[:6]:
        lines = _wrap(c, item.get("headline",""), _REG, 8, CW - CARD_PX*2 - 46)
        item_heights.append(max(1, len(lines)) * LINE_H + 10)

    total_h = sum(item_heights) + CARD_PY * 2
    _card(c, ML, y, CW, total_h)

    iy = y - CARD_PY
    for item, ih in zip(news[:6], item_heights):
        tag = item.get("tag", "macro")
        bg_t, fg_t = TAG.get(tag, TAG["macro"])
        # tag pill
        c.setFillColor(bg_t)
        c.roundRect(ML + CARD_PX, iy - 10, 38, 11, 3, fill=1, stroke=0)
        c.setFillColor(fg_t)
        c.setFont(_BOLD, 6)
        c.drawCentredString(ML + CARD_PX + 19, iy - 3, tag.upper())
        # headline
        lines = _wrap(c, item.get("headline",""), _REG, 8, CW - CARD_PX*2 - 46)
        c.setFillColor(C_TEXT)
        c.setFont(_REG, 8)
        lx = ML + CARD_PX + 44
        ly = iy - 1
        for ln in lines:
            c.drawString(lx, ly, ln)
            ly -= LINE_H
        # separator
        c.setStrokeColor(C_BORD)
        c.setLineWidth(0.3)
        c.line(ML + CARD_PX, iy - ih + 2, ML + CW - CARD_PX, iy - ih + 2)
        iy -= ih

    return y - total_h


def _draw_events_calendar(c, y, data):
    """Today's events calendar. Returns new y."""
    events = data.get("events_calendar", [])
    if not events:
        return y

    IMPACT_COL = {"high": C_RED, "medium": C_YELL, "low": C_MUTED}
    row_count = len(events)
    card_h = HDR_ROW + row_count * ROW_H + 2 * CARD_PY
    _card(c, ML, y, CW, card_h)

    ey = y - CARD_PY
    ey = _tbl_header(c, ML + CARD_PX, ey, ["Time (IST)", "Event", "Impact"],
                     [70, 380, 60])
    for ev in events:
        impact     = ev.get("impact","medium")
        impact_col = IMPACT_COL.get(impact, C_MUTED)
        impact_lbl = impact.capitalize()
        ey = _tbl_row(c, ML + CARD_PX, ey,
                      [ev.get("time",""), ev.get("event",""), impact_lbl],
                      [70, 380, 60],
                      colors_=[C_TEXT, C_TEXT, impact_col],
                      bold_=[False, False, True],
                      aligns=["L","L","R"])

    return y - card_h


def _draw_page2(c, data):
    _bg(c)
    y = C_TOP

    # ── Market Sentiment ──────────────────────────────────────────────────
    _sec_title(c, ML, y, "Market Sentiment")
    y -= SEC_H + 2
    y = _draw_sentiment_row(c, y, data)
    y -= ELEM_G

    # ── Hawala v2 Signal Check ────────────────────────────────────────────
    _sec_title(c, ML, y, "Hawala v2 — Pre-Market Filter Check")
    y -= SEC_H + 2
    y = _draw_signal_grid(c, y, data)
    y -= ELEM_G

    # ── Key News ──────────────────────────────────────────────────────────
    _sec_title(c, ML, y, "Key News & Events")
    y -= SEC_H + 2
    y = _draw_news(c, y, data)
    y -= ELEM_G

    # ── Events Calendar ───────────────────────────────────────────────────
    _sec_title(c, ML, y, "Today's Events Calendar")
    y -= SEC_H + 2
    y = _draw_events_calendar(c, y, data)

    _footer(c, 2)


# ─────────────────────────────────────────────────────────────────────────────
# Page 3 — BankNifty + FII/DII + Nifty Deep Dive
# ─────────────────────────────────────────────────────────────────────────────

def _draw_stats_bar(c, y, label, stats, accent, bar_w=CW):
    """Draw a stats summary bar card (horizontal). Returns new y."""
    bar_h = 46
    _card(c, ML, y, bar_w, bar_h, accent=accent)
    sw = bar_w / len(stats)
    for i, (lbl, val) in enumerate(stats):
        sx = ML + i * sw + sw / 2
        c.setFillColor(C_MUTED)
        c.setFont(_BOLD, 6.5)
        c.drawCentredString(sx, y - 14, lbl.upper())
        c.setFillColor(C_WHITE)
        c.setFont(_BOLD, 10)
        c.drawCentredString(sx, y - 30, str(val)[:18])
    return y - bar_h


def _draw_pivots_table(c, x, y, pivots_c, pivots_f, card_w):
    """Draw classic + fib pivots inside a card. Returns new y."""
    levels   = ["R3","R2","R1","PP","S1","S2","S3"]
    lbl_w    = 28
    val_w    = (card_w - 2*CARD_PX - lbl_w) / 2
    card_h   = HDR_ROW + len(levels) * ROW_H + 2*CARD_PY
    _card(c, x, y, card_w, card_h)

    hy = y - CARD_PY
    hy = _tbl_header(c, x+CARD_PX, hy, ["Level","Classic","Fibonacci"],
                     [lbl_w, val_w, val_w])
    for lv in levels:
        cv = pivots_c.get(lv,"—")
        fv = pivots_f.get(lv,"—")
        if lv in ("R1","R2","R3"):   lv_col = C_GREEN
        elif lv in ("S1","S2","S3"): lv_col = C_RED
        else:                         lv_col = C_ACC
        hy = _tbl_row(c, x+CARD_PX, hy,
                      [lv,
                       f"{cv:,.0f}" if isinstance(cv,(int,float)) else str(cv),
                       f"{fv:,.0f}" if isinstance(fv,(int,float)) else str(fv)],
                      [lbl_w, val_w, val_w],
                      colors_=[lv_col, C_TEXT, C_TEXT],
                      bold_=[True, False, False],
                      aligns=["L","R","R"])
    return y - card_h


def _draw_option_chain_card(c, x, y, chain, card_w, label):
    """Top CE/PE strikes + PCR. Returns new y."""
    ce_s = chain.get("top_ce_strikes",[])
    pe_s = chain.get("top_pe_strikes",[])
    pcr  = chain.get("pcr","—")
    exp  = chain.get("near_expiry","—")
    atm  = chain.get("atm","—")

    rows = max(len(ce_s), len(pe_s))
    card_h = CARD_PY*2 + 14 + 4 + 14 + rows*ROW_H + 8 + 14 + rows*ROW_H
    _card(c, x, y, card_w, card_h)

    oy = y - CARD_PY
    c.setFillColor(C_MUTED)
    c.setFont(_BOLD, 7)
    c.drawString(x+CARD_PX, oy - 8, f"Near Expiry: {exp}   ATM: {atm}   PCR: {pcr}")
    oy -= 16

    # PE (support)
    c.setFillColor(C_GREEN)
    c.setFont(_BOLD, 7.5)
    c.drawString(x+CARD_PX, oy - 8, "TOP PE STRIKES — SUPPORT (OI in Lakhs)")
    oy -= 16
    for s in pe_s:
        c.setFillColor(C_TEXT)
        c.setFont(_REG, 8.5)
        c.drawString(x+CARD_PX+4, oy, f"Strike {s['strike']:,.0f}")
        c.setFillColor(C_GREEN)
        c.setFont(_BOLD, 8.5)
        c.drawRightString(x+card_w-CARD_PX, oy, f"{s['oi']:.1f}L OI")
        c.setStrokeColor(C_BORD)
        c.setLineWidth(0.3)
        c.line(x+CARD_PX, oy-3, x+card_w-CARD_PX, oy-3)
        oy -= ROW_H

    oy -= 4
    # CE (resistance)
    c.setFillColor(C_RED)
    c.setFont(_BOLD, 7.5)
    c.drawString(x+CARD_PX, oy - 8, "TOP CE STRIKES — RESISTANCE (OI in Lakhs)")
    oy -= 16
    for s in ce_s:
        c.setFillColor(C_TEXT)
        c.setFont(_REG, 8.5)
        c.drawString(x+CARD_PX+4, oy, f"Strike {s['strike']:,.0f}")
        c.setFillColor(C_RED)
        c.setFont(_BOLD, 8.5)
        c.drawRightString(x+card_w-CARD_PX, oy, f"{s['oi']:.1f}L OI")
        c.setStrokeColor(C_BORD)
        c.setLineWidth(0.3)
        c.line(x+CARD_PX, oy-3, x+card_w-CARD_PX, oy-3)
        oy -= ROW_H

    return y - card_h


def _draw_fiidii(c, y, data):
    fiidii = data.get("fii_dii", [])
    if not fiidii:
        return y
    row_count = len(fiidii)
    card_h = HDR_ROW + row_count * ROW_H + 2*CARD_PY
    _card(c, ML, y, CW, card_h)
    fy = y - CARD_PY
    fy = _tbl_header(c, ML+CARD_PX, fy, ["Category","Buy (Cr)","Sell (Cr)","Net (Cr)"],
                     [180, 100, 100, 100])
    for row in fiidii:
        net = row.get("net","—")
        nc  = _chg_color(net)
        fy = _tbl_row(c, ML+CARD_PX, fy,
                      [row.get("category",""), str(row.get("buy","—")),
                       str(row.get("sell","—")), str(net)],
                      [180, 100, 100, 100],
                      colors_=[C_TEXT, C_TEXT, C_TEXT, nc],
                      bold_=[False, False, False, True],
                      aligns=["L","R","R","R"])
    return y - card_h


def _draw_index_block(c, y, name, analysis, accent, chain):
    """Draw stats bar + pivots + option chain for one index. Returns new y."""
    bn_c = analysis.get("prev_close","—")
    bn_h = analysis.get("prev_high","—")
    bn_l = analysis.get("prev_low","—")
    bn_d = analysis.get("day_chg","—")
    bn_p = analysis.get("day_chg_pct","—")
    atr  = analysis.get("atr14","—")
    gap  = analysis.get("gap_pts","—")

    def _v(val, dec=0, prefix=""):
        try:
            v = float(val)
            return f"{prefix}{v:,.{dec}f}"
        except:
            return str(val)

    def _day_chg_str(d, p):
        try:
            dv = float(d); pv = float(p)
            return f"{'+' if dv>=0 else ''}{dv:,.0f} ({'+' if pv>=0 else ''}{pv:.2f}%)"
        except:
            return "—"

    stats = [
        ("Prev Close", _v(bn_c, 2) if bn_c not in ("—",None) else "—"),
        ("Day Chg",    _day_chg_str(bn_d, bn_p)),
        ("H / L",      f"{_v(bn_h,0)} / {_v(bn_l,0)}" if bn_h not in ("—",None) else "—"),
        ("ATR14",      f"{_v(atr,0)} pts" if atr not in ("—",None) else "—"),
    ]
    if gap not in ("—", None) and name == "BankNifty":
        try:
            gf = float(gap)
            stats.append(("Gap Est.", f"{'+'if gf>=0 else ''}{gf:,.0f} pts"))
        except:
            pass

    y = _draw_stats_bar(c, y, name, stats, accent)
    y -= 8

    # Pivots (left) + Option chain (right)
    pivot_w  = COL2_W
    chain_w  = COL2_W
    pc = analysis.get("pivots_classic", {})
    pf = analysis.get("pivots_fib", {})

    piv_y   = y
    chain_y = y

    piv_end   = _draw_pivots_table(c, ML,                    piv_y,   pc, pf, pivot_w)
    chain_end = _draw_option_chain_card(c, ML+COL2_W+COL2_G, chain_y, chain, chain_w, name)

    return min(piv_end, chain_end)


def _draw_page3(c, data):
    _bg(c)
    y = C_TOP

    bn     = data.get("banknifty_analysis", {})
    nf     = data.get("nifty_analysis", {})
    bn_ch  = bn.get("option_chain", {})
    nf_ch  = nf.get("option_chain", {})
    prev   = data.get("prev_day_label","Prev")

    # ── BankNifty ─────────────────────────────────────────────────────────
    _sec_title(c, ML, y, f"BankNifty — Deep Dive ({prev} Close)")
    y -= SEC_H + 2
    y = _draw_index_block(c, y, "BankNifty", bn, C_ACC, bn_ch)
    y -= ELEM_G + 4

    # ── FII / DII Flows ───────────────────────────────────────────────────
    _sec_title(c, ML, y, "FII / DII Flows (Previous Day)")
    y -= SEC_H + 2
    y = _draw_fiidii(c, y, data)
    y -= ELEM_G + 4

    # ── Nifty ─────────────────────────────────────────────────────────────
    _sec_title(c, ML, y, f"Nifty 50 — Deep Dive ({prev} Close)")
    y -= SEC_H + 2
    y = _draw_index_block(c, y, "Nifty", nf, C_BLUE, nf_ch)

    _footer(c, 3)


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def build_pdf(data: dict, output_path: str) -> str:
    c = rl_canvas.Canvas(str(output_path), pagesize=A4)

    _draw_page1(c, data)
    c.showPage()

    _draw_page2(c, data)
    c.showPage()

    _draw_page3(c, data)
    c.showPage()

    c.save()
    print(f"  \u2705 PDF written \u2192 {output_path}")
    return str(output_path)


if __name__ == "__main__":
    import json, sys
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            d = json.load(f)
    else:
        from data.fetch_report_data import fetch_all
        d = fetch_all()
    build_pdf(d, f"market_report_{d.get('date_iso','test')}.pdf")
