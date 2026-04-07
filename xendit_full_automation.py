"""
╔══════════════════════════════════════════════════════════════════════╗
║              XENDIT FULL AUTOMATION — v3                            ║
║                                                                      ║
║  PART A  Xendit Dashboard                                            ║
║    1. Login → Switch to onboarding+id → Transactions → Export        ║
║       (all 28 cols, 7-day date, send to swarnraj@tazapay.com)       ║
║                                                                      ║
║  PART B  Gmail IMAP Download                                         ║
║    2. Connect via IMAP → find Xendit export email → save file        ║
║                                                                      ║
║  PART C  XenPlatform (inside Xendit dashboard)                       ║
║    3. Apps & Partners → XenPlatform → Accounts                       ║
║    4. Select ALL accounts + sub-accounts                             ║
║    5. Export → Transaction → Custom date (same 7-day) → Send mail   ║
║                                                                      ║
║  PART D  Gmail IMAP Download (XenPlatform file)                     ║
║    6. Connect via IMAP → find XenPlatform export → save file         ║
╚══════════════════════════════════════════════════════════════════════╝

  ⚠️  GMAIL SETUP REQUIRED (one-time):
      Gmail blocks regular passwords for IMAP access when 2FA is on.
      You must generate a Gmail App Password:
        1. Go to: https://myaccount.google.com/apppasswords
        2. App: "Mail"  |  Device: "Windows Computer"
        3. Copy the 16-char password → paste in GMAIL_APP_PASSWORD below
"""

import asyncio
import csv
import imaplib
import email as email_lib
import os
import sys
import pyotp
from datetime import datetime, timedelta
from email.header import decode_header

try:
    import pyotp
except ImportError:
    pass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from playwright.async_api import async_playwright

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
    _BOTO3_OK = True
except ImportError:
    _BOTO3_OK = False

# ══════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════
_today     = datetime.now()
_from_date = _today - timedelta(days=6)   # inclusive 7-day window ending today

CONFIG = {
    # ── Xendit Dashboard login (main account) ─────────────────────────
    "XENDIT_EMAIL":       "Anubhavjain@tazapay.com",
    "XENDIT_PASSWORD":    "Tazapay@2025",
    "XENDIT_TOTP":        "GBWDMNSPOFUGKXSNFFWUU62LJFVUI5TW",
    # Switch Business: search for business name first, then email as fallback
    "TARGET_BUSINESS":    "Tazapay Pte Ltd",
    "TARGET_ACCOUNT":     "onboarding+id@tazapay.com",

    # ── Export destination email ──────────────────────────────────────
    "EXPORT_EMAIL":    "swarnraj@tazapay.com",

    # ── Gmail IMAP (to download exported files) ───────────────────────
    # Generate App Password at: https://myaccount.google.com/apppasswords
    "GMAIL_EMAIL":       "swarnraj@tazapay.com",
    "GMAIL_APP_PASSWORD": "jdetidaxxybnhrlr",

    # ── Date range ────────────────────────────────────────────────────
    "FROM_DAY":   _from_date.day,
    "FROM_MONTH": _from_date.month,
    "FROM_YEAR":  _from_date.year,
    "TO_DAY":     _today.day,
    "TO_MONTH":   _today.month,
    "TO_YEAR":    _today.year,

    # ── Paths ─────────────────────────────────────────────────────────
    "SLOW_MO":        int(os.environ.get("SLOW_MO", "150")),
    "SESSION_STATE": os.path.join(os.path.dirname(os.path.abspath(__file__)), "xendit_session.json"),

    # ── AWS S3 Upload ─────────────────────────────────────────────────
    "AWS_ACCESS_KEY_ID":     os.environ.get("AWS_ACCESS_KEY_ID",     ""),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "AWS_REGION":            "ap-southeast-1",
    "S3_BUCKET":             "payout-recon",
    "S3_PREFIX":             "xendit/txn/raw_daily/",

    # ── Slack Notifications ───────────────────────────────────────────
    "SLACK_TOKEN":   os.environ.get("SLACK_TOKEN",   ""),
    "SLACK_CHANNEL": os.environ.get("SLACK_CHANNEL", "C0ANN4HTC9L"),

    # ── CI overrides (set via env vars in GitHub Actions) ─────────────
    "HEADLESS": os.environ.get("HEADLESS", "false").lower() == "true",
    "DOWNLOAD_DIR":   os.environ.get("DOWNLOAD_DIR",   r"C:\Users\TazaDH 511\Downloads\xendit_exports"),
    "SCREENSHOT_DIR": os.environ.get("SCREENSHOT_DIR",
                        os.path.join(os.path.dirname(os.path.abspath(__file__)), "xendit_screenshots")),
}

# ══════════════════════════════════════════════════════════════════════
#  S3 UPLOAD HELPER
# ══════════════════════════════════════════════════════════════════════
def upload_to_s3(local_path: str) -> bool:
    """Upload a single file to S3 under the configured prefix.
    Returns True on success, False on failure.
    Only uploads: xendit_ALL_TRANSACTIONS_REPORT_* and xp_activity_* files.
    Set env var SKIP_S3=true to bypass all uploads (useful for test runs).
    """
    if os.environ.get("SKIP_S3", "false").lower() == "true":
        print("  ⏭️   S3 upload skipped (SKIP_S3=true)")
        return True
    if not _BOTO3_OK:
        print("  ⚠️  boto3 not installed — skipping S3 upload")
        return False
    fname = os.path.basename(local_path)
    # Only upload the two categories the user wants
    if not (fname.startswith("xendit_ALL_TRANSACTIONS_REPORT_") or
            fname.startswith("xp_activity_")):
        return False
    s3_key = CONFIG["S3_PREFIX"] + fname
    try:
        s3 = boto3.client(
            "s3",
            region_name=CONFIG["AWS_REGION"],
            aws_access_key_id=CONFIG["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=CONFIG["AWS_SECRET_ACCESS_KEY"],
        )
        s3.upload_file(local_path, CONFIG["S3_BUCKET"], s3_key)
        print(f"  ☁️   S3 uploaded → s3://{CONFIG['S3_BUCKET']}/{s3_key}")
        return True
    except Exception as e:
        print(f"  ⚠️  S3 upload failed for {fname}: {e}")
        return False

# ══════════════════════════════════════════════════════════════════════
#  SLACK NOTIFICATION HELPER
# ══════════════════════════════════════════════════════════════════════
def slack_notify(success: bool, results: dict = None, error: str = ""):
    """Post a success or failure summary to Slack."""
    import urllib.request, json as _json
    token   = CONFIG.get("SLACK_TOKEN", "")
    channel = CONFIG.get("SLACK_CHANNEL", "")
    if not token or not channel:
        return
    now_ist = datetime.now().strftime("%d %b %Y %H:%M IST")
    if success:
        header = f":white_check_mark: *Xendit Automation — SUCCESS* | {now_ist}"
        lines  = [header]
        if results:
            for key, val in results.items():
                icon = "✅" if val and val != "SKIPPED" else ("⏭️" if val == "SKIPPED" else "❌")
                name = str(val)[-60:] if isinstance(val, str) and len(str(val)) > 60 else str(val)
                lines.append(f"  {icon}  *{key}*: {name}")
    else:
        header = f":x: *Xendit Automation — FAILED* | {now_ist}"
        lines  = [header, f"  Error: `{error}`" if error else "  Check GitHub Actions logs for details."]
    text = "\n".join(lines)
    try:
        payload = _json.dumps({"channel": channel, "text": text}).encode()
        req = urllib.request.Request(
            "https://slack.com/api/chat.postMessage",
            data=payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = _json.loads(resp.read())
            if body.get("ok"):
                print(f"  📣  Slack notified (channel {channel})")
            else:
                print(f"  ⚠️  Slack error: {body.get('error')}")
    except Exception as e:
        print(f"  ⚠️  Slack notify failed: {e}")


STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', {
    get: () => { const a=[1,2,3,4,5]; a.__proto__=PluginArray.prototype; return a; }
});
Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
if (!window.chrome) {
    window.chrome = { runtime:{}, loadTimes:function(){}, csi:function(){}, app:{} };
}
const _origQuery = window.navigator.permissions.query;
window.navigator.permissions.query = p =>
    p.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : _origQuery(p);
"""

os.makedirs(CONFIG["DOWNLOAD_DIR"],   exist_ok=True)
os.makedirs(CONFIG["SCREENSHOT_DIR"], exist_ok=True)

print(f"\n  Date range : {_from_date.strftime('%d %b %Y')} → {_today.strftime('%d %b %Y')}")
print(f"  Export to  : {CONFIG['EXPORT_EMAIL']}")
print(f"  Download → : {CONFIG['DOWNLOAD_DIR']}\n")


# ══════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════
async def ss(page, name):
    path = os.path.join(CONFIG["SCREENSHOT_DIR"], f"{name}.png")
    try:
        await page.screenshot(path=path)
        print(f"    📸  {path}")
    except Exception:
        pass


async def dismiss_feedback_modal(page):
    """Aggressively close any feedback / survey / announcement / cookie popups."""
    try:
        closed = await page.evaluate("""
            () => {
                let dismissed = 0;

                // Keywords that indicate a popup we should close
                const BAD = /(feedback|survey|rate|nps|how.?are.?you|tell.?us|cookie|consent|announce|promo|banner|dismiss|close|got.?it|okay|ok!/i;

                // 1. Try every visible dialog / modal
                for (const el of document.querySelectorAll(
                    '[role="dialog"], [class*="modal"], [class*="popup"], '
                    + '[class*="overlay"], [class*="banner"], [class*="toast"], '
                    + '[class*="notification"], [class*="intercom"], [class*="pendo"]'
                )) {
                    if (el.offsetParent === null) continue;  // hidden
                    // Click any close / dismiss / X button inside it
                    const closeBtn = el.querySelector(
                        'button[aria-label*="close" i], button[aria-label*="dismiss" i], '
                        + 'button[title*="close" i], [class*="close"], [class*="dismiss"], '
                        + 'button:last-of-type'
                    );
                    if (closeBtn) { closeBtn.click(); dismissed++; continue; }
                    // If text suggests feedback, click first button
                    if (BAD.test(el.innerText || '')) {
                        const b = el.querySelector('button');
                        if (b) { b.click(); dismissed++; }
                    }
                }

                // 2. Standalone close buttons floating on screen
                for (const btn of document.querySelectorAll('button')) {
                    if (btn.offsetParent === null) continue;
                    const lbl = (btn.getAttribute('aria-label') || btn.innerText || '').trim();
                    if (/^(x|✕|✗|close|dismiss|×|got it|okay|ok!|no thanks)$/i.test(lbl)) {
                        btn.click();
                        dismissed++;
                    }
                }

                return dismissed;
            }
        """)
        if closed:
            print(f"  🚫  Dismissed {closed} popup(s)")
            await page.wait_for_timeout(600)
    except Exception:
        pass


async def recover_unexpected_page(page, label):
    try:
        if "unexpected" in (await page.content()).lower():
            await page.click("button:has-text('Retry')", timeout=3000)
            await page.wait_for_timeout(3000)
    except Exception:
        pass


async def close_modal(page):
    try:
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(800)
    except Exception:
        pass


async def get_modal_emails(page):
    """Return email addresses currently visible inside the active modal."""
    try:
        emails = await page.evaluate("""
            () => {
                const modal = document.querySelector('[data-testid="modal"]')
                           || document.querySelector('[role="dialog"]')
                           || document.querySelector('.modal-content');
                if (!modal) return [];

                const bag = new Set();
                const emailRe = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}/ig;

                const collect = (text) => {
                    if (!text) return;
                    for (const match of text.match(emailRe) || []) {
                        bag.add(match.toLowerCase());
                    }
                };

                collect(modal.innerText || '');
                for (const el of modal.querySelectorAll('input, textarea, [value]')) {
                    collect(el.value || '');
                    collect(el.getAttribute('value') || '');
                    collect(el.getAttribute('aria-label') || '');
                    collect(el.getAttribute('title') || '');
                }

                return [...bag];
            }
        """)
        return emails or []
    except Exception:
        return []


async def get_modal_email_field_values(page):
    """Return actual email-ish values from inputs/textareas inside the active modal."""
    try:
        values = await page.evaluate("""
            () => {
                const modal = document.querySelector('[data-testid="modal"]')
                           || document.querySelector('[role="dialog"]')
                           || document.querySelector('.modal-content');
                if (!modal) return [];

                const emailRe = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}/i;
                const out = [];
                for (const el of modal.querySelectorAll('input, textarea')) {
                    const raw = (el.value || el.getAttribute('value') || '').trim().toLowerCase();
                    const hint = (
                        (el.id || '') + ' ' + (el.name || '') + ' ' +
                        (el.placeholder || '') + ' ' + (el.getAttribute('aria-label') || '')
                    ).toLowerCase();
                    if (raw && emailRe.test(raw)) out.push(raw);
                    else if (hint.includes('email') && raw) out.push(raw);
                }
                return [...new Set(out)];
            }
        """)
        return values or []
    except Exception:
        return []


async def ensure_only_export_email(page, target_email: str, input_selectors: list[str]) -> bool:
    """Remove any existing recipients in the export modal and keep only target_email."""
    target_email = target_email.lower()

    # Explicitly remove Anubhav's email tag first (it is the Xendit account default)
    await page.evaluate("""
        () => {
            const modal = document.querySelector('[data-testid="modal"]')
                       || document.querySelector('[role="dialog"]')
                       || document.querySelector('.modal-content')
                       || document.body;
            const anubhavRe = /anubhav/i;
            for (const el of modal.querySelectorAll('div, span, li, p')) {
                const txt = (el.innerText || el.textContent || '').trim();
                if (!anubhavRe.test(txt)) continue;
                // Click remove/close button inside or next to the tag
                const btn = el.querySelector('button, [role="button"], img, svg, span[aria-label]')
                         || el.nextElementSibling
                         || el.parentElement?.querySelector('button, [role="button"]');
                if (btn) { btn.click(); return; }
                el.click();
            }
        }
    """)
    await page.wait_for_timeout(300)

    for attempt in range(3):
        await page.evaluate("""
            () => {
                const modal = document.querySelector('[data-testid="modal"]')
                           || document.querySelector('[role="dialog"]')
                           || document.querySelector('.modal-content');
                if (!modal) return;

                const clickIfVisible = (el) => {
                    if (!el || !el.click) return false;
                    const style = window.getComputedStyle(el);
                    if (style.display === 'none' || style.visibility === 'hidden') return false;
                    if (el.offsetParent === null && style.position !== 'fixed') return false;
                    el.click();
                    return true;
                };

                const emailRe = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}/i;
                const removeWords = /(remove|delete|close|clear|dismiss)/i;

                for (const el of modal.querySelectorAll('button, [role="button"], span, div')) {
                    const text = ((el.innerText || el.textContent || '') + ' ' +
                                  (el.getAttribute('aria-label') || '') + ' ' +
                                  (el.getAttribute('title') || '')).trim();
                    if (!text) continue;
                    const compact = text.replace(/\\s+/g, ' ').trim();
                    if (removeWords.test(compact) || compact === '×' || compact.toLowerCase() === 'x') {
                        clickIfVisible(el);
                    }
                }

                for (const input of modal.querySelectorAll('input, textarea')) {
                    const hint = ((input.getAttribute('id') || '') + ' ' +
                                  (input.getAttribute('name') || '') + ' ' +
                                  (input.getAttribute('placeholder') || '') + ' ' +
                                  (input.getAttribute('aria-label') || '')).toLowerCase();
                    if (input.type === 'email' || hint.includes('email') || emailRe.test(input.value || '')) {
                        const nativeSetter = Object.getOwnPropertyDescriptor(
                            HTMLInputElement.prototype, 'value'
                        )?.set;
                        const setter = nativeSetter || ((value) => { input.value = value; });
                        input.focus();
                        setter.call ? setter.call(input, '') : setter('');
                        input.dispatchEvent(new Event('input', { bubbles: true }));
                        input.dispatchEvent(new Event('change', { bubbles: true }));
                    }
                }

                // Remove visible recipient chips or inline tags that still contain emails.
                for (const el of modal.querySelectorAll('div, span, p, li')) {
                    const text = (el.innerText || el.textContent || '').trim().toLowerCase();
                    if (!emailRe.test(text) || text.length > 120) continue;
                    const btn = el.querySelector('button, [role="button"], img[alt*="close" i], span[aria-label*="remove" i]')
                             || el.parentElement?.querySelector('button, [role="button"], img[alt*="close" i], span[aria-label*="remove" i]');
                    if (btn) clickIfVisible(btn);
                }
            }
        """)
        await page.wait_for_timeout(500)

        typed = False
        for selector in input_selectors:
            try:
                loc = page.locator(selector).first
                if await loc.count() == 0 or not await loc.is_visible():
                    continue
                await loc.click()
                try:
                    await loc.fill("")
                except Exception:
                    pass
                await loc.type(target_email)
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(400)
                typed = True
                break
            except Exception:
                continue

        if not typed:
            try:
                handle = await page.evaluate_handle("""
                    () => {
                        const modal = document.querySelector('[data-testid="modal"]')
                                   || document.querySelector('[role="dialog"]')
                                   || document.querySelector('.modal-content');
                        if (!modal) return null;
                        for (const el of modal.querySelectorAll('input, textarea')) {
                            const hint = ((el.id || '') + ' ' + (el.name || '') + ' ' +
                                          (el.placeholder || '') + ' ' +
                                          (el.getAttribute('aria-label') || '')).toLowerCase();
                            if (el.type === 'email' || hint.includes('email')) return el;
                        }
                        return modal.querySelector('input, textarea');
                    }
                """)
                element = handle.as_element()
                if element:
                    await element.click()
                    try:
                        await element.fill("")
                    except Exception:
                        pass
                    await element.type(target_email)
                    await page.keyboard.press("Enter")
                    await page.wait_for_timeout(400)
                    typed = True
            except Exception:
                pass

        # Force any modal email input to the target email value.
        await page.evaluate("""
            (targetEmail) => {
                const modal = document.querySelector('[data-testid="modal"]')
                           || document.querySelector('[role="dialog"]')
                           || document.querySelector('.modal-content');
                if (!modal) return;

                for (const el of modal.querySelectorAll('input, textarea')) {
                    const hint = (
                        (el.id || '') + ' ' + (el.name || '') + ' ' +
                        (el.placeholder || '') + ' ' + (el.getAttribute('aria-label') || '')
                    ).toLowerCase();
                    if (el.type !== 'email' && !hint.includes('email')) continue;

                    const proto = el.tagName.toLowerCase() === 'textarea'
                        ? HTMLTextAreaElement.prototype
                        : HTMLInputElement.prototype;
                    const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
                    if (setter) setter.call(el, targetEmail);
                    else el.value = targetEmail;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    el.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, key: 'Enter' }));
                    el.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'Enter' }));
                }
            }
        """, target_email)
        await page.wait_for_timeout(400)

        field_values = await get_modal_email_field_values(page)
        other_field_values = [mail for mail in field_values if mail != target_email]
        emails = await get_modal_emails(page)
        others = [mail for mail in emails if mail != target_email]
        if typed and target_email in field_values and not other_field_values:
            print(f"  📧  Export recipient set to only: {target_email}")
            return True
        if typed and target_email in emails and not others:
            print(f"  📧  Export recipient set to only: {target_email}")
            return True

    field_values = await get_modal_email_field_values(page)
    emails = await get_modal_emails(page)
    print(f"  ⚠️  Could not fully confirm recipient cleanup. Field values: {field_values} | Visible emails: {emails}")
    return target_email in field_values


def _date_label_variants(dt: datetime) -> list[str]:
    day_plain = str(dt.day)
    day_padded = f"{dt.day:02d}"
    year = str(dt.year)
    short_month = dt.strftime("%b")
    long_month = dt.strftime("%B")
    return [
        f"{long_month} {day_plain}, {year}".lower(),
        f"{long_month} {day_padded}, {year}".lower(),
        f"{day_plain} {long_month} {year}".lower(),
        f"{day_padded} {long_month} {year}".lower(),
        f"{short_month} {day_plain}, {year}".lower(),
        f"{short_month} {day_padded}, {year}".lower(),
        f"{day_plain} {short_month} {year}".lower(),
        f"{day_padded} {short_month} {year}".lower(),
        dt.strftime("%m/%d/%Y").lower(),
        dt.strftime("%d/%m/%Y").lower(),
    ]


async def _click_calendar_nav(page, direction: str) -> bool:
    clicked = await page.evaluate("""
        (direction) => {
            const modal = document.querySelector('[data-testid="modal"]')
                       || document.querySelector('[role="dialog"]')
                       || document.querySelector('.modal-content')
                       || document.body;
            const words = direction === 'prev'
                ? ['previous', 'prev', 'back', 'left']
                : ['next', 'forward', 'right'];

            const buttons = Array.from(modal.querySelectorAll('button, [role="button"]'))
                .filter(el => el.offsetParent !== null);

            for (const btn of buttons) {
                const txt = (
                    (btn.innerText || btn.textContent || '') + ' ' +
                    (btn.getAttribute('aria-label') || '') + ' ' +
                    (btn.getAttribute('title') || '')
                ).toLowerCase();
                if (words.some(word => txt.includes(word))) {
                    btn.click();
                    return true;
                }
            }
            return false;
        }
    """, direction)
    if clicked:
        await page.wait_for_timeout(700)
    return bool(clicked)


async def _pick_date_from_open_calendar(page, target_dt: datetime) -> bool:
    payload = {
        "day": str(target_dt.day),
        "month_year": target_dt.strftime("%B %Y").lower(),
        "variants": _date_label_variants(target_dt),
    }
    picked = await page.evaluate("""
        (payload) => {
            const modal = document.querySelector('[data-testid="modal"]')
                       || document.querySelector('[role="dialog"]')
                       || document.querySelector('.modal-content')
                       || document.body;
            const visible = Array.from(modal.querySelectorAll(
                'button, td, [role="gridcell"], [role="button"], span, div'
            )).filter(el => el.offsetParent !== null);

            const offMonth = /(disabled|outside|off|adjacent|other-month|not-current)/i;
            const labelOf = (el) => (
                (el.innerText || el.textContent || '') + ' ' +
                (el.getAttribute('aria-label') || '') + ' ' +
                (el.getAttribute('title') || '')
            ).replace(/\\s+/g, ' ').trim().toLowerCase();

            for (const el of visible) {
                const label = labelOf(el);
                const cls = ((el.className || '') + ' ' + (el.parentElement?.className || '')).toLowerCase();
                if (!label || offMonth.test(cls)) continue;
                if (payload.variants.some(v => label.includes(v))) {
                    el.click();
                    return true;
                }
            }

            const calendarRoots = visible.filter(el => {
                const label = labelOf(el);
                return label.includes(payload.month_year);
            });
            if (!calendarRoots.length) return false;

            for (const root of calendarRoots) {
                const container = root.closest('[role="dialog"], table, [class*="calendar"], [class*="picker"]')
                               || root.parentElement
                               || modal;
                const dayTargets = Array.from(container.querySelectorAll(
                    'button, td, [role="gridcell"], span, div'
                )).filter(el => el.offsetParent !== null);
                for (const dayEl of dayTargets) {
                    const label = labelOf(dayEl);
                    const cls = ((dayEl.className || '') + ' ' + (dayEl.parentElement?.className || '')).toLowerCase();
                    if (!label || offMonth.test(cls)) continue;
                    if (label === payload.day || label.startsWith(payload.day + ' ')) {
                        dayEl.click();
                        return true;
                    }
                }
            }
            return false;
        }
    """, payload)
    if picked:
        await page.wait_for_timeout(900)
    return bool(picked)


async def set_modal_custom_date_range(page) -> bool:
    """Prefer the modal's calendar UI; fall back to input-based filling if needed."""
    try:
        await page.evaluate("""
            () => {
                const modal = document.querySelector('[data-testid="modal"]')
                           || document.querySelector('[role="dialog"]')
                           || document.querySelector('.modal-content');
                const heading = modal?.querySelector('.modal-body p, p');
                if (heading) heading.click();
            }
        """)
        await page.wait_for_timeout(400)
    except Exception:
        pass

    fm, fd, fy = CONFIG["FROM_MONTH"], CONFIG["FROM_DAY"], CONFIG["FROM_YEAR"]
    tm, td, ty = CONFIG["TO_MONTH"], CONFIG["TO_DAY"], CONFIG["TO_YEAR"]
    candidate_pairs = [
        (f"{fm:02d}/{fd:02d}/{fy}", f"{tm:02d}/{td:02d}/{ty}"),
        (f"{fd:02d}/{fm:02d}/{fy}", f"{td:02d}/{tm:02d}/{ty}"),
        (f"{fm:02d}/{fd:02d}/{fy % 100:02d}", f"{tm:02d}/{td:02d}/{ty % 100:02d}"),
    ]

    rw_ids = await page.evaluate("""
        () => {
            const modal = document.querySelector('[data-testid="modal"]')
                       || document.querySelector('[role="dialog"]')
                       || document.querySelector('.modal-content');
            if (!modal) return [];
            return [...modal.querySelectorAll('input[id^="rw_"][id$="_input"]')]
                .filter(el => el.offsetParent !== null)
                .map(el => '#' + el.id);
        }
    """)

    if len(rw_ids) >= 2:
        from_id = rw_ids[0]
        to_id = rw_ids[2] if len(rw_ids) >= 3 else rw_ids[1]
        for from_val, to_val in candidate_pairs:
            await page.evaluate(f"""
                () => {{
                    const pairs = [
                        ['{from_id}', '{from_val}'],
                        ['{to_id}', '{to_val}'],
                    ];
                    for (const [selector, value] of pairs) {{
                        const el = document.querySelector(selector);
                        if (!el) continue;
                        const setter = Object.getOwnPropertyDescriptor(
                            HTMLInputElement.prototype, 'value'
                        )?.set;
                        el.focus();
                        if (setter) setter.call(el, '');
                        else el.value = '';
                        if (setter) setter.call(el, value);
                        else el.value = value;
                        el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                        el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                        try {{ el.blur(); }} catch (e) {{}}
                    }}
                }}
            """)
            await page.wait_for_timeout(900)
            values = await page.evaluate(f"""
                () => {{
                    return {{
                        from: document.querySelector('{from_id}')?.value || '',
                        to: document.querySelector('{to_id}')?.value || '',
                    }};
                }}
            """)
            print(f"  📅  Modal date values -> From: '{values.get('from')}'  To: '{values.get('to')}'")
            if values.get("from") and values.get("to"):
                return True

    targets = [_from_date, _today]

    for index, target_dt in enumerate(targets):
        clicked_input = False
        for selector in [
            '[data-testid="modal"] input[id^="rw_"][id$="_input"]',
            '[role="dialog"] input[id^="rw_"][id$="_input"]',
            '[data-testid="modal"] input[type="date"]',
            '[role="dialog"] input[type="date"]',
        ]:
            try:
                loc = page.locator(selector)
                count = await loc.count()
                if count <= index:
                    continue
                target = loc.nth(index)
                if not await target.is_visible():
                    continue
                await target.click(force=True)
                await page.wait_for_timeout(700)
                clicked_input = True
                break
            except Exception:
                continue

        if not clicked_input:
            return False

        if await _pick_date_from_open_calendar(page, target_dt):
            print(f"  📅  Calendar picked: {target_dt.strftime('%d %b %Y')}")
            continue

        current_month = target_dt.strftime("%B %Y")
        moved = False
        for direction in ["prev", "next", "prev"]:
            nav_clicked = await _click_calendar_nav(page, direction)
            if not nav_clicked:
                continue
            moved = True
            if await _pick_date_from_open_calendar(page, target_dt):
                print(f"  📅  Calendar picked after {direction}: {target_dt.strftime('%d %b %Y')}")
                break
        else:
            print(f"  ⚠️  Calendar pick failed for {current_month}")
            return False

        if moved:
            await page.wait_for_timeout(300)

    return True


async def set_dates(page, prefer_ddmm: bool = False):
    """Set the From/To dates in the export modal using the same input logic everywhere."""
    fm, fd, fy = CONFIG["FROM_MONTH"], CONFIG["FROM_DAY"], CONFIG["FROM_YEAR"]
    tm, td, ty = CONFIG["TO_MONTH"],   CONFIG["TO_DAY"],   CONFIG["TO_YEAR"]

    if prefer_ddmm:
        from_str = f"{fd:02d}/{fm:02d}/{fy}"
        to_str   = f"{td:02d}/{tm:02d}/{ty}"
        fmt = "DD/MM/YYYY"
    else:
        from_str = f"{fm:02d}/{fd:02d}/{fy}"
        to_str   = f"{tm:02d}/{td:02d}/{ty}"
        fmt = "MM/DD/YYYY"

    print(f"  Dates ({fmt}): {from_str} -> {to_str}")

    rw_ids = await page.evaluate("""
        () => [...document.querySelectorAll('[role="dialog"] input[id^="rw_"][id$="_input"], [data-testid="modal"] input[id^="rw_"][id$="_input"], .modal-content input[id^="rw_"][id$="_input"]')]
                .map(el => '#' + el.id)
    """)

    # Use index 0 (FROM date) and index 2 (TO date) — skipping index 1 which is
    # the time field. rw_ids[-1] was accidentally targeting the last time input.
    if len(rw_ids) >= 3:
        pairs = [(rw_ids[0], from_str), (rw_ids[2], to_str)]
    elif len(rw_ids) == 2:
        pairs = [(rw_ids[0], from_str), (rw_ids[1], to_str)]
    else:
        pairs = [("#rw_1_input", from_str), ("#rw_3_input", to_str)]

    for inp_id, val in pairs:
        for _ in range(3):
            try:
                await page.wait_for_selector(inp_id, timeout=5000)
                await page.evaluate(f"""
                    () => {{
                        const el = document.querySelector('{inp_id}');
                        if (!el) return;
                        const s = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
                        el.focus();
                        s.call(el, '');
                        s.call(el, '{val}');
                        el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                        el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                        setTimeout(() => {{
                            try {{ el.blur(); }} catch (e) {{}}
                            el.dispatchEvent(new FocusEvent('blur', {{ bubbles: true, relatedTarget: document.body }}));
                        }}, 150);
                    }}
                """)
                await page.wait_for_timeout(900)
                actual = await page.evaluate(f"() => document.querySelector('{inp_id}')?.value")
                if actual == val:
                    print(f"    OK {inp_id} = '{actual}'")
                    break
            except Exception:
                await page.wait_for_timeout(300)

    final = await page.evaluate("""
        () => {
            const inputs = [...document.querySelectorAll('[role="dialog"] input[id^="rw_"][id$="_input"], [data-testid="modal"] input[id^="rw_"][id$="_input"], .modal-content input[id^="rw_"][id$="_input"]')];
            if (!inputs.length) return { from: null, to: null };
            return {
                from: inputs[0]?.value ?? null,
                to: inputs[inputs.length - 1]?.value ?? null,
            };
        }
    """)
    return final


# ══════════════════════════════════════════════════════════════════════
#  PART A — XENDIT: LOGIN
# ══════════════════════════════════════════════════════════════════════
async def do_login(page, context) -> bool:
    print("\n" + "="*65)
    print("  PART A — STEP 1: Xendit Login")
    print("="*65)

    await page.goto("https://dashboard.xendit.co/home", wait_until="domcontentloaded")
    await page.wait_for_timeout(4000)

    if "login" not in page.url:
        print("  ✅  Already logged in.")
        return True

    print(f"  ✉️   {CONFIG['XENDIT_EMAIL']}")
    try:
        await page.wait_for_selector("input", timeout=20000)
    except Exception:
        return False

    inputs = await page.query_selector_all("input")
    email_input = None
    for i in inputs:
        if await i.is_visible():
            email_input = i
            break
    if not email_input:
        return False

    await email_input.fill(CONFIG["XENDIT_EMAIL"])
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(3000)

    print("  🔒  Password...")
    try:
        await page.wait_for_selector('input[type="password"]', timeout=20000)
    except Exception:
        return False

    pwd = await page.query_selector('input[type="password"]')
    await pwd.fill(CONFIG["XENDIT_PASSWORD"])
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(5000)

    # OTP
    await page.wait_for_timeout(2000)
    all_inputs = await page.query_selector_all("input")
    otp_inputs = [i for i in all_inputs if await i.is_visible()]
    if otp_inputs:
        otp = pyotp.TOTP(CONFIG["XENDIT_TOTP"]).now()
        print(f"  🔐  OTP: {otp}")
        await otp_inputs[0].click()
        for d in otp:
            await page.keyboard.type(d)
            await page.wait_for_timeout(100)
        await page.wait_for_timeout(2000)
        try:
            await page.click("button:has-text('Verify')", timeout=3000)
        except Exception:
            pass

    try:
        await page.wait_for_url(
            lambda url: "dashboard.xendit.co/login" not in url or "verification" in url,
            timeout=25000
        )
    except Exception:
        pass

    if "verification" in page.url:
        print(f"  ⚠️  Email verification required — check inbox (waiting 3 min)...")
        try:
            await page.wait_for_url(
                lambda url: "dashboard.xendit.co/login" not in url, timeout=180000
            )
        except Exception:
            pass

    await page.wait_for_timeout(3000)
    if "dashboard.xendit.co/login" in page.url and "verification" not in page.url:
        print("  ❌  Login failed.")
        return False

    print("  ✅  Logged in!")
    await ss(page, "A1_login")
    try:
        await context.storage_state(path=CONFIG["SESSION_STATE"])
        print(f"  Session saved -> {CONFIG['SESSION_STATE']}")
    except Exception as e:
        print(f"  Could not save session: {e}")
    return True


# ══════════════════════════════════════════════════════════════════════
#  PART A — XENDIT: SWITCH ACCOUNT
# ══════════════════════════════════════════════════════════════════════
async def switch_account(page, context) -> bool:
    biz_name = CONFIG["TARGET_BUSINESS"]   # "Tazapay Pte Ltd"
    email    = CONFIG["TARGET_ACCOUNT"]    # "onboarding+id@tazapay.com"
    print(f"\n  STEP 2: Switch Business → '{biz_name}' ({email})")

    for _goto_attempt in range(3):
        try:
            await page.goto("https://dashboard.xendit.co/home", wait_until="domcontentloaded", timeout=45000)
            break
        except Exception as _e:
            if _goto_attempt < 2:
                print(f"  ⚠️  goto home timeout (attempt {_goto_attempt+1}/3), retrying...")
                await page.wait_for_timeout(3000)
            else:
                raise
    # Wait for the sidebar nav to fully render before looking for the account button
    try:
        await page.wait_for_selector('nav, [class*="sidebar"], a[href*="/home"]', timeout=20000)
    except Exception:
        pass
    await page.wait_for_timeout(5000)

    # Find the account switcher button.
    # In the current Xendit UI it is the bottom-left sidebar element showing
    # the business name + "Live Mode". Also try legacy top-nav selectors.
    profile_sel = None
    profile_candidates = [
        # Bottom-left sidebar account info (current UI)
        '[class*="BusinessInfo"]',
        '[class*="business-info"]',
        '[class*="SidebarFooter"] button',
        '[class*="sidebar-footer"] button',
        '[class*="sidebar"] [class*="account"]',
        '[class*="sidebar"] [class*="business"]',
        # Legacy top-nav selectors
        '[data-testid="profile"]',
        '[data-testid="user-profile"]',
        '[data-testid="avatar"]',
        '[aria-label*="profile" i]',
        '[aria-label*="account" i]',
        '[aria-label*="user" i]',
        '[class*="profile-btn"]',
        '[class*="user-menu"]',
        '[class*="avatar"]',
        'button[class*="profile"]',
        'img[alt*="avatar" i]',
        'img[alt*="profile" i]',
    ]
    for cand in profile_candidates:
        try:
            el = page.locator(cand).first
            if await el.count() > 0:
                await el.wait_for(state="visible", timeout=15000)
                profile_sel = cand
                print(f"  ✅  Profile button found: {cand}")
                break
        except Exception:
            pass

    if not profile_sel:
        # JS fallback: look for bottom-left sidebar account element first, then top-right
        profile_sel_js = await page.evaluate("""
            () => {
                const h = window.innerHeight, w = window.innerWidth;
                // 1. Bottom-left sidebar area (current Xendit UI)
                for (const el of document.querySelectorAll(
                    'button, [role="button"], div, a'
                )) {
                    if (!el.offsetParent) continue;
                    const rect = el.getBoundingClientRect();
                    const txt = (el.innerText || el.getAttribute('aria-label') || '').trim();
                    if (rect.bottom > h * 0.7 && rect.left < 280 && txt.length > 2 && txt.length < 80) {
                        return 'bottom-left';
                    }
                }
                // 2. Top-right nav (legacy UI)
                for (const el of document.querySelectorAll('button, [role="button"], a')) {
                    if (!el.offsetParent) continue;
                    const rect = el.getBoundingClientRect();
                    if (rect.top < 100 && rect.right > w * 0.6) {
                        const t = (el.innerText || el.getAttribute('aria-label') || '').trim();
                        if (t.length < 50) return 'top-right';
                    }
                }
                return null;
            }
        """)
        if not profile_sel_js:
            print("  ❌  Profile button not found.")
            await ss(page, "fail_profile_btn")
            return False
        # Click the matching element
        await page.evaluate(f"""
            () => {{
                const h = window.innerHeight, w = window.innerWidth;
                const region = '{profile_sel_js}';
                for (const el of document.querySelectorAll('button, [role="button"], div, a')) {{
                    if (!el.offsetParent) continue;
                    const rect = el.getBoundingClientRect();
                    const txt = (el.innerText || el.getAttribute('aria-label') || '').trim();
                    if (region === 'bottom-left') {{
                        if (rect.bottom > h * 0.7 && rect.left < 280 && txt.length > 2 && txt.length < 80) {{
                            el.click(); return;
                        }}
                    }} else {{
                        if (rect.top < 100 && rect.right > w * 0.6) {{
                            el.click(); return;
                        }}
                    }}
                }}
            }}
        """)
        await page.wait_for_timeout(1500)

    # Open profile menu and wait for Switch Business option
    for attempt in range(5):
        if profile_sel:
            try:
                await page.click(profile_sel)
            except Exception:
                await page.evaluate(f"""
                    () => document.querySelector('{profile_sel}')?.click()
                """)
        await page.wait_for_timeout(1500)
        found = await page.evaluate("""
            () => Array.from(document.querySelectorAll('*'))
                       .some(el => el.textContent.trim() === 'Switch Business')
        """)
        if found:
            break
        print(f"  ↺  Profile menu attempt {attempt+1}/5...")

    # Click Switch Business
    await page.evaluate("""
        () => {
            const el = Array.from(document.querySelectorAll('*'))
                            .find(e => e.textContent.trim() === 'Switch Business');
            if (el) el.click();
        }
    """)
    # Wait longer for switcher panel to load in headless CI
    await page.wait_for_timeout(5000)
    await ss(page, "A2_switch_panel")

    # Search by business name first, then email as fallback
    # Try each term; scroll down and retry if not found on first pass
    async def try_click_account(search_term):
        for scroll_pass in range(5):
            clicked = await page.evaluate(f"""
                () => {{
                    const tgt = '{search_term}'.toLowerCase();
                    const all = Array.from(document.querySelectorAll('*'));
                    for (const el of all) {{
                        const txt = el.textContent.toLowerCase().trim();
                        if (txt.includes(tgt) && el.children.length < 6
                            && el.offsetParent !== null) {{
                            el.click();
                            return true;
                        }}
                    }}
                    return false;
                }}
            """)
            if clicked:
                return True
            # Scroll the switcher panel down and retry
            await page.evaluate("""
                () => {
                    const p = document.querySelector('[role="dialog"]')
                           || document.querySelector('[class*="switch"]')
                           || document.querySelector('[class*="modal"]');
                    if (p) p.scrollTop += 300;
                }
            """)
            await page.wait_for_timeout(1200)
        return False

    clicked = await try_click_account(biz_name)
    if not clicked:
        print(f"  ⚠️  '{biz_name}' not found, trying email: {email}")
        clicked = await try_click_account(email)

    if not clicked:
        print(f"  ❌  Neither '{biz_name}' nor '{email}' found in switcher.")
        await ss(page, "fail_switch")
        return False

    await page.wait_for_timeout(4000)
    print(f"  ✅  Switched to: {biz_name} ({email})")
    await ss(page, "A2_switched")
    return True


# ══════════════════════════════════════════════════════════════════════
#  PART A — XENDIT: TRANSACTIONS EXPORT
# ══════════════════════════════════════════════════════════════════════
async def xendit_export(page, context) -> bool:
    print("\n  STEP 3: Transactions Export")

    try:
        await page.goto("https://dashboard.xendit.co/transactions-new",
                        wait_until="domcontentloaded", timeout=45000)
    except Exception as _ge:
        print(f"  ⚠️  transactions-new goto error: {_ge}")
    await page.wait_for_timeout(5000)
    await dismiss_feedback_modal(page)
    await recover_unexpected_page(page, "txn")
    await ss(page, "A3_txn_page")

    for attempt in range(4):
        try:
            await page.wait_for_selector('[role="tab"]', timeout=20000)
            print(f"  ✅  [role=tab] found (attempt {attempt+1})")
            break
        except Exception:
            print(f"  ⚠️  [role=tab] not found (attempt {attempt+1}/4) — reloading...")
            await ss(page, f"A3_tab_wait_fail_{attempt+1}")
            if attempt < 3:
                await page.reload(wait_until="domcontentloaded")
                await page.wait_for_timeout(6000)
            else:
                print("  ❌  Tabs never appeared — aborting Part A")
                return False

    try:
        tab = page.get_by_role("tab", name="Transactions", exact=True)
        await tab.wait_for(timeout=10000)
        await tab.click()
        await page.wait_for_timeout(3000)
        print("  ✅  Transactions tab")
    except Exception as e:
        print(f"  ❌  Tab error: {e}")
        return False

    try:
        await page.wait_for_selector("button:has-text('Export'):not([disabled])", timeout=15000)
    except Exception:
        pass

    # Open modal
    try:
        await dismiss_feedback_modal(page)
        export_btn = page.locator("button:has-text('Export')").first
        await export_btn.wait_for(timeout=10000)
        await export_btn.click()
        await page.wait_for_selector('[role="dialog"]', timeout=10000)
        await page.wait_for_timeout(1500)
        print("  ✅  Export modal opened")
    except Exception as e:
        print(f"  ❌  Modal error: {e}")
        return False

    # ── Select ALL 28 columns ─────────────────────────────────────────
    print(f"  ☑️   Selecting ALL 28 columns...")

    trigger = page.locator('#export-tour button').first
    if await trigger.count() > 0:
        # Open the column selector panel
        await page.evaluate("() => document.querySelector('#export-tour button')?.click()")
        for _ in range(20):
            n = await page.evaluate(
                "() => [...document.querySelectorAll('input[type=\"checkbox\"]')]"
                ".filter(el => el.offsetParent !== null).length"
            )
            if n > 0:
                break
            await page.wait_for_timeout(250)

        # Check ALL checkboxes (no exclusions)
        await page.evaluate("""
            () => {
                const cbs = [...document.querySelectorAll('input[type="checkbox"]')]
                             .filter(cb => cb.offsetParent !== null);
                cbs.forEach(cb => { if (!cb.checked) cb.click(); });
            }
        """)
        await page.wait_for_timeout(800)

        # Count final selected
        selected_count = await page.evaluate("""
            () => [...document.querySelectorAll('input[type="checkbox"]')]
                   .filter(cb => cb.offsetParent !== null && cb.checked).length
        """)
        print(f"  ☑️   {selected_count} columns selected")

        # Close the column selector panel
        await page.evaluate("() => document.querySelector('#export-tour button')?.click()")
        await page.wait_for_timeout(400)

    # Set dates
    final = await set_dates(page)
    if final.get("from") != f"{CONFIG['FROM_MONTH']:02d}/{CONFIG['FROM_DAY']:02d}/{CONFIG['FROM_YEAR']}":
        await set_dates(page, prefer_ddmm=True)

    # Export email - force the modal to keep only the configured recipient
    exp_email = CONFIG["EXPORT_EMAIL"]
    email_ok = await ensure_only_export_email(
        page,
        exp_email,
        [
            "#new-transactions-export-text-area",
            'input[type="email"]',
            'textarea[id*="email" i]',
            'input[name*="email" i]',
            'input[placeholder*="email" i]',
        ],
    )
    if not email_ok:
        print("  ❌  Export email could not be set correctly")
        return False

    # Scroll to bottom
    await page.evaluate("""
        () => {
            const d = document.querySelector('[role="dialog"]');
            if (d) { (d.querySelector('.modal-body') || d).scrollTop = 99999; }
        }
    """)
    await page.wait_for_timeout(600)

    # Wait for Send button
    send_btn = page.locator('[role="dialog"] button:has-text("Send to Email")').first
    try:
        await send_btn.wait_for(timeout=5000)
    except Exception:
        print("  ❌  Send button not found")
        return False

    await ss(page, "A3_before_send")

    send_enabled = False
    for _ in range(20):
        try:
            if await send_btn.is_enabled():
                send_enabled = True
                break
        except Exception:
            pass
        await page.wait_for_timeout(500)

    if not send_enabled:
        await set_dates(page, prefer_ddmm=True)
        await page.wait_for_timeout(600)
        for _ in range(20):
            try:
                if await send_btn.is_enabled():
                    send_enabled = True
                    break
            except Exception:
                pass
            await page.wait_for_timeout(500)

    if not send_enabled:
        print("  ❌  Send button disabled")
        return False

    try:
        await send_btn.scroll_into_view_if_needed()
        await send_btn.click(timeout=5000)
    except Exception:
        await page.evaluate("""
            () => {
                const btn = [...document.querySelectorAll('[role="dialog"] button')]
                  .find(b => /Send to Email/i.test(b.innerText));
                if (btn && !btn.disabled) btn.click();
            }
        """)

    await page.wait_for_timeout(3000)
    print("  🎉  Export sent!")
    await ss(page, "A3_sent")

    try:
        okay = page.locator('button:has-text("Okay")').first
        await okay.wait_for(state="visible", timeout=6000)
        await okay.click()
        await page.wait_for_timeout(500)
    except Exception:
        await close_modal(page)

    return True


# ══════════════════════════════════════════════════════════════════════
#  PART B / D — GMAIL IMAP DOWNLOAD
# ══════════════════════════════════════════════════════════════════════
def _extract_s3_url(html: str) -> str | None:
    """Extract an S3 pre-signed CSV download URL from email HTML."""
    import re
    # Find href containing s3 and .csv
    patterns = [
        r'href=["\']([^"\']*s3[^"\']+\.csv[^"\']*)["\']',
        r'href=["\']([^"\']*amazonaws[^"\']+)["\']',
        r'href=["\']([^"\']*download[^"\']+\.csv[^"\']*)["\']',
        r'"(https://[^"]*s3[^"]*\.csv[^"]*)"',
        r"'(https://[^']*s3[^']*\.csv[^']*)'",
    ]
    for pat in patterns:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            url = m.group(1).replace("&amp;", "&").replace("&#38;", "&")
            return url
    return None


def _download_url_to_file(url: str, save_path: str) -> bool:
    """Download a URL directly to a file using urllib."""
    import urllib.request
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            with open(save_path, "wb") as f:
                f.write(resp.read())
        size = os.path.getsize(save_path)
        print(f"  ✅  Saved → {save_path}  ({size:,} bytes)")
        upload_to_s3(save_path)   # upload xendit_ALL_* and xp_activity_* files to S3
        return True
    except Exception as e:
        print(f"  ❌  URL download failed: {e}")
        return False


def get_latest_imap_uid() -> int:
    '''
    Connect to Gmail, return the highest UID currently in INBOX.
    Used to mark a 'before' point so Part D only downloads emails
    that arrive AFTER Part C's export was submitted.
    '''
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        mail.login(CONFIG["GMAIL_EMAIL"], CONFIG["GMAIL_APP_PASSWORD"].strip())
        mail.select("INBOX")
        status, data = mail.uid("search", None, "ALL")
        mail.logout()
        if status == "OK" and data[0]:
            uids = data[0].split()
            highest = int(uids[-1]) if uids else 0
            print(f"  📬  Current inbox highest UID: {highest}")
            return highest
    except Exception as e:
        print(f"  ⚠️  Could not get latest UID: {e}")
    return 0


def download_from_gmail_imap(label: str, wait_seconds: int = 60,
                              after_uid: int = 0,
                              subject_must: str = "",
                              subject_exclude: str = "") -> str | None:
    '''
    Connect to Gmail via IMAP, find the latest Xendit export email,
    extract the S3 download link, and save the CSV to DOWNLOAD_DIR.

    after_uid       — only consider emails with UID > this value.
    subject_must    — if set, email subject must contain this string (case-insensitive).
    subject_exclude — if set, skip emails whose subject contains this string.

    Part B uses subject_must="transactions report", subject_exclude="xenplatform"
    Part D uses subject_must="xenplatform" (or just after_uid filtering)
    '''
    print(f"\n{'='*65}")
    print(f"  GMAIL IMAP — Download [{label}]  (after_uid={after_uid})")
    if subject_must:
        print(f"  🔍  subject_must='{subject_must}'")
    if subject_exclude:
        print(f"  🚫  subject_exclude='{subject_exclude}'")
    print(f"{'='*65}")

    app_password = CONFIG["GMAIL_APP_PASSWORD"].strip()
    if not app_password:
        print("  ❌  GMAIL_APP_PASSWORD not set.")
        return None

    import time

    import re as _re

    # Poll every 15 s until we find a new email OR time runs out
    deadline = time.time() + wait_seconds
    poll_interval = 15
    saved_path = None
    first_wait = True

    while True:
        # ── Wait before each check ─────────────────────────────────────
        now = time.time()
        if first_wait:
            wait_now = min(poll_interval, max(deadline - now, 1))
            print(f"  ⏳  Waiting {int(wait_now)}s then checking inbox...")
            time.sleep(wait_now)
            first_wait = False
        elif now > deadline:
            print(f"  ⚠️  Timeout — no new email found after {wait_seconds}s")
            break

        try:
            print(f"  📬  Connecting to Gmail IMAP ({CONFIG['GMAIL_EMAIL']})...")
            mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
            mail.login(CONFIG["GMAIL_EMAIL"], app_password)

            # Search INBOX first, then All Mail (catches promotions/spam tabs too)
            uid_list = []
            folders_to_search = ["INBOX", '"[Gmail]/All Mail"', '"[Gmail]/Spam"']

            for folder in folders_to_search:
                try:
                    rv, _ = mail.select(folder)
                    if rv != "OK":
                        continue

                    # ── UID-based search so we can filter by after_uid ─────
                    for term in [b'FROM "xendit.co"', b'SUBJECT "Transactions Report"',
                                 b'SUBJECT "xendit"', b'SUBJECT "report"']:
                        try:
                            st, dat = mail.uid("search", None, term)
                            if st == "OK" and dat[0]:
                                raw_ids = dat[0].split()
                                if after_uid > 0:
                                    raw_ids = [u for u in raw_ids if int(u) > after_uid]
                                if raw_ids:
                                    print(f"  🔍  [{folder}] {term!r} → {len(raw_ids)} new UID(s)")
                                    uid_list = raw_ids
                                    break
                        except Exception:
                            pass

                    if not uid_list:
                        # Fallback: all emails with UID > after_uid in this folder
                        st, dat = mail.uid("search", None, "ALL")
                        if st == "OK" and dat[0]:
                            all_uids = dat[0].split()
                            if after_uid > 0:
                                all_uids = [u for u in all_uids if int(u) > after_uid]
                            if all_uids:
                                uid_list = all_uids[-10:]
                                print(f"  🔍  [{folder}] fallback: {len(uid_list)} email(s) after UID {after_uid}")

                    if uid_list:
                        break  # Found emails — stop searching folders

                except Exception as folder_err:
                    print(f"  ⚠️  Folder {folder}: {folder_err}")
                    continue

            if not uid_list:
                mail.logout()
                remaining = deadline - time.time()
                if remaining <= 0:
                    print(f"  ⚠️  No new emails found — timeout")
                    break
                wait_now = min(poll_interval, remaining)
                print(f"  ⏳  No new email yet — waiting {int(wait_now)}s more...")
                time.sleep(wait_now)
                continue

            # ── Scan from newest to oldest ─────────────────────────────
            for uid in reversed(uid_list):
                try:
                    st, msg_data = mail.uid("fetch", uid, "(RFC822)")
                    if st != "OK" or not msg_data or not msg_data[0]:
                        continue
                    raw = msg_data[0][1]
                    msg = email_lib.message_from_bytes(raw)

                    subject_raw = msg.get("Subject", "")
                    subject = "".join(
                        p.decode(e or "utf-8", errors="replace") if isinstance(p, bytes) else str(p)
                        for p, e in decode_header(subject_raw)
                    )
                    sender = msg.get("From", "")
                    date   = msg.get("Date", "")
                    print(f"  📧  UID={uid.decode()} [{date[:16]}] '{subject}' | {sender[:40]}")

                    is_xendit = any(kw in subject.lower() or kw in sender.lower()
                                    for kw in ["xendit", "export", "transaction", "report",
                                               "xenplatform"])
                    if not is_xendit:
                        continue

                    # Subject-based filtering
                    subj_lower = subject.lower()
                    if subject_must and subject_must.lower() not in subj_lower:
                        print(f"  ⏭️  Skip (subject doesn't contain '{subject_must}'): '{subject}'")
                        continue
                    if subject_exclude and subject_exclude.lower() in subj_lower:
                        print(f"  ⏭️  Skip (subject contains '{subject_exclude}'): '{subject}'")
                        continue

                    # Strategy 1: CSV attachment
                    for part in msg.walk():
                        disp  = str(part.get("Content-Disposition", ""))
                        ctype = part.get_content_type()
                        if "attachment" in disp or "csv" in ctype or "spreadsheet" in ctype:
                            fname = part.get_filename() or f"xendit_{label}_export.csv"
                            fname = "".join(
                                p.decode(e or "utf-8", errors="replace") if isinstance(p, bytes) else str(p)
                                for p, e in decode_header(fname)
                            )
                            save_path = os.path.join(CONFIG["DOWNLOAD_DIR"], f"{label}_{fname}")
                            with open(save_path, "wb") as f:
                                f.write(part.get_payload(decode=True))
                            size = os.path.getsize(save_path)
                            print(f"  ✅  Attachment saved → {save_path}  ({size:,} B)")
                            saved_path = save_path
                            break

                    if saved_path:
                        break

                    # Strategy 2: S3 pre-signed link in HTML body
                    for part in msg.walk():
                        if part.get_content_type() == "text/html":
                            html = part.get_payload(decode=True).decode("utf-8", errors="replace")
                            s3_url = _extract_s3_url(html)
                            if s3_url:
                                print(f"  🔗  S3 URL: {s3_url[:80]}...")
                                fname_m = _re.search(r'/([^/?]+\.csv)', s3_url)
                                fname   = fname_m.group(1) if fname_m else f"xendit_{label}_export.csv"
                                save_path = os.path.join(CONFIG["DOWNLOAD_DIR"], f"{label}_{fname}")
                                if _download_url_to_file(s3_url, save_path):
                                    saved_path = save_path
                            else:
                                html_path = os.path.join(CONFIG["DOWNLOAD_DIR"], f"{label}_email.html")
                                with open(html_path, "w", encoding="utf-8") as f:
                                    f.write(html)
                                print(f"  💾  No S3 link — HTML saved → {html_path}")
                                saved_path = html_path
                            break

                    if saved_path:
                        break

                except Exception as ex:
                    print(f"  ⚠️  Email read error: {ex}")
                    continue

            mail.logout()
            print("  ✅  IMAP disconnected")

            if saved_path:
                break  # done!

            # No suitable email found in this pass — retry if time remains
            remaining = deadline - time.time()
            if remaining <= 0:
                print("  ⚠️  Email arrived but no downloadable content found")
                break
            wait_now = min(poll_interval, remaining)
            print(f"  ⏳  No downloadable email yet — waiting {int(wait_now)}s...")
            time.sleep(wait_now)

        except imaplib.IMAP4.error as e:
            print(f"  ❌  IMAP auth error: {e}")
            break
        except Exception as e:
            print(f"  ❌  IMAP error: {e}")
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            time.sleep(min(10, remaining))

    return saved_path


# ══════════════════════════════════════════════════════════════════════
#  PART C — XENPLATFORM (inside Xendit dashboard: Apps & Partners)
# ══════════════════════════════════════════════════════════════════════
def extract_unique_business_ids(csv_path: str) -> list[dict]:
    """
    Read ALL XenPlatform CSVs in the download folder and return ALL unique
    Business ID / Business Name pairs ever seen — not just today's file.
    This ensures businesses with no transactions today still get exported.
    """
    download_dir = CONFIG.get("DOWNLOAD_DIR", "")
    seen = {}   # business_id -> business_name

    # Collect all xenplatform CSVs (historical + today's)
    candidate_files = []
    if download_dir and os.path.isdir(download_dir):
        for fn in os.listdir(download_dir):
            if fn.lower().startswith("xenplatform") and fn.lower().endswith(".csv"):
                candidate_files.append(os.path.join(download_dir, fn))
    # Also include the explicit csv_path passed in (today's file)
    if csv_path and os.path.exists(csv_path) and csv_path not in candidate_files:
        candidate_files.append(csv_path)

    if not candidate_files:
        return []

    for fpath in candidate_files:
        try:
            with open(fpath, "r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    business_id = (row.get("Business ID") or "").strip()
                    business_name = (row.get("Business Name") or "").strip()
                    if business_id and business_id not in seen:
                        seen[business_id] = business_name
        except Exception:
            pass

    items = [{"business_id": bid, "business_name": name} for bid, name in seen.items()]
    print(f"  📊  Business IDs found across {len(candidate_files)} xenplatform file(s): {len(items)} unique")
    return items


async def xp_search_business_id(page, business_id: str, business_name: str = "") -> bool:
    search_selectors = [
        'input[type="search"]',
        'input[placeholder*="name, id or email" i]',
        'input[placeholder*="search" i]',
        'input[aria-label*="search" i]',
        '[data-testid*="search" i] input',
        'input[name*="search" i]',
    ]
    search_box = None

    for sel in search_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.wait_for(state="visible", timeout=3000)
                search_box = loc
                break
        except Exception:
            pass

    if not search_box:
        print(f"    Search input not found for Business ID {business_id}")
        return False

    async def _search_once(value: str):
        if not value:
            return
        try:
            await search_box.scroll_into_view_if_needed()
            await search_box.click()
            await search_box.fill("")
            await search_box.fill(value)
            await page.keyboard.press("Enter")
        except Exception:
            await page.evaluate("""
                ({ selectors, value }) => {
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (!el || el.offsetParent === null) continue;
                        const set = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
                        el.focus();
                        if (set) set.call(el, '');
                        el.value = '';
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        if (set) set.call(el, value);
                        el.value = value;
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                        return true;
                    }
                    return false;
                }
            """, {"selectors": search_selectors, "value": value})
            await page.keyboard.press("Enter")
        await page.wait_for_timeout(2200)

    async def _visible_results_state():
        return await page.evaluate("""
            ({ bid, bname }) => {
                const norm = v => (v || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                const bodyRows = Array.from(document.querySelectorAll('tbody tr'))
                    .filter(row => row.offsetParent !== null);
                const genericRows = Array.from(document.querySelectorAll('[role="row"]'))
                    .filter(row => row.offsetParent !== null && row.querySelector('button, [role="button"]'));
                const rows = bodyRows.length ? bodyRows : genericRows;
                const rowTexts = rows.map(row => norm(row.innerText || ''));
                return {
                    row_count: rows.length,
                    id_match: rowTexts.some(text => text.includes(norm(bid))),
                    name_match: !!bname && rowTexts.some(text => text.includes(norm(bname))),
                };
            }
        """, {"bid": business_id, "bname": business_name})

    await _search_once(business_id)
    state = await _visible_results_state()
    if state.get("id_match") or state.get("row_count", 0) > 0:
        print(f"    Search narrowed to {state.get('row_count', 0)} visible row(s)")
        return True

    if business_name:
        print(f"    Retrying search with business name: {business_name}")
        await _search_once(business_name)
        state = await _visible_results_state()
        if state.get("name_match") or state.get("row_count", 0) > 0:
            print(f"    Search narrowed to {state.get('row_count', 0)} visible row(s)")
            return True

    print(f"    Search results did not clearly narrow for Business ID {business_id}")
    return False

async def xp_open_view_activity(page, context, business_id: str, business_name: str = ""):
    print(f"    Opening row menu for {business_id}...")
    menu_opened = False

    row = page.locator("tbody tr").filter(has_text=business_id).first
    try:
        if await row.count() == 0 and business_name:
            row = page.locator("tbody tr").filter(has_text=business_name).first
        if await row.count() == 0:
            row = page.locator("tbody tr").first
        await row.wait_for(state="visible", timeout=5000)

        candidates = [
            row.locator("button, [role='button']").last,
            row.locator("svg").last,
            row.locator("[aria-label*='more' i], [aria-label*='action' i], [aria-label*='menu' i]").last,
            row.locator("td, div").last,
        ]
        for candidate in candidates:
            try:
                if await candidate.count() > 0:
                    await candidate.scroll_into_view_if_needed()
                    await candidate.click(force=True, timeout=2000)
                    menu_opened = True
                    break
            except Exception:
                pass

        if not menu_opened:
            box = await row.bounding_box()
            if box:
                click_x = box["x"] + box["width"] - 20
                click_y = box["y"] + (box["height"] / 2)
                await page.mouse.click(click_x, click_y)
                menu_opened = True
    except Exception:
        menu_opened = False

    if not menu_opened:
        print(f"    Could not open row menu for {business_id}")
        return None

    await page.wait_for_timeout(1200)
    previous_url = page.url
    page_count_before = len(context.pages)
    clicked = False

    for txt in ["View activity", "View Activity"]:
        try:
            item = page.locator(f'text="{txt}"').first
            if await item.count() > 0:
                await item.click()
                clicked = True
                await page.wait_for_timeout(1800)
                break
        except Exception:
            pass

    if not clicked:
        try:
            clicked = bool(await page.evaluate("""
                () => {
                    const all = Array.from(document.querySelectorAll('a, button, [role="menuitem"], [role="button"], li, div, span'));
                    for (const el of all) {
                        const txt = (el.innerText || '').trim().toLowerCase();
                        if (txt === 'view activity' && el.offsetParent !== null) {
                            el.click();
                            return true;
                        }
                    }
                    return false;
                }
            """))
        except Exception:
            clicked = False

    if not clicked:
        print(f"    View activity menu item not found for {business_id}")
        return None

    for _ in range(30):
        if len(context.pages) > page_count_before:
            activity_page = context.pages[-1]
            try:
                await activity_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            print("    Activity opened in a new tab")
            return activity_page
        if page.url != previous_url:
            print(f"    Activity opened in same tab: {page.url}")
            return page
        try:
            export_btn = page.locator('button:has-text("Export")').first
            search_box = page.locator('input[type="search"], input[placeholder*="search" i]').first
            if (await export_btn.count() > 0 and await export_btn.is_visible()) or (await search_box.count() > 0 and await search_box.is_visible()):
                print("    Activity page is ready")
                return page
        except Exception:
            pass
        await page.wait_for_timeout(400)

    print(f"    Activity page did not open for {business_id}")
    return None

async def xp_select_all_export_columns(page) -> int:
    trigger_selectors = [
        '[role="dialog"] button:has-text("Additional column")',
        '[role="dialog"] button:has-text("Additional columns")',
        '[role="dialog"] button:has-text("Columns")',
        '[role="dialog"] button:has-text("Select columns")',
        '[role="dialog"] button:has-text("Choose columns")',
        '[data-testid="modal"] button:has-text("Additional column")',
        '[data-testid="modal"] button:has-text("Additional columns")',
        '[data-testid="modal"] button:has-text("Columns")',
        '[aria-label*="additional column" i]',
        '[aria-label*="columns" i]',
        '#export-tour button',
    ]
    trigger = None

    for sel in trigger_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible():
                trigger = loc
                break
        except Exception:
            pass

    if trigger:
        try:
            await trigger.click(force=True)
            await page.wait_for_timeout(1000)
        except Exception:
            pass
    else:
        try:
            await page.evaluate("""
                () => {
                    const all = Array.from(document.querySelectorAll('button, [role="button"], div, span, a'));
                    for (const el of all) {
                        const txt = ((el.innerText || '') + ' ' + (el.getAttribute('aria-label') || '')).trim().toLowerCase();
                        if ((txt.includes('additional column') || txt.includes('additional columns') || txt === 'columns') && el.offsetParent !== null) {
                            el.click();
                            return true;
                        }
                    }
                    return false;
                }
            """)
            await page.wait_for_timeout(1000)
        except Exception:
            pass

    # Wait for checkboxes to appear after trigger (up to 3 seconds)
    for _ in range(12):
        n = await page.evaluate(
            "() => [...document.querySelectorAll('input[type=\"checkbox\"]')]"
            ".filter(el => el.offsetParent !== null).length"
        )
        if n > 0:
            break
        await page.wait_for_timeout(250)

    checked = await page.evaluate("""
        () => {
            const roots = [
                document.querySelector('[data-testid="modal"]'),
                document.querySelector('[role="dialog"]'),
                document.querySelector('.modal-content'),
                document,
            ].filter(Boolean);

            const visible = el => !!el && (el.offsetParent !== null || el.getClientRects().length > 0);
            let bestRoot = roots[0] || document;
            let bestBoxes = [];
            for (const root of roots) {
                const boxes = Array.from(root.querySelectorAll('input[type="checkbox"]')).filter(visible);
                if (boxes.length > bestBoxes.length) {
                    bestBoxes = boxes;
                    bestRoot = root;
                }
            }

            const clickEl = el => {
                if (!el) return false;
                try { el.click(); } catch (e) {}
                return true;
            };

            const labels = Array.from(bestRoot.querySelectorAll('label, span, div')).filter(visible);
            const selectAllLabel = labels.find(el => (el.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase() === 'select all');
            if (selectAllLabel) {
                clickEl(selectAllLabel);
            }

            const selectAllCheckbox = Array.from(bestRoot.querySelectorAll('input[type="checkbox"]')).find(cb => {
                if (!visible(cb)) return false;
                const wrap = cb.closest('label, div, span');
                const txt = ((wrap?.innerText) || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                return txt.includes('select all');
            });
            if (selectAllCheckbox && !selectAllCheckbox.checked) {
                clickEl(selectAllCheckbox);
                if (!selectAllCheckbox.checked) clickEl(selectAllCheckbox.closest('label') || selectAllCheckbox.parentElement);
            }

            const boxes = Array.from(bestRoot.querySelectorAll('input[type="checkbox"]')).filter(visible);
            let count = 0;
            for (const cb of boxes) {
                if (!cb.checked) cb.click();
                if (!cb.checked) {
                    const label = cb.closest('label') || cb.parentElement;
                    if (label) label.click();
                }
                if (cb.checked) count++;
            }
            return count;
        }
    """)
    await page.wait_for_timeout(800)

    print(f"    Selected {checked} visible export columns")
    return checked

async def xp_submit_activity_export(page, business_id: str) -> bool:
    print(f"    Opening export for {business_id}...")
    await ss(page, f"E2a_before_transactions_{business_id[:8]}")

    # Wait for the subtab switcher to appear before trying to click Transactions
    try:
        await page.wait_for_selector('.xp-activity-subtab-switcher', timeout=12000)
        print(f"    Subtab switcher found")
    except Exception:
        print(f"    ⚠️  Subtab switcher not found within 12s — retrying after page wait...")
        await page.wait_for_timeout(3000)

    transactions_ready = False
    transaction_selectors = [
        '.xp-activity-subtab-switcher [data-testid="tab-switcher-item-1"]',
        '.xp-activity-subtab-switcher .tab-switcher-item-container:nth-of-type(2)',
        '.xp-activity-subtab-switcher .tab-switcher-item:has-text("Transactions")',
        'xpath=//*[normalize-space()="Activity"]/following::*[@data-testid="tab-switcher-item-1"][1]',
        'xpath=//*[normalize-space()="Activity"]/following::*[normalize-space()="Transactions"][1]',
    ]
    # Retry tab click up to 3 times (headless may need extra time)
    for _tab_attempt in range(3):
        for sel in transaction_selectors:
            try:
                tab = page.locator(sel).first
                if await tab.count() > 0 and await tab.is_visible():
                    await tab.scroll_into_view_if_needed()
                    await tab.click(force=True)
                    transactions_ready = True
                    print(f"    Transactions tab opened via: {sel} (attempt {_tab_attempt+1})")
                    break
            except Exception:
                pass
        if transactions_ready:
            break
        # JS fallback
        try:
            transactions_ready = await page.evaluate("""
                () => {
                    const visible = el => !!el && (el.offsetParent !== null || el.getClientRects().length > 0);
                    const switcher = document.querySelector('.xp-activity-subtab-switcher')
                        || Array.from(document.querySelectorAll('div')).find(el => visible(el) && /balance.*transactions/i.test(el.innerText || ''));
                    if (!switcher) return false;
                    const exact = switcher.querySelector('[data-testid="tab-switcher-item-1"]')
                        || switcher.querySelector('.tab-switcher-item-container:nth-of-type(2)')
                        || Array.from(switcher.querySelectorAll('div, button, a, span')).find(el => visible(el) && (el.innerText || '').trim().toLowerCase() === 'transactions');
                    if (exact && visible(exact)) { exact.click(); return true; }
                    return false;
                }
            """)
        except Exception:
            transactions_ready = False
        if transactions_ready:
            print(f"    Transactions tab opened via JS (attempt {_tab_attempt+1})")
            break
        print(f"    ⚠️  Tab click attempt {_tab_attempt+1}/3 failed, waiting 2s...")
        await page.wait_for_timeout(2000)

    if not transactions_ready:
        print(f"    ❌  Could not switch to Transactions tab — aborting to avoid wrong export type")
        await ss(page, f"E2b_tab_fail_{business_id[:8]}")
        return False

    # Confirm tab switch took effect — wait for Transactions content
    try:
        await page.wait_for_selector('input[placeholder*="reference" i], input[placeholder*="search" i]', timeout=12000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)
    await ss(page, f"E2b_transactions_tab_{business_id[:8]}")

    # Wait for the toolbar to fully render in headless mode
    try:
        await page.wait_for_selector('button:has-text("Export"), [class*="toolbar"] button', timeout=15000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)

    export_opened = False

    strict_selectors = [
        'input[placeholder*="reference" i] >> xpath=ancestor::*[2]//button[normalize-space()="Export"]',
        'input[placeholder*="reference" i] >> xpath=ancestor::*[3]//button[normalize-space()="Export"]',
        'input[placeholder*="search" i] >> xpath=ancestor::*[2]//button[normalize-space()="Export"]',
        'input[placeholder*="search" i] >> xpath=ancestor::*[3]//button[normalize-space()="Export"]',
        'xpath=//input[contains(translate(@placeholder,"SEARCHBYREFERENCE","searchbyreference"),"search")]/ancestor::*[self::div or self::section][1]//button[normalize-space()="Export"]',
        'xpath=//*[contains(normalize-space(),"Search by Reference")]/ancestor::*[self::div or self::section][1]//button[normalize-space()="Export"]',
    ]
    for sel in strict_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0 and await btn.is_visible():
                await btn.scroll_into_view_if_needed()
                await btn.click(force=True)
                export_opened = True
                print(f"    Export opened via toolbar selector: {sel}")
                break
        except Exception:
            pass

    if not export_opened:
        try:
            export_opened = await page.evaluate("""
                () => {
                    const visible = el => !!el && (el.offsetParent !== null || el.getClientRects().length > 0);
                    const search = document.querySelector('input[placeholder*="reference" i]')
                        || document.querySelector('input[placeholder*="search" i]');
                    if (!search) return false;
                    let root = search.parentElement;
                    for (let i = 0; i < 6 && root; i++, root = root.parentElement) {
                        const buttons = Array.from(root.querySelectorAll('button, [role="button"]')).filter(visible);
                        for (const el of buttons) {
                            const txt = (el.innerText || '').trim().toLowerCase();
                            if (txt === 'export') {
                                el.click();
                                return true;
                            }
                        }
                    }
                    return false;
                }
            """)
        except Exception:
            export_opened = False

    if not export_opened:
        print(f"    Toolbar Export button not found on activity page for {business_id}")
        return False

    # Wait for the export modal to actually appear before proceeding
    modal_appeared = False
    for _ in range(20):
        has_modal = await page.evaluate("""
            () => !!(document.querySelector('[role="dialog"]') ||
                     document.querySelector('[data-testid="modal"]') ||
                     document.querySelector('.modal-content'))
        """)
        if has_modal:
            modal_appeared = True
            break
        await page.wait_for_timeout(300)
    if not modal_appeared:
        await page.wait_for_timeout(2000)
    # Wait for date input fields to render AND have pre-filled values (important for headless)
    for _rw_wait in range(20):
        rw_filled = await page.evaluate("""
            () => [...document.querySelectorAll('input[id^="rw_"]')]
                    .filter(el => el.offsetParent !== null &&
                                  /\d{2}\/\d{2}\/\d{4}/.test((el.value || '').trim()))
                    .length
        """)
        if rw_filled >= 2:
            break
        await page.wait_for_timeout(500)
    await ss(page, f"E2c_export_open_{business_id[:8]}")

    # Set date range using the calendar icon → month navigation → day click.
    # This is format-agnostic and bypasses text input entirely.
    # fill()/el.value= approaches do not properly update react-widgets state.
    print(f"    Setting dates via calendar picker: {_from_date.strftime('%d %b %Y')} -> {_today.strftime('%d %b %Y')}")
    _MONTH_NAMES_E = [
        "January","February","March","April","May","June",
        "July","August","September","October","November","December"
    ]

    async def _e_pick_cal(btn_index: int, dd: int, mm: int, yy: int) -> bool:
        target_label = f"{_MONTH_NAMES_E[mm - 1]} {yy}"
        # Pre-check: if the date input already shows the correct value, skip calendar interaction
        target_str = f"{dd:02d}/{mm:02d}/{yy}"
        try:
            pre_vals = await page.evaluate("""
                () => [...document.querySelectorAll('input[id^="rw_"][id$="_input"]')]
                        .filter(el => el.offsetParent !== null).map(el => el.value)
            """)
            # date inputs are at even indices (0, 2) and times at odd (1, 3)
            date_vals = [pre_vals[i] for i in range(0, len(pre_vals), 2)]
            if btn_index < len(date_vals) and target_str in date_vals[btn_index]:
                print(f"    ✅  Calendar [{btn_index}] already correct: {target_str}")
                return True
        except Exception:
            pass
        # Click the calendar button for the btn_index-th DATE input only.
        # IMPORTANT: each DateTimePicker has TWO .rw-btn-select buttons (date + time).
        # Using btns[btn_index] would accidentally click the time clock button for btn_index=1.
        # Instead: find DATE inputs specifically (value looks like DD/MM/YYYY), then use
        # input-relative approach to get only the date calendar button (not the time button).
        clicked = await page.evaluate(f"""
            () => {{
                // Filter to inputs whose value looks like a date (contains '/' and a 4-digit year)
                const dateInputs = [...document.querySelectorAll('input[id^="rw_"]')]
                  .filter(b => b.offsetParent !== null &&
                               /\\d{{2}}\\/\\d{{2}}\\/\\d{{4}}/.test((b.value || '').trim()));
                const inp = dateInputs[{btn_index}];
                if (!inp) return false;
                // Navigate up to the widget-picker container and find its calendar button
                const widget = inp.closest('.rw-widget-picker')
                            || inp.closest('.rw-widget-container')
                            || inp.parentElement;
                const btn = widget && widget.querySelector('.rw-btn-select');
                if (btn) {{ btn.click(); return true; }}
                return false;
            }}
        """)
        if not clicked:
            print(f"    ⚠️  Calendar icon [{btn_index}] not found")
            return False
        await page.wait_for_timeout(700)
        # Navigate to target month
        for _ in range(36):
            cur = await page.evaluate(
                "() => document.querySelector('.rw-calendar-btn-view')?.textContent?.trim() || ''"
            )
            if not cur or target_label in cur:
                break
            parts = cur.strip().split()
            if len(parts) == 2:
                try:
                    c_mm = _MONTH_NAMES_E.index(parts[0]) + 1
                    c_yy = int(parts[1])
                except (ValueError, IndexError):
                    c_mm, c_yy = mm, yy
                nav = ".rw-calendar-btn-left" if (c_yy, c_mm) > (yy, mm) else ".rw-calendar-btn-right"
                await page.evaluate(f"() => {{ const b = document.querySelector('{nav}'); if(b) b.click(); }}")
                await page.wait_for_timeout(350)
            else:
                break
        # Click the target day using Playwright native click (so React events fire correctly)
        ok = False
        padded = f"{dd:02d}"
        try:
            all_cells = await page.locator('.rw-cell').all()
            for cell in all_cells:
                try:
                    cls = await cell.get_attribute("class") or ""
                    if "rw-state-disabled" in cls:
                        continue
                    t = (await cell.inner_text()).strip()
                    if t == padded:
                        await cell.click(timeout=3000)
                        ok = True
                        break
                except Exception:
                    continue
        except Exception as _ce:
            print(f"    ⚠️  Cell click error: {_ce}")
        await page.wait_for_timeout(700)
        if ok:
            print(f"    ✅  Calendar picked: {dd:02d} {_MONTH_NAMES_E[mm-1]} {yy}")
        else:
            print(f"    ⚠️  Day {dd} not clickable in {target_label}")
        return bool(ok)

    ok_from = await _e_pick_cal(0, _from_date.day, _from_date.month, _from_date.year)
    await page.wait_for_timeout(300)
    ok_to   = await _e_pick_cal(1, _today.day, _today.month, _today.year)
    await page.wait_for_timeout(300)
    print(f"    Calendar date set: from={ok_from}, to={ok_to}")

    # Time inputs are intentionally left untouched — FROM defaults to 12:00 AM,
    # TO defaults to the current end-of-day time set by the page.
    rw_vals = await page.evaluate("""
        () => [...document.querySelectorAll('input[id^="rw_"][id$="_input"]')]
              .filter(el => el.offsetParent !== null).map(el => el.value)
    """)
    print(f"    Date inputs after set: {rw_vals}")
    await page.wait_for_timeout(600)
    await ss(page, f"E2d_dates_set_{business_id[:8]}")

    await xp_select_all_export_columns(page)
    await page.wait_for_timeout(600)
    await ss(page, f"E2e_columns_selected_{business_id[:8]}")

    email_ok = await ensure_only_export_email(
        page,
        CONFIG["EXPORT_EMAIL"],
        [
            '#new-transactions-export-text-area',
            'input[type="email"]',
            'textarea[id*="email" i]',
            'input[name*="email" i]',
            'input[placeholder*="email" i]',
            '[contenteditable="true"]',
        ],
    )
    if not email_ok:
        print(f"    Export email could not be set for {business_id}")
        await ss(page, f"E2f_email_failed_{business_id[:8]}")
        return False

    await ss(page, f"E2f_email_set_{business_id[:8]}")
    await page.wait_for_timeout(600)
    await page.evaluate("""
        () => {
            const body = document.querySelector('.modal-body')
                      || document.querySelector('[data-testid="modal"]')
                      || document.querySelector('[role="dialog"]')
                      || document;
            body.scrollTop = body.scrollHeight;
        }
    """)
    await page.wait_for_timeout(800)

    for label in ["Send to Email", "Send To Email", "Send"]:
        try:
            btn = page.locator(f'button:has-text("{label}"), [role="button"]:has-text("{label}")').last
            if await btn.count() > 0 and await btn.is_visible():
                for _ in range(20):
                    try:
                        if await btn.is_enabled():
                            break
                    except Exception:
                        pass
                    await page.wait_for_timeout(300)
                await btn.scroll_into_view_if_needed()
                await btn.click(force=True)
                await page.wait_for_timeout(2500)
                print(f"    Export submitted via '{label}'")
                return True
        except Exception:
            pass

    clicked = await page.evaluate("""
        () => {
            const all = Array.from(document.querySelectorAll('button, [role="button"], a, div, span'));
            for (const label of ['send to email', 'send']) {
                for (const el of all) {
                    const txt = ((el.innerText || '') + ' ' + (el.getAttribute('aria-label') || '')).trim().toLowerCase();
                    if (txt.includes(label) && el.offsetParent !== null) {
                        el.click();
                        return label;
                    }
                }
            }
            return null;
        }
    """)
    if clicked:
        await page.wait_for_timeout(2500)
        print(f"    Export submitted via '{clicked}'")
        return True

    print(f"    Send button not found for {business_id}")
    return False


async def run_single_xp_account_download(target_business_id: str, business_name: str = ""):
    print(f"\n{'='*65}")
    print(f"  SINGLE XP ACCOUNT EXPORT -> {target_business_id}")
    print(f"{'='*65}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=CONFIG["HEADLESS"],
            slow_mo=CONFIG["SLOW_MO"],
            executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            args=["--disable-blink-features=AutomationControlled"],
        )
        context_kwargs = {
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "viewport": {"width": 1280, "height": 900},
            "locale": "en-US",
            "accept_downloads": True,
        }
        if os.path.exists(CONFIG["SESSION_STATE"]):
            context_kwargs["storage_state"] = CONFIG["SESSION_STATE"]
            print(f"  Reusing saved session: {CONFIG['SESSION_STATE']}")
        context = await browser.new_context(**context_kwargs)
        await context.add_init_script(STEALTH_JS)
        page = await context.new_page()

        downloaded_path = None
        uid_before_export = 0
        try:
            if not await do_login(page, context):
                raise RuntimeError("Xendit login failed")
            switched = False
            try:
                switched = await switch_account(page, context)
            except Exception as e:
                print(f"  switch_account exception: {e}")
                switched = False
            if not switched:
                print("  Skipping account switch for single-account run; using current saved-session context.")

            activity_url = os.environ.get("XP_ACTIVITY_URL") or f"https://dashboard.xendit.co/xenplatform/accounts/{target_business_id}/activity"
            await page.goto(activity_url, wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            hydration_selectors = [
                'text=Account information',
                '.xp-activity-subtab-switcher',
                '[data-testid="tab-switcher-item-1"]',
                'text=Activity',
            ]
            hydrated = False
            for sel in hydration_selectors:
                try:
                    await page.wait_for_selector(sel, timeout=15000)
                    print(f"  Activity page hydrated via: {sel}")
                    hydrated = True
                    break
                except Exception:
                    pass
            if not hydrated:
                await page.wait_for_timeout(10000)
            await dismiss_feedback_modal(page)
            await ss(page, f"single_E1_direct_activity_{target_business_id[:8]}")

            activity_page = page
            await dismiss_feedback_modal(activity_page)
            await ss(activity_page, f"single_E2_activity_{target_business_id[:8]}")

            if CONFIG["GMAIL_APP_PASSWORD"]:
                uid_before_export = await asyncio.get_event_loop().run_in_executor(None, get_latest_imap_uid)
                print(f"  Inbox watermark before single-account export: {uid_before_export}")

            if not await xp_submit_activity_export(activity_page, target_business_id):
                raise RuntimeError(f"Could not submit export for {target_business_id}")

            await ss(activity_page, f"single_E3_exported_{target_business_id[:8]}")

            if CONFIG["GMAIL_APP_PASSWORD"]:
                downloaded_path = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: download_from_gmail_imap(
                        f"xp_activity_{target_business_id[:8]}",
                        wait_seconds=240,
                        after_uid=uid_before_export,
                    )
                )
                print(f"  Downloaded -> {downloaded_path}")
            else:
                print("  Gmail download skipped - no app password configured")

            try:
                await activity_page.close()
            except Exception:
                pass
        finally:
            await browser.close()

        return downloaded_path


async def xenplatform_activity_exports(page, context, source_csv_path: str) -> dict:
    print(f"\n{'='*65}")
    print("  PART E - XenPlatform Activity Exports by Business ID")
    print(f"{'='*65}")

    business_rows = extract_unique_business_ids(source_csv_path)
    if not business_rows:
        print("  No Business ID values found in the XenPlatform CSV")
        return {"count": 0, "success": 0, "failed_ids": []}

    print(f"  Found {len(business_rows)} unique Business ID(s)")
    for row in business_rows:
        suffix = f" ({row['business_name']})" if row.get("business_name") else ""
        print(f"    - {row['business_id']}{suffix}")

    try:
        await page.evaluate("() => document.title")
    except Exception:
        page = await context.new_page()

    await page.goto("https://dashboard.xendit.co/xenplatform/accounts", wait_until="domcontentloaded")
    await page.wait_for_timeout(4000)

    success = 0
    failed_ids = []
    downloads = {}
    for idx, row in enumerate(business_rows, start=1):
        business_id = row["business_id"]
        business_name = row.get("business_name") or ""
        print(f"\n  [{idx}/{len(business_rows)}] Business ID: {business_id}  {business_name}")

        try:
            await page.goto("https://dashboard.xendit.co/xenplatform/accounts", wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)
            await dismiss_feedback_modal(page)

            await xp_search_business_id(page, business_id, business_name)
            await ss(page, f"E1_search_{business_id[:10]}")

            activity_page = await xp_open_view_activity(page, context, business_id, business_name)
            if not activity_page:
                failed_ids.append(business_id)
                downloads[business_id] = None
                continue

            await dismiss_feedback_modal(activity_page)
            await ss(activity_page, f"E2_activity_{business_id[:10]}")

            uid_before_export = 0
            if CONFIG["GMAIL_APP_PASSWORD"]:
                uid_before_export = await asyncio.get_event_loop().run_in_executor(
                    None, get_latest_imap_uid
                )
                print(f"    Inbox watermark before export: {uid_before_export}")

            if await xp_submit_activity_export(activity_page, business_id):
                await ss(activity_page, f"E3_exported_{business_id[:10]}")
                downloaded_path = None
                if CONFIG["GMAIL_APP_PASSWORD"]:
                    downloaded_path = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: download_from_gmail_imap(
                            f"xp_activity_{business_id[:8]}",
                            wait_seconds=240,
                            after_uid=uid_before_export,
                        )
                    )
                downloads[business_id] = downloaded_path
                success += 1
            else:
                failed_ids.append(business_id)
                downloads[business_id] = None

            if activity_page is not page:
                try:
                    await activity_page.close()
                except Exception:
                    pass
        except Exception as e:
            print(f"    Activity export failed for {business_id}: {e}")
            failed_ids.append(business_id)
            downloads[business_id] = None

    print(f"\n  Activity exports submitted: {success}/{len(business_rows)}")
    if failed_ids:
        print(f"  Failed Business IDs: {failed_ids}")

    return {
        "count": len(business_rows),
        "success": success,
        "failed_ids": failed_ids,
        "downloads": downloads,
    }

async def xenplatform_export(page, context) -> bool:  # noqa: C901
    print(f"\n{'='*65}")
    print("  PART C — XenPlatform Export  [precision rewrite]")
    print(f"{'='*65}")

    exp_email = CONFIG["EXPORT_EMAIL"]
    fm, fd, fy = CONFIG["FROM_MONTH"], CONFIG["FROM_DAY"], CONFIG["FROM_YEAR"]
    tm, td, ty = CONFIG["TO_MONTH"],   CONFIG["TO_DAY"],   CONFIG["TO_YEAR"]
    from_str_mm = f"{fm:02d}/{fd:02d}/{fy}"   # MM/DD/YYYY for react-widgets
    to_str_mm   = f"{tm:02d}/{td:02d}/{ty}"

    # ── Verify session is still alive (no re-login needed when Part C
    #    runs right after Part A) ────────────────────────────────────
    try:
        await page.evaluate("() => document.title")
    except Exception:
        page = await context.new_page()

    # Navigate to home briefly to confirm we're logged in
    await page.goto("https://dashboard.xendit.co/home", wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)
    if "login" in page.url:
        # Session lapsed — attempt quick re-login (TOTP only, no re-switch needed)
        print("  ⚠️  Session lapsed — re-logging in...")
        logged = await do_login(page, context)
        if not logged:
            print("  ❌  Re-login failed. Aborting Part C.")
            return False
        # Re-switch to the target account
        await switch_account(page, context)
        await page.goto("https://dashboard.xendit.co/home", wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

    await dismiss_feedback_modal(page)

    # ══════════════════════════════════════════════════════════════════
    #  STEP 1 — Navigate directly to XenPlatform → Accounts
    # ══════════════════════════════════════════════════════════════════
    print("  🔗  Step 1: Navigate to XenPlatform Accounts...")
    await page.goto("https://dashboard.xendit.co/xenplatform/accounts",
                    wait_until="domcontentloaded")
    await page.wait_for_timeout(5000)
    await dismiss_feedback_modal(page)
    print(f"  ✅  URL: {page.url}")
    await ss(page, "C1_accounts_page")

    # ══════════════════════════════════════════════════════════════════
    #  STEP 2 — Wait for accounts table
    # ══════════════════════════════════════════════════════════════════
    print("  ⏳  Step 2: Waiting for accounts table to load...")
    # Try networkidle first, then fall back to element detection
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    for attempt in range(3):
        try:
            await page.wait_for_selector('tbody tr, table, [class*="account-list"], [class*="AccountList"]', timeout=30000)
            await page.wait_for_timeout(3000)
            row_count = await page.evaluate("() => document.querySelectorAll('tbody tr').length")
            print(f"  ✅  Table loaded — {row_count} rows visible")
            break
        except Exception:
            if attempt < 2:
                print(f"  ⏳  Table not ready (attempt {attempt+1}/3), reloading...")
                await page.reload(wait_until="domcontentloaded")
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
                await page.wait_for_timeout(3000)
            else:
                print("  ⚠️  Table rows not detected — continuing anyway")

    # ══════════════════════════════════════════════════════════════════
    #  STEP 3 — Click the header checkbox to select all visible accounts
    # ══════════════════════════════════════════════════════════════════
    print("  ☑️   Step 3: Clicking header checkbox (Select All visible accounts)...")
    try:
        header_cb = page.locator('thead input[type="checkbox"]').first
        await header_cb.wait_for(state="visible", timeout=30000)
        await header_cb.scroll_into_view_if_needed()
        await header_cb.click(force=True)
        await page.wait_for_timeout(2500)
        print("  ✅  Header checkbox clicked")
    except Exception as e:
        print(f"  ⚠️  Header checkbox via Playwright failed ({e}) — trying JS")
        await page.evaluate("""
            () => {
                const cb = document.querySelector('thead input[type="checkbox"]');
                if (cb) cb.click();
            }
        """)
        await page.wait_for_timeout(2500)

    await ss(page, "C2_after_header_checkbox")

    # ══════════════════════════════════════════════════════════════════
    #  STEP 4 — Click "Select all N sub-accounts" link/button
    #
    #  After the header checkbox is checked, a blue banner appears:
    #  "20 sub-accounts are selected. Select all 77 sub-accounts."
    #  The second sentence is a clickable link.
    # ══════════════════════════════════════════════════════════════════
    print("  🔗  Step 4: Clicking 'Select all sub-accounts' link...")
    await page.wait_for_timeout(1000)

    select_all_sub_clicked = False

    # Strategy A: Playwright text locator with regex
    try:
        import re as _re
        link = page.get_by_text(_re.compile(r'Select all \d+ sub.?account', _re.IGNORECASE))
        if await link.count() > 0:
            await link.first.scroll_into_view_if_needed()
            await link.first.click()
            await page.wait_for_timeout(2000)
            select_all_sub_clicked = True
            print("  ✅  Strategy A: Playwright get_by_text regex clicked")
    except Exception as e:
        print(f"  ⚠️  Strategy A failed: {e}")

    # Strategy B: Use JS with innerText (NOT textContent — avoids matching <style> tags)
    if not select_all_sub_clicked:
        result = await page.evaluate("""
            () => {
                // Walk DOM using innerText to skip <style>/<script> content
                const all = Array.from(document.querySelectorAll(
                    'a, button, span, p, div, li'
                ));
                for (const el of all) {
                    // innerText only returns visible rendered text
                    const txt = (el.innerText || '').trim();
                    if (/select all/i.test(txt)
                        && /\\d+\\s*sub.?account/i.test(txt)
                        && el.offsetParent !== null) {
                        el.click();
                        return txt.substring(0, 100);
                    }
                }
                return null;
            }
        """)
        if result:
            print(f"  ✅  Strategy B: JS innerText clicked → '{result}'")
            select_all_sub_clicked = True
            await page.wait_for_timeout(2000)

    # Strategy C: find the banner container and click the second <a>/<span> inside it
    if not select_all_sub_clicked:
        result = await page.evaluate("""
            () => {
                // Find any element whose innerText contains "sub-accounts are selected"
                const banners = Array.from(document.querySelectorAll('*')).filter(el => {
                    const txt = el.innerText || '';
                    return /sub.?accounts? are selected/i.test(txt)
                        && el.offsetParent !== null;
                });
                for (const banner of banners) {
                    // Look for a clickable child link
                    const link = banner.querySelector('a, button, [role="button"], span[class*="link"]');
                    if (link) {
                        link.click();
                        return (link.innerText || link.textContent || '').trim().substring(0, 80);
                    }
                    // Directly click the banner if it's small enough
                    if ((banner.innerText || '').length < 200) {
                        banner.click();
                        return 'banner: ' + banner.innerText.substring(0, 80);
                    }
                }
                return null;
            }
        """)
        if result:
            print(f"  ✅  Strategy C: banner child clicked → '{result}'")
            select_all_sub_clicked = True
            await page.wait_for_timeout(2000)

    if not select_all_sub_clicked:
        print("  ⚠️  Could not click 'Select all sub-accounts' — proceeding with current selection")

    # Log what's actually selected now
    sel_status = await page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('*')) {
                const txt = (el.innerText || '').trim();
                if (/sub.?account/i.test(txt) && /selected|select all/i.test(txt)
                    && txt.length < 200 && el.offsetParent !== null) {
                    return txt;
                }
            }
            return null;
        }
    """)
    if sel_status:
        print(f"  📊  Selection: '{sel_status}'")

    await ss(page, "C3_all_selected")

    # ══════════════════════════════════════════════════════════════════
    #  STEP 5 — Click the blue "Export" button (top right of page)
    # ══════════════════════════════════════════════════════════════════
    print("  📤  Step 5: Clicking Export button...")
    export_clicked = False
    for attempt in range(3):
        try:
            # The Export button is a blue primary button at the top right
            export_btn = page.locator('button:has-text("Export"):not([disabled])').first
            await export_btn.wait_for(state="visible", timeout=25000)
            await export_btn.scroll_into_view_if_needed()
            await export_btn.click()
            await page.wait_for_timeout(3000)
            export_clicked = True
            print("  ✅  Export button clicked")
            break
        except Exception as e:
            print(f"  ⚠️  Attempt {attempt+1}: {e}")
            # JS fallback using innerText exact match
            ok = await page.evaluate("""
                () => {
                    for (const btn of document.querySelectorAll('button')) {
                        if ((btn.innerText || '').trim().toLowerCase() === 'export'
                            && !btn.disabled && btn.offsetParent !== null) {
                            btn.click();
                            return true;
                        }
                    }
                    return false;
                }
            """)
            if ok:
                export_clicked = True
                await page.wait_for_timeout(3000)
                print("  ✅  Export clicked via JS")
                break
            await page.wait_for_timeout(1000)

    if not export_clicked:
        print("  ❌  Could not click Export button")
        await ss(page, "fail_C_export_btn")
        return False

    await ss(page, "C4_export_panel")

    # ══════════════════════════════════════════════════════════════════
    #  STEP 6 — Wait for Export modal to appear
    #  (Modal title: "Export" — contains email, date-range dropdown,
    #   radio buttons: "Balance history" | "Transactions", Export btn)
    # ══════════════════════════════════════════════════════════════════
    print("  ⏳  Step 6: Waiting for Export modal...")
    modal_sel = '[data-testid="modal"], [role="dialog"], .modal-content'
    modal_loc = page.locator('[data-testid="modal"]').first
    if await modal_loc.count() == 0:
        modal_loc = page.locator('[role="dialog"]').first
    if await modal_loc.count() == 0:
        modal_loc = page.locator('.modal-content').first
    modal_ready = False
    for _ in range(20):
        has = await page.evaluate("""
            () => ({
                email:  !!(document.querySelector('[data-testid="modal"]') || document.querySelector('[role="dialog"]') || document.querySelector('.modal-content'))
                    ?.querySelector('input[type="email"], input[placeholder*="email" i], input[name*="email" i], textarea[id*="email" i]'),
                radios: ((document.querySelector('[data-testid="modal"]') || document.querySelector('[role="dialog"]') || document.querySelector('.modal-content'))
                    ?.querySelectorAll('input[type="radio"]').length) || 0,
                panel:  /transaction|balance|date range|send to email|time zone/i.test(
                    ((document.querySelector('[data-testid="modal"]') || document.querySelector('[role="dialog"]') || document.querySelector('.modal-content'))?.innerText || '')
                )
            })
        """)
        if has.get("email") or has.get("radios", 0) >= 2 or has.get("panel"):
            modal_ready = True
            print(f"  ✅  Modal ready: {has}")
            break
        await page.wait_for_timeout(400)

    if not modal_ready:
        print("  ⚠️  Modal readiness unclear — proceeding")

    # Dump modal HTML for debugging email tag structure
    try:
        modal_html = await page.evaluate("""
            () => {
                const d = document.querySelector('[data-testid="modal"]')
                       || document.querySelector('[role="dialog"]')
                       || document.querySelector('.modal-content');
                return d ? d.innerHTML.substring(0, 20000) : 'NO DIALOG';
            }
        """)
        dbg_path = os.path.join(CONFIG["SCREENSHOT_DIR"], "C6_modal_html.txt")
        with open(dbg_path, "w", encoding="utf-8") as f:
            f.write(modal_html)
        print(f"  🔍  Modal HTML saved → {dbg_path}")
    except Exception:
        pass

    # ══════════════════════════════════════════════════════════════════
    #  STEP 7 — Replace email with swarnraj@tazapay.com
    #
    #  The "Send to email" field is a TAG INPUT.
    #  By default it has anubhavjain@tazapay.com as an existing tag.
    #  We must:
    #    1. Delete ALL existing email tags
    #    2. Type swarnraj@tazapay.com into the text input
    #    3. Press Enter to confirm it as a tag
    # ══════════════════════════════════════════════════════════════════
    print(f"  📧  Step 7: Setting export email → {exp_email}")
    email_set = False
    try:
        email_input = modal_loc.locator('#email_recipients').first
        await email_input.wait_for(state="visible", timeout=5000)
        await email_input.click()
        await email_input.fill("")
        await page.wait_for_timeout(200)
        await email_input.fill(exp_email)
        await page.wait_for_timeout(300)
        actual_email = (await email_input.input_value()).strip().lower()
        print(f"  📧  Modal email value: '{actual_email}'")
        email_set = actual_email == exp_email.lower()
    except Exception as e:
        print(f"  ⚠️  Direct modal email fill failed: {e}")

    if not email_set:
        email_set = await ensure_only_export_email(
            page,
            exp_email,
            [
                '[data-testid="modal"] #email_recipients',
                '[role="dialog"] #email_recipients',
                '[data-testid="modal"] input[type="email"]',
                '[role="dialog"] input[type="email"]',
            ],
        )
    if not email_set:
        print("  ❌  XenPlatform export email could not be set correctly")
        return False

    await page.wait_for_timeout(500)
    await ss(page, "C5_email_set")

    # ══════════════════════════════════════════════════════════════════
    #  STEP 8 — Open "Date range" dropdown and choose "Custom"
    #  ORDER: Date → Custom → set dates → THEN Transaction radio
    #  (Per user instruction: click date → custom → set dates → Transaction → Export)
    #  Note: Custom date range is set to 7 days back from today (already calculated in CONFIG)
    # ══════════════════════════════════════════════════════════════════
    print("  📅  Step 8: Opening Date range dropdown → Custom...")
    custom_done = False

    # Scroll down inside the modal so the Date range row is visible
    await page.evaluate("""
        () => {
            const body = document.querySelector('[data-testid="modal"] .modal-body')
                      || document.querySelector('[role="dialog"] .modal-body')
                      || document.querySelector('.modal-body');
            if (body) body.scrollTop = body.scrollHeight;
        }
    """)
    await page.wait_for_timeout(800)

    # ── STEP 8a: Find the Date Range selector specifically ──────────────────────
    # The modal has TWO [data-testid="selector"] elements:
    #   1. Time zone  (comes first in DOM)
    #   2. Date range (comes second)
    # We must scope our clicks to the DATE RANGE selector only.
    # Strategy: find the selector whose label contains "Date range" text.

    dr_trigger_clicked = False

    # Strategy 1: Playwright locator scoped to the selector that has "Date range" label
    try:
        dr_sel_loc = modal_loc.locator('[data-testid="selector"]').filter(
            has_text="Date range"
        ).first
        if await dr_sel_loc.count() > 0:
            trigger = dr_sel_loc.locator('.selector-selected').first
            if await trigger.count() > 0:
                await trigger.click()
                await page.wait_for_timeout(800)
                dr_trigger_clicked = True
                print("  ✅  Date range trigger opened (scoped selector)")
    except Exception as _e:
        print(f"  ℹ️  Scoped trigger: {_e}")

    # Strategy 2: JS — find selector with "Date range" label → click its .selector-selected
    if not dr_trigger_clicked:
        clicked = await page.evaluate("""
            () => {
                const modal = document.querySelector('[data-testid="modal"]')
                           || document.querySelector('[role="dialog"]')
                           || document.body;
                const selectors = modal.querySelectorAll('[data-testid="selector"]');
                for (const sel of selectors) {
                    const label = sel.querySelector('.selector-label');
                    if (label && label.textContent.toLowerCase().includes('date')) {
                        const trigger = sel.querySelector('.selector-selected');
                        if (trigger) { trigger.click(); return true; }
                    }
                }
                // Fallback: click the SECOND selector-selected (first=timezone, second=date range)
                const allTriggers = [...modal.querySelectorAll('.selector-selected')]
                  .filter(b => b.offsetParent !== null);
                if (allTriggers.length >= 2) { allTriggers[1].click(); return 'second'; }
                if (allTriggers.length === 1) { allTriggers[0].click(); return 'first'; }
                return false;
            }
        """)
        if clicked:
            await page.wait_for_timeout(800)
            dr_trigger_clicked = True
            print(f"  ✅  Date range trigger opened via JS ({clicked})")

    if not dr_trigger_clicked:
        print("  ⚠️  Could not open Date range trigger — trying direct Custom click")

    # ── STEP 8b: Click the "Custom" option (index 5) WITHIN the Date Range selector ──
    await page.wait_for_timeout(400)

    # Strategy 1: Click [data-testid="selector-option-5"] SCOPED to the date range selector
    if not custom_done:
        try:
            dr_sel_loc2 = modal_loc.locator('[data-testid="selector"]').filter(
                has_text="Date range"
            ).first
            if await dr_sel_loc2.count() > 0:
                custom_opt = dr_sel_loc2.locator('[data-testid="selector-option-5"]').first
                if await custom_opt.count() > 0:
                    await custom_opt.click(timeout=5000)
                    await page.wait_for_timeout(1500)
                    custom_done = True
                    print("  ✅  Custom clicked: date-range-scoped selector-option-5")
        except Exception as _e:
            print(f"  ℹ️  Scoped option-5: {_e}")

    # Strategy 2: JS — find the date range selector → click option with text "Custom"
    if not custom_done:
        result = await page.evaluate("""
            () => {
                const modal = document.querySelector('[data-testid="modal"]')
                           || document.querySelector('[role="dialog"]')
                           || document.body;
                const selectors = modal.querySelectorAll('[data-testid="selector"]');
                for (const sel of selectors) {
                    const label = sel.querySelector('.selector-label');
                    if (label && label.textContent.toLowerCase().includes('date')) {
                        // found date range selector — find Custom option
                        const opts = sel.querySelectorAll('.selector-option-item');
                        for (const opt of opts) {
                            if ((opt.textContent || '').trim().toLowerCase() === 'custom') {
                                opt.click();
                                return 'label-scoped';
                            }
                        }
                        // try by data-testid="selector-option-5" inside this selector
                        const opt5 = sel.querySelector('[data-testid="selector-option-5"]');
                        if (opt5) { opt5.click(); return 'option-5-scoped'; }
                    }
                }
                // Last resort: all visible option items whose text is exactly "Custom"
                const allOpts = [...modal.querySelectorAll('.selector-option-item')]
                  .filter(b => b.offsetParent !== null);
                const customOpt = allOpts.find(b =>
                    (b.textContent || '').trim().toLowerCase() === 'custom');
                if (customOpt) { customOpt.click(); return 'text-match'; }
                return false;
            }
        """)
        if result:
            await page.wait_for_timeout(1500)
            custom_done = True
            print(f"  ✅  Custom clicked via JS ({result})")
        else:
            print("  ⚠️  Could not select Custom — check screenshot")

    # Give the Custom date pickers time to render
    await page.wait_for_timeout(2000)
    await ss(page, "C7_custom_selected")

    print(
        "  📅  Step 10: Choosing Custom range from calendar "
        f"({_from_date.strftime('%d %b %Y')} → {_today.strftime('%d %b %Y')})"
    )

    # ── Calendar picker (DD/MM/YY format confirmed from live browser) ──────────
    fd, fm, fy = CONFIG["FROM_DAY"], CONFIG["FROM_MONTH"], CONFIG["FROM_YEAR"]
    td, tm, ty = CONFIG["TO_DAY"],   CONFIG["TO_MONTH"],   CONFIG["TO_YEAR"]

    _MONTH_NAMES = [
        "January","February","March","April","May","June",
        "July","August","September","October","November","December"
    ]

    async def _pick_cal(btn_index: int, dd: int, mm: int, yy: int) -> bool:
        """Click the btn_index-th date calendar icon, navigate to the correct
        month, then click the day. Format = DD/MM/YY (confirmed from live browser).
        Tries multiple strategies with retries to find calendar button robustly."""
        target_label = f"{_MONTH_NAMES[mm - 1]} {yy}"
        print(f"    📅  Calendar [{btn_index}]: {dd:02d}/{mm:02d}/{yy % 100:02d}  ({target_label})")

        # Retry up to 4 times — date pickers may render after a short delay
        clicked = False
        for _attempt in range(4):
            clicked = await page.evaluate(f"""
                () => {{
                    // Strategy 1: input[placeholder="selectDate"] → parent widget → .rw-btn-select
                    let dateInputs = [...document.querySelectorAll('input[placeholder="selectDate"]')]
                      .filter(b => b.offsetParent !== null);
                    // Strategy 2: .rw-widget-input class (react-widgets)
                    if (!dateInputs.length) {{
                        dateInputs = [...document.querySelectorAll('input.rw-widget-input')]
                          .filter(b => b.offsetParent !== null && b.type !== 'hidden');
                    }}
                    // Strategy 3: react-widgets id pattern rw_*_input
                    if (!dateInputs.length) {{
                        dateInputs = [...document.querySelectorAll('input[id^="rw_"]')]
                          .filter(b => b.offsetParent !== null && b.type !== 'hidden');
                    }}
                    const inp = dateInputs[{btn_index}];
                    if (inp) {{
                        const widget = inp.closest('.rw-widget-picker')
                                    || inp.closest('.rw-widget-container')
                                    || inp.parentElement;
                        const btn = widget && widget.querySelector('.rw-btn-select');
                        if (btn) {{ btn.click(); return 'input-relative'; }}
                    }}
                    // Strategy 4: all visible .rw-btn-select buttons, pick by index
                    const allBtns = [...document.querySelectorAll('.rw-btn-select')]
                      .filter(b => b.offsetParent !== null);
                    if (allBtns[{btn_index}]) {{
                        allBtns[{btn_index}].click();
                        return 'direct-btn';
                    }}
                    return false;
                }}
            """)
            if clicked:
                print(f"    ✅  Calendar button [{btn_index}] clicked via: {clicked} (attempt {_attempt+1})")
                break
            if _attempt < 3:
                await page.wait_for_timeout(600)

        if not clicked:
            print(f"    ⚠️  Calendar button [{btn_index}] not found after 4 attempts")
            return False
        await page.wait_for_timeout(700)

        for _ in range(36):
            cur = await page.evaluate(
                "() => document.querySelector('.rw-calendar-btn-view')?.textContent?.trim() || ''"
            )
            if not cur:
                break
            if target_label in cur:
                print(f"    ✅  Month: '{cur}'")
                break
            parts = cur.strip().split()
            if len(parts) == 2:
                try:
                    c_mm = _MONTH_NAMES.index(parts[0]) + 1
                    c_yy = int(parts[1])
                except (ValueError, IndexError):
                    c_mm, c_yy = mm, yy
                nav = ".rw-calendar-btn-left" if (c_yy, c_mm) > (yy, mm) else ".rw-calendar-btn-right"
                await page.evaluate(f"() => {{ const b = document.querySelector('{nav}'); if(b) b.click(); }}")
                await page.wait_for_timeout(350)
            else:
                break

        # Use Playwright native click (not JS .click()) so React synthetic events fire correctly
        ok = False
        try:
            padded = f"{dd:02d}"
            all_cells = await page.locator('.rw-cell').all()
            for cell in all_cells:
                try:
                    cls = await cell.get_attribute("class") or ""
                    if "rw-state-disabled" in cls:
                        continue
                    t = (await cell.inner_text()).strip()
                    if t == padded:
                        await cell.click(timeout=3000)
                        ok = True
                        break
                except Exception:
                    continue
        except Exception as _ce:
            print(f"    ⚠️  Cell click error: {_ce}")
        await page.wait_for_timeout(700)

        # Verify: read the date input value for this picker by index
        # Try placeholder="selectDate" inputs first, then rw_*_input as fallback
        vals = await page.evaluate("""
            () => {
                let inputs = [...document.querySelectorAll('input[placeholder="selectDate"]')]
                  .filter(el => el.offsetParent !== null);
                if (!inputs.length) {
                    inputs = [...document.querySelectorAll('input[id^="rw_"][id$="_input"]')]
                      .filter(el => el.offsetParent !== null);
                }
                return inputs.map(el => el.value);
            }
        """)
        got = vals[btn_index] if btn_index < len(vals) else ""
        p = got.split("/")
        if len(p) == 3 and p[0] == f"{dd:02d}" and p[1] == f"{mm:02d}" and p[2] == f"{yy%100:02d}":
            print(f"    ✅  Verified: '{got}'")
            return True
        # Also check rw_ ids as fallback for verification
        rw_vals = await page.evaluate("""
            () => [...document.querySelectorAll('input[id^="rw_"][id$="_input"]')]
                    .filter(el => el.offsetParent !== null).map(el => el.value)
        """)
        print(f"    ⚠️  Got '{got}' (click_ok={ok}), expected {dd:02d}/{mm:02d}/{yy%100:02d} | rw_vals={rw_vals}")
        return False

    ok_from = await _pick_cal(0, fd, fm, fy)
    await page.wait_for_timeout(500)
    ok_to   = await _pick_cal(1, td, tm, ty)
    await page.wait_for_timeout(500)

    final_vals = await page.evaluate("""
        () => {
            let inputs = [...document.querySelectorAll('input[placeholder="selectDate"]')]
              .filter(el => el.offsetParent !== null);
            if (!inputs.length) {
                inputs = [...document.querySelectorAll('input[id^="rw_"][id$="_input"]')]
                  .filter(el => el.offsetParent !== null);
            }
            return inputs.map(el => el.value);
        }
    """)
    print(f"  📅  Date inputs: {final_vals}  (from_ok={ok_from}, to_ok={ok_to})")

    if not (ok_from and ok_to):
        print("  ⚠️  One or both dates could not be confirmed — continuing anyway")

    '''

    # Save modal HTML after Custom is selected for debugging date inputs
    try:
        after_custom_html = await page.evaluate("""
            () => {
                const d = document.querySelector('[data-testid="modal"]')
                       || document.querySelector('[role="dialog"]')
                       || document.querySelector('.modal-content');
                return d ? d.innerHTML.substring(0, 5000) : 'NO MODAL';
            }
        """)
        dbg2 = os.path.join(CONFIG["SCREENSHOT_DIR"], "C7_after_custom_html.txt")
        with open(dbg2, "w", encoding="utf-8") as f:
            f.write(after_custom_html)
        print(f"  🔍  After-Custom HTML → {dbg2}")
    except Exception:
        pass

    # ══════════════════════════════════════════════════════════════════
    #  STEP 10 — Set date range: 7 days back → today
    #
    #  KEY FIX: Do NOT press Escape inside the modal — it will close it!
    #  Close the timezone dropdown by clicking the modal title paragraph,
    #  then focus each date input with click(force=True) only.
    # ══════════════════════════════════════════════════════════════════
    print(f"  📅  Step 10: Typing date range {from_str_mm} → {to_str_mm}")

    # Close timezone dropdown by clicking the modal heading (safe neutral click)
    try:
        await page.evaluate("""
            () => {
                const title = document.querySelector('[data-testid="modal"] p')
                           || document.querySelector('.modal-body p');
                if (title) title.click();
            }
        """)
        await page.wait_for_timeout(500)
    except Exception:
        pass

    MONTH_NAMES = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    async def pick_date_via_calendar(btn_index: int, target_dd: int,
                                     target_mm: int, target_yy: int) -> bool:
        """
        Set a date in the XenPlatform export modal using the calendar picker UI.

        Format confirmed from live browser: DD/MM/YY (e.g. 28/03/26 for March 28 2026).
        Native JS setter does NOT work for react-widgets — calendar picker is required.

        btn_index: 0 = FROM calendar button, 1 = TO calendar button
        """
        target_label = f"{MONTH_NAMES[target_mm - 1]} {target_yy}"
        print(f"    📅  Calendar picker [{btn_index}]: targeting {target_dd:02d}/{target_mm:02d}/{target_yy % 100:02d}  ({target_label})")

        # Click the calendar icon button — scoped to modal, visible only
        clicked = await page.evaluate(f"""
            () => {{
                const modal = document.querySelector('[data-testid="modal"]')
                           || document.querySelector('[role="dialog"]')
                           || document.body;
                const btns = [...modal.querySelectorAll('.rw-btn-select')]
                  .filter(b => b.offsetParent !== null);
                if (btns[{btn_index}]) {{ btns[{btn_index}].click(); return true; }}
                return false;
            }}
        """)
        if not clicked:
            print(f"    ⚠️  Calendar button [{btn_index}] not found")
            return False
        await page.wait_for_timeout(700)

        # Navigate to the correct month/year (max 36 steps)
        for _ in range(36):
            current = await page.evaluate(
                "() => document.querySelector('.rw-calendar-btn-view')?.textContent?.trim() || ''"
            )
            if not current:
                print("    ⚠️  Calendar header not visible")
                break
            if target_label in current:
                print(f"    ✅  Month matched: '{current}'")
                break

            # Parse current month to decide direction
            parts = current.strip().split()
            if len(parts) == 2:
                cur_month_name, cur_year_str = parts
                try:
                    cur_month = MONTH_NAMES.index(cur_month_name) + 1
                    cur_year  = int(cur_year_str)
                except (ValueError, IndexError):
                    cur_month, cur_year = 1, 2026

                # Navigate: go left (prev) or right (next)
                if (cur_year, cur_month) > (target_yy, target_mm):
                    nav_sel = ".rw-calendar-btn-left"
                else:
                    nav_sel = ".rw-calendar-btn-right"

                await page.evaluate(f"""
                    () => {{
                        const btn = document.querySelector('{nav_sel}');
                        if (btn) btn.click();
                    }}
                """)
                await page.wait_for_timeout(350)
            else:
                break

        # Click the target day cell
        clicked_day = await page.evaluate(f"""
            () => {{
                const day = [...document.querySelectorAll('.rw-cell')]
                  .find(b => b.textContent.trim() === '{target_dd}' &&
                             !b.classList.contains('rw-state-disabled'));
                if (day) {{ day.click(); return true; }}
                // Fallback: .rw-btn-primary
                const alt = [...document.querySelectorAll('.rw-btn-primary')]
                  .find(b => b.textContent.trim() === '{target_dd}');
                if (alt) {{ alt.click(); return true; }}
                return false;
            }}
        """)
        await page.wait_for_timeout(600)

        # Verify the input value (format: DD/MM/YY)
        rw_inputs = await page.evaluate("""
            () => [...document.querySelectorAll('input[id^="rw_"][id$="_input"]')]
                    .filter(el => el.offsetParent !== null)
                    .map(el => el.value)
        """)
        expected_dd = f"{target_dd:02d}"
        expected_mm = f"{target_mm:02d}"
        expected_yy = f"{target_yy % 100:02d}"

        got_val = rw_inputs[btn_index] if btn_index < len(rw_inputs) else ""
        parts_check = got_val.split("/")
        if (len(parts_check) == 3 and
                parts_check[0] == expected_dd and
                parts_check[1] == expected_mm and
                parts_check[2] == expected_yy):
            print(f"    ✅  Verified: '{got_val}'")
            return True
        else:
            print(f"    ⚠️  Got '{got_val}' (day_clicked={clicked_day}), expected {expected_dd}/{expected_mm}/{expected_yy}")
            return False

    # ── Set FROM date ──────────────────────────────────────────────────────────
    print(f"  📅  Setting FROM date: {fd:02d}/{fm:02d}/{fy % 100:02d}")
    ok_from = await pick_date_via_calendar(0, fd, fm, fy)
    await page.wait_for_timeout(500)

    # ── Set TO date ────────────────────────────────────────────────────────────
    print(f"  📅  Setting TO date:   {td:02d}/{tm:02d}/{ty % 100:02d}")
    ok_to = await pick_date_via_calendar(1, td, tm, ty)
    await page.wait_for_timeout(500)

    # Log final input values
    final_vals = await page.evaluate("""
        () => [...document.querySelectorAll('input[id^="rw_"][id$="_input"]')]
                .filter(el => el.offsetParent !== null)
                .map(el => el.value)
    """)
    print(f"  📅  Final date inputs: {final_vals}  (from_ok={ok_from}, to_ok={ok_to})")

    '''
    # Wait for sub-account count to refresh
    await page.wait_for_timeout(2500)

    # Log the updated sub-account count
    sub_count_txt = await page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('*')) {
                const t = (el.innerText || '').trim();
                if (/sub.?account.*will be exported/i.test(t) && t.length < 120) return t;
            }
            return null;
        }
    """)
    if sub_count_txt:
        print(f"  📊  Export count: '{sub_count_txt}'")

    await ss(page, "C8_dates_set")

    # ══════════════════════════════════════════════════════════════════
    #  STEP 9 — Select "Transactions" radio button  (AFTER date is set)
    #
    #  The modal has EXACTLY 2 radios:
    #    [0] Balance history   ← default selected
    #    [1] Transactions      ← we need this one
    # ══════════════════════════════════════════════════════════════════
    print("  📋  Step 9: Selecting 'Transactions' radio...")

    # Scroll the modal down to make sure the radios are visible
    await page.evaluate("""
        () => {
            const body = document.querySelector('.modal-body')
                      || document.querySelector('[data-testid="modal"]');
            if (body) body.scrollTop = body.scrollHeight;
        }
    """)
    await page.wait_for_timeout(600)

    txn_done = False

    try:
        txn_radio = page.locator('[data-testid="modal"] input[type="radio"][value="XP_TRANSACTIONS"], [role="dialog"] input[type="radio"][value="XP_TRANSACTIONS"], .modal-content input[type="radio"][value="XP_TRANSACTIONS"]').first
        if await txn_radio.count() > 0:
            await txn_radio.click(force=True)
            print("  ✅  Direct strategy: clicked XP_TRANSACTIONS radio")
            txn_done = True
            await page.wait_for_timeout(500)
    except Exception as e:
        print(f"  ⚠️  Direct XP_TRANSACTIONS strategy: {e}")

    # CRITICAL: All selectors MUST be scoped inside [data-testid="modal"] to
    # avoid clicking the "Transactions" navigation tab in the left sidebar!
    modal_sel = '[data-testid="modal"], [role="dialog"], .modal-content'

    # Strategy A: Playwright — second radio button inside the modal
    try:
        modal_radios = page.locator(f'{modal_sel} input[type="radio"]')
        count = await modal_radios.count()
        print(f"  🔘  Radios in modal: {count}")
        if count >= 2:
            # Transactions is always the 2nd radio (index 1)
            await modal_radios.nth(1).click(force=True)
            print(f"  ✅  Strategy A: clicked radio[1] of {count} in modal")
            txn_done = True
            await page.wait_for_timeout(500)
    except Exception as e:
        print(f"  ⚠️  Strategy A: {e}")

    # Strategy B: click the label text "Transactions" WITHIN the modal only
    if not txn_done:
        try:
            txn_label = page.locator(modal_sel).get_by_text('Transactions', exact=True)
            if await txn_label.count() > 0:
                await txn_label.first.click(force=True)
                print("  ✅  Strategy B: clicked 'Transactions' label in modal")
                txn_done = True
                await page.wait_for_timeout(500)
        except Exception as e:
            print(f"  ⚠️  Strategy B: {e}")

    # Strategy C: JS — scope to modal, find radio by sibling/label text
    if not txn_done:
        result3 = await page.evaluate(f"""
            () => {{
                const modal = document.querySelector('{modal_sel}')
                           || document.querySelector('.modal-content');
                if (!modal) return 'no modal';
                const radios = Array.from(modal.querySelectorAll('input[type="radio"]'));
                if (radios.length >= 2) {{
                    radios[1].click();
                    return 'JS radio[1] of ' + radios.length;
                }}
                return 'only ' + radios.length + ' radios';
            }}
        """)
        print(f"  ✅  Strategy C: {result3}")
        txn_done = True
        await page.wait_for_timeout(500)

    # Verify: read which radio is now checked (scoped to modal)
    checked_lbl = await page.evaluate(f"""
        () => {{
            const modal = document.querySelector('{modal_sel}')
                       || document.querySelector('.modal-content');
            if (!modal) return 'no modal found';
            for (const r of modal.querySelectorAll('input[type="radio"]')) {{
                if (!r.checked) continue;
                const lbl = r.id ? document.querySelector(`label[for="${{r.id}}"]`) : null;
                if (lbl) return lbl.innerText.trim();
                const wrap = r.closest('label');
                if (wrap) {{
                    const c = wrap.cloneNode(true);
                    c.querySelectorAll('input').forEach(e => e.remove());
                    return c.innerText.trim();
                }}
                let sib = r.nextSibling;
                while (sib) {{
                    const t = (sib.textContent || '').trim();
                    if (t) return t;
                    sib = sib.nextSibling;
                }}
                return '(radio found, label unclear)';
            }}
            return 'none checked';
        }}
    """)
    print(f"  📻  Radio checked: '{checked_lbl}'")
    if "transaction" not in checked_lbl.lower():
        print("  ⚠️  WARNING: Transactions radio may not be selected!")

    await ss(page, "C9_transaction_selected")

    # ══════════════════════════════════════════════════════════════════
    #  STEP 10 (was 11) — Click the "Export" button inside the modal
    # ══════════════════════════════════════════════════════════════════
    print("  📤  Step 10: Clicking modal Export button...")
    submitted = False

    # Click the Export button INSIDE the modal only (scoped to [data-testid="modal"])
    # to avoid accidentally clicking the page-level Export button.
    for btn_label in ["Export", "Send to Email", "Send", "Submit"]:
        try:
            # Scope to the modal
            modal_loc = page.locator('[data-testid="modal"]')
            btns = modal_loc.locator(f'button:has-text("{btn_label}")')
            cnt = await btns.count()
            if cnt == 0:
                # Fallback: last matching button on the page
                btns = page.locator(f'button:has-text("{btn_label}")').last
                if not await btns.is_visible():
                    continue
                btn = btns
            else:
                btn = btns.last
                if not await btn.is_visible():
                    continue
            # Wait up to 10 s for it to be enabled
            for _ in range(20):
                if await btn.is_enabled():
                    break
                await page.wait_for_timeout(500)
            await btn.scroll_into_view_if_needed()
            await btn.click()
            await page.wait_for_timeout(3000)
            submitted = True
            print(f"  ✅  Clicked '{btn_label}' (modal-scoped)")
            break
        except Exception as e:
            print(f"  ⚠️  '{btn_label}' btn: {e}")

    if not submitted:
        # JS fallback — pick the Export button INSIDE the modal
        result = await page.evaluate("""
            () => {
                const modal = document.querySelector('[data-testid="modal"]')
                           || document.querySelector('.modal-content');
                if (!modal) return null;
                const all = Array.from(modal.querySelectorAll('button')).reverse();
                for (const btn of all) {
                    const t = (btn.innerText || '').trim().toLowerCase();
                    if (/(^export$|send|submit)/.test(t) && !btn.disabled
                        && btn.offsetParent !== null) {
                        btn.click();
                        return btn.innerText.trim();
                    }
                }
                return null;
            }
        """)
        if result:
            print(f"  ✅  JS submit: '{result}'")
            submitted = True
            await page.wait_for_timeout(3000)

    if not submitted:
        print("  ❌  Submit button not found")
        await ss(page, "fail_C_submit")
        return False

    await ss(page, "C9_exported")
    print("  🎉  XenPlatform export submitted!")

    # Dismiss success popup (Close / Okay / OK / Done)
    popup_dismissed = False
    for popup_txt in ["Close", "Okay", "OK", "Done", "Got it"]:
        try:
            btn = page.locator(f'button:has-text("{popup_txt}")').first
            await btn.wait_for(state="visible", timeout=5000)
            await btn.click()
            print(f"  ✅  Popup dismissed ('{popup_txt}')")
            popup_dismissed = True
            break
        except Exception:
            pass
    if not popup_dismissed:
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass

    return True


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════
async def main():
    print("=" * 65)
    print("  XENDIT FULL AUTOMATION v3")
    print(f"  {_from_date.strftime('%d %b %Y')} → {_today.strftime('%d %b %Y')}")
    print(f"  Export → {CONFIG['EXPORT_EMAIL']}")
    print(f"  Download → {CONFIG['DOWNLOAD_DIR']}")
    print("=" * 65)

    if not CONFIG["GMAIL_APP_PASSWORD"]:
        print("\n⚠️  GMAIL_APP_PASSWORD is not set!")
        print("   Gmail IMAP requires an App Password (not your regular password).")
        print("   1. Go to: https://myaccount.google.com/apppasswords")
        print("   2. Create one for Mail → Windows Computer")
        print("   3. Paste the 16-char code into CONFIG['GMAIL_APP_PASSWORD']")
        print("   Parts B & D (Gmail download) will be skipped.\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=CONFIG["HEADLESS"],
            slow_mo=CONFIG["SLOW_MO"],
            executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            args=["--disable-blink-features=AutomationControlled"],
        )
        context_kwargs = {
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "viewport": {"width": 1280, "height": 900},
            "locale": "en-US",
            "accept_downloads": True,
        }
        if os.path.exists(CONFIG["SESSION_STATE"]):
            context_kwargs["storage_state"] = CONFIG["SESSION_STATE"]
            print(f"\n  Reusing saved session: {CONFIG['SESSION_STATE']}")
        context = await browser.new_context(**context_kwargs)
        await context.add_init_script(STEALTH_JS)
        page = await context.new_page()

        results = {}

        # ══════════════════════════════════════════════════════════════
        #  SNAPSHOT inbox UID BEFORE any exports so we can filter later
        # ══════════════════════════════════════════════════════════════
        uid_before_any = 0
        if CONFIG["GMAIL_APP_PASSWORD"]:
            uid_before_any = await asyncio.get_event_loop().run_in_executor(
                None, get_latest_imap_uid
            )
            print(f"\n  📌  UID watermark (before exports): {uid_before_any}")

        # ══════════════════════════════════════════════════════════════
        #  PART A: Xendit Transactions Export
        # ══════════════════════════════════════════════════════════════
        try:
            if not await do_login(page, context):
                raise RuntimeError("Xendit login failed")
            if not await switch_account(page, context):
                # Saved session may have stale profile in headless — force fresh login
                print("  ⚠️  Switch failed with cached session — forcing fresh login...")
                try:
                    await page.goto("https://dashboard.xendit.co/logout",
                                    wait_until="domcontentloaded", timeout=15000)
                except Exception:
                    pass
                await page.wait_for_timeout(3000)
                if not await do_login(page, context):
                    raise RuntimeError("Xendit login failed after fresh login")
                if not await switch_account(page, context):
                    raise RuntimeError("Account switch failed")
            results["A_xendit_export"] = await xendit_export(page, context)
        except Exception as e:
            print(f"\n❌  PART A failed: {e}")
            results["A_xendit_export"] = False

        # ── Snapshot UID between A and C ──────────────────────────────
        uid_after_partA = uid_before_any
        if CONFIG["GMAIL_APP_PASSWORD"]:
            uid_after_partA = await asyncio.get_event_loop().run_in_executor(
                None, get_latest_imap_uid
            )
            print(f"\n  📌  UID watermark (after Part A, before Part C): {uid_after_partA}")

        # ══════════════════════════════════════════════════════════════
        #  PART C: XenPlatform Export  ← runs RIGHT AFTER Part A
        #  (while the browser session is still fresh — no IMAP wait gap)
        # ══════════════════════════════════════════════════════════════
        try:
            await page.evaluate("() => document.title")  # health check
        except Exception:
            print("\n  ⚠️  Page was closed — creating fresh page...")
            page = await context.new_page()

        try:
            results["C_xenplatform_export"] = await xenplatform_export(page, context)
        except Exception as e:
            print(f"\n❌  PART C failed: {e}")
            results["C_xenplatform_export"] = False

        # ══════════════════════════════════════════════════════════════
        #  PART B: Download Xendit transactions export via Gmail IMAP
        #  (We wait for Part A's email; after_uid = uid_before_any)
        # ══════════════════════════════════════════════════════════════
        if CONFIG["GMAIL_APP_PASSWORD"]:
            try:
                _uid_b = uid_before_any
                file_path = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: download_from_gmail_imap(
                        "xendit",
                        wait_seconds=120,
                        after_uid=_uid_b,
                        # Part A sends "Tazapay Pte Ltd Transactions Report"
                        # Part C sends "xenPlatform report exported"
                        # Pick ONLY the Transactions Report, not the XenPlatform one
                        subject_exclude="xenplatform",
                    )
                )
                results["B_xendit_download"] = file_path
            except Exception as e:
                print(f"\n❌  PART B failed: {e}")
                results["B_xendit_download"] = None
        else:
            print("\n  ⏭️  PART B skipped — no GMAIL_APP_PASSWORD")
            results["B_xendit_download"] = "SKIPPED"

        # ══════════════════════════════════════════════════════════════
        #  PART D: Download XenPlatform export via Gmail IMAP
        #  (only emails with UID > uid_after_partA  = those from Part C)
        # ══════════════════════════════════════════════════════════════
        if CONFIG["GMAIL_APP_PASSWORD"]:
            try:
                _uid_d = uid_after_partA
                file_path = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: download_from_gmail_imap(
                        "xenplatform",
                        wait_seconds=300,   # 5 min — XenPlatform 77-account export is slow
                        after_uid=_uid_d,
                        # Part C sends "xenPlatform report exported"
                        subject_must="xenplatform",
                    )
                )
                results["D_xenplatform_download"] = file_path
            except Exception as e:
                print(f"\n❌  PART D failed: {e}")
                results["D_xenplatform_download"] = None
        else:
            print("\n  ⏭️  PART D skipped — no GMAIL_APP_PASSWORD")
            results["D_xenplatform_download"] = "SKIPPED"

        # ── Summary ───────────────────────────────────────────────────
        try:
            source_csv = results.get("D_xenplatform_download")
            if isinstance(source_csv, str) and os.path.exists(source_csv):
                results["E_xp_activity_exports"] = await xenplatform_activity_exports(
                    page, context, source_csv
                )
            else:
                print("\n  â­ï¸  PART E skipped â€” XenPlatform CSV not available")
                results["E_xp_activity_exports"] = "SKIPPED"
        except Exception as e:
            print(f"\nâŒ  PART E failed: {e}")
            results["E_xp_activity_exports"] = False

        print("\n" + "="*65)
        print("  FINAL SUMMARY")
        print("="*65)
        for key, val in results.items():
            if val == "SKIPPED":
                icon = "⏭️ "
            elif val is None or val is False:
                icon = "❌"
            else:
                icon = "✅"
            display = str(val) if not isinstance(val, str) or len(val) < 60 else f"...{val[-50:]}"
            print(f"  {icon}  {key}: {display}")

        # ── Slack success notification ────────────────────────────────
        slack_notify(True, results)


async def run_xenplatform_only():
    print("=" * 65)
    print("  XENDIT XENPLATFORM AUTOMATION")
    print(f"  {_from_date.strftime('%d %b %Y')} -> {_today.strftime('%d %b %Y')}")
    print(f"  Export -> {CONFIG['EXPORT_EMAIL']}")
    print(f"  Download -> {CONFIG['DOWNLOAD_DIR']}")
    print("=" * 65)

    if not CONFIG["GMAIL_APP_PASSWORD"]:
        print("\nWARNING: GMAIL_APP_PASSWORD is not set.")
        print("Gmail download step will be skipped.\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=CONFIG["HEADLESS"],
            slow_mo=CONFIG["SLOW_MO"],
            executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            args=["--disable-blink-features=AutomationControlled"],
        )
        context_kwargs = {
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "viewport": {"width": 1280, "height": 900},
            "locale": "en-US",
            "accept_downloads": True,
        }
        if os.path.exists(CONFIG["SESSION_STATE"]):
            context_kwargs["storage_state"] = CONFIG["SESSION_STATE"]
            print(f"\n  Reusing saved session: {CONFIG['SESSION_STATE']}")
        context = await browser.new_context(**context_kwargs)
        await context.add_init_script(STEALTH_JS)
        page = await context.new_page()

        results = {}
        uid_before_export = 0

        if CONFIG["GMAIL_APP_PASSWORD"]:
            uid_before_export = await asyncio.get_event_loop().run_in_executor(
                None, get_latest_imap_uid
            )
            print(f"\n  Inbox watermark before XenPlatform export: {uid_before_export}")

        try:
            if not await do_login(page, context):
                raise RuntimeError("Xendit login failed")
            if not await switch_account(page, context):
                raise RuntimeError("Account switch failed")
            results["xenplatform_export"] = await xenplatform_export(page, context)
        except Exception as e:
            print(f"\nFAILED during XenPlatform export: {e}")
            results["xenplatform_export"] = False

        if CONFIG["GMAIL_APP_PASSWORD"]:
            try:
                file_path = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: download_from_gmail_imap(
                        "xenplatform",
                        wait_seconds=300,
                        after_uid=uid_before_export,
                        subject_must="xenplatform",
                    )
                )
                results["xenplatform_download"] = file_path
            except Exception as e:
                print(f"\nFAILED during XenPlatform Gmail download: {e}")
                results["xenplatform_download"] = None
        else:
            results["xenplatform_download"] = "SKIPPED"

        await browser.close()

        print("\n" + "=" * 65)
        print("  FINAL SUMMARY")
        print("=" * 65)
        for key, val in results.items():
            if val == "SKIPPED":
                icon = "SKIP"
            elif val is None or val is False:
                icon = "FAIL"
            else:
                icon = "OK"
            print(f"  {icon:<4} {key}: {val}")


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "single_xp_account":
        asyncio.run(run_single_xp_account_download(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ""))
    else:
        try:
            asyncio.run(main())
        except Exception as _fatal_err:
            print(f"\n💥  Fatal error — automation aborted: {_fatal_err}")
            slack_notify(False, error=str(_fatal_err))
            sys.exit(1)
