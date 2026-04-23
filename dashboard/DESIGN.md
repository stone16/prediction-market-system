# Dashboard Design System (Cathedral v1)

Source of truth: [`docs/designs/dashboard-design-system.md`](../docs/designs/dashboard-design-system.md)

**Status**: spec — to be implemented as `dashboard/DESIGN.md` in PR 1 of Cathedral v1.

Produced by `/plan-design-review` on 2026-04-23 as the codified source of truth
for all Cathedral v1 design decisions. PR 1 creates `dashboard/DESIGN.md`
populated from this spec; subsequent PRs reference and extend it.

Related docs:
- [`pms-dashboard-cathedral.md`](./pms-dashboard-cathedral.md) — canonical CEO plan
- [`pms-cathedral-review-findings.md`](./pms-cathedral-review-findings.md) — implementation details from reviews

---

## Minimum Viewport

**Hard floor: 1280px.** Below that, the dashboard renders a full-page "Desktop required" message with a short explanation + link to the mobile roadmap note in TODOs.

```
[≥1280px]   full Cathedral UI
[<1280px]   full-page fallback message, no partial render
```

Responsive / mobile roadmap is v2 work. Cathedral v1 is desktop-only by design.

---

## Color Tokens (`:root` in `globals.css`)

**Brand / base**:

```css
--paper:     #f4f8f2;   /* warm cream background */
--ink:       #171916;   /* primary text */
--muted:     #5d665b;   /* secondary text, grid lines */
--line:      #cbd8c4;   /* borders, dividers */
--panel:     #ffffff;   /* card/panel backgrounds */
--shadow:    0 18px 45px rgb(23 25 22 / 11%);  /* single elevation */
```

**Brand accent** (composition only, NOT for errors):

```css
--coral:       #d84f3f;                  /* eyebrow, gradient, brand-mark */
--coral-weak:  rgb(216 79 63 / 14%);     /* subtle accent backgrounds */
```

**Semantic**:

```css
--green:       #1f8a70;                  /* YES / up / success / positive fills */
--green-weak:  rgb(31 138 112 / 13%);
--amber:       #d6a72d;                  /* warn / pending / stale indicator */
--cyan:        #2d8aa6;                  /* NO / info / neutral */
--error:       #b23a2a;                  /* NEW — validation fails, 409, 5xx toasts */
--idea-bg:     #faf4ea;                  /* NEW — IdeaCard background (warm paper tint) */
```

**Usage rules**:
- `--coral` and `--error` are NEVER interchangeable. Coral is brand. Error is error.
- `--green` means "YES direction" in market context. It is NOT a generic success color.
- `--panel` and `--paper` are the ONLY background colors. No gradients outside the existing coral hero bar.

---

## Type Scale

**Families**:

```css
--font-display: Georgia, "Times New Roman", serif;        /* large headings, wordmark */
--font-body:    Avenir, "Gill Sans", "Trebuchet MS", sans-serif;  /* body, labels */
--font-mono:    "SF Mono", Menlo, Consolas, monospace;    /* numeric cells, hashes */
```

**Scale** (use the token, not the pixel value):

| Token | Size / line-height | Role |
|---|---|---|
| `--text-54` | 54 / 60 | display-xl — wordmark, `/share` H1 |
| `--text-30` | 30 / 36 | display-lg — page H1 |
| `--text-24` | 24 / 30 | display-md — card H3, brand |
| `--text-20` | 20 / 28 | display-sm — secondary headings |
| `--text-18` | 18 / 26 | lede — intro paragraphs, muted |
| `--text-14` | 14 / 20 | body-lg |
| `--text-13` | 13 / 18 | body (default) |
| `--text-11` | 11 / 14 | label — pills, eyebrow, meta |

Numeric (monospace): `--text-13` and `--text-18` with `font-variant-numeric: tabular-nums`.

**Weights**:

```css
--weight-regular:  400;
--weight-medium:   500;
--weight-bold:     800;  /* matches existing eyebrow 800 */
```

---

## Space Scale (8pt grid)

```css
--space-1:   4px;
--space-2:   8px;
--space-3:   12px;
--space-4:   16px;
--space-5:   20px;
--space-6:   24px;
--space-8:   32px;
--space-10:  40px;
--space-12:  48px;
--space-16:  64px;
--space-24:  96px;
```

**Intentional**: do NOT add `--space-7`, `--space-9`, etc. If you need 28px, pick 24 or 32.

---

## Radius Scale (use intentionally, not uniformly)

```css
--radius-sm:   4px;   /* inline pills, badges */
--radius-md:   8px;   /* buttons, cards, drawers */
--radius-lg:   16px;  /* onboarding panel, modal */
```

**Rule**: NO uniform bubble-radius on every element. Different surfaces earn different radii. Anti-pattern: `* { border-radius: 12px; }` (AI slop blacklist #5).

---

## Motion Tokens

```css
--duration-fast:   150ms;   /* button state, hover */
--duration-med:    250ms;   /* drawer open, toast slide */
--duration-slow:   400ms;   /* page transition, skeleton pulse */
--ease-out:        cubic-bezier(0.4, 0, 0.2, 1);
--ease-in-out:     cubic-bezier(0.4, 0, 0.6, 1);
```

**Reduced motion**: respected for ALL motion. Wrap all animations in:
```css
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
    transition-duration: 0.01ms !important;
  }
}
```

---

## Lucide Icon Subset (approved ≤ 12)

Only these are allowed in v1. Import individually for tree-shaking.

**Navigation**:
- `Home` (Dashboard `/`)
- `Compass` (`/markets`)
- `Eye` (`/watchlist`)
- `Lightbulb` (`/ideas`)
- `Receipt` (`/trades`)
- `LayoutGrid` (`/positions`)
- `Activity` (`/performance`)

**Interactive**:
- `Play` (Runner start)
- `Pause` (Runner stop)
- `HelpCircle` (WhyPopover trigger)
- `X` (dismiss)
- `RotateCcw` (retry)

**Rules**:
- Size: 14px or 16px ONLY (no decorative oversizing)
- Color: `currentColor` (inherit), never filled in circles
- Never in colored bubble backgrounds (AI slop blacklist #3)
- Weight: default Lucide stroke 2; do NOT use stroke 3 for "bolder look"

Adding a 13th icon requires a design review entry in this file.

---

## Component Contracts

### `<StatusPill>`
- **Props**: `variant` (`live` | `muted` | `error`), `icon` (optional 12px), `label`, `onClick?`
- **States**: idle, hover, active, disabled
- **A11y**: `role="status"` + `aria-live="polite"`; if `onClick`, also `role="button"` + keyboard support
- **Min touch**: 44×44px (entire pill hit-target)

### `<EmptyState>`
- **Props**: `title`, `body`, `cta` (`{label, href}`), `icon` (optional 40px)
- **Layout**: centered, `max-width: 28rem`, vertical rhythm using `--space-4`
- **Rule**: never render without `cta` unless explicitly no action exists

### `<IdeaCard>`
Structure:
- Row 1: question — `--text-20 --ink --weight-medium`
- Row 2: side badge (YES/NO, green/cyan) + limit price + edge % (muted)
- Row 3: rationale — `--text-13 --muted`, max 2 lines with ellipsis
- Row 4: action bar — `[Why]` (text link) ... `[Accept]` (green pill, right-aligned)

Visual:
- `border-radius: --radius-md`
- `background: --idea-bg` (warm paper tint, NOT white)
- **NO colored left-border** (AI slop blacklist #8)

### `<AcceptIdeaButton>`
- **States**: idle, pending ("Accepting..."), success (momentary), disabled, error
- **A11y**: `aria-busy` on pending; `disabled` during inflight
- **Loading indicator**: inline spinner, not full-page
- **Success acknowledgment**: 150ms `scale(1 → 1.03 → 1)` ack, then toast

### `<WhyPopover>`
- **Interaction**: click `[Why]` OR hover after 400ms delay (no immediate hover prevents accidental triggering)
- **Content**: factor contributions (stacked horizontal bar) + rationale text + LLM trace (collapsed "Show reasoning" link)
- **Dismissal**: Esc key, click-outside, or X button
- **A11y**: `role="dialog" aria-modal="true"` when click-opened; `role="tooltip"` when hover-opened

### `<EventLogDrawer>`
- **Default**: collapsed tab on right edge, vertical text "Event log"
- **Expanded**: 320px wide, slides in 250ms ease-out
- **Pin state**: persisted in `localStorage('pms.eventlog.pinned')`
- **Content**: ring buffer 20 entries — time + sensor + summary
- **A11y**: `aria-expanded` on tab trigger; focus management on open

### `<ConnectionBanner>`
- **Placement**: top of every page, sticky
- **States**: connected (hidden), reconnecting (amber), disconnected (`--error` red)
- **Content**: status text + "last fetch N seconds ago" + retry button
- **A11y**: `role="alert"` when state changes to disconnected

### `<OnboardingPanel>`
- **Default**: shown on first visit to `/`
- **Dismissal**: X button OR any successful navigation to another route
- **Persistence**: `localStorage('pms.onboarded')` boolean
- **Re-open**: `?` icon top-right of every page
- **A11y**: focus trap while open; restores focus on dismiss

---

## Status Pill Click Targets (top bar)

All 3 pills are clickable nav shortcuts:

| Pill | Click action |
|---|---|
| `●Live · paper` | Opens dropdown menu with Runner start/stop + mode switch |
| `Calibration 67%` | Navigates to `/performance` |
| `Open positions 0` | Navigates to `/positions` |

---

## Anti-Slop Guardrails (hard rules)

`code-reviewer` rejects violations of these. AI implementers (including Claude Code) must check against this list before opening any dashboard PR.

1. NO `icon-in-colored-circle` pattern (blacklist #3)
2. NO uniform `border-radius` on every element (blacklist #5)
3. NO colored left-border on `<IdeaCard>` or any list row (blacklist #8)
4. NO decorative blobs, floating circles, wavy SVG dividers (blacklist #6)
5. NO gradient backgrounds except the existing coral brand bar
6. NO centered-everything (`text-align: center` only on pills, badges, empty states)
7. NO uninformative toast copy ("Success!", "Done!") — always name the action
8. NO default font stack (`system-ui, sans-serif` alone) — always use token
9. NO purple/indigo/violet anywhere — palette is warm-only
10. NO 3-column feature grid with icons in circles (blacklist #2)

---

## Accessibility Contract (WCAG AA minimum)

- Color contrast ≥ 4.5:1 for text, 3:1 for large text (>18.66px) and UI components
- All interactive elements reachable by Tab in logical reading order
- `:focus-visible` styles on every interactive element: 2px ring in `--coral` at `--space-1` offset
- No `outline: none` without replacement
- Skip-to-main link (visually hidden until focused)
- `aria-live` regions for status changes (pills, toast, `<ConnectionBanner>`)
- Reduced motion handled for all animations
- Touch target min size: 44×44px (iOS guideline)
- Screen reader order matches visual order (no CSS order reshuffling DOM-meaningful content)
- Table semantics for `/markets`, `/trades`, `/positions` (proper `<table>`, `<thead>`, `<th scope>`, not div soup)
- Form fields have `<label>` associations; errors use `aria-describedby` pointing to error text
- Live trading gated with explicit confirmation dialog (future v2), not inline button

---

## Success Toast

- Position: bottom-right, 16px from edge
- Duration: 4s (default); 6s if includes a link (e.g., "View in /trades")
- Motion: slide up `--duration-med` `--ease-out`, fade out `--duration-fast`
- Content: named action + affordance. Good: "First trade placed · View in /trades". Bad: "Success!"

---

## Content Hierarchy (per page)

| Route | H1 | H2 | Notes |
|---|---|---|---|
| `/` | "Today" (`--text-30`) | section breaks in feed (invisible) | Status pills pre-H1, not headings |
| `/markets` | "Markets" (`--text-30`) | n/a | Controls row under H1 |
| `/watchlist` | "Watchlist" (`--text-30`) | n/a | Empty-state link to /markets |
| `/ideas` | "Ideas" (`--text-30`) | IdeaCard questions are H3 | — |
| `/trades` | "Trades" (`--text-30`) | n/a | Row is `<tr>`, no heading |
| `/positions` | "Positions" (`--text-30`) | n/a | Same |
| `/performance` | "Performance" (`--text-30`) | "Brier calibration", "PnL" | H2 for sections |
| `/share/{id}` | Strategy title (`--text-54`) | "Theory", "Performance", "Calibration" | Public page owns H1, no nav wordmark |

---

## Interaction State Matrix (full)

Every new surface must specify all five states:

| Feature | LOADING | EMPTY | PARTIAL | SUCCESS | ERROR |
|---|---|---|---|---|---|
| Dashboard `/` | 3 skeleton pills + skeleton feed rows (5) | Onboarding panel | Feed renders + "Event log unavailable" badge | n/a | ConnectionBanner |
| `/markets` | Skeleton table 10 rows | "No markets yet. Runner is [paused/running]" + Start CTA | Cached + "last fetch 3m ago" banner | n/a | Full-page error w/ retry |
| `/watchlist` | Skeleton rows | **"Your watchlist is empty. Browse 20 markets →"** CTA | Depth stale badge | n/a | Per-row retry |
| `/ideas` | Skeleton IdeaCards 3 | "No ideas yet. Controller hasn't found..." | Ideas render + Why popover disabled | n/a | Per-card retry |
| `/trades` | Skeleton 5 rows | "No trades yet. Accept an idea..." | Cached + stale badge | n/a | Full-page error |
| `/positions` | Skeleton 3 rows | "No open positions..." | Cached | n/a | Full-page error |
| `/performance` | Skeleton charts | "Need at least 5 settled trades..." | n/a | n/a | Retry button |
| `/share/{id}` | Skeleton title + body | 404 or archived message | n/a | n/a | Generic 500 (no stack trace) |
| Accept idea | Button "Accepting..." disabled | n/a | n/a | Toast: "First trade placed · View →" | Toast w/ reason + retry |
| Onboarding dismiss | Instant | n/a | n/a | Subtle fade, no toast | n/a |
| Runner Start/Stop | Pill "Starting..." disabled | n/a | n/a | Pill transitions to ●Live green | Modal with error + retry |

---

## User Journeys

### New-user happy path (Cathedral success metric)

```
t=0s     Open /                       curious
         │
         ▼ sees 3 status pills + feed with 2 ideas + Onboarding panel
t=5s                                  oriented
         │
         ▼ clicks first idea, Why popover expands
t=15s                                 intrigued
         │
         ▼ reads factor contributions, clicks [Accept]
t=25s    → toast "First trade placed · View in /trades"   confident
         │
         ▼ auto-advance to /trades (or click toast)
t=45s    sees trade row: Avalanche YES 0.21 $10 notional  satisfied
         │
         ▼ Onboarding step 2: "check your positions"
t=60s    user has completed the loop                       looped

Target: p50 pms.ui.first_trade_time_seconds < 120s
```

### Returning user (day 2)

```
t=0s   Opens /              sees "Today" feed with NEW items since last visit
                            (small cyan dot on unseen rows)
t=3s   Scans for wins       RESOLVED rows highlight with +$ totals
t=10s  Checks calibration   status pill shows 67%; hover shows 30-day trend
t=30s  Takes action or bounces

Hook: "unseen since last visit" indicator + calibration improvement trajectory
```

### Failure recovery map

| User state | Trigger | UI response |
|---|---|---|
| Confused (empty) | Lands, feed has 0 ideas | Onboarding panel: "Runner is scanning 20 markets. Ideas typically appear in 2 min." + spinner + live event count |
| Frustrated (timeout) | API down > 10s | Full-page message "Backend restarting. Auto-reconnecting." + last-successful-fetch timestamp |
| Lost (wrong page) | On /positions with 0 rows | Empty CTA + "If you haven't placed a trade, start with /ideas" cross-link |
| Bored (no activity) | Runner idle > 10min, no new ideas | Feed shows "Quiet market period. Your strategy is being selective." (anti-anxiety copy) |

### Time-horizon design (Norman)

- **5s (visceral)**: warm palette + serif wordmark + calm feed (not busy dashboard)
- **5min (behavioral)**: one-click Accept + Why popover + cross-links between pages
- **5yr (reflective)**: calibration trajectory + strategy share link + "your first 1000 trades" milestones
