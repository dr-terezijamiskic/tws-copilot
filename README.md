# tws-copilot[README.md](https://github.com/user-attachments/files/25803391/README.md)[README.md](https://github.com/user-attachments/files/25803416/README.md)
# Exit-First Trading Copilot

Manual-first discipline tool for exits and risk reduction. Enforces pre-trade contracts, triggers alarms on invalidation, and tracks rule adherence.

## Philosophy

### What This Is
A discipline enforcement system that:
- Remembers what you decided when calm
- Alarms loudly when invalidation is touched
- Tracks whether you followed your own rules
- Never makes decisions for you

### What This Is Not
- Not financial advice
- Not an auto-trading bot
- Not predictive or analytical
- Not trying to close trades early
- Not allowed to force exits (except invalidation alarm)

### Core Principles
1. **Server-side authority**: Rules engine makes all decisions. UI reacts.
2. **Pure reactor UI**: Client never calculates status. Only displays what server decides.
3. **Manual-first**: Every action requires user confirmation. No automation.
4. **Centralized rules**: All trade logic lives in `lib/rulesEngine.ts`.
5. **Awareness, not prevention**: Friction and logging, never blocking.

## Decision Logic

### EXIT_NOW (Loud, Red, Escalating)
Triggered when price touches invalidation level:
- **LONG**: `price <= invalidationPrice`
- **SHORT**: `price >= invalidationPrice`

**Behavior:**
- Status changes to `EXIT_NOW`
- 880Hz alarm plays every 2 seconds
- Red banner displays: "EXIT NOW - INVALIDATION TRIGGERED"
- Only two buttons: EXIT or OVERRIDE
- 60-second escalation timer starts
- If ignored for 60 seconds: logs `RULE_VIOLATION` action (once per episode)
- Re-triggers if invalidation hit again after override

**Implementation:**
- `lib/rulesEngine.ts`: `checkInvalidation()`
- Returns `{ status: 'EXIT_NOW', playSound: true, escalate: true }`
- `exitNowTriggeredAt` timestamp set on first trigger
- Cleared when returning to ACTIVE (allows fresh episodes)

### SUGGEST_REDUCE (Quiet, Yellow, Optional)
Suggested when conditions indicate uncertainty:
- Chop detected (price flat/sideways)
- Unsure about direction
- No follow-through after N updates

**No-Follow-Through Thresholds:**
- **0DTE trades** (`is0DTE = true`): 3 price updates
- **Regular trades**: 4 price updates

**Anti-Nagging:**
- Triggers exactly once at threshold (`updateCount === threshold`)
- Never triggers again for same reason
- Changed from `>=` to `===` to prevent repeated suggestions

**Behavior:**
- Yellow banner displays with reason
- "REDUCE 50%" button appears
- No alarm, no escalation, no timer
- User can ignore indefinitely

**Implementation:**
- `lib/rulesEngine.ts`: `evaluateRules()`
- Returns `{ status: 'SUGGEST_REDUCE', playSound: false }`
- Conditional logic checks `chop`, `unsure`, and `updateCount`

## Workflow

### 1. Pre-Trade Contract (`/new`)
1. Check for existing ACTIVE trades
2. If found, show modal: "Active Trade Detected"
   - Buttons: "Continue Anyway" | "Cancel"
   - If continued, logs `MULTI_TRADE_ACK` action
3. Fill form: ticker, direction, setup, timeframe, invalidation price, 0DTE flag
4. Submit → creates trade with status `ACTIVE`
5. Redirect to `/trade/[id]`

**Multi-Trade Friction:**
- Awareness only, not blocking
- Modal appears on page load if active trade exists
- No auto-close of existing trades
- Acknowledgment logged for review

### 2. Live Monitor (`/trade/[id]`)
**Active Trade:**
- Enter price updates with flags: chop, unsure, inProfit
- Server evaluates rules, returns decision
- UI displays banners/buttons based on server response
- Manual "EXIT TRADE" button always visible (voluntary exit)

**EXIT_NOW Triggered:**
- Red banner, alarm starts
- Escalation timer counts up
- At 60 seconds: logs `RULE_VIOLATION` (once)
- Two actions: EXIT or OVERRIDE

**SUGGEST_REDUCE Displayed:**
- Yellow banner with reason
- "REDUCE 50%" button
- Can be ignored indefinitely

**Voluntary Exit:**
- "EXIT TRADE" button visible when `status === 'ACTIVE'`
- No confirmation, no alarm, no override logic
- Logs EXIT action, sets status to CLOSED, redirects to `/review`

### 3. Review (`/review`)
Displays metrics:
- Total trades
- Avg exit delay (EXIT_NOW → EXIT action)
- EXIT_NOW ignored >60s (percentage)
- Rule violations (count)
- Avg reduces per trade
- Avg hold time

Shows trade journal table with all historical trades.

## Rules Engine

**Location:** `lib/rulesEngine.ts`

**Function:** `evaluateRules(trade, priceUpdate, updateCount)`

**Returns:** `RulesEngineOutput`
```typescript
{
  status: 'EXIT_NOW' | 'SUGGEST_REDUCE' | 'CONTINUE',
  tradeStatus: 'ACTIVE' | 'EXIT_NOW',
  reason: string,
  uiHints: {
    playSound: boolean,
    escalate: boolean
  }
}
```

**Decision Hierarchy:**
1. Check invalidation → EXIT_NOW (immediate)
2. Check chop → SUGGEST_REDUCE (quiet)
3. Check unsure → SUGGEST_REDUCE (quiet)
4. Check no follow-through → SUGGEST_REDUCE (quiet)
5. Default → CONTINUE

**No Trade-Specific Overrides:**
- No SPY-specific logic
- No ticker-based thresholds
- Uses `is0DTE` boolean field for risk assessment

## Color Semantics

**STRICT LIMITS:**
- **Red** (`bg-red-600`): ONLY for EXIT_NOW banner and EXIT button
- **Yellow** (`bg-yellow-500`): ONLY for SUGGEST_REDUCE banner
- **Everything else**: Monochrome (grays, whites, blacks)

**Not Allowed:**
- No blue, green, purple anywhere
- No colored direction buttons
- No colored nav links
- No colored badges (except EXIT_NOW/SUGGEST_REDUCE contexts)

**Implementation:**
- `app/trade/[id]/page.tsx`: Class constants
  - `DANGER_CLASSES = 'bg-red-600 border-red-600 text-white'`
  - `WARNING_CLASSES = 'bg-yellow-500 border-yellow-500 text-black'`

## Architecture

### Tech Stack
- **Next.js 14+** (App Router)
- **TypeScript**
- **SQLite** (better-sqlite3)
- **Tailwind CSS v4** (utility-based, no config themes)

### Key Patterns
**Server-Side Authority:**
- `/api/price-updates` delegates to rules engine
- Returns decision to client
- Client stores and displays, never calculates

**Pure Reactor UI:**
- `lastDecision` state holds server response
- All banners/buttons conditional on `lastDecision.status`
- No client-side business logic

**Centralized Rules:**
- `lib/rulesEngine.ts` is single source of truth
- API routes are thin wrappers
- Never duplicated logic in UI

### Database Schema

**trades:**
```sql
id                  INTEGER PRIMARY KEY
createdAt           TEXT (ISO datetime)
ticker              TEXT
direction           TEXT ('LONG' | 'SHORT')
setup               TEXT
timeframe           TEXT
invalidationPrice   REAL
entryTime           TEXT (ISO datetime)
status              TEXT ('ACTIVE' | 'EXIT_NOW' | 'CLOSED')
closedAt            TEXT (nullable)
notes               TEXT (nullable)
exitNowTriggeredAt  TEXT (nullable, first EXIT_NOW trigger)
is0DTE              INTEGER (0/1 boolean)
```

**price_updates:**
```sql
id          INTEGER PRIMARY KEY
tradeId     INTEGER (FK)
time        TEXT (ISO datetime)
price       REAL
chop        INTEGER (0/1)
unsure      INTEGER (0/1)
inProfit    INTEGER (0/1)
statusAfter TEXT ('ACTIVE' | 'EXIT_NOW' | 'CLOSED')
reason      TEXT (nullable)
```

**actions:**
```sql
id       INTEGER PRIMARY KEY
tradeId  INTEGER (FK)
time     TEXT (ISO datetime)
type     TEXT ('EXIT' | 'REDUCE' | 'NOTE' | 'OVERRIDE' | 'RULE_VIOLATION' | 'MULTI_TRADE_ACK')
payload  TEXT (JSON string)
```

**ActionPayload Schema:**
```typescript
{
  reason?: string;        // OVERRIDE, RULE_VIOLATION
  amount?: string;        // REDUCE
  note?: string;          // NOTE
  seconds?: number;       // RULE_VIOLATION (delay time)
  tradeId?: number;       // MULTI_TRADE_ACK
  acknowledged?: boolean; // MULTI_TRADE_ACK
}
```

## File Structure

```
app/
├── layout.tsx              # Root layout, nav
├── page.tsx                # Home (→ /review)
├── globals.css             # Tailwind, monochrome theme
├── new/page.tsx            # Pre-trade contract, multi-trade warning
├── trade/[id]/page.tsx     # Live monitor, alarm, escalation
├── review/page.tsx         # Metrics, journal
└── api/
    ├── trades/             # CRUD
    ├── price-updates/      # Rules engine delegation
    ├── actions/            # Action logging
    └── metrics/            # Performance calculations

lib/
├── db/
│   ├── index.ts           # Connection
│   ├── schema.ts          # Table definitions
│   └── queries.ts         # CRUD functions
├── rulesEngine.ts         # Decision logic (SERVER AUTHORITY)
└── types.ts               # TypeScript definitions

data/
└── trades.db              # SQLite (auto-created)
```

## Intentionally NOT Implemented

**No Auto-Trading:**
- No automatic exits
- No automatic position sizing
- No automatic entry signals

**No Analysis:**
- No charts or visualizations
- No P&L tracking
- No win rate calculations
- No pattern detection
- No indicators or technical analysis

**No Multi-Asset:**
- One trade at a time (recommended)
- Multi-trade is allowed but warned against
- No portfolio-level logic

**No External Data:**
- No live price feeds
- No API integrations
- No market data services
- Manual price entry only

**No Customization:**
- Fixed alarm sound (880Hz)
- Fixed escalation threshold (60s)
- Fixed reduce amount (50%)
- Fixed thresholds (3/4 updates)

**No Export/Import:**
- No CSV export
- No trade backup
- No cloud sync
- Local-only database

**No Authentication:**
- Single-user system
- No login
- No permissions

**No Mobile Optimization:**
- Desktop-first design
- Large typography assumes desktop screens

## Key Behaviors

### EXIT_NOW Re-Trigger After Override
**Problem:** After override, subsequent invalidation touches wouldn't re-trigger EXIT_NOW.

**Solution:**
- When status becomes ACTIVE, clear `exitNowTriggeredAt`
- When EXIT_NOW triggers, always update timestamp (no NULL check)
- Allows fresh EXIT_NOW episodes after override

**Implementation:** `lib/db/queries.ts` - `updateTradeStatus()`

### RULE_VIOLATION Logging
**Behavior:**
- Logs exactly once per EXIT_NOW episode
- Triggered 60 seconds after EXIT_NOW
- Includes delay time in payload
- Cleared when status changes (prevents double-logging)

**Implementation:**
- `app/trade/[id]/page.tsx` - `useEffect` with escalation timer
- `violationLogged` state prevents duplicates

### Anti-Nagging for SUGGEST_REDUCE
**Problem:** Using `>=` threshold caused repeated suggestions.

**Solution:** Changed to `===` exact match
- Triggers once at threshold
- Never repeats for same reason

**Implementation:** `lib/rulesEngine.ts` - threshold checks

### Missing Trade Redirect
**Behavior:**
- If `/trade/[id]` receives 404, redirect to `/review`
- Prevents broken page state
- Handles database recreation gracefully

**Implementation:** `app/trade/[id]/page.tsx` - `loadTrade()` error handling

## Development

**Install:**
```bash
npm install
```

**Run:**
```bash
npm run dev
# → http://localhost:3000
```

**Reset Database:**
```bash
rm -f data/trades.db
# Recreates on next run
```

**Build:**
```bash
npm run build
npm start
```

## Version Control

**Initialized:** 2025-12-13

**Commits:**
1. Initial implementation (MVP baseline)
2. Multi-trade awareness + error handling

**Branch:** main

## Testing Checklist

**EXIT_NOW Flow:**
- [ ] Create trade
- [ ] Enter price at invalidation
- [ ] Verify alarm plays
- [ ] Verify red banner
- [ ] Wait 60 seconds
- [ ] Verify RULE_VIOLATION logged
- [ ] Override with reason
- [ ] Verify alarm stops
- [ ] Hit invalidation again
- [ ] Verify EXIT_NOW re-triggers

**SUGGEST_REDUCE Flow:**
- [ ] Create trade
- [ ] Enter 3-4 updates with chop/unsure
- [ ] Verify yellow banner appears
- [ ] Verify no alarm
- [ ] Click REDUCE
- [ ] Verify logged in timeline

**Manual Exit:**
- [ ] Create trade
- [ ] Click "EXIT TRADE"
- [ ] Verify no alarm
- [ ] Verify redirected to /review
- [ ] Verify status is CLOSED

**Multi-Trade Warning:**
- [ ] Create first trade
- [ ] Navigate to /new
- [ ] Verify modal appears
- [ ] Click "Continue Anyway"
- [ ] Create second trade
- [ ] Verify MULTI_TRADE_ACK in timeline

## Notes to Future Self

**If EXIT_NOW stops working:**
- Check `lib/rulesEngine.ts` - `checkInvalidation()`
- Verify price comparison logic (LONG: <=, SHORT: >=)
- Check `exitNowTriggeredAt` is being set in `updateTradeStatus()`

**If alarm won't stop:**
- Check `trade.status` state update
- Verify `useEffect` cleanup runs on unmount
- Check `stopAlarm()` is called in both EXIT and OVERRIDE handlers

**If SUGGEST_REDUCE repeats:**
- Verify `===` threshold check (not `>=`)
- Check `updateCount` from price_updates query
- Verify `is0DTE` boolean is correct

**If database schema changes:**
- Delete `data/trades.db`
- Restart dev server
- Schema auto-recreates from `lib/db/schema.ts`

**If colors disappear:**
- Tailwind v4 uses utility classes directly
- No `@theme` block needed in globals.css
- Check `DANGER_CLASSES` and `WARNING_CLASSES` constants

