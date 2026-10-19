"""ProjectedWrite, one row per entry: a throttled row leaves the record partially projected, each input is answered on
its own, a sweeper writes only the missing row and then clears the flag, and a late duplicate row is found present."""

from hud import AMBER, CYAN, DIM, FAINT, GREEN, PINK, RED, Scene

T = 14.0
STAGE_Y, SWEEP_Y = 150, 300
COMMIT_X, PROJECT_X, SWEEP_X = 222, 382, 382     # left edges, 112 wide
STORE_X, STORE_W = 640, 250
LOG_Y, MARK_Y, VIEW_Y = 168, 214, 266

# When each box is lit
ACTIVE = {"COMMIT": [(1.0, 2.2)], "PROJECT": [(2.4, 4.3)], "SWEEPER": [(5.8, 8.4)]}

# Each entry's row: when it lands in the view, and where (rows are kept in landing order)
ROWS = {1: (2.9, 0), 3: (3.7, 1), 2: (7.4, 2)}


def build() -> Scene:
    sc = Scene(
        STORE_X + STORE_W + 28, 460, T,
        "ProjectedWrite · one row per entry",
        "a row per entry · an answer per input · resume",
        "rows are idempotent; the flag clears only when every row is in, fenced on the record",
        "ProjectedWrite with a view of one row per entry: a batch of three is committed, which sets the record's flag. "
        "The projection writes a row per entry; the second is throttled, so the record is partially projected. Each "
        "input is answered on its own: the two whose rows landed are Projected, the throttled one is Committed with "
        "its cause. A sweeper finds the flag set, writes only the missing row, and clears the flag. A late duplicate "
        "of a row is found present and is not written twice.",
    )
    sc.add(sc.text(28, 88, "BATCH", "tiny"), sc.text(COMMIT_X, 88, "ONE CALL", "tiny"),
           sc.text(STORE_X, 88, "STORE · ONE RECORD PER KEY", "tiny"))

    # The batch, and what each caller is told
    for i, n in enumerate([1, 2, 3]):
        y = 150 + i * 40
        sc.add(sc.chip(n, PINK, sc.path([(0.2, 20, y, 0), (0.6, 60, y), (T - 0.4, 60, y), (T - 0.1, 60, y, 0)])))
        if n == 2:
            sc.add(sc.text(80, y + 4, "Committed", "tag", AMBER, style=sc.shown([(4.3, T - 0.3)])))
        else:
            sc.add(sc.text(80, y + 4, "Projected", "tag", GREEN, style=sc.shown([(4.3 + 0.1 * i, T - 0.3)])))
    sc.add(sc.text(28, 286, "2: durable · row pending", "hud", AMBER, style=sc.shown([(4.5, T - 0.3)])),
           sc.text(28, 304, "cause: Throttled", "hud", AMBER, style=sc.shown([(4.5, T - 0.3)])))

    # The two writes of the call, and the sweeper that runs anywhere
    def box(name, x, y, windows):
        sc.add(f'<rect x="{x}" y="{y - 22}" width="112" height="44" rx="8" fill="#070b16" stroke="#334155"/>',
               f'<rect x="{x}" y="{y - 22}" width="112" height="44" rx="8" fill="{CYAN}" fill-opacity=".08" '
               f'stroke="{CYAN}" filter="url(#glow)" style="{sc.shown(windows, 0.12)}"/>',
               sc.text(x + 56, y + 5, name, "big", anchor="middle"))
    box("COMMIT", COMMIT_X, STAGE_Y, ACTIVE["COMMIT"])
    box("PROJECT", PROJECT_X, STAGE_Y, ACTIVE["PROJECT"])
    box("SWEEPER", SWEEP_X, SWEEP_Y, ACTIVE["SWEEPER"])
    sc.add(f'<path d="M{COMMIT_X + 112} {STAGE_Y} H{PROJECT_X}" stroke="#334155" stroke-width="1.5"/>',
           sc.text(COMMIT_X + 56, STAGE_Y + 42, "fenced on the log", "hud", None, "middle"),
           sc.text(PROJECT_X + 56, STAGE_Y + 42, "a write per row", "hud", None, "middle"),
           sc.text(SWEEP_X + 56, SWEEP_Y + 42, "resume(key)", "hud", CYAN, "middle"))

    # The store: the log, the flag, and the view's rows
    sc.add(sc.panel(STORE_X, 112, STORE_W, 200))
    sc.add(sc.text(STORE_X + 14, LOG_Y + 4, "log", "tiny"), sc.text(STORE_X + 14, VIEW_Y + 4, "rows", "tiny"))
    cx = STORE_X + 74
    sc.add(sc.chip("x", FAINT, "", cx, LOG_Y), sc.chip("x", FAINT, "", cx, VIEW_Y))
    for j, n in enumerate([1, 2, 3]):
        at, slot = ROWS[n]
        sc.add(sc.chip(n, PINK, sc.shown([(2.0 + 0.1 * j, T - 0.2)]), cx + 28 * (j + 1), LOG_Y),
               sc.chip(n, PINK, sc.shown([(at, T - 0.2)]), cx + 28 * (slot + 1), VIEW_Y))
    mid = STORE_X + STORE_W / 2
    sc.add(sc.text(mid, MARK_Y + 4, "flag clear", "tag", GREEN, "middle", sc.shown([(0.0, 2.0), (T - 0.2, T)])),
           sc.text(mid, MARK_Y + 4, "flag set: pending", "tag", AMBER, "middle", sc.shown([(2.0, 4.0)])),
           sc.text(mid, MARK_Y + 4, "flag set: row 2 missing", "tag", AMBER, "middle", sc.shown([(4.0, 7.9)])),
           sc.text(mid, MARK_Y + 4, "flag clear: every row in", "tag", GREEN, "middle", sc.shown([(7.9, T - 0.2)])),
           sc.pulse(mid, MARK_Y, AMBER, 2.0, 18), sc.pulse(mid, MARK_Y, CYAN, 6.4, 18),
           sc.pulse(mid, MARK_Y, GREEN, 7.9, 18))

    # Writes travel between the call, the sweeper and the store
    def trip(t0, t1, x0, x1, y, color, label, w=34):
        return sc.chip(label, color, sc.path([(t0, x0, y, 0), (t0 + 0.15, x0, y), (t1 - 0.15, x1, y), (t1, x1, y, 0)]),
                       w=w)
    sc.add(trip(1.2, 2.0, COMMIT_X + 112, STORE_X, STAGE_Y - 46, PINK, "1 2 3", 44),
           sc.text(STORE_X - 12, STAGE_Y - 62, "✓ committed", "tag", GREEN, "end", sc.shown([(2.0, 2.7)])),
           sc.pulse(STORE_X, STAGE_Y - 46, GREEN, 2.0, 14))
    # One write per row: rows 1 and 3 land, row 2 is throttled, so the flag stays set and the verdict lists 2
    for n, verdict in [(1, GREEN), (2, RED), (3, GREEN)]:
        at = 2.9 + 0.4 * (n - 1)
        sc.add(trip(at - 0.45, at, PROJECT_X + 112, STORE_X, STAGE_Y, CYAN, f"row {n}", 44),
               sc.pulse(STORE_X, STAGE_Y, verdict, at, 14))
    sc.add(sc.text(STORE_X - 12, STAGE_Y - 30, "✗ row 2 throttled", "tag", RED, "end", sc.shown([(3.3, 4.6)])),
           sc.text(PROJECT_X, STAGE_Y + 70, "pending: {2 → Throttled}", "tag", AMBER, style=sc.shown([(3.9, 5.6)])),
           sc.text(PROJECT_X, STAGE_Y + 88, "a verdict, not a failure", "hud", DIM, style=sc.shown([(4.1, 5.6)])))
    # The sweeper reads the record and the rows, writes only the missing one, then clears the flag
    sc.add(trip(6.0, 6.4, STORE_X, SWEEP_X + 112, SWEEP_Y, CYAN, "read", 40),
           sc.text(SWEEP_X + 124, SWEEP_Y - 30, "missing: row 2", "hud", CYAN, style=sc.shown([(6.4, 7.2)])),
           trip(6.8, 7.4, SWEEP_X + 112, STORE_X, SWEEP_Y, CYAN, "row 2", 44),
           sc.pulse(STORE_X, SWEEP_Y, GREEN, 7.4, 14),
           trip(7.5, 7.9, SWEEP_X + 112, STORE_X, SWEEP_Y, GREEN, "clear", 44),
           sc.text(STORE_X - 12, SWEEP_Y + 30, "✓ clear: fenced on the record", "tag", GREEN, "end",
                   sc.shown([(7.9, 9.0)])),
           sc.pulse(STORE_X, SWEEP_Y, GREEN, 7.9, 14))
    # A duplicate of row 3, late on the wire, arrives after it: present, not written again
    sc.add(trip(9.2, 10.0, PROJECT_X + 112, STORE_X, STAGE_Y, AMBER, "row 3", 44),
           sc.pulse(STORE_X, STAGE_Y, CYAN, 10.0, 14),
           sc.text(STORE_X - 12, STAGE_Y - 30, "already present", "tag", CYAN, "end", sc.shown([(10.0, 11.6)])),
           sc.text(PROJECT_X, STAGE_Y + 70, "each row is in the view once", "hud", DIM, style=sc.shown([(10.4, T - 0.3)])))
    return sc
