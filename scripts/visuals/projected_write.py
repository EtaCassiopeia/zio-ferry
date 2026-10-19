"""ProjectedWrite: commit, then project; a projection that did not happen is left pending in the store, a sweeper
finishes it, and the projection's fence refuses a second one."""

from hud import AMBER, CYAN, DIM, FAINT, GREEN, PINK, RED, Scene

T = 13.0
STAGE_Y, SWEEP_Y = 150, 286
COMMIT_X, PROJECT_X, SWEEP_X = 222, 382, 382     # left edges, 112 wide
STORE_X, STORE_W = 640, 250
LOG_Y, MARK_Y, VIEW_Y = 168, 210, 262

# When each box is lit
ACTIVE = {"COMMIT": [(1.0, 2.2)], "PROJECT": [(2.4, 4.0)], "SWEEPER": [(5.6, 7.6)]}


def build() -> Scene:
    sc = Scene(
        STORE_X + STORE_W + 28, 440, T,
        "ProjectedWrite",
        "commit · then project · pending until projected · resume from the store",
        "the marker lives in the store, so any process can finish a projection; its fence refuses a second one",
        "ProjectedWrite: a batch is committed to the key's log, which leaves the record's projection pending. The "
        "projection into the read-side view is refused until its retries run out, so the callers are answered "
        "Committed: durable, view pending. A sweeper finds the pending record, projects it, and the marker is "
        "cleared. A late projection of the old record is then refused by the fence, so nothing is projected twice.",
    )
    sc.add(sc.text(28, 88, "BATCH", "tiny"), sc.text(COMMIT_X, 88, "ONE CALL", "tiny"),
           sc.text(STORE_X, 88, "STORE · ONE RECORD PER KEY", "tiny"))

    # The batch, and what its callers are told
    for i, n in enumerate([1, 2]):
        y = 150 + i * 50
        sc.add(sc.chip(n, PINK, sc.path([(0.2, 20, y, 0), (0.6, 60, y)])))
        sc.add(sc.text(80, y + 4, "Committed", "tag", AMBER, style=sc.shown([(4.2 + 0.15 * i, T - 0.3)])))
    sc.add(sc.text(28, 260, "durable · view pending", "hud", AMBER, style=sc.shown([(4.4, T - 0.3)])))

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
           sc.text(PROJECT_X + 56, STAGE_Y + 42, "fenced on the record", "hud", None, "middle"),
           sc.text(SWEEP_X + 56, SWEEP_Y + 42, "resume(key)", "hud", CYAN, "middle"))

    # The store: the log, the marker, and the view
    sc.add(sc.panel(STORE_X, 112, STORE_W, 196))
    sc.add(sc.text(STORE_X + 14, LOG_Y + 4, "log", "tiny"), sc.text(STORE_X + 14, VIEW_Y + 4, "view", "tiny"))
    cx = STORE_X + 74
    sc.add(sc.chip("x", FAINT, "", cx, LOG_Y), sc.chip("x", FAINT, "", cx, VIEW_Y))
    for j, n in enumerate([1, 2]):
        sc.add(sc.chip(n, PINK, sc.shown([(2.0 + 0.1 * j, T - 0.2)]), cx + 28 * (j + 1), LOG_Y),
               sc.chip(n, PINK, sc.shown([(7.2 + 0.1 * j, T - 0.2)]), cx + 28 * (j + 1), VIEW_Y))
    mid = STORE_X + STORE_W / 2
    sc.add(sc.text(mid, MARK_Y + 4, "projected 1 / 1", "tag", GREEN, "middle", sc.shown([(0.0, 2.0)])),
           sc.text(mid, MARK_Y + 4, "pending: projected 1 / 3", "tag", AMBER, "middle", sc.shown([(2.0, 7.3)])),
           sc.text(mid, MARK_Y + 4, "projected 3 / 3", "tag", GREEN, "middle", sc.shown([(7.3, T - 0.2)])),
           sc.pulse(mid, MARK_Y, AMBER, 2.0, 18), sc.pulse(mid, MARK_Y, CYAN, 6.2, 18),
           sc.pulse(mid, MARK_Y, GREEN, 7.3, 18))

    # Writes travel between the call, the sweeper and the store
    def trip(t0, t1, x0, x1, y, color, label, w=34):
        return sc.chip(label, color, sc.path([(t0, x0, y, 0), (t0 + 0.15, x0, y), (t1 - 0.15, x1, y), (t1, x1, y, 0)]),
                       w=w)
    sc.add(trip(1.2, 2.0, COMMIT_X + 112, STORE_X, STAGE_Y - 46, PINK, "1 2"),
           sc.text(STORE_X - 12, STAGE_Y - 62, "✓ committed", "tag", GREEN, "end", sc.shown([(2.0, 2.7)])),
           sc.pulse(STORE_X, STAGE_Y - 46, GREEN, 2.0, 14))
    # The projection is refused, retried, refused again: the call gives up and answers Committed
    for k, at in enumerate([2.9, 3.6]):
        sc.add(trip(at - 0.5, at, PROJECT_X + 112, STORE_X, STAGE_Y, CYAN, "view", 40),
               sc.pulse(STORE_X, STAGE_Y, RED, at, 14))
    sc.add(sc.text(STORE_X - 12, STAGE_Y - 30, "✗ projection refused", "tag", RED, "end", sc.shown([(2.9, 4.4)])),
           sc.text(PROJECT_X, STAGE_Y + 70, "retries ran out: answer, don't fail", "tag", AMBER,
                   style=sc.shown([(4.0, 5.4)])))
    # The sweeper reads the marker, then projects the record it read
    sc.add(trip(5.8, 6.2, STORE_X, SWEEP_X + 112, SWEEP_Y, CYAN, "read", 40),
           sc.text(SWEEP_X + 124, SWEEP_Y - 30, "pending: project it", "hud", CYAN, style=sc.shown([(6.2, 7.0)])),
           trip(6.6, 7.2, SWEEP_X + 112, STORE_X, SWEEP_Y, CYAN, "view", 40),
           sc.text(STORE_X - 12, SWEEP_Y + 30, "✓ view caught up", "tag", GREEN, "end", sc.shown([(7.2, 8.4)])),
           sc.pulse(STORE_X, SWEEP_Y, GREEN, 7.2, 14))
    # A late projection of the old record, still on the wire, arrives after it
    sc.add(trip(8.6, 9.4, PROJECT_X + 112, STORE_X, STAGE_Y, AMBER, "late", 40),
           sc.pulse(STORE_X, STAGE_Y, RED, 9.4, 14),
           sc.text(STORE_X - 12, STAGE_Y - 30, "✗ fence: record moved on", "tag", RED, "end", sc.shown([(9.4, 11.0)])),
           sc.text(PROJECT_X, STAGE_Y + 70, "nothing is projected twice", "hud", DIM, style=sc.shown([(9.8, T - 0.3)])))
    return sc
