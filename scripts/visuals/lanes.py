"""Ferry.perKey under the hood: lane states, the ready line, one worker taking keys in turn, a direct call."""

from hud import AMBER, BLUE, CYAN, DIM, PINK, Scene

T = 12.4
ROWS = {"a": 150, "b": 230, "c": 310}
COLORS = {"a": PINK, "b": AMBER, "c": CYAN}
LANE_X, DOCK_X, SHUTTLE_W, CROSS = 272, 400, 110, 250
STORE_X = DOCK_X + SHUTTLE_W + CROSS + 20
LINE_Y = 392

# The worker's trips: (key, start). Each: load 0.3 s, cross 1.2 s, unload 0.4 s, return 0.9 s, move rows 0.3 s.
TRIPS = [("a", 0.0), ("b", 3.1), ("a", 6.2), ("b", 9.3)]


def slot_x(slot: int) -> float:
    return DOCK_X - 26 - slot * 24


def seat_x(i: int, n: int) -> float:
    return DOCK_X + SHUTTLE_W / 2 - (n - 1) * 12 + i * 24


def build() -> Scene:
    sc = Scene(
        STORE_X + 88, 470, T,
        "Ferry.perKey · under the hood",
        "maxInFlightBatches = 1 · all state in one TRef[Protocol] · STM",
        "the worker takes the first ready key; a key that still has inputs rejoins the back of the line; idle c skips it",
        "Inside Ferry.perKey: each key has a lane that is ready, busy or idle. One worker takes the first key in the "
        "ready line, carries its batch, and puts the key back at the end of the line if more inputs arrived, so a "
        "flooded key a cannot starve key b. Key c is idle, so its caller sends its input directly without the worker.",
    )
    sc.add(sc.text(28, 88, "KEYS", "tiny"), sc.text(150, 88, "LANE STATE", "tiny"), sc.text(LANE_X, 88, "LANE", "tiny"),
           sc.text(DOCK_X, 88, "CALLS", "tiny"), sc.text(STORE_X + 30, 88, "STORE", "tiny", anchor="middle"))
    sc.add(sc.panel(STORE_X, 116, 60, 228), sc.text(STORE_X + 30, 234, "write", "tiny", anchor="middle"))
    for k, y in ROWS.items():
        sc.add(sc.text(28, y + 5, f"key = {k}", "key"), sc.bracket(LANE_X, y, DOCK_X - LANE_X - 12),
               sc.rail(DOCK_X, y, STORE_X, COLORS[k]))

    # Lane states over the loop
    states = {
        "a": {"BUSY": [(0.0, 2.8), (6.2, 9.0)], "READY": [(2.8, 6.2), (9.0, T)]},
        "b": {"READY": [(0.0, 3.1), (5.9, 9.3), (12.1, T)], "BUSY": [(3.1, 5.9), (9.3, 12.1)]},
        "c": {"IDLE": [(0.0, 1.0), (4.1, 7.4), (10.5, T)], "BUSY · direct": [(1.0, 4.1), (7.4, 10.5)]},
    }
    tone = {"BUSY": PINK, "READY": "#34d399", "IDLE": DIM, "BUSY · direct": CYAN}
    for k, by_state in states.items():
        for s, windows in by_state.items():
            color = COLORS[k] if s.startswith("BUSY") else tone[s]
            sc.add(sc.badge(150, ROWS[k], s, color, sc.shown(windows, 0.1)))

    # The ready line: snapshots of its contents, front on the right
    sc.add(sc.text(28, LINE_Y + 5, "ready line", "big"),
           f'<rect x="150" y="{LINE_Y - 20}" width="150" height="40" rx="6" fill="#070b16" stroke="#334155"/>',
           sc.text(312, LINE_Y + 5, "next → worker", "hud"))
    line = [(["b"], 0.0, 2.8), (["b", "a"], 2.8, 3.1), (["a"], 3.1, 5.9), (["a", "b"], 5.9, 6.2),
            (["b"], 6.2, 9.0), (["b", "a"], 9.0, 9.3), (["a"], 9.3, 12.1), (["a", "b"], 12.1, T)]
    for keys, on, off in line:
        chips = [sc.chip(k, COLORS[k], "", 280 - i * 30, LINE_Y) for i, k in enumerate(keys)]
        sc.add(f'<g style="{sc.shown([(on, off)], 0.08)}">', *chips, "</g>")

    # Inputs: (key, arrive, board, slot, trip index or None for one still waiting at the loop's end)
    inputs = (
        [("a", None, 0.0, i, 0) for i in range(3)]
        + [("a", t, 6.2, i, 2) for i, t in enumerate([0.8, 1.6, 2.4])]
        + [("a", t, None, i, None) for i, t in enumerate([7.0, 7.8, 8.6])]
        + [("b", None, 3.1, i, 1) for i in range(2)]
        + [("b", 5.0, 9.3, 0, 3)]
        + [("b", t, None, i, None) for i, t in enumerate([10.5, 11.3])]
    )
    sizes = {0: 3, 1: 2, 2: 3, 3: 1}
    seats: dict[int, int] = {}
    riders: list[str] = []  # drawn last, so they show on top of the shuttles
    for k, arrive, board, slot, trip in inputs:
        y, x = ROWS[k], slot_x(slot)
        pts = [(0.0, x, y, 1)] if arrive is None else [(arrive, x - 30, y, 0), (arrive + 0.3, x, y)]
        if trip is not None:
            seat = seats.get(trip, 0)
            seats[trip] = seat + 1
            sx = seat_x(seat, sizes[trip])
            pts += [(board, x, y), (board + 0.3, sx, y), (board + 0.3 + 0.01, sx, y),
                    (board + 1.5, sx + CROSS, y), (board + 1.9, sx + CROSS, y, 0)]
        riders.append(sc.chip(k, COLORS[k], sc.path(pts)))

    # The worker: one shuttle that moves to the row of the key it takes
    track = [(0.0, 0, 0)]
    for i, (k, s) in enumerate(TRIPS):
        dy = ROWS[k] - ROWS["a"]
        nxt = ROWS[TRIPS[(i + 1) % len(TRIPS)][0]] - ROWS["a"]
        track += [(s, 0, dy), (s + 0.3, 0, dy), (s + 1.5, CROSS, dy), (s + 1.9, CROSS, dy), (s + 2.8, 0, dy),
                  (s + 3.1, 0, nxt)]
    tags = [sc.text(DOCK_X + SHUTTLE_W / 2, ROWS["a"] - 24, f"worker · batch of {sizes[i]}", "tag", BLUE, "middle",
                    sc.shown([(s, s + 1.9)])) for i, (_, s) in enumerate(TRIPS)]
    # tags sit above the shuttle, so they must follow it on every row
    sc.add(f'<g style="{sc.path(track)}">', sc.shuttle(SHUTTLE_W, BLUE, DOCK_X, ROWS["a"]), *tags, "</g>")
    sc.add(*(sc.pulse(STORE_X, ROWS[k], COLORS[k], s + 1.5) for k, s in TRIPS))

    # Key c: idle, so its caller sends each input itself, while the worker is busy elsewhere
    y = ROWS["c"]
    for at in [1.0, 7.4]:
        sx = DOCK_X + SHUTTLE_W / 2
        riders.append(sc.chip("c", CYAN, sc.path([(at - 0.4, slot_x(0) - 30, y, 0), (at, slot_x(0), y), (at + 0.3, sx, y),
                                            (at + 1.5, sx + CROSS, y), (at + 1.9, sx + CROSS, y, 0)])))
        shuttle = sc.path([(at, 0, 0, 0), (at + 0.25, 0, 0, 1), (at + 0.3, 0, 0, 1), (at + 1.5, CROSS, 0, 1),
                           (at + 1.9, CROSS, 0, 1), (at + 2.9, 0, 0, 1), (at + 3.1, 0, 0, 0)])
        tag = sc.text(DOCK_X + SHUTTLE_W / 2, y - 24, "direct call", "tag", CYAN, "middle")
        sc.add(f'<g style="{shuttle}">', sc.shuttle(SHUTTLE_W, CYAN, DOCK_X, y), tag, "</g>")
        sc.add(sc.pulse(STORE_X, y, CYAN, at + 1.5))
    sc.add(*riders)
    return sc
