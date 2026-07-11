# salias Message Flow Animation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a standalone interactive HTML animation that explains salias channel layers and all four message-flow modes, then link it from README.

**Architecture:** Use one self-contained HTML document with semantic controls, inline CSS, and an SVG scene driven by a small JavaScript state machine. Keep mode metadata and animation stages declarative so all four modes reuse the same renderer while changing ordering, fanout, cursor, and backpressure behavior.

**Tech Stack:** HTML5, CSS, inline SVG, vanilla JavaScript, shell-based static validation

---

### Task 1: Create the static animation scene

**Files:**
- Create: `doc/salias-message-flow.html`

**Step 1: Add semantic page structure**

Create the header, mode selector, playback controls, SVG viewport, timeline explanation, legend, and implementation notes.

**Step 2: Add responsive styling**

Define the dark theme, component cards, ring slots, animated paths, message particles, cursor markers, focus states, narrow-screen layout, and reduced-motion fallback using only inline CSS.

**Step 3: Validate standalone structure**

Run: `rtk rg -n "https?://|<script[^>]+src=|<link[^>]+href=" doc/salias-message-flow.html`

Expected: no output.

### Task 2: Implement the animation state machine

**Files:**
- Modify: `doc/salias-message-flow.html`

**Step 1: Define declarative modes and stages**

Add mode metadata for `FifoMpsc`, `FifoFanout`, `OrderedMpsc`, and `OrderedFanout`, plus stages for API offer, frame encoding, ring publication, polling/merge, delivery, release, backpressure, and recovery.

**Step 2: Render mode-specific topology**

Update subscriber count, global sequence visibility, merge-gate visibility, cursor count, labels, and highlighted paths whenever the selected mode changes.

**Step 3: Animate message transfer**

Move labeled message particles across SVG paths, fill ring slots, advance producer positions and consumer cursors, and show ordered waiting or fanout duplication as required by the selected mode.

**Step 4: Implement controls**

Wire play/pause, single-step, replay, speed selection, mode buttons, keyboard activation, and the reduced-motion behavior.

**Step 5: Run JavaScript syntax validation**

Extract the inline script to a temporary file and run: `node --check /tmp/salias-message-flow.js`

Expected: exit code 0 with no syntax errors.

### Task 3: Add README navigation

**Files:**
- Modify: `README.md`

**Step 1: Add an architecture documentation section**

Add a concise link to `doc/salias-message-flow.html`, describing it as an interactive explanation of Channel, endpoints, per-producer rings, four modes, ordering, fanout, release, and backpressure.

**Step 2: Verify the relative target exists**

Run: `test -f doc/salias-message-flow.html && rtk rg -n "salias-message-flow.html" README.md`

Expected: the file test succeeds and README contains one navigation entry.

### Task 4: Validate the completed artifact

**Files:**
- Verify: `doc/salias-message-flow.html`
- Verify: `README.md`

**Step 1: Check required controls and modes**

Run: `rtk rg -n "FifoMpsc|FifoFanout|OrderedMpsc|OrderedFanout|play|pause|step|replay|speed" doc/salias-message-flow.html`

Expected: all four modes and control identifiers are present.

**Step 2: Check repository diff**

Run: `rtk git diff --check && rtk git diff -- README.md doc/salias-message-flow.html doc/plans/2026-07-11-message-flow-animation-design.md doc/plans/2026-07-11-message-flow-animation.md`

Expected: no whitespace errors; diff is limited to the planned documentation and animation files.

**Step 3: Perform browser smoke review**

Open `doc/salias-message-flow.html` in a browser and verify mode switching, play/pause, stepping, replay, speed changes, responsive layout, and explanatory text.
