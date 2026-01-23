# MeeMee Screener Smoke Test Procedure

**Time Required:** 5–8 minutes  
**Goal:** Verify core functionality is intact without regression.  
**Pass Condition:** All REQUIRED steps complete without crash, error dialog, DB lock error, or UI freeze (no permanent unresponsive state).

---

## 0. Preflight (REQUIRED)
- [ ] Prepare one trade CSV (SBI or Rakuten) on local disk.
- [ ] (Dev) Run `run.ps1` OR (Release) run `MeeMeeScreener.exe`.
- [ ] If a console/log shows backend URL/port, note it for optional API checks.

---

## 1. Launch Re-Check (REQUIRED)
1. Action: Launch the app.
2. Check:
   - [ ] Window appears within 10 seconds.
   - [ ] Title "MeeMee Screener" is visible.
   - [ ] No "Python Error" / "Backend Failed" dialogs.

---

## 2. Startup & Lock Check (REQUIRED)
1. Action: Start app once.
2. Check:
   - [ ] `app.lock` exists in Data Directory (default: `%LOCALAPPDATA%\MeeMeeScreener\data`).
3. Action: Start a second instance immediately.
4. Expectation:
   - [ ] Second instance exits quickly (no two instances running).
   - [ ] Logs mention lock ownership (PID check) or "holds lock" equivalent.

---

## 3. List Data Load (REQUIRED)
1. Action: Wait for the main screen.
2. Check:
   - [ ] Stock grid/list is populated (not empty).
   - [ ] Scrolling works.
   - [ ] Ticker/Name (or equivalent identifiers) are visible.

---

## 4. Basic View Integrity (REQUIRED)
- [ ] Switch between main tabs (e.g., Screener / Ranking / Watchlist). No freeze.
- [ ] Click one stock tile -> Chart/Detail view opens.
- [ ] Return to list view. No freeze.

---

## 5. TXT Update (Job Path) (REQUIRED)
### 5.1 UI Path (REQUIRED)
1. Action: Click **"TXT更新"** (or "TXT Update") button.
2. Check:
   - [ ] UI remains responsive (no permanent hang).
   - [ ] A notification/toast appears (Started/Queued/Running is acceptable).
3. Wait (up to reasonable time for your dataset):
   - [ ] Completion toast appears (Completed) OR a clear error toast appears (Failed).
   - [ ] App still responsive after completion.

### 5.2 Double-Trigger Guard (REQUIRED)
1. Action: While TXT update is running, click the same button again (or trigger again quickly).
2. Expectation:
   - [ ] It is rejected or ignored (e.g., "already running") and does NOT start a second concurrent update.
   - [ ] No DB lock errors.

---

## 6. Force Sync (Job Path) (OPTIONAL / If Implemented)
1. Action: Click **"強制同期"** (Force Sync).
2. Check:
   - [ ] UI remains responsive.
   - [ ] Process finishes (success or clear failure message).
   - [ ] No "Database Locked" errors in UI/log.

---

## 7. CSV Import (Trades) (REQUIRED)
1. Action: Drag & Drop a valid trade CSV (SBI or Rakuten) onto the app.
2. Check:
   - [ ] Notification: Import success (or explicit failure reason).
   - [ ] Positions/held count or related UI state updates.

---

## 8. Persistence & Restart (REQUIRED)
1. Action: Close the app (X).
2. Check:
   - [ ] Process terminates completely (no orphaned backend / webview processes).
   - [ ] `app.lock` is removed from Data Directory.
3. Action: Relaunch the app.
4. Check:
   - [ ] App starts normally, data loads, no corruption/repair dialogs.

---

## 9. Job API Sanity (OPTIONAL / Dev Only)
Use this only if you need API-level confirmation.

- Start TXT update:
  - POST `/api/jobs/txt-update`
  - Expect: `200 OK` + `{ "ok": true, "job_id": "..." }`
- Start again immediately:
  - Expect: `409 Conflict` OR `{ "ok": false, "error": "already_running" }`
- Check status:
  - GET `/api/txt_update/status`
  - Expect: `running: true`, `phase` is `queued` or `running`
- Wait for completion and check status:
  - GET `/api/txt_update/status`
  - Expect: `running: false`, `phase` is `done` (success) or `error`

---

**STOP RULE:** If any REQUIRED step fails, STOP. Do not merge PR. Revert and debug.
