package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration0030PreservesNotificationColumnsUpAndDown(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "ao.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	upTo(t, db, 29)
	if _, err := db.Exec(`INSERT INTO notifications (id, session_id, project_id, pr_url, type, title, body, status, created_at)
		VALUES ('ntf-1', 'session-1', 'project-1', '', 'human_gate', 'Decision required', 'Choose A', 'unread', '2026-07-17T08:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	upTo(t, db, 30)
	assertNotificationMigrationRow(t, db, true)
	if _, err := db.Exec(`UPDATE notifications SET dedupe_key = 'gate-1' WHERE id = 'ntf-1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO notifications (id, session_id, project_id, pr_url, dedupe_key, type, title, body, status, created_at)
		VALUES ('ntf-older', 'session-1', 'project-1', '', 'gate-older', 'human_gate', 'Older decision', 'Choose old', 'unread', '2026-07-17T07:00:00Z')`); err != nil {
		t.Fatal(err)
	}

	gooseMu.Lock()
	goose.SetBaseFS(migrationsFS)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("sqlite3"); err != nil {
		gooseMu.Unlock()
		t.Fatal(err)
	}
	err = goose.Down(db, "migrations")
	gooseMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	assertNotificationMigrationRow(t, db, false)
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM notifications`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("downgraded notification count=%d err=%v", count, err)
	}
}

func assertNotificationMigrationRow(t *testing.T, db *sql.DB, withDedupe bool) {
	t.Helper()
	query := `SELECT id, session_id, project_id, pr_url, type, title, body, status, created_at FROM notifications WHERE id = 'ntf-1'`
	var id, sessionID, projectID, prURL, notificationType, title, body, status, createdAt string
	if err := db.QueryRow(query).Scan(&id, &sessionID, &projectID, &prURL, &notificationType, &title, &body, &status, &createdAt); err != nil {
		t.Fatal(err)
	}
	if id != "ntf-1" || sessionID != "session-1" || projectID != "project-1" || prURL != "" || notificationType != "human_gate" || title != "Decision required" || body != "Choose A" || status != "unread" || createdAt == "" {
		t.Fatalf("notification row changed: id=%q session=%q project=%q pr=%q type=%q title=%q body=%q status=%q created=%q", id, sessionID, projectID, prURL, notificationType, title, body, status, createdAt)
	}
	var columnCount int
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info('notifications') WHERE name = 'dedupe_key'`).Scan(&columnCount); err != nil {
		t.Fatal(err)
	}
	want := 0
	if withDedupe {
		want = 1
	}
	if columnCount != want {
		t.Fatalf("dedupe_key columns=%d want=%d", columnCount, want)
	}
}
