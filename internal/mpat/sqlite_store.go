package mpat

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
	"github.com/google/uuid"

	_ "modernc.org/sqlite"
)

type MPATSQLiteStore struct {
	db *sql.DB
}

func NewMPATSQLiteStore(path string) (*MPATSQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	s := &MPATSQLiteStore{db: db}

	if err := s.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}

	return s, nil
}

func (s *MPATSQLiteStore) Close() error {
	return s.db.Close()
}

func (s *MPATSQLiteStore) init(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS tasks (
	uuid          TEXT PRIMARY KEY,
	status        TEXT NOT NULL,
	created       TIMESTAMP NOT NULL,
	retina_stream TEXT
);
`)
	return err
}

func (s *MPATSQLiteStore) CreateTask(ctx context.Context, req api.CreateTaskRequest) (api.Task, error) {
	if err := ctx.Err(); err != nil {
		return api.Task{}, err
	}

	task := api.Task{
		UUID:         uuid.NewString(),
		Status:       api.TaskStatusQueued,
		Created:      time.Now(),
		RetinaStream: req.RetinaStream,
	}

	retinaStreamJSON, err := json.Marshal(task.RetinaStream)
	if err != nil {
		return api.Task{}, err
	}

	_, err = s.db.ExecContext(
		ctx,
		`INSERT INTO tasks (uuid, status, created, retina_stream) VALUES (?, ?, ?, ?)`,
		task.UUID,
		string(task.Status),
		task.Created,
		string(retinaStreamJSON),
	)
	if err != nil {
		return api.Task{}, err
	}

	return task, nil
}

func (s *MPATSQLiteStore) GetTask(ctx context.Context, taskUUID string) (api.Task, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT uuid, status, created, retina_stream
FROM tasks
WHERE uuid = ?
`, taskUUID)

	task, err := scanTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return api.Task{}, ErrTaskNotFound
	}
	if err != nil {
		return api.Task{}, err
	}

	return task, nil
}

func (s *MPATSQLiteStore) ListTasks(ctx context.Context) ([]api.Task, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT uuid, status, created, retina_stream
FROM tasks
ORDER BY created DESC
`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	return scanTasks(rows)
}

func (s *MPATSQLiteStore) ListTasksByStatus(ctx context.Context, statuses ...api.TaskStatus) ([]api.Task, error) {
	if len(statuses) == 0 {
		return s.ListTasks(ctx)
	}

	args := make([]any, 0, len(statuses))
	query := `
SELECT uuid, status, created, retina_stream
FROM tasks
WHERE status IN (`

	for i, status := range statuses {
		if i > 0 {
			query += ", "
		}

		query += "?"
		args = append(args, string(status))
	}

	query += `)
ORDER BY created DESC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	return scanTasks(rows)
}

func (s *MPATSQLiteStore) UpdateTaskStatus(ctx context.Context, taskUUID string, status api.TaskStatus) error {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE tasks SET status = ? WHERE uuid = ?`,
		string(status),
		taskUUID,
	)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return ErrTaskNotFound
	}

	return nil
}

func (s *MPATSQLiteStore) CancelTask(ctx context.Context, taskUUID string) (api.Task, error) {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE tasks SET status = ? WHERE uuid = ?`,
		string(api.TaskStatusCancelled),
		taskUUID,
	)
	if err != nil {
		return api.Task{}, err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return api.Task{}, err
	}

	if n == 0 {
		return api.Task{}, ErrTaskNotFound
	}

	return s.GetTask(ctx, taskUUID)
}

type taskScanner interface {
	Scan(dest ...any) error
}

func scanTask(row taskScanner) (api.Task, error) {
	var task api.Task
	var status string
	var retinaStreamJSON sql.NullString

	err := row.Scan(
		&task.UUID,
		&status,
		&task.Created,
		&retinaStreamJSON,
	)
	if err != nil {
		return api.Task{}, err
	}

	task.Status = api.TaskStatus(status)

	if retinaStreamJSON.Valid && retinaStreamJSON.String != "" && retinaStreamJSON.String != "null" {
		if err := json.Unmarshal([]byte(retinaStreamJSON.String), &task.RetinaStream); err != nil {
			return api.Task{}, err
		}
	}

	return task, nil
}

func scanTasks(rows *sql.Rows) ([]api.Task, error) {
	var tasks []api.Task

	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tasks, nil
}
