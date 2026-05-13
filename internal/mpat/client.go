package mpat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dioptra-io/ufuk-research/internal/api"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) ListTasks(ctx context.Context, status string) ([]api.Task, error) {
	url := c.baseURL + "/tasks/" + status
	if status == "" {
		url = c.baseURL + "/tasks"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list tasks: unexpected status %s", resp.Status)
	}

	var tasks []api.Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}

	return tasks, nil
}

func (c *Client) CreateTask(ctx context.Context, task api.Task) (*api.Task, error) {
	body, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}

	url := c.baseURL + "/tasks"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("create task: unexpected status %s", resp.Status)
	}

	var createdTask api.Task
	if err := json.NewDecoder(resp.Body).Decode(&createdTask); err != nil {
		return nil, err
	}

	return &createdTask, nil
}

func (c *Client) CancelTask(ctx context.Context, uuid string) error {
	if uuid == "" {
		return fmt.Errorf("cancel task: uuid is required")
	}

	url := c.baseURL + "/tasks/" + uuid + "/cancel"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cancel task: unexpected status %s", resp.Status)
	}

	return nil
}
