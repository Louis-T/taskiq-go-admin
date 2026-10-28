package redisadmin

import "encoding/json"

// ParseTaskMessage reads the taskiq BrokerMessage JSON stored in an entry's
// "data" field. ok=false for anything else.
func ParseTaskMessage(data []byte) (taskID, taskName string, ok bool) {
	var m struct {
		TaskID   string `json:"task_id"`
		TaskName string `json:"task_name"`
	}
	if err := json.Unmarshal(data, &m); err != nil || m.TaskID == "" {
		return "", "", false
	}
	return m.TaskID, m.TaskName, true
}
