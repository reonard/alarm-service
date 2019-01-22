package processor

type MonitorData struct {
	DeviceId          int64                    `json:"ID"`
	DeviceStatus      int8                     `json:"status"`
	TimeStamp         string                   `json:"once"`
	Customer          string                   `json:"Customer"`
	Project           string                   `json:"Project"`
	Data              []map[string]interface{} `json:"ProbeData"`
	AlarmHandleStatus int8                     `json:"AlarmHandleStatus"`
	AlarmHandle       string                   `json:"AlarmHandle"`
	AlarmDescription  []string                 `json:"AlarmDesc"`
}

type DeviceInfo struct {
	DeviceId   int64
	DeviceName string
	Customer   string
	Project    string
}
