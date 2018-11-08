package processor

type MonitorData struct {
	DeviceId          int64                    `json:"ID"`
	DeviceStatus      int8                     `json:"status"`
	TimeStamp         string                   `json:"once"`
	Data              []map[string]interface{} `json:"ProbeData"`
	AlarmHandleStatus int8                     `json:"AlarmHandleStatus"`
	AlarmHandle       string                   `json:"AlarmHandle"`
	AlarmDescription  []string                 `json:"AlarmDesc"`
}

func Save() {

}
