package processor

type MonitorData struct {
	DeviceId     int64                    `json:"ID"`
	DeviceStatus int8                     `json:"status"`
	TimeStamp    string                   `json:"once"`
	Data         []map[string]interface{} `json:"ProbeData"`
}

func Save() {

}
