package processor

import (
	"alarm-service/db"
	"fmt"
	"strconv"
	"time"
)

func GetDeviceInfo(deviceId int64) (error, DeviceInfo) {

	deviceInfo := DeviceInfo{}

	err := db.MySqlDB.QueryRow("select device_id, device_name, t_device.project as project from t_device "+
		"left join t_customer on project = t_customer.id where device_id = ? ", deviceId).Scan(&deviceInfo.DeviceId,
		&deviceInfo.DeviceName,
		&deviceInfo.Project)

	if err != nil {
		return err, deviceInfo
	}

	return nil, deviceInfo

}

func InsertAlarmItem(data *MonitorData, metricKey string) {

	unixTime, _ := strconv.Atoi(data.TimeStamp)

	err := db.ExecuteSQL(
		"INSERT INTO t_alarm_item (`device_id`, `timestamp`, `alarm_item`, `alarm_value`) VALUES (?,?,?,?)",
		data.DeviceId, time.Unix(int64(unixTime/1000), 0), metricKey, data.Data[0][metricKey])

	if err != nil {
		fmt.Println(err)
	}
}
