package processor

import (
	"alarm-service/db"
	"fmt"
	"strconv"
	"time"
)

func GetDeviceInfo(deviceId int64) (error, DeviceInfo) {

	deviceInfo := DeviceInfo{}

	err := db.MySqlDB.QueryRow("select device_id, device_name, device.project as project, customer.name as customer from device "+
		"left join project on device.project = project.name "+
		"left join customer on project.customer = customer.name where device_id = ?", deviceId).Scan(&deviceInfo.DeviceId,
		&deviceInfo.DeviceName,
		&deviceInfo.Project,
		&deviceInfo.Customer)

	if err != nil {
		return err, deviceInfo
	}

	return nil, deviceInfo

}

func InsertAlarmItem(data *MonitorData, metricKey string) {

	unixTime, _ := strconv.Atoi(data.TimeStamp)

	err := db.ExecuteSQL(
		"INSERT INTO alarm_item (`device_id`, `timestamp`, `alarm_item`, `alarm_value`) VALUES (?,?,?,?)",
		data.DeviceId, time.Unix(int64(unixTime/1000), 0), metricKey, data.Data[0][metricKey])

	if err != nil {
		fmt.Println(err)
	}
}
