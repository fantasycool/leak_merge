package u_leak_merge

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"gzcommonutil"
	"kafka_tool"
	"log"
	"math"
	"strings"
	"timetool"
)

const (
	ComputeMinutesSpan = -60
	FILE_DRAG          = "filedrag"
)

var (
	SubTypeMap = map[string]*struct{}{
		"extstorage": {},
	}
)

type LatestEventArg struct {
	Properties string
	EventId    int64
}

func GetleakEventById(db *sql.DB, eventId int64) (*LeakEvent, error) {
	row := db.QueryRow(`select id, employee_id, start_time, end_time, sub_type, 
		properties, source, target from summary_events where id=?`, eventId)
	leaEvent, err := fillLeakEvent(row)
	if err != nil {
		log.Printf("fillLeak event failed!err %s \n", err)
		return nil, err
	}
	return leaEvent, nil
}

func Run(userIds string, arg *LatestEventArg, db *sql.DB, producer sarama.SyncProducer) error {
	var leakEvent *LeakEvent
	var err error
	var resultLeakEvent *LeakEvent
	log.Printf("Have found message resultLeakEvent:%s to consume \n", resultLeakEvent)
	if arg.EventId > 0 {
		leakEvent, err = GetleakEventById(db, arg.EventId)
		if err != nil {
			log.Printf("Get leak event by id failed !err:%s \n", err)
			return err
		}
	} else if arg.Properties == "" {
		leakEvent, err = GetLatestLeakEvent(db, math.MaxInt64, "extstorage")
		if err != nil {
			log.Printf("Get latest leak event failed !err:%s \n", err)
			return err
		}
	} else {
		leakEvent, err = GetLeakEventFromProperties(arg.Properties)
		//log.Printf("GetleakEventFromProperties %s \n", leakEvent)
		if err != nil {
			log.Printf("GetLeakEventFromProperties leak event failed !err:%s \n", err)
			return err
		}
	}
	if userIds != "0" {
		bReseult, _ := gzcommonutil.Contains(fmt.Sprintf("%s", resultLeakEvent.StructProperties.MapStruct["userid"]), strings.Split(userIds, ","))
		if !bReseult {
			log.Printf("user not in white list\n")
			return nil
		}
	}
	//check type
	_, ex := SubTypeMap[leakEvent.SubType]
	if !ex {
		log.Println("message subType is not in SubTypeMap!")
		return nil
	}
	leakEvents, err := GetNeighbourNtimeRecords(leakEvent, ComputeMinutesSpan, db)
	if err != nil {
		log.Printf("GetNeighbourNTimeRecords failed! err:%s", err)
		return err
	}
	log.Printf("leakEvents length is %d \n", len(leakEvents))
	resultLeakEvent, err = MergeEvents(leakEvents, leakEvent, db, producer)

	log.Printf("resultLeakEvent:%s \n", resultLeakEvent)
	return nil
}

func insertNewRecordAndSendKafka(leakEvent *LeakEvent, producer sarama.SyncProducer, db *sql.DB) error {
	stmt, err := db.Prepare(`insert into summary_events(ignore_rule_id, user_id, uuid, host_ip,employee_id, hostname, os_login_user,
			department_id, type, sub_type, rule, source, source_path, target, target_path, properties, start_time, end_time,
		priority_key) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	defer stmt.Close()
	pMap := leakEvent.StructProperties.MapStruct
	var ignoreRuleId string
	if pMap["ignoreRuleId"] != nil {
		ignoreRuleId = pMap["ignoreRuleId"].(string)
	} else {
		return fmt.Errorf("ignoreRuleId not exists!")
	}
	userId := pMap["userid"].(string)
	uuid := pMap["uuid"].(string)
	hostIp := pMap["hostip"].(string)
	var employeeId string
	if pMap["employee_id"] != nil {
		employeeId = pMap["employee_id"].(string)
	} else {
		return fmt.Errorf("employee_id cannot be null!")
	}
	hostName := pMap["hostname"].(string)
	osLoginUser := pMap["osloginuser"].(string)
	departmentid := pMap["departmentId"].(string)
	t := pMap["warnType"].(string)
	var subType string
	if pMap["subType"] != nil {
		subType = pMap["subType"].(string)
	} else {
		return fmt.Errorf("subType can not be null!")
	}
	rule := pMap["ruleName"].(string)
	source := pMap["srcName"].(string)
	sourcePath := pMap["srcPath"].(string)
	dst := pMap["dstName"].(string)
	targetPath := pMap["dstName"].(string)
	properties := leakEvent.Properties
	startTime := leakEvent.StartTime
	endTime := leakEvent.EndTime
	prioritykey := pMap["priorityKey"].(string)
	er, err := stmt.Exec(ignoreRuleId, userId, uuid, hostIp, employeeId, hostName, osLoginUser, departmentid, t, subType,
		rule, source, sourcePath, dst, targetPath, properties, startTime.Format(timetool.TimeFormat), endTime.Format(timetool.TimeFormat),
		prioritykey)
	if err != nil {
		log.Printf("insert new to record to db failed !message:%s, err:%s \n", leakEvent.Properties, err)
		return nil
	}
	lastEventId, err := er.LastInsertId()
	if err != nil {
		log.Printf("get last insert id failed ! err:%s\n", err)
		return err
	}
	leakEvent.StructProperties.MapStruct["event_id"] = lastEventId
	err = sendMessageToTeacherHua(leakEvent.StructProperties.MapStruct, pMap["path"].(string), producer)
	if err != nil {
		log.Printf("Send messages to Kafka failed!")
		return err
	}
	return nil
}

func removePitchedOnEvent(updateEventsId []int64, eventId int64) []int64 {
	resultEventsId := make([]int64, 0)
	for _, l := range updateEventsId {
		if eventId != l {
			resultEventsId = append(resultEventsId, l)
		}
	}
	return resultEventsId
}

func MergeEvents(leakEvents []*LeakEvent, leakEvent *LeakEvent, db *sql.DB, producer sarama.SyncProducer) (*LeakEvent, error) {
	log.Printf("leakEvent Id is %d \n", leakEvent.Id)
	if len(leakEvents) == 0 && leakEvent.Id == math.MaxInt64 {
		log.Printf("it's our first time found this event!So we just insert it!")
		insertNewRecordAndSendKafka(leakEvent, producer, db)
		return leakEvent, nil
	}
	tmpEvents := make([]*LeakEvent, 0)
	updateEventsId := make([]int64, 0)
	log.Printf("l file suffix is %s \n", leakEvent.File_suffix)
	for _, l := range leakEvents {
		if l.File_suffix == leakEvent.File_suffix {
			log.Printf("Has found one file suffix equals! \n")
			tmpEvents = append(tmpEvents, l)
			updateEventsId = append(updateEventsId, l.Id)
		}
	}
	if len(updateEventsId) == 0 && leakEvent.Id > 0 {
		return leakEvent, nil
	}
	log.Printf("UpdateEventsId is %s \n", updateEventsId)
	mergedLeakEvents := make([]*LeakEvent, 0)
	mergedLeakEvents = append(mergedLeakEvents, leakEvent)
	mergedLeakEvents = append(mergedLeakEvents, tmpEvents...)
	var resultLeakEvent *LeakEvent
	if leakEvent.Id != math.MaxInt64 && leakEvent.Id > 0 {
		resultLeakEvent = leakEvent
	} else {
		resultLeakEvent = mergedLeakEvents[1]
		updateEventsId = removePitchedOnEvent(updateEventsId, resultLeakEvent.Id)
		if len(updateEventsId) == 0 {
			log.Printf("We just need to insert ")
			insertNewRecordAndSendKafka(leakEvent, producer, db)
			return leakEvent, nil
		}
	}
	log.Printf("ohohohoh!we found something that can let us be high!updateEventsId:%s \n", updateEventsId)
	resultLeakEvent.StartTime = mergedLeakEvents[len(mergedLeakEvents)-1].StartTime
	log.Printf("resultEvent endtime is %s \n", mergedLeakEvents[0].StartTime.Format(timetool.TimeFormat))
	resultLeakEvent.EndTime = mergedLeakEvents[0].StartTime
	log.Printf("updateEventsId length is %d, mergeLeakEvents length is %d \n", len(updateEventsId), len(mergedLeakEvents))
	if updateEventsId[0] == mergedLeakEvents[0].Id {
		updateEventsId = updateEventsId[1:]
	}
	log.Printf("We should delete these events_id records %s and update these ids to eventid:%d\n",
		updateEventsId, resultLeakEvent.Id)
	//update resultLeakEvent.Id
	log.Printf("Start to update info \n")
	err := updateInfo(resultLeakEvent, db, updateEventsId)
	if err != nil {
		return nil, err
	}
	log.Printf("result leak event is %d \n", resultLeakEvent.Id)
	//send a message to Teacher Hua and let him get a 3.75 kpi
	if leakEvent.Id == math.MaxInt64 {
		sendMessageToTeacherHua(resultLeakEvent.StructProperties.MapStruct, resultLeakEvent.StructProperties.Path, producer)
	}
	return resultLeakEvent, nil
}

func sendMessageToTeacherHua(m map[string]interface{}, path string, producer sarama.SyncProducer) error {
	m["path"] = path
	propertiesStr, er := json.Marshal(m)
	if er != nil {
		return er
	}
	partition, offset, err := kafka_tool.SendMessage("", string(propertiesStr), FILE_DRAG, producer)
	if err != nil {
		log.Printf("send Kafka message failed, err:%s", err)
	}
	log.Printf("Has sent a message to TeacherHua, partition:%d, offset:%d\n ", partition, offset)
	return nil
}

func updateInfo(resultLeakEvent *LeakEvent, db *sql.DB, updateEventsId []int64) error {
	if resultLeakEvent.StructProperties.MapStruct == nil {
		return fmt.Errorf("not illegal properties str:%s \n", resultLeakEvent)
	}
	resultLeakEvent.StructProperties.MapStruct["file_suffix"] = resultLeakEvent.File_suffix
	resultLeakEvent.StructProperties.MapStruct["merged"] = true
	propertiesBytes, err := json.Marshal(resultLeakEvent.StructProperties.MapStruct)
	log.Printf("propertyBytes:%s \n", propertiesBytes)
	if err != nil {
		log.Printf("to json StructProperties failed !", err)
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Begin transaction failed!errMessage:%s", err)
		return err
	}
	stmt, err := tx.Prepare("update summary_events set start_time=?, end_time=?, properties=? where id=?")
	if err != nil {
		log.Printf("db prepare udpate info failed ! %s", resultLeakEvent)
		return err
	}
	defer stmt.Close()
	r, err := stmt.Exec(resultLeakEvent.StartTime.Format(timetool.TimeFormat), resultLeakEvent.EndTime.Format(timetool.TimeFormat),
		string(propertiesBytes), resultLeakEvent.Id)
	if err != nil {
		log.Printf("updateInfo failed!err:%s", err)
		tx.Rollback()
		return err
	}
	num, err := r.RowsAffected()
	if err != nil {
		log.Printf("rowaffected failed!err:%s \n", err)
	}
	log.Printf("%d num records take effect! \n", num)
	//update summary_events records
	log.Println("start to delete update events!")
	err = deleteUpdateEvents(updateEventsId, tx)
	if err != nil {
		log.Printf("delete update events failed ! roll back, err:%s\n", err)
		tx.Rollback()
		return err
	}
	//update summary_events_files records
	log.Println("start to delete summary_events_files infos!")
	err = deleteSummaryEventsFiles(resultLeakEvent, updateEventsId, tx)
	if err != nil {
		log.Printf("delete summary_events files failed !roll back, err:%s\n", err)
		tx.Rollback()
		return err
	}
	log.Printf("Start to commit all the update info! \n")
	err = tx.Commit()
	if err != nil {
		log.Printf("err :%s\n", err)
	}
	return nil
}

func deleteUpdateEvents(updateEventsId []int64, tx *sql.Tx) error {
	stmt, err := tx.Prepare("update summary_events set type='beta' where id=?")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, u := range updateEventsId {
		_, err := stmt.Exec(u)
		if err != nil {
			log.Printf("update summary_events file failed!%s \n", err)
			return err
		}
	}
	return nil
}

func deleteSummaryEventsFiles(leakEvent *LeakEvent, updateEventsId []int64, tx *sql.Tx) error {
	strs := make([]string, 0)
	for _, s := range updateEventsId {
		strs = append(strs, fmt.Sprintf("%d", s))
	}
	sql := fmt.Sprintf("update summary_events_file set event_id=%d where event_id in(%s)", leakEvent.Id, strings.Join(strs, ","))
	r, err := tx.Exec(sql)
	if err != nil {
		log.Printf("delete record in summary_events_file failed !%s ", err)
		return err
	}
	num, _ := r.RowsAffected()
	log.Printf("%d file has been deleted in summary events files", num)
	return nil
}

func GetLeakEventFromProperties(properties string) (*LeakEvent, error) {
	leakEvent := &LeakEvent{}
	sp, err := StructProperties(properties)
	if err != nil {
		return nil, err
	}
	leakEvent.EndTime = sp.EndTime
	leakEvent.EmployeeId = sp.EmployeeId
	leakEvent.Properties = sp.Src
	leakEvent.Source = sp.Source
	leakEvent.StartTime = sp.StartTime
	leakEvent.StructProperties = sp
	path, err := GetFileSuffix(sp.Path)
	if err != nil {
		return nil, err
	}
	leakEvent.File_suffix = path
	leakEvent.EndTime = sp.EndTime
	leakEvent.Id = math.MaxInt64
	leakEvent.SubType = sp.SubType
	leakEvent.Target = sp.Target
	return leakEvent, nil
}
