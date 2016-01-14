package u_leak_merge

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strings"
	"time"
	"timetool"
)

type Scanner interface {
	Scan(src ...interface{}) error
}

type LeakEvent struct {
	EmployeeId       int64
	Id               int64
	StartTime        time.Time
	EndTime          time.Time
	SubType          string
	Properties       string
	Merge_paths      []string
	File_suffix      string
	Source           string
	Target           string
	StructProperties *Properties
}

type Properties struct {
	Path       string `json:"path,omitempty"`
	Files      string `json:"files,omitempty"`
	FileSuffix string
	Src        string
	EmployeeId int64 `json:"employee_id,string,omitempty"`
	StartTime  time.Time
	EndTime    time.Time
	SubType    string `json:"_topic"`
	Source     string `json:"app.src.name,omitempty"`
	Target     string `json:"app.dest.name,omitempty"`
	Eventtime  int64  `json:"eventtime,string,omitempty"`
	MapStruct  map[string]interface{}
}

func GetLatestLeakEvent(db *sql.DB, ignoreId int64, subType string) (*LeakEvent, error) {
	row := db.QueryRow(`select id, employee_id, start_time, end_time, sub_type, 
		properties, source, target from summary_events where sub_type=? and id<? order by id desc limit 1`, subType, ignoreId)
	leakEvent, err := fillLeakEvent(row)
	if err != nil {
		log.Printf("fill Leak event failed!err:%s \n", err)
		return nil, err
	}
	return leakEvent, nil
}

func GetNeighbourNtimeRecords(leakEvent *LeakEvent, minutes int, db *sql.DB) ([]*LeakEvent, error) {
	leakEvents := make([]*LeakEvent, 0)
	var endUpperLimit string
	var err error
	if nil == leakEvent {
		leakEvent = &LeakEvent{Id: 0}
		endUpperLimit, err = timetool.GetStrTimeNowBeforNMinutes(int64(minutes))
	} else {
		endUpperLimit, err = timetool.GetStrTimeRangeNSeconds(leakEvent.StartTime.Format(timetool.TimeFormat), int64(minutes))
	}
	log.Printf("endUpperLimit is %s \n", endUpperLimit)
	if err != nil {
		return nil, err
	}
	st := leakEvent.StartTime
	for {
		rows, err := db.Query(`select id, employee_id, start_time, end_time, sub_type,
		properties, source, target from summary_events where sub_type=? and source=? and target=? and start_time>=? and start_time<? and
		employee_id=? and type=? and rule=?
		order by start_time limit 1000`, leakEvent.SubType, leakEvent.Source, leakEvent.Target,
			endUpperLimit, st.Format("2006-01-02 15:04:05"), leakEvent.EmployeeId,
			leakEvent.StructProperties.MapStruct["warnType"], leakEvent.StructProperties.MapStruct["ruleName"])
		defer rows.Close()

		if err != nil {
			log.Printf("error query is failed err:%s \n", err)
			return nil, err
		}
		num := 0
		for rows.Next() {
			num++
			l, err := fillLeakEvent(rows)
			if err != nil {
				log.Printf("fillLeakEvents failed !\n")
				return nil, err
			}
			leakEvents = append(leakEvents, l)
		}
		if num == 0 {
			break
		}
		st = leakEvents[len(leakEvents)-1].StartTime
		if num < 1000 && num > 0 {
			break
		}
	}
	return leakEvents, nil
}

func fillLeakEvent(row Scanner) (*LeakEvent, error) {
	leakEvent := &LeakEvent{}
	err := row.Scan(&leakEvent.Id, &leakEvent.EmployeeId, &leakEvent.StartTime, &leakEvent.EndTime, &leakEvent.SubType,
		&leakEvent.Properties, &leakEvent.Source, &leakEvent.Target)
	if err != nil {
		return nil, err
	}
	var properties *Properties
	if properties, err = StructProperties(leakEvent.Properties); err != nil {
		log.Printf("Get suffix failed!leakEvent:%s \n", leakEvent)
		return nil, err
	}
	log.Printf("fileSuffix is %s \n", properties.FileSuffix)
	leakEvent.File_suffix = properties.FileSuffix
	leakEvent.StructProperties = properties
	return leakEvent, nil
}

func StructProperties(properties string) (*Properties, error) {
	//log.Printf("Start to struct properties!%s \n", properties)
	_properties := &Properties{}
	if err := json.Unmarshal([]byte(properties), _properties); err != nil {
		log.Printf("Unmarshal failed!err:%s\n", err)
		return nil, err
	}
	dotIndex := strings.LastIndex(_properties.Path, `.`)
	slashIndex := strings.LastIndex(_properties.Path, `\`)
	if slashIndex >= dotIndex {
		return _properties, nil
	}
	_properties.FileSuffix = _properties.Path[dotIndex:]
	_properties.Src = properties
	_properties.StartTime = time.Unix(int64(_properties.Eventtime)/10000000, 0)
	_properties.EndTime = time.Unix(int64(_properties.Eventtime)/10000000, 0)
	m := make(map[string]interface{}, 30)
	err := json.Unmarshal([]byte(properties), &m)
	if err != nil {
		log.Printf("analyze properties%s to map failed !\n", properties)
		return nil, err
	}
	_properties.MapStruct = m
	return _properties, nil
}

func GetFileSuffix(path string) (string, error) {
	dotIndex := strings.LastIndex(path, `.`)
	slashIndex := strings.LastIndex(path, `\`)
	if slashIndex >= dotIndex {
		return "", fmt.Errorf("illegal path arg")
	}
	return path[dotIndex:], nil
}
