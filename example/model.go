package example

import (
	"context"
	"database/sql"
	"fmt"
	"go-artisan/pkg/cache"
	"go-artisan/pkg/model"
	"time"
)

type (
	TaskModel interface {
		// Insert 插入新纪录
		Insert(ctx context.Context, task *Task) (res sql.Result, err error)
		// FindOne 通过主键查询Task
		FindOne(ctx context.Context, intId int64) (task *Task, err error)
		// FindOneByTaskId 通过key查询Task
		FindOneByTaskId(ctx context.Context, taskId string) (task *Task, err error)
		// Update 更新纪录
		Update(ctx context.Context, task *Task) (err error)
	}

	defaultTask struct {
		model model.Model
		cache cache.Cache
		table string
	}

	Task struct {
		IntId            int64      `db:"int_id"`
		TaskId           string     `db:"task_id"`
		Name             string     `db:"name"`               // 工单名称
		CreatorId        string     `db:"creator_id"`         // 创建工单用户
		UserId           string     `db:"user_id"`            // 接单用户
		Address          string     `db:"address"`            // 工单地址
		Location         string     `db:"location"`           // 派单地点
		Status           int8       `db:"status"`             // 0=待接单；1=已接单；2=已签到；3=已签出
		Level            int8       `db:"level"`              // 0=一般；1=紧急
		ActiveAt         *time.Time `db:"active_at"`          // 接单时间
		CheckInAt        *time.Time `db:"check_in_at"`        // 签到时间
		CheckOutAt       *time.Time `db:"check_out_at"`       // 签出时间
		Description      string     `db:"description"`        // 工单说明
		Comment          string     `db:"comment"`            // 接单人评论
		CheckRange       float64    `db:"check_range"`        // 有效打卡半径
		ActiveLocation   string     `db:"active_location"`    // 接单地点
		CheckInLocation  string     `db:"check_in_location"`  // 签到地点
		CheckOutLocation string     `db:"check_out_location"` // 签出地点
		CreatedAt        time.Time  `db:"created_at"`
		UpdatedAt        time.Time  `db:"updated_at"`
	}
)

func (m defaultTask) primaryCachedKey(intId interface{}) string {
	return fmt.Sprintf("cached#%s#intId#%v", m.table, intId)
}

func (m defaultTask) taskIdCachedKey(taskId interface{}) string {
	return fmt.Sprintf("cached#%s#taskId#%v", m.table, taskId)
}

func (m *defaultTask) Insert(ctx context.Context, task *Task) (res sql.Result, err error) {
	res, err = m.model.Insert(ctx, map[string]interface{}{
		"task_id":            task.TaskId,
		"name":               task.Name,
		"creator_id":         task.CreatorId,
		"user_id":            task.UserId,
		"address":            task.Address,
		"location":           task.Location,
		"status":             task.Status,
		"level":              task.Level,
		"active_at":          task.ActiveAt,
		"check_in_at":        task.CheckInAt,
		"check_out_at":       task.CheckOutAt,
		"description":        task.Description,
		"comment":            task.Comment,
		"check_range":        task.CheckRange,
		"active_location":    task.ActiveLocation,
		"check_in_location":  task.CheckInLocation,
		"check_out_location": task.CheckOutLocation,
		"created_at":         task.CreatedAt,
		"updated_at":         task.UpdatedAt,
	})
	if err == nil {
		id, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}
		go func() {
			if err2 := m.cache.Del(ctx, m.primaryCachedKey(id)); err2 != nil {
				// log
			}
		}()

	}
	return res, err
}

func (m *defaultTask) FindOne(ctx context.Context, intId int64) (task *Task, err error) {
	var resp Task

	err = m.cache.Take(ctx, m.primaryCachedKey(intId), &resp, func(ctx context.Context, i interface{}) error {
		return m.model.Find(ctx, i, map[string]interface{}{
			"int_id": intId,
			"_limit": []uint{1},
		})
	})

	switch err {
	case nil:
		return &resp, nil
	default:
		return nil, err
	}
}

func (m *defaultTask) FindOneByTaskId(ctx context.Context, taskId string) (task *Task, err error) {
	var resp Task
	var primaryKey interface{}
	var found bool

	err = m.cache.Take(ctx, m.taskIdCachedKey(taskId), &primaryKey, func(ctx context.Context, i interface{}) error {
		if err := m.model.Find(ctx, &resp, map[string]interface{}{
			"task_id": taskId,
			"_limit":  []uint{1},
		}); err != nil {
			return err
		}
		found = true
		// 设置主键映射
		return m.cache.Set(ctx, m.primaryCachedKey(resp.IntId), resp)
	})

	if err == nil {
		if found {
			return &resp, nil
		}
		// 通过主键获取
		if err := m.cache.Take(ctx, m.primaryCachedKey(primaryKey), &resp, func(ctx context.Context, i interface{}) error {
			return m.model.Find(ctx, i, map[string]interface{}{
				"int_id": primaryKey,
				"_limit": []uint{1},
			})
		}); err != nil {
			return nil, err
		}

	}

	return nil, err
}

func (m *defaultTask) Update(ctx context.Context, val, cond map[string]interface{}) (err error) {
	var tasks []*Task
	if err := m.model.Find(ctx, &tasks, cond); err != nil {
		return err
	}

	_, err = m.model.Update(ctx, val, cond)
	if err != nil {
		return err
	}

	var keys []string
	_, hasTaskId := cond["task_id"]
	for _, task := range tasks {
		keys = append(keys, m.primaryCachedKey(task.IntId))
		if hasTaskId {
			keys = append(keys, m.taskIdCachedKey(task.TaskId))
		}
	}

	return m.cache.Del(ctx, keys...)
}

func (m *defaultTask) Delete(ctx context.Context, cond map[string]interface{}) (err error) {
	var tasks []*Task
	if err := m.model.Find(ctx, &tasks, cond); err != nil {
		return err
	}

	_, err = m.model.Delete(ctx, cond)
	if err != nil {
		return err
	}

	var keys []string
	for _, task := range tasks {
		keys = append(keys, m.primaryCachedKey(task.IntId))
		keys = append(keys, m.taskIdCachedKey(task.TaskId))
	}

	return m.cache.Del(ctx, keys...)
}
