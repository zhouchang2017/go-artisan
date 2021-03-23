package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"go-artisan/pkg/cache"
	"go-artisan/pkg/sqlx"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/didi/gendry/scanner"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
)

func init() {
	scanner.SetTagName("db")
}

type Project struct {
	ID          int64     `db:"project_int_id"`
	Uin         int64     `db:"uin"`
	ProjectName string    `db:"project_name"`
	ProjectKey  string    `db:"project_key"`
	ProjectDesc string    `db:":"project_desc"`
	CreateTime  time.Time `db:"create_time"`
	UpdateTime  time.Time `db:"update_time"`
}

var notFoundPlaceholder = []byte("*")
var errNotFount = errors.New("not found")

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()
	//rand.Seed(time.Now().Unix())
	//ticker := time.NewTicker(time.Second * 10)
	//defer ticker.Stop()
	//go func() {
	//	for {
	//		select {
	//		case <-ticker.C:
	//			intn := rand.Intn(999999)
	//			key := fmt.Sprintf("project#int_id#%d", intn)
	//			if err := client.Publish(context.Background(), "cached.delete", key).Err(); err != nil {
	//				log.Printf("pub err: %s\n", err)
	//			}
	//		}
	//	}
	//}()
	//
	//pubsub := client.Subscribe(context.Background(), "cached.delete")
	//
	//channel := pubsub.Channel()
	//
	//go func() {
	//	for {
	//		select {
	//		case msg := <-channel:
	//			fmt.Println(msg.Channel, msg.Payload)
	//		}
	//	}
	//}()

	conf := sqlx.MysqlConf{
		Host: "localhost",
		Port: 3306,
		DB:   "db_iotservice",
		User: "root",
		Pass: "12345678",
	}
	db, err := sql.Open("mysql", conf.String())
	if err != nil {
		panic(err)
	}
	defer db.Close()
	store := cache.New(client, scanner.ErrEmptyResult)

	s := service{
		db:    db,
		store: store,
	}

	mux := http.NewServeMux()
	mux.Handle("/", s)

	log.Println("Starting v2 httpserver")
	log.Fatal(http.ListenAndServe(":3003", mux))
}

type service struct {
	db    *sql.DB
	store cache.Cache
}

type response struct {
	ErrMsg string `json:",omitempty"`
	Data   interface{}
}

func responseJson(w http.ResponseWriter, data interface{}) {
	marshal, err := json.Marshal(response{
		Data: data,
	})
	if err != nil {
		responseErr(w, 500, err)
	} else {
		w.WriteHeader(200)
		w.Write(marshal)
	}
}

func responseErr(w http.ResponseWriter, code int, err error) {
	w.WriteHeader(code)
	w.Write([]byte(fmt.Sprintf("{\"ErrMsg\":%s}", strconv.Quote(err.Error()))))
}

func (s service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/json")

	query := r.URL.Query()
	key := query.Get("key")

	id := query.Get("id")

	if id != "" {
		parseInt, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			responseErr(w, 400, err)
			return
		}
		project, err := s.fetch(r.Context(), parseInt)
		if err != nil {
			responseErr(w, 500, err)
			return
		}
		responseJson(w, project)
		return
	} else if key != "" {
		project, err := s.fetchByKey(r.Context(), key)
		if err != nil {
			responseErr(w, 500, err)
			return
		}
		responseJson(w, project)
		return
	}

	responseErr(w, 400, errors.New("参数异常"))
}

func (s service) fetch(ctx context.Context, id int64) (*Project, error) {
	var res Project
	key := fmt.Sprintf("project#id#%d", id)
	err := s.store.Take(context.Background(), key, &res, func(ctx context.Context, i interface{}) error {
		rows, err := s.db.QueryContext(ctx, "select * from t_project where project_int_id = ? limit 1", id)
		if err != nil {
			return err
		}
		return scanner.ScanClose(rows, i)
	})
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (s service) fetchByKey(ctx context.Context, projectKey string) (res *Project, err error) {
	res = &Project{}
	var primaryKey int64
	var found bool
	key := fmt.Sprintf("project#key#%v", projectKey)

	err = s.store.Take(context.Background(), key, &primaryKey, func(ctx context.Context, i interface{}) error {
		rows, err := s.db.QueryContext(ctx, "select * from t_project where project_key = ? limit 1", projectKey)
		if err != nil {
			return err
		}
		if err := scanner.ScanClose(rows, res); err != nil {
			return err
		}
		primaryKey = res.ID
		found = true

		return s.store.Set(ctx, fmt.Sprintf("project#id#%d", res.ID), res)
	})
	if err != nil {
		return nil, err
	}
	if found {
		return res, nil
	}

	return s.fetch(ctx, primaryKey)
}
