package sqlx

import (
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Conf interface {
	String() string    // 返回dsn
	GetDriver() string // 驱动名称
	GetHost() string   // host
	GetProt() int      // port
	GetDBName() string // dbName
	GetUser() string   // username
	GetPass() string   // password
	Setup(db *sql.DB)
}

// https://github.com/go-sql-driver/mysql#parameters
type MysqlConf struct {
	Driver          string `json:",default=mysql"`
	Host            string `json:",default=127.0.0.1"`
	Port            int    `json:",optional=3306"`
	DB              string
	User            string        `json:",optional"`
	Pass            string        `json:",optional"`
	Charset         string        `json:",default=utf8mb4"`
	Timeout         time.Duration `json:",default=5s"`
	MaxIdleConns    int           `json:",default=64"`
	MaxOpenConns    int           `json:",default=64"`
	ConnMaxLifetime time.Duration `json:",default=1m0s"`
}

func (c MysqlConf) GetUser() string {
	return c.User
}

func (c MysqlConf) GetPass() string {
	return c.Pass
}

func (c MysqlConf) String() string {
	var buf strings.Builder

	if c.User != "" && c.Pass != "" {
		buf.WriteString(url.UserPassword(c.User, c.Pass).String())
		buf.WriteByte('@')
	}
	if c.Host != "" {
		buf.WriteString("(")
		buf.WriteString(url.PathEscape(c.Host))
		buf.WriteString(":")
		if c.Port == 0 {
			c.Port = 3306
		}
		buf.WriteString(strconv.Itoa(c.Port))
		buf.WriteString(")")
	}
	if c.DB != "" && c.DB[0] != '/' && c.Host != "" {
		buf.WriteByte('/')
	}
	buf.WriteString(c.DB)

	values := url.Values{}
	if c.Charset == "" {
		c.Charset = "utf8mb4"
	}
	values.Add("charset", c.Charset)
	values.Add("parseTime", "True")
	values.Add("loc", "Local")
	if c.Timeout == 0 {
		c.Timeout = time.Minute * 5
	}
	values.Add("timeout", fmt.Sprintf("%s", c.Timeout))
	query := values.Encode()
	if query != "" {
		buf.WriteString("?")
		buf.WriteString(query)
	}
	return buf.String()
}

func (c MysqlConf) GetDriver() string {
	return c.Driver
}

func (c MysqlConf) GetHost() string {
	return c.Host
}

func (c MysqlConf) GetProt() int {
	return c.Port
}

func (c MysqlConf) GetDBName() string {
	return c.DB
}

func (c MysqlConf) Setup(db *sql.DB) {
	// we need to do this until the issue https://github.com/golang/go/issues/9851 get fixed
	// discussed here https://github.com/go-sql-driver/mysql/issues/257
	// if the discussed SetMaxIdleTimeout methods added, we'll change this behavior
	// 8 means we can't have more than 8 goroutines to concurrently access the same database.
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = maxIdleConns
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = maxOpenConns
	}
	if c.ConnMaxLifetime == time.Duration(0) {
		c.ConnMaxLifetime = maxLifetime
	}

	db.SetMaxIdleConns(maxIdleConns)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetConnMaxLifetime(maxLifetime)
}

// Manager cache key
func getManagerKey(c Conf) string {
	// mysql#127.0.0.1:3306#user#pass#dbname
	return fmt.Sprintf("%s#%s:%d#%s#%s#%s", c.GetDriver(), c.GetHost(), c.GetProt(), c.GetUser(), c.GetPass(), c.GetDBName())
}
