package example

import (
	"context"
	"database/sql"
	"github.com/didi/gendry/scanner"
	cache2 "go-artisan/pkg/cache"
)

type UserModel interface {
	// 普通查询
	Find(ctx context.Context, condition map[string]interface{}) (user *User, err error)
	// 通过主键查询
	FindOne(ctx context.Context, intId int64) (user *User, err error)
	// 通过ID唯一健查询
	FindById(ctx context.Context, id string) (user *User, err error)
	// 通过PhoneNumber唯一健查询
	FindByPhoneNumber(ctx context.Context, phoneNumber string) (user *User, err error)
	// 通过OpenId唯一健查询
	FindByOpenId(ctx context.Context, openId string) (user *User, err error)

	Create(ctx context.Context, user User) (res sql.Result, err error)
	Update(ctx context.Context, user User) (err error)
	Query(ctx context.Context, condition map[string]interface{}) (users []*User, err error)
	Destroy(ctx context.Context, intId int64) (err error)

	Count(ctx context.Context, condition map[string]interface{}) (res int64, err error)
}

type User struct {
	IntId       int64
	Id          string
	Name        string
	PhoneNumber string
	OpenId      string
	Avatar      string
}

type defaultUserModel struct {
	db    *sql.DB
	cache cache2.Cache
	table string
}

// cache
//type defaultUserModelCache struct {
//	client             redis.Cmdable
//	local              *cache.Cache
//	g                  singleflight.Group
//	dbErrNotFound      error
//	expiration         time.Duration
//	notFoundExpiration time.Duration
//}
//
//func (d defaultUserModelCache) Get(ctx context.Context, key string, query func(ctx context.Context) (*User, error)) (user *User, err error) {
//	user = &User{}
//	value, err, _ := d.g.Do(key, func() (i interface{}, err error) {
//		if d.local != nil {
//			val, exist := d.local.Get(key)
//			if exist {
//				// if val is placeholder then returns errNotFound
//				return val, nil
//			}
//		}
//		if d.client != nil {
//			result, err := d.client.Get(ctx, key).Result()
//			if err == redis.Nil {
//				// if redis.Nil then call query
//				goto QUERY
//
//			} else if err != nil {
//				// maybe redis err
//				return nil, err
//			}
//
//			// if result is placeholder then returns errNotFound
//
//			if err := json.Unmarshal([]byte(result), user); err != nil {
//				return nil, err
//			}
//			return user, nil
//		}
//	QUERY:
//		user, err = query(ctx)
//		if err == d.dbErrNotFound {
//			// set placeholder to cache
//
//			return nil, d.dbErrNotFound
//		} else if err != nil {
//			// db err
//			return nil, err
//		}
//		// queried from db,set val to cache
//		d.Set(ctx, key, *user)
//
//		return user, nil
//	})
//	if err != nil {
//		return nil, err
//	}
//	return value.(*User), nil
//}
//
//func (d defaultUserModelCache) Set(ctx context.Context, key string, user User) error {
//	if d.local != nil {
//		d.local.Set(key, user, d.expiration)
//	}
//	if d.client != nil {
//		marshal, err := json.Marshal(user)
//		if err != nil {
//			return err
//		}
//		return d.client.Set(ctx, key, string(marshal), d.expiration).Err()
//	}
//	return nil
//}
//
//func (d defaultUserModelCache) setPlaceHolder(ctx context.Context,key string) error  {
//
//}
//
//func (d defaultUserModelCache) GetPrimaryKey(ctx context.Context, key string) (id int64, err error) {
//
//}
//
//func (d defaultUserModelCache) SetPrimaryKey(ctx context.Context, key string, id int64) error {
//
//}

// keys
func (d defaultUserModel) intIdCachedKey(intId int64) string {

}

func (d defaultUserModel) idCachedKey(id string) string {

}

func (d defaultUserModel) phoneNumberCachedKey(phoneNumber string) string {

}

func (d defaultUserModel) openIdCachedKey(openId string) string {

}

func (d *defaultUserModel) Find(ctx context.Context, condition map[string]interface{}) (user *User, err error) {
	panic("implement me")
}

func (d *defaultUserModel) FindOne(ctx context.Context, intId int64) (user *User, err error) {
	user = &User{}
	query := func(ctx context.Context, val interface{}) error {
		row, err := d.db.QueryContext(ctx, "select * from ? where int_id = ? limit 1", d.table, intId)
		if err != nil {
			return err
		}
		return scanner.ScanClose(row, val)
	}
	if d.cache != nil {
		if err := d.cache.Take(ctx, d.intIdCachedKey(intId), user, query); err != nil {
			return nil, err
		}
		return user, nil
	} else {
		if err := query(ctx, user); err != nil {
			return nil, err
		}
		return user, nil
	}
}

func (d *defaultUserModel) FindById(ctx context.Context, id string) (user *User, err error) {
	user = &User{}
	var intId int64
	var found bool
	if d.cache != nil {
		d.cache.Take(ctx, d.idCachedKey(id), &intId, func(ctx context.Context, val interface{}) error {
			rows, err := d.db.QueryContext(ctx, "select * from ? where id = ?", d.table, id)
			if err != nil {
				return err
			}
			if err := scanner.ScanClose(rows, user); err != nil {
				return err
			}
			intId = user.IntId
			found = true
			return d.cache.Set(ctx, d.intIdCachedKey(intId), user)
		})
	}
}

func (d *defaultUserModel) FindByPhoneNumber(ctx context.Context, phoneNumber string) (user *User, err error) {
	panic("implement me")
}

func (d *defaultUserModel) FindByOpenId(ctx context.Context, openId string) (user *User, err error) {
	panic("implement me")
}

func (d *defaultUserModel) Create(ctx context.Context, user User) (res sql.Result, err error) {
	panic("implement me")
}

func (d *defaultUserModel) Update(ctx context.Context, user User) (err error) {
	panic("implement me")
}

func (d *defaultUserModel) Query(ctx context.Context, condition map[string]interface{}) (users []*User, err error) {
	panic("implement me")
}

func (d *defaultUserModel) Destroy(ctx context.Context, intId int64) (err error) {
	panic("implement me")
}

func (d *defaultUserModel) Count(ctx context.Context, condition map[string]interface{}) (res int64, err error) {
	panic("implement me")
}
