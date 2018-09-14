package main

import (
	"github.com/globalsign/mgo"
	"github.com/go-redis/redis"
	"github.com/vorkytaka/easyvk-go/easyvk"
	"os"
	"time"
)

const (
	MaxGoroutines = 10
	VkRPS         = 3
	MaxRedisBatch = 10000
)

type User struct {
	UserId int   `bson:"user_id"`
	Groups []int `bson:"groups"`
}

type Group struct {
	Id           int   `bson:"id"`
	MembersCount int   `bson:"members_count"`
	Members      []int `bson:"members"`
}

var RPSLimiter = time.Tick(time.Second / VkRPS)
var guard chan struct{}

func main() {
	vk := easyvk.WithToken(os.Getenv("VK_TOKEN"))

	mongo, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	collection := mongo.DB("friendzone").C("groups")

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	guard = make(chan struct{}, MaxGoroutines)

	parseUsersGroups(&vk, collection, redisClient)

	//parseUsers(&vk, collection, redisClient)
}
