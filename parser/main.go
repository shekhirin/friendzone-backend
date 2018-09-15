package main

import (
	"github.com/globalsign/mgo"
	"github.com/go-redis/redis"
	"github.com/vorkytaka/easyvk-go/easyvk"
	"os"
	"sync"
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

type VkUser struct {
	Id int `json:"id" bson:"id"`
	FirstName string `json:"first_name" bson:"first_name"`
	LastName string `json:"last_name" bson:"last_name"`
	LastSeen struct {
		Time int `json:"time" bson:"time"`
		Platform int `json:"platform" bson:"platform"`
	} `json:"last_seen" bson:"last_seen"`
	Sex int `json:"sex" bson:"sex"`
	Photo200 string `json:"photo_200" bson:"photo_200"`
	PhotoMax string `json:"photo_max" bson:"photo_max"`
	City int `json:"city" bson:"city"`
	Status int `json:"status" bson:"status"`
}

var RPSLimiter = time.Tick(time.Second / VkRPS)
var guard chan struct{}
var wg sync.WaitGroup

func main() {
	vk := easyvk.WithToken(os.Getenv("VK_TOKEN"))

	mongo, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	mongoDB := mongo.DB("friendzone")

	mongoGroups := mongoDB.C("groups")
	mongoUsers := mongoDB.C("users")
	monogoTargetGroups := mongoDB.C("targetGroups")

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	guard = make(chan struct{}, MaxGoroutines)
	wg = sync.WaitGroup{}

	wg.Add(1)
	go parseVk(&vk, mongoGroups, mongoUsers, redisClient)

	wg.Add(1)
	go parseTargetGroups(monogoTargetGroups, redisClient)

	wg.Wait()
}
