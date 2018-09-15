package main

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"net/http"
	"time"
)

const (
	GroupsPerPage = 12
	UrlTemplate   = "https://allsocial.ru/entity?category_id=%d&offset=%d&direction=1&list_type=3&order_by=quantity&period=week&type_id=1"
)

type AllSocialResponse struct {
	ErrorStatus  int    `json:"error_status"`
	ErrorMessage string `json:"error_message"`
	Response     struct {
		TotalCount  int `json:"total_count"`
		Entity      []struct {
			ID       int     `json:"id" bson:"id"`
			VkID     int     `json:"vk_id" bson:"vk_id"`
			Quantity int     `json:"quantity" bson:"quantity"`
			Caption  string  `json:"caption" bson:"caption"`
			Avatar   string  `json:"avatar" bson:"avatar"`
			DiffAbs  int     `json:"diff_abs" bson:"diff_abs"`
			DiffRel  float64 `json:"diff_rel" bson:"diff_rel"`
			Visitors int     `json:"visitors" bson:"visitors"`
			Reach    int     `json:"reach" bson:"reach"`
			Cpp      int     `json:"cpp" bson:"cpp"`
			CanChangeCpp  int  `json:"can_change_cpp" bson:"can_change_cpp"`
			IsClosed      int  `json:"is_closed" bson:"is_closed"`
			IsVerified    int  `json:"is_verified" bson:"is_verified"`
			Promoted      bool `json:"promoted" bson:"promoted"`
		} `json:"entity"`
	} `json:"response"`
}

var categories = map[string]int{
	"erotics": 9,
}
var httpClient = http.Client{
	Timeout: time.Second * 2,
}
var mongoTargetGroups *mgo.Collection
var redisTargetGroups *redis.Client

func parseTargetGroups(mongoTargetGroups_ *mgo.Collection, redisClient_ *redis.Client) {
	mongoTargetGroups = mongoTargetGroups_
	redisTargetGroups = redisClient_
	for {
		for category, id := range categories {
			offset := 0
			for {
				response, err := httpClient.Get(fmt.Sprintf(UrlTemplate, id, offset))
				if err != nil {
					panic(err)
				}

				var decodedResponse AllSocialResponse
				json.NewDecoder(response.Body).Decode(&decodedResponse)
				if decodedResponse.ErrorStatus != -1 {
					panic(decodedResponse.ErrorMessage)
				}

				if len(decodedResponse.Response.Entity) == 0 {
					break
				}

				for _, group := range decodedResponse.Response.Entity {
					redisTargetGroups.SAdd(fmt.Sprintf("target_groups:%s", category), group.VkID)
					mongoTargetGroups.Upsert(bson.M{"vk_id": group.VkID}, bson.M{"$set": group})
				}

				offset += GroupsPerPage
				response.Body.Close()
			}
		}
	}
}
