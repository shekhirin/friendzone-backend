package main

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/vorkytaka/easyvk-go/easyvk"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

var vk *easyvk.VK
var mongoGroups *mgo.Collection
var mongoUsers *mgo.Collection
var redisVk *redis.Client

func parseVk(vk_ *easyvk.VK, mongoGroups_ *mgo.Collection, mongoUsers_ *mgo.Collection, redis_ *redis.Client) {
	vk = vk_
	mongoGroups = mongoGroups_
	mongoUsers = mongoUsers_
	redisVk = redis_

	for {
		fmt.Println("Loading groups list... ")
		groupsList := loadGroups(os.Getenv("GROUPS_FILE"))

		fmt.Println("Parsing groups' members...")
		groups := getGroupsMembers(groupsList)
		allUsers := make([]int, 0)
		allUsersMarked := make(map[int]bool)
		for _, group := range groups {
			for _, member := range group.Members {
				if _, ok := allUsersMarked[member]; !ok {
					allUsers = append(allUsers, member)
					allUsersMarked[member] = true
				}
			}
		}
		fmt.Println("Total users to parseVk:", len(allUsers))

		fmt.Println("Parsing users' groups...")
		parseGroups(allUsers)
		fmt.Print("\n\n")
	}
}

func loadGroups(filename string) []string {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		lines[i] = strings.Trim(strings.Trim(line, "http://vk.com/public"), "http://vk.com/club")
	}

	return lines
}

func parseGroups(users []int) {
	//C := pb.StartNew(len(users))
	for i := 0; i < len(users); i += 25 {
		var shrinkedUsers []User
		topBound := (i + 1) * 25
		if topBound > len(users) {
			shrinkedUsers = getUsersGroups(vk, users[i*25:])
		} else {
			shrinkedUsers = getUsersGroups(vk, users[i*25:topBound])
		}
		go func() {
			guard <- struct{}{}
			wg.Add(1)
			var toInsert = make([]interface{}, 0)
			var toUpdate = make([]interface{}, 0)
			for _, user := range shrinkedUsers {
				if exists := redisVk.Exists(strconv.Itoa(user.UserId)).Val(); exists == 1 {
					toUpdate = append(toUpdate, bson.M{"user_id": user.UserId}, user)
				} else {
					toInsert = append(toInsert, user)
				}
			}

			if len(toInsert) > 0 {
				bulkInsert := mongoGroups.Bulk()
				bulkInsert.Unordered()
				bulkInsert.Insert(toInsert...)
				_, err := bulkInsert.Run()
				if err != nil {
					panic(err)
				}
				for _, user := range toInsert {
					redisVk.Set(strconv.Itoa(user.(User).UserId), 1, 0)
				}
			}

			if len(toUpdate) > 0 {
				bulkUpdate := mongoGroups.Bulk()
				bulkUpdate.Unordered()
				bulkUpdate.Update(toUpdate...)
				_, err := bulkUpdate.Run()
				if err != nil {
					panic(err)
				}
			}

			wg.Done()
			<-guard
		}()
		//bar.Add(len(shrinkedUsers))
	}
	//bar.Finish()
}

func getGroupsMembers(groups []string) []Group {
	var groupsData []map[string]interface{}

	byteArray := vkRequestWrapper(vk, "groups.getById", map[string]string{
		"group_ids": strings.Join(groups, ","),
		"fields":    "members_count",
	}, 0)
	json.Unmarshal(byteArray, &groupsData)

	resultGroups := make([]Group, 0)

	for _, groupData := range groupsData {
		if _, ok := groupData["members_count"]; ok {
			resultGroups = append(resultGroups, Group{
				Id:           int(groupData["id"].(float64)),
				MembersCount: int(groupData["members_count"].(float64)),
				Members:      make([]int, int(groupData["members_count"].(float64))),
			})
		}
	}

	//bar := pb.StartNew(len(resultGroups))
	for i, resultGroup := range resultGroups {
		membersForRedis := make([]interface{}, resultGroup.MembersCount)
		for j := 0; j < resultGroup.MembersCount; j += 1000*25 {
			var members [][]VkUser

			byteArray := vkRequestWrapper(vk, "execute", map[string]string{
				"code": fmt.Sprintf("var members = [];\n"+
					"var offset = %d;\n"+
					"var i = offset;\n"+
					"while (i < offset + 25) {\n"+
					"	members.push(API.groups.getMembers({" +
					"		\"group_id\": %d, " +
					"		\"count\": 1000, " +
					"		\"offset\": i*1000," +
					"		\"fields\": \"first_name,last_name,last_seen,sex,photo_200,photo_max,city,status\"" +
					"	}).items);\n"+
					"	i = i + 1;\n"+
					"}\n"+
					"return members;", j/1000, resultGroup.Id),
			}, 0)
			json.Unmarshal(byteArray, &members)
			for k, batch := range members {
				for m, user := range batch {
					if time.Now().Sub(time.Unix(int64(user.LastSeen.Time), 0)) >= time.Hour * 24 * 2 {
						continue
					}
					resultGroups[i].Members[j+k*1000+m] = user.Id
					membersForRedis[j+k*1000+m] = strconv.Itoa(user.Id)

					mongoUsers.Insert(user)
				}
			}
		}

		redisKey := "public_" + strconv.Itoa(resultGroup.Id)
		redisVk.Del(redisKey)
		for k := 0; k < len(resultGroups[i].Members); k += MaxRedisBatch {
			if k+MaxRedisBatch > len(membersForRedis) {
				redisVk.SAdd(redisKey, membersForRedis[k:]...)
			} else {
				redisVk.SAdd(redisKey, membersForRedis[k:k+MaxRedisBatch]...)
			}
		}

		//bar.Increment()
	}
	//bar.Finish()

	return resultGroups
}

func getUsersGroups(vk *easyvk.VK, ids []int) []User {
	stringIds := make([]string, len(ids))
	for i, id := range ids {
		stringIds[i] = strconv.Itoa(id)
	}

	var groups [][]int

	byteArray := vkRequestWrapper(vk, "execute", map[string]string{
		"code": fmt.Sprintf("var groups = [];\n"+
			"var ids = \"%s\".split(\",\");\n"+
			"var i = 0;\n"+
			"while (i < %d) {\n"+
			"	groups.push(API.groups.get({\"user_id\": ids[i], \"count\": 1000}).items);\n"+
			"	i = i + 1;\n"+
			"}\n"+
			"return groups;", strings.Join(stringIds, ","), len(ids)),
	}, 0)
	json.Unmarshal(byteArray, &groups)

	mappedGroups := make(map[int][]int)
	for i, batch := range groups {
		mappedGroups[ids[i]] = batch
	}

	var users []User
	for userId, groups := range mappedGroups {
		users = append(users, User{UserId: userId, Groups: groups})
	}

	return users
}
