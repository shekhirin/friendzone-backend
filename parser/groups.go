// FIXME: Use only make or only empty slice with append()
// FIXME: id = string or int (ONE TYPE WITHOUT A LOT OF CASTS)
package main

import (
	"encoding/json"
	"fmt"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/go-redis/redis"
	"github.com/vorkytaka/easyvk-go/easyvk"
	"io/ioutil"
	"strconv"
	"strings"
)


func parseUsersGroups(vk *easyvk.VK, collection *mgo.Collection, redis *redis.Client) {
	for {
		fmt.Println("Loading groups list... ")
		groupsList := loadGroups("groups.txt")

		fmt.Println("Parsing groups' members...")
		groups := getGroupsMembers(vk, redis, groupsList)
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
		fmt.Println("Total users to parse:", len(allUsers))

		fmt.Println("Parsing users' groups...")
		parseGroups(vk, collection, redis, allUsers)
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

func parseGroups(vk *easyvk.VK, collection *mgo.Collection, redis *redis.Client, users []int) {
	//bar := pb.StartNew(len(users))
	for i := 0; i < len(users); i += 25 {
		topBound := (i + 1) * 25
		if topBound > len(users)-1 {
			topBound = len(users)-1
		}
		shrinkedUsers := getUsersGroups(vk, users[i*25:topBound])
		go func() {
			guard <- struct{}{}
			wg.Add(1)
			var toInsert = make([]interface{}, 0)
			var toUpdate = make([]interface{}, 0)
			for _, user := range shrinkedUsers {
				if exists := redis.Exists(strconv.Itoa(user.UserId)).Val(); exists == 1 {
					toUpdate = append(toUpdate, bson.M{"user_id": user.UserId}, user)
				} else {
					toInsert = append(toInsert, user)
				}
			}

			if len(toInsert) > 0 {
				bulkInsert := collection.Bulk()
				bulkInsert.Unordered()
				bulkInsert.Insert(toInsert...)
				_, err := bulkInsert.Run()
				if err != nil {
					panic(err)
				}
				for _, user := range toInsert {
					redis.Set(strconv.Itoa(user.(User).UserId), 1, 0)
				}
			}

			if len(toUpdate) > 0 {
				bulkUpdate := collection.Bulk()
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

func getGroupsMembers(vk *easyvk.VK, redis *redis.Client, groups []string) []Group {
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
				Members:      make([]int, 0),
			})
		}
	}

	//bar := pb.StartNew(len(resultGroups))
	for i, resultGroup := range resultGroups {
		membersForRedis := make([]interface{}, 0)
		for j := 0; j < resultGroup.MembersCount; j += 1000*25 {
			var members [][]int

			byteArray := vkRequestWrapper(vk, "execute", map[string]string{
				"code": fmt.Sprintf("var members = [];\n"+
					"var offset = %s;\n"+
					"var i = offset;\n"+
					"while (i < offset + 25) {\n"+
					"	members.push(API.groups.getMembers({\"group_id\": %s, \"count\": 1000, \"offset\": i*1000}).items);\n"+ // TODO: Add last_seen checking
					"	i = i + 1;\n"+
					"}\n"+
					"return members;", strconv.Itoa(j*25), strconv.Itoa(resultGroup.Id)),
			}, 0)
			json.Unmarshal(byteArray, &members)

			for _, batch := range members {
				resultGroups[i].Members = append(resultGroups[i].Members, batch...)
				for _, id := range batch {
					membersForRedis = append(membersForRedis, strconv.Itoa(id))
				}
			}
		}

		redisKey := "public_" + strconv.Itoa(resultGroup.Id)
		redis.Del(redisKey)
		for k := 0; k < len(membersForRedis)-MaxRedisBatch; k += MaxRedisBatch {
			redis.SAdd(redisKey, membersForRedis[k:k+MaxRedisBatch]...)
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
			"while (i < %s) {\n"+
			"	groups.push(API.groups.get({\"user_id\": ids[i], \"count\": 1000}).items);\n"+
			"	i = i + 1;\n"+
			"}\n"+
			"return groups;", strings.Join(stringIds, ","), strconv.Itoa(len(ids))),
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
