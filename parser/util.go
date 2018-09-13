package main

import (
	"fmt"
	"github.com/vorkytaka/easyvk-go/easyvk"
)

func vkRequestWrapper(vk *easyvk.VK, method string, params map[string]string, retriesCount int) []byte {
	if retriesCount > 0 {
		fmt.Println(retriesCount, "retry...")
	}
	<-RPSLimiter
	res, err := vk.Request(method, params)
	if err != nil {
		if retriesCount > 3 {
			panic(err)
		}
		vkRequestWrapper(vk, method, params, retriesCount+1)
	}
	return res
}
