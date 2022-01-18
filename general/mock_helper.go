package main

import "encoding/json"

func mockDataList(count int) []map[string]interface{} {
	dataList := make([]map[string]interface{}, count)
	for i := 0; i < count; i++ {
		dataList[i] = mockData()
	}
	return dataList
}

func mockData() map[string]interface{} {
	// Fields not included in the standard schema can be transmitted through the 'extra_info' field,
	// and the extra_info value format should be json string
	extraInfo := map[string]interface{}{
		"session_id":"sess_89j9ifuqrbplk0rti2va2k1ha0",
		"store_num": 12,
		"user_tags": []string{"1", "2", "3", "xxx"},
	}
	result := make(map[string]interface{})
	result["user_id"] = "1457789"
	result["event_type"] = "purchase"
	result["event_timestamp"] = 1623681767
	result["scene_scene_name"] = "product detail page"
	result["scene_page_number"] = 2
	result["scene_offset"] = 10
	result["product_id"] = "632461"
	result["device_platform"] = "android"
	result["device_os_type"] = "phone"
	result["device_app_version"] = "9.2.0"
	result["device_device_model"] = "huawei-mate30"
	result["device_device_brand"] = "huawei"
	result["device_os_version"] = "10"
	result["device_browser_type"] = "chrome"
	result["device_user_agent"] = "Mozilla/5.0 (Linux; Android 10; TAS-AN00; HMSCore 5.3.0.312) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 HuaweiBrowser/11.0.8.303 Mobile Safari/537.36"
	result["device_network"] = "3g"
	result["context_query"] = ""
	result["context_root_product_id"] = "441356"
	result["attribution_token"] = "eyJpc3MiOiJuaW5naGFvLm5ldCIsImV4cCI6IjE0Mzg5NTU0NDUiLCJuYW1lIjoid2FuZ2hhbyIsImFkbWluIjp0cnVlfQ"
	result["rec_info"] = "CiRiMjYyYjM1YS0xOTk1LTQ5YmMtOGNkNS1mZTVmYTczN2FkNDASJAobcmVjZW50X2hvdF9jbGlja3NfcmV0cmlldmVyFQAAAAAYDxoKCgNjdHIdog58PBoKCgNjdnIdANK2OCIHMjcyNTgwMg=="
	result["traffic_source"] = "self"
	result["purchase_count"] = 20
	extraInfoBytes, _ := json.Marshal(extraInfo)
	result["extra_info"] = string(extraInfoBytes)
	return result
}
