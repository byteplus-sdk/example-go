package main

import (
	"strconv"

	"github.com/byteplus-sdk/sdk-go/media/protocol"
)

func mockUsers(count int) []*protocol.User {
	users := make([]*protocol.User, count)
	for i := 0; i < count; i++ {
		user := mockUser()
		user.UserId += strconv.Itoa(i)
		users[i] = user
	}
	return users
}

func mockUser() *protocol.User {
	return &protocol.User{
		UserId:                "1457789",
		Gender:                "female",
		Age:                   "18-25",
		Tags:                  []string{"new user", "low purchasing power", "bargain seeker"},
		DeviceId:              "abc123",
		DeviceType:            "app",
		SubscriberType:        "free",
		Language:              "English",
		ViewHistory:           []string{"632461", "632462"},
		ActivationChannel:     "AppStore",
		MembershipLevel:       "silver",
		RegistrationTimestamp: 1623593487,
		Country:               "USA",
		City:                  "Kirkland",
		DistrictOrArea:        "King County",
		Postcode:              "98033",
		// Extra:                 map[string]string{"additionalProp1": "additionalVal1"},
	}
}

func mockContents(count int) []*protocol.Content {
	contents := make([]*protocol.Content, count)
	for i := 0; i < count; i++ {
		content := mockContent()
		content.ContentId += strconv.Itoa(i)
		contents[i] = content
	}
	return contents
}

func mockContent() *protocol.Content {
	return &protocol.Content{
		ContentId:               "632461",
		IsRecommendable:         1,
		Categories:              "[{\"category_depth\":1,\"category_nodes\":[{\"id_or_name\":\"Movie\"}]},{\"category_depth\":2,\"category_nodes\":[{\"id_or_name\":\" Comedy\"}]}]",
		ContentTitle:            "Video #1",
		Description:             "This is a test video",
		ContentType:             "video",
		ContentOwner:            "testuser#1",
		Language:                "English",
		Tags:                    []string{"New", "Trending"},
		ListingPageDisplayTags:  []string{"popular", "recommend"},
		DetailPageDisplayTags:   []string{"popular", "recommend"},
		ListingPageDisplayType:  "image",
		CoverMultimediaUrl:      "https://images-na.ssl-images-amazon.com/images/I/81WmojBxvbL._AC_UL1500_.jpg",
		UserRating:              3.0,
		ViewsCount:              10000,
		CommentsCount:           100,
		LikesCount:              1000,
		SharesCount:             50,
		IsPaidContent:           1,
		OriginPrice:             12300,
		CurrentPrice:            12100,
		PublishRegion:           "US",
		AvailableRegion:         []string{"Singapore", "India", "US"},
		EntityId:                "1",
		EntityName:              "Friends",
		SeriesId:                "11",
		SeriesIndex:             1,
		SeriesName:              "Friends Season 1",
		SeriesCount:             10,
		VideoId:                 "111",
		VideoIndex:              6,
		VideoName:               "The One With Ross' New Girlfriend",
		VideoCount:              10,
		VideoType:               "series",
		VideoDuration:           2400000,
		PublishTimestamp:        1623193487,
		CopyrightStartTimestamp: 1623193487,
		CopyrightEndTimestamp:   1623493487,
		Actors:                  []string{"Rachel Green", "Ross Geller"},
		Source:                  "self",
		// Extra:                 map[string]string{"additionalProp1": "additionalVal1"},
	}
}

func mockUserEvents(count int) []*protocol.UserEvent {
	userEvents := make([]*protocol.UserEvent, count)
	for i := 0; i < count; i++ {
		userEvents[i] = mockUserEvent()
	}
	return userEvents
}

func mockUserEvent() *protocol.UserEvent {
	return &protocol.UserEvent{
		UserId:           "1457789",
		EventType:        "impression",
		EventTimestamp:   1623681888,
		ContentId:        "632461",
		TrafficSource:    "self",
		RequestId:        "67a9fcf74a82fdc55a26ab4ee12a7b96890407fc0042f8cc014e07a4a560a9ac",
		RecInfo:          "CiRiMjYyYjM1YS0xOTk1LTQ5YmMtOGNkNS1mZTVmYTczN2FkNDASJAobcmVjZW50X2hvdF9jbGlja3NfcmV0cmlldmVyFQAAAAAYDxoKCgNjdHIdog58PBoKCgNjdnIdANK2OCIHMjcyNTgwMg==",
		AttributionToken: "eyJpc3MiOiJuaW5naGFvLm5ldCIsImV4cCI6IjE0Mzg5NTU0NDUiLCJuYW1lIjoid2FuZ2hhbyIsImFkbWluIjp0cnVlfQ",
		SceneName:        "Home Page",
		PageNumber:       2,
		Offset:           10,
		PlayType:         "0",
		PlayDuration:     6000,
		StartTime:        150,
		EndTime:          300,
		EntityId:         "1",
		SeriesId:         "11",
		VideoId:          "111",
		ParentContentId:  "630000",
		DetailStayTime:   10,
		Query:            "comedy",
		Device:           "app",
		OsType:           "android",
		AppVersion:       "9.2.0",
		DeviceModel:      "huawei-mate30",
		DeviceBrand:      "huawei",
		OsVersion:        "10",
		BrowserType:      "chrome",
		UserAgent:        "Mozilla/5.0 (Linux; Android 10; TAS-AN00; HMSCore 5.3.0.312) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 HuaweiBrowser/11.0.8.303 Mobile Safari/537.36",
		Network:          "4g",
		// Extra:                 map[string]string{"additionalProp1": "additionalVal1"},
	}
}
