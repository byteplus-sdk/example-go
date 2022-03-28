package main

import (
	"strconv"
	"time"

	. "github.com/byteplus-sdk/sdk-go/retailv2/protocol"
)

func mockUsers(count int) []*User {
	users := make([]*User, count)
	for i := 0; i < count; i++ {
		user := mockUser()
		user.UserId += strconv.Itoa(i)
		users[i] = user
	}
	return users
}

func mockUser() *User {
	location := &User_Location{
		Country:        "china",
		City:           "beijing",
		DistrictOrArea: "haidian",
		Postcode:       "123456",
	}
	return &User{
		UserId:                "user_id",
		Gender:                "male",
		Age:                   "23",
		Tags:                  []string{"tag1", "tag2", "tag3"},
		ActivationChannel:     "AppStore",
		MembershipLevel:       "silver",
		RegistrationTimestamp: time.Now().Unix(),
		Location:              location,
		Extra:                 map[string]string{"first_name": "first"},
	}
}

func mockProducts(count int) []*Product {
	products := make([]*Product, count)
	for i := 0; i < count; i++ {
		product := mockProduct()
		product.ProductId += strconv.Itoa(i)
		products[i] = product
	}
	return products
}

func mockProduct() *Product {
	category1Node1 := &Product_Category_CategoryNode{
		IdOrName: "cate_1_1",
	}
	category1 := &Product_Category{
		CategoryDepth: 1,
		CategoryNodes: []*Product_Category_CategoryNode{category1Node1},
	}
	category2Node1 := &Product_Category_CategoryNode{
		IdOrName: "cate_2_1",
	}
	category2Node2 := &Product_Category_CategoryNode{
		IdOrName: "cate_2_2",
	}
	category2 := &Product_Category{
		CategoryDepth: 2,
		CategoryNodes: []*Product_Category_CategoryNode{category2Node1, category2Node2},
	}

	brand1 := &Product_Brand{
		BrandDepth: 1,
		IdOrName:   "brand_1",
	}
	brand2 := &Product_Brand{
		BrandDepth: 2,
		IdOrName:   "brand_2",
	}

	price := &Product_Price{
		CurrentPrice: 10,
		OriginPrice:  10,
	}

	display := &Product_Display{
		DetailPageDisplayTags:  []string{"tag1", "tag2"},
		ListingPageDisplayTags: []string{"taga", "tagb"},
		ListingPageDisplayType: "image",
		CoverMultimediaUrl:     "https://www.google.com",
	}

	spec := &Product_ProductSpec{
		ProductGroupId:   "group_id",
		UserRating:       0.23,
		CommentCount:     100,
		Source:           "self",
		PublishTimestamp: time.Now().Unix(),
	}

	seller := &Product_Seller{
		Id:           "seller_id",
		SellerLevel:  "level1",
		SellerRating: 3.5,
	}

	return &Product{
		ProductId:       "product_id",
		Categories:      []*Product_Category{category1, category2},
		Brands:          []*Product_Brand{brand1, brand2},
		Price:           price,
		IsRecommendable: true,
		Title:           "title",
		QualityScore:    3.4,
		Tags:            []string{"tag1", "tag2", "tag3"},
		Display:         display,
		ProductSpec:     spec,
		Seller:          seller,
		Extra:           map[string]string{"count": "20"},
	}
}

func mockUserEvents(count int) []*UserEvent {
	userEvents := make([]*UserEvent, count)
	for i := 0; i < count; i++ {
		userEvents[i] = mockUserEvent()
	}
	return userEvents
}

func mockUserEvent() *UserEvent {
	scene := &UserEvent_Scene{
		SceneName:  "scene_name",
		PageNumber: 2,
		Offset:     10,
	}

	device := mockDevice()

	context := &UserEvent_Context{
		Query:         "query",
		RootProductId: "root_product_id",
	}

	return &UserEvent{
		UserId:           "user_id",
		EventType:        "purchase",
		EventTimestamp:   time.Now().Unix(),
		Scene:            scene,
		ProductId:        "product_id",
		Device:           device,
		Context:          context,
		AttributionToken: "attribution_token",
		RecInfo:          "trans_data",
		TrafficSource:    "self",
		PurchaseCount:    20,
		Extra:            map[string]string{"children": "true"},
	}
}

func mockDevice() *UserEvent_Device {
	return &UserEvent_Device{
		Platform:    "android",
		OsType:      "phone",
		AppVersion:  "app_version",
		DeviceModel: "device_model",
		DeviceBrand: "device_brand",
		OsVersion:   "os_version",
		BrowserType: "firefox",
		UserAgent:   "user_agent",
		Network:     "3g",
	}
}
