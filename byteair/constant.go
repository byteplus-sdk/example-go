package main

const (
	/*
	 * 租户相关信息
	 */
	// AK 在推荐平台->密钥管理生成的AK，用于鉴权
	AK = ""
	// SK 在推荐平台->密钥管理生成的SK，用于鉴权
	SK = ""

	// TenantId 火山引擎申请的账号id/租户id(tenant_id)，如"2100021"
	TenantId = "xxxxxxxxxxxx"

	// ProjectId 个性化推荐服务新建的项目id(project_id)，如"1231314"
	ProjectId = "xxxxxxxxxxx"

	/*
	 * stage枚举值，与推荐平台四种同步阶段相对应
	 */
	// StageIncrementalSyncStreaming 增量实时数据同步阶段
	StageIncrementalSyncStreaming = "incremental_sync_streaming"

	// StageIncrementalSyncDaily 增量天级数据同步阶段
	StageIncrementalSyncDaily = "incremental_sync_daily"

	// StagePreSync 测试数据/预同步阶段
	StagePreSync = "pre_sync"

	// StageHistorySync 历史数据同步阶段
	StageHistorySync = "history_sync"

	/*
	 * 标准数据topic枚举值，包括：item(物品，如商品、媒资数据、社区内容等)、user(用户)、behavior(行为)
	 */
	// TopicItem 物品
	TopicItem = "item"

	// TopicUser 用户
	TopicUser = "user"

	// TopicBehavior 行为
	TopicBehavior = "behavior"
)
