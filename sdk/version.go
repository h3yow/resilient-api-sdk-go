package sdk

// 版本兼容约定：
//   - 服务端新增字段：解析时忽略，不报错（向前兼容）。
//   - 能力被移除/废弃：服务端返回 410 或 {"deprecated": true} 时，
//     归类为 ErrRemoved，绝不静默降级。
const (
	// HeaderVersion 是请求携带的接口版本头。
	HeaderVersion = "X-API-Version"
	// HeaderDeprecated 是服务端标记能力废弃的响应头。
	HeaderDeprecated = "X-API-Deprecated"
)

// Supports 报告当前 SDK 兼容的服务端主版本范围。
func Supports(serverMajor int) bool {
	return serverMajor >= 1 && serverMajor <= 1
}
