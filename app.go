package go_library

import (
	"time"
)

type App struct {
	BaseURL        string
	ApiVersion     string
	PageStartTime  time.Time
	NOW            string
	CLIENT_BUILD   string
	CLIENT_VERSION string
	DEVICE_MODEL   string
	OS_VERSION     string

	AppID       string
	DefaultRole string
	UserID      string
	RoleID      string
	OfficeCode  string
	Localize    string

	Log      logRequest
	Response responese
	// MyInput       map[string]string
	// MyAccessToken models.ItAcccessTokenModel
	// MyLog         models.ItLogRequest
	// MyResponse    models.VResponse
}

type responese struct {
	Code    int         `json:"code"`
	Status  string      `json:"status"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
	Errors  interface{} `json:"errors"`
	Path    string      `json:"path"`
}

type logRequest struct {
	Url           string    `json:"url"`
	Method        string    `json:"method"`
	Header        string    `json:"header"`
	Body          string    `json:"body"`
	ErrorResponse string    `json:"error_response"`
	Platform      string    `json:"platform"`
	Browser       string    `json:"browser"`
	AppID         string    `json:"app_id"`
	AccessToken   string    `json:"access_token"`
	UserID        string    `json:"user_id"`
	RoleID        string    `json:"role_id"`
	TimeLoad      float64   `json:"time_load"`
	CreatedAt     time.Time `json:"-"`
	IpAddress     string    `json:"ip_address"`
}
