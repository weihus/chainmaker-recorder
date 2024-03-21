package recorderfile

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

var (
	serverPort   uint16 = 9527
	accessLock   sync.RWMutex
	accessConfig = make(map[string]bool)
	configLock   sync.RWMutex
	configValue  = make(map[string]interface{})
)

func startConfigListener(port uint16) {
	if port > 0 {
		serverPort = port
	}
	go safeGoroutine(runServer, nil)
}

func runServer() error {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	config := router.Group("/config")
	// config.GET("/registerinfo", getRegisterInfo)
	// config.GET("/accessconfig", getAccessConfig)
	config.PUT("/accessconfig", updateAccessConfig)
	config.GET("/configvalue", getConfigValue)
	config.PUT("/configvalue", updateConfigValue)
	return router.Run(fmt.Sprintf(":%d", serverPort))
}

func getRegisterInfo(c *gin.Context) {
	c.YAML(http.StatusOK, registerInfo)
}

func getAccessConfig(c *gin.Context) {
	accessLock.RLock()
	defer accessLock.RUnlock()
	c.YAML(http.StatusOK, accessConfig)
}

func updateAccessConfig(c *gin.Context) {
	accessLock.Lock()
	defer accessLock.Unlock()
	err := c.BindYAML(accessConfig)
	if err != nil {
		c.String(http.StatusBadRequest, "update accessConfig err: %s", err.Error())
		return
	}
	c.YAML(http.StatusOK, accessConfig)
}

func getConfigValue(c *gin.Context) {
	configLock.RLock()
	defer configLock.RUnlock()
	c.YAML(http.StatusOK, configValue)
}

func updateConfigValue(c *gin.Context) {
	configLock.Lock()
	defer configLock.Unlock()
	err := c.BindYAML(configValue)
	if err != nil {
		c.String(http.StatusBadRequest, "update configValue err: %s", err.Error())
		return
	}
	c.YAML(http.StatusOK, configValue)
}
