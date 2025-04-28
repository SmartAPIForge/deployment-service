package main

import (
	"deployment-service/internal/app"
	"deployment-service/internal/config"
	"deployment-service/pkg/logger"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := config.MustLoad()
	log := logger.SetupLogger(cfg.Env)

	application, err := app.NewApp(log, cfg)
	if err != nil {
		log.Error("Error creating application: ", err)
		os.Exit(1)
	}

	err = application.Start()
	if err != nil {
		return
	}

	stopWait(application)
}

func stopWait(application *app.App) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	application.GRPCServer.Stop()
}
