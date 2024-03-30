package main

import (
	"errors"
	"fmt"
	"message_go/pkg/models"
	"strconv"

	"github.com/gin-gonic/gin"
)

const (
	ProducerPort       = ":8080"
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

// ============== HELPER FUNCTIONS ==============
var ErrUserNotFoundInProducer = errors.New("user not found")

func findUserByID(id int, users []models.User) (models.User, error) {
	for _, user := range users {
		if user.ID == id {
			return user, nil
		}
	}

	return models.User{}, ErrUserNotFoundInProducer
}

func getIDFromRequest(formValue string, ctx *gin.Context) (int, error) {
	id, err := strconv.Atoi(ctx.PostForm(formValue))

	if err != nil {
		return 0, fmt.Errorf("failed to parse ID FROM VALUE %s: %w")
	}

	return id, nil
}

// ============== KAFKA RELATED FUNCTIONS ==============
