package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"message_go/pkg/models"
	"net/http"
	"strconv"

	"github.com/IBM/sarama"
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

func sendKafkaMessage(producer sarama.SyncProducer, users []models.User, ctx *gin.Context, fromID, toId int) error {

	fromUser, err := findUserByID(fromID, users)
	if err != nil {
		return err
	}

	toUser, err := findUserByID(toId, users)
	if err != nil {
		return err
	}

	notification := models.Notification{
		From: fromUser,
		To:   toUser,
	}

	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(toUser.ID)),
		Value: sarama.StringEncoder(notificationJSON),
	}

	_, _, err = producer.SendMessage(msg)
	return err

}

func sendMessageHandle(producer sarama.SyncProducer, users []models.User) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		fromID, err := getIDFromRequest("fromID", ctx)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
			return
		}

		toID, err := getIDFromRequest("toID", ctx)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		}

		err = sendKafkaMessage(producer, users, ctx, fromID, toID)
		if errors.Is(err, ErrUserNotFoundInProducer) {
			ctx.JSON(http.StatusNotFound, gin.H{"message": "User not found"})
			return
		}

		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		}
	}
}
