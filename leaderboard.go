package main

import (
	"database/sql"
	"log"
	"sort"

	pb "github.com/cpjudge/proto/leaderboard"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gobuffalo/uuid"
)

var db *sql.DB

// Establishes a connection with the database.
func connectToDatabase() {
	var err error
	db, err = sql.Open("mysql", "root:root@(172.18.0.2)/cpjudge_webserver_development")
	if err != nil {
		log.Println("Error while trying to open", err.Error())
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("Error while trying to ping", err.Error())
	}
}

func processLeaderboard(contestID string) *pb.Participants {
	participants := []*pb.Participant{}
	questionIDs, err := getQuestions(contestID)
	if err != nil {
		log.Println(err.Error())
	}
	for _, v := range questionIDs {
		log.Println(v)
	}

	var userID uuid.UUID
	rows, err := db.Query("SELECT user_id FROM participate_in WHERE contest_id=?", contestID)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&userID)
		if err != nil {
			log.Fatal(err)
		}
		username, rating := getUser(userID)
		count := getCorrectSubmissions(userID, questionIDs)
		participant := &pb.Participant{
			UserId:        userID.String(),
			Username:      username,
			Rating:        int32(rating),
			NoOfQuestions: int32(count),
		}
		participants = append(participants, participant)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	sort.Slice(participants, func(i, j int) bool {
		return participants[i].NoOfQuestions > participants[j].NoOfQuestions
	})
	particpantsResult := &pb.Participants{
		Participant: participants,
	}
	return particpantsResult
}

func getQuestions(contestID string) ([]uuid.UUID, error) {
	rows, err := db.Query("SELECT id FROM questions WHERE contest_id=?", contestID)
	if err != nil {
		log.Fatal(err)
	}
	var questionIDs []uuid.UUID
	defer rows.Close()
	for rows.Next() {
		var questionID uuid.UUID
		err := rows.Scan(&questionID)
		if err != nil {
			log.Println("Error while scanning", err.Error())
			return nil, err
		}
		questionIDs = append(questionIDs, questionID)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return questionIDs, nil
}

func getCorrectSubmissions(studentID uuid.UUID, questionIDs []uuid.UUID) int {
	questionSet := make(map[uuid.UUID]int)
	for _, v := range questionIDs {
		rows, err := db.Query("SELECT question_id FROM submissions WHERE user_id=? AND question_id=? AND status=0",
			studentID.String(),
			v.String())
		if err != nil {
			log.Fatal(err)
		}
		var questionID uuid.UUID
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&questionID)
			if err != nil {
				log.Fatal(err)
			}
			questionSet[questionID] = 1
		}
		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}
	}
	return len(questionSet)
}

func getUser(userID uuid.UUID) (string, int) {
	var (
		username string
		rating   int
	)
	db.QueryRow("SELECT username,rating FROM users WHERE id=?",
		userID.String()).Scan(&username, &rating)
	return username, rating
}
