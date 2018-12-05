package stored

/*
import (
	"errors"
	"fmt"
	"strconv"
)

type testsUser struct {
	ID    int    `stored:"id,primary"`
	Login string `stored:"login"`
}

type testsBigUser struct {
	ID           int64          `stored:"id,primary"`
	Name         string         `stored:"name"`
	Login        string         `stored:"login"`
	Score        int            `stored:"score"`
	FullName     string         `stored:"full_name"`
	Reactions    map[string]int `stored:"reactions"`
	Subscription bool           `stored:"subscription"`
	Sandbox      bool           `stored:"sandbox"`
}

// TestsNewRun just run all the tests
func TestsNewRun(cluster *Cluster) {
	dir := cluster.Directory("tests_new")

	user := dir.Object("user", testsUser{})
	user.Index("login")
	user.AutoIncrement("id")
	dir.Clear()
	bigUser := dir.Object("big_user", testsBigUser{})
	bigUser.AutoIncrement("id")
	db := dir.Build()

	fmt.Println("NEW TESTS RUNNING")
	assert("SetGet", testsSetGet(db))
	assert("GetByIndex", testsIndex(db))
	assert("GetByUnique", testsUnique(db))
	assert("AutoIncrement", testsAutoIncrement(db))
	assert("MultiGet", testsMultiGet(db))
	assert("VariousTypes", testsTypes(db))
}

func testsTypes(db *Database) error {
	u := testsBigUser{
		Login: "wow",
		Score: 1,
		Reactions: map[string]int{
			"hello": 1,
			"world": 2,
		},
		Subscription: true,
		Sandbox:      false,
	}
	db.Add(&u).Err()

	fetchedUser := testsBigUser{
		ID: u.ID,
	}
	err := db.Get(&fetchedUser).Err()
	if err != nil {
		return err
	}

	if fetchedUser.Score != 1 {
		return errors.New("score has incorrent value after Update")
	}
	if !fetchedUser.Subscription {
		return errors.New("Subscription is false should be true")
	}
	if fetchedUser.Sandbox {
		return errors.New("Sandbox is true should be false")
	}

	return nil
}

func testsMultiGet(db *Database) error {
	users := []*testsUser{}
	for i := 0; i < 10; i++ {
		toAdd := testsUser{
			Login: "sam" + strconv.Itoa(i),
		}
		err := db.Add(&toAdd).Err()
		if err != nil {
			return err
		}
		users = append(users, &testsUser{
			ID: toAdd.ID,
		})
	}

	err := db.MultiGet(users).Err()
	if err != nil {
		return err
	}
	if len(users) < 10 {
		return errors.New("user count is incorrect")
	}
	prev := 0
	for k, v := range users {
		if prev != 0 && prev != v.ID-1 {
			return fmt.Errorf("user Id is incorrect (%d != %d)", prev+1, v.ID)
		}
		if "sam"+strconv.Itoa(k) != v.Login {
			return errors.New("user Login is incorrect: " + v.Login)
		}
		if v.ID == 0 {
			return errors.New("user ID is 0")
		}
		prev = v.ID
	}
	return nil
}

func testsAutoIncrement(db *Database) error {
	user1 := testsUser{
		Login: "john",
	}
	err := db.Add(&user1).Err()
	if err != nil {
		return err
	}
	if user1.ID != 1 || user1.Login != "john" {
		return fmt.Errorf("new user1 incorrect, id: %d, login: %s", user1.ID, user1.Login)
	}
	user2 := testsUser{
		Login: "sam",
	}
	err = db.Add(&user2).Err()
	if err != nil {
		return err
	}
	if user2.ID != 2 || user2.Login != "sam" {
		return errors.New("new user2 incorrect")
	}
	userGet := testsUser{
		ID: 1,
	}
	err = db.Get(&userGet).Err()
	if err != nil {
		return err
	}
	if userGet.ID != 1 || userGet.Login != "john" {
		return errors.New("get user1 incorrect")
	}
	userGet.ID = 2
	err = db.Get(&userGet).Err()
	if err != nil {
		return err
	}
	if userGet.ID != 2 || userGet.Login != "sam" {
		return errors.New("get user2 incorrect")
	}
	return nil
}

func testsUnique(db *Database) error {
	err := db.Set(testsUser{ID: 40, Login: "john25"}).Err() // user setted
	if err != nil {
		return err
	}

	gotUser := testsUser{Login: "john25"}
	err = db.GetBy(&gotUser, "login").Err()
	if err != nil {
		return err
	}
	if gotUser.ID == 0 || gotUser.Login != "john25" {
		return errors.New("User not fetched")
	}
	return nil
}

func testsIndex(db *Database) error {
	err := db.Set(testsUser{ID: 30, Login: "john24"}).Err() // user setted
	if err != nil {
		return err
	}
	gotUser := testsUser{Login: "john24"}
	err = db.GetBy(&gotUser, "login").Err()
	if err != nil {
		return err
	}
	if gotUser.ID == 0 {
		return errors.New("User not fetched")
	}
	if gotUser.Login != "john24" {
		return errors.New("User fetched incorrectly")
	}
	return nil
}

func testsSetGet(db *Database) error {
	err := db.Set(testsUser{
		ID:    20,
		Login: "John23",
	}).Err()
	if err != nil {
		return err
	}

	newUser := testsUser{
		ID: 20,
	}
	err = db.Get(&newUser).Err()
	if err != nil {
		return err
	}
	if newUser.Login != "John23" {
		return errors.New("User not fetched")
	}
	return nil
}*/
