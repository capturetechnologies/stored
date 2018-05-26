package stored

import (
	"errors"
	"fmt"
	"time"
)

type smallUser struct {
	ID    int64  `stored:"i,primary"`
	Login string `stored:"l"`
}

type bigUser struct {
	ID           int64  `stored:"i,primary"`
	Name         string `stored:"n"`
	Login        string `stored:"l"`
	Score        int    `stored:"score"`
	FullName     string `stored:"fn"`
	Bio          []byte `stored:"b"`
	Relationship string `stored:"rl"`
	Date         int64  `stored:"d"`
	TimeStamp    int64  `stored:"ts"`
	Friends      string `stored:"friends"`
	Money        int64  `stored:"money"`
	Rating       int64  `stored:"r"`
	FriendsCount int64  `stored:"fc"`
	Distance     int64  `stored:"dist"`
	Scheme       string `stored:"sch"`
	Audio        string `stored:"audio"`
	Photo        string `stored:"photo"`
	Video        string `stored:"video"`
}

func TestsSetGet(db *Connection) {
	fmt.Println("start testing")
	smUser := db.Object("small_user", smallUser{})
	bgUser := db.Object("big_user", bigUser{})

	for i := 0; i < 1; i++ {
		var startTime = time.Now()
		err := smUser.Set(smallUser{-1, "some relevant amount of information for all the data should be passed with full object"})
		fmt.Println("small user set", time.Since(startTime), err)

		startTime = time.Now()
		err = bgUser.Set(bigUser{
			ID:       1,
			Name:     "hello",
			FullName: "Jared sull",
			Bio:      []byte("just some basic info"),
		})
		fmt.Println("big user set", time.Since(startTime), err)

		startTime = time.Now()
		newUser := smallUser{}
		err = smUser.Get(-1).Scan(&newUser)
		fmt.Println("small user get", time.Since(startTime), err)

		startTime = time.Now()
		newUser2 := bigUser{}
		err = bgUser.Get(1).Scan(&newUser2)
		fmt.Println("big user get", time.Since(startTime), err)
		fmt.Println("USER GOT", newUser, newUser2)
	}
}

func TestsUnique(db *Connection) {
	smUser := db.Object("small_user", smallUser{})
	smUser.Unique("l")
	err := smUser.Set(smallUser{5, "john2"}) // user setted
	fmt.Println("set err 1", err)

	gotUser := smallUser{}
	err = smUser.GetBy("l", "john2").Scan(&gotUser)
	fmt.Println("get err 2", err, gotUser)

	gotUser2 := smallUser{}
	err = smUser.Get(5).Scan(&gotUser2)
	fmt.Println("GOT BY ID", err, gotUser2)
}

func TestsIndex(db *Connection) error {
	smUser := db.Object("small_user", smallUser{})
	smUser.Index("l")
	err := smUser.Set(smallUser{6, "john2"}) // user setted
	if err != nil {
		return err
	}
	gotUser := smallUser{}
	err = smUser.GetBy("l", "john2").Scan(&gotUser)
	if err != nil {
		return err
	}
	if gotUser.Login != "john2" {
		return errors.New("User not fetched")
	}
	return nil
}

func TestsRun(db *Connection) {
	//TestsSetGet(db)
	err := TestsIndex(db)

	if err != nil {
		fmt.Println("Tests Error", err)
	}
}
