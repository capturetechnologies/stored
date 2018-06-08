package stored

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

type smallUser struct {
	ID    int    `stored:"i,primary"`
	Login string `stored:"l"`
}

type smallChat struct {
	ID   int64  `stored:"chat_id"`
	Name string `stored:"name"`
}

type userAutoInc struct {
	ID    int    `stored:"i,primary,autoincrement"`
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

func asset(name string, err error) {
	if err == nil {
		fmt.Println("Success «" + name + "»")
	} else {
		fmt.Println("Fail «"+name+"»:", err)
	}
}

func testsSetGet(smUser *Object) error {

	err := smUser.Set(smallUser{
		ID:    20,
		Login: "John23",
	})
	if err != nil {
		return err
	}

	newUser := smallUser{}
	err = smUser.Get(20).Scan(&newUser)
	if err != nil {
		return err
	}
	if newUser.Login != "John23" {
		return errors.New("User not fetched")
	}

	return nil
}

func testsSetGetPerformance(dir *Directory) error {
	smUser := dir.Object("small_user", smallUser{})
	bgUser := dir.Object("big_user", bigUser{})

	for i := 0; i < 1; i++ {
		err := smUser.Set(smallUser{2, "some relevant amount of information for all the data should be passed with full object"})
		if err != nil {
			return err
		}

		err = bgUser.Set(bigUser{
			ID:       3,
			Name:     "hello",
			FullName: "Jared sull",
			Bio:      []byte("just some basic info"),
		})
		if err != nil {
			return err
		}

		newUser := smallUser{}
		err = smUser.Get(2).Scan(&newUser)
		if err != nil {
			return err
		}

		newUser2 := bigUser{}
		err = bgUser.Get(3).Scan(&newUser2)
		if err != nil {
			return err
		}
	}
	return nil
}

func testsUnique(smUser *Object) error {
	err := smUser.Set(smallUser{40, "john25"}) // user setted
	if err != nil {
		return err
	}

	gotUser := smallUser{}
	err = smUser.GetBy("l", "john25").Scan(&gotUser)
	if err != nil {
		return err
	}
	if gotUser.Login != "john25" {
		return errors.New("User not fetched")
	}
	return nil
}

func testsIndex(smUser *Object) error {
	err := smUser.Set(smallUser{30, "john24"}) // user setted
	if err != nil {
		return err
	}
	gotUser := smallUser{}
	err = smUser.GetBy("l", "john24").Scan(&gotUser)
	if err != nil {
		return err
	}
	if gotUser.Login != "john24" {
		return errors.New("User not fetched")
	}
	return nil
}

func testsClear(dir *Directory) error {
	smUser := dir.Object("small_user", smallUser{})
	err := smUser.Set(smallUser{1, "TmpJohn"})
	if err != nil {
		return err
	}

	err = dir.Clear()
	if err != nil {
		return err
	}

	newUser := smallUser{}
	err = smUser.Get(1).Scan(&newUser)
	if err != nil {
		if err == ErrNotFound {
			return nil
		}
		return err
	}
	if newUser.Login == "TmpJohn" {
		return errors.New("TestsClear Failed: clear do not work")
	}
	return errors.New("TestsClear Failed: should return proper error")
}

func testsAutoIncrement(dbUser *Object) error {
	user1 := userAutoInc{
		Login: "john",
	}
	err := dbUser.Add(&user1)
	if err != nil {
		return err
	}
	if user1.ID != 1 || user1.Login != "john" {
		return errors.New("new user1 incorrect")
	}
	user2 := userAutoInc{
		Login: "sam",
	}
	err = dbUser.Add(&user2)
	if err != nil {
		return err
	}
	if user2.ID != 2 || user2.Login != "sam" {
		return errors.New("new user2 incorrect")
	}
	userGet := userAutoInc{}
	err = dbUser.Get(1).Scan(&userGet)
	if err != nil {
		return err
	}
	if userGet.ID != 1 || userGet.Login != "john" {
		return errors.New("get user1 incorrect")
	}
	err = dbUser.Get(2).Scan(&userGet)
	if err != nil {
		return err
	}
	if userGet.ID != 2 || userGet.Login != "sam" {
		return errors.New("get user2 incorrect")
	}
	return nil
}

func testsMultiGet(dbUser *Object) error {
	need := []int{}
	for i := 0; i < 10; i++ {
		toAdd := userAutoInc{
			Login: "sam" + strconv.Itoa(i),
		}
		err := dbUser.Add(&toAdd)
		if err != nil {
			return err
		}
		need = append(need, i+1)
	}
	users := []userAutoInc{}
	dbUser.MultiGet(need).ScanAll(&users)
	if len(users) < 10 {
		return errors.New("user count is incorrect")
	}
	for k, v := range users {
		if k+1 != v.ID {
			fmt.Println("test failed", k+1, v)
			return errors.New("user Id is incorrect")
		}
		if "sam"+strconv.Itoa(k) != v.Login {
			return errors.New("user Login is incorrect")
		}
	}
	return nil
}

// TestsRun runs tests for STORED FoundationdDB layer
func TestsRun(db *Connection) {
	dir := db.Directory("tests")
	smUser := dir.Object("setget", smallUser{})
	smUserIndex := dir.Object("index", smallUser{})
	smUserIndex.Index("l")
	smUserUnique := dir.Object("unique", smallUser{})
	smUserUnique.Unique("l")
	userAutoIncrement := dir.Object("increment", userAutoInc{})
	userMulti := dir.Object("multi", userAutoInc{})
	//n2nUser := dir.Object("n2n_user", smallUser{})
	//n2nChat := dir.Object("n2n_chat", smallChat{})
	//n2nUserChat := n2nUser.N2N(n2nChat)
	dir.Clear()
	asset("Clear", testsClear(dir))
	start := time.Now()
	asset("SetGet", testsSetGet(smUser))
	asset("Index", testsIndex(smUserIndex))
	asset("Unique", testsUnique(smUserUnique))
	asset("AutoIncrement", testsAutoIncrement(userAutoIncrement))
	asset("MultiGet", testsMultiGet(userMulti))
	//asset("n2n", testsN2N(n2nUser, n2nChat))
	fmt.Println("elapsed", time.Since(start))
	start = time.Now()
}
