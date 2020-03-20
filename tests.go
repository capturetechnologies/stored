package stored

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"capture/stored/packed"
)

type user struct {
	ID    int    `stored:"id,primary"`
	Login string `stored:"login"`
}

type chat struct {
	ID   int    `stored:"id,primary"`
	Name string `stored:"name"`
}

type userN2N struct {
	ID      int    `stored:"id,primary"`
	Login   string `stored:"login"`
	N2NData struct {
		Mute bool
	} `unstored:"n2n"`
}

type chatN2N struct {
	ID      int    `stored:"id,primary"`
	Name    string `stored:"name"`
	N2NData struct {
		Mute bool
	} `unstored:"n2n"`
}

type message struct {
	ID     int    `stored:"id"`
	ChatID int    `stored:"chat_id"`
	Text   string `stored:"text"`
}

type smallChat struct {
	ID   int64  `stored:"chat_id"`
	Name string `stored:"name"`
}

type userAutoInc struct {
	ID    int    `stored:"id,primary,autoincrement"`
	Login string `stored:"login"`
}

type bigUser struct {
	ID           int64          `stored:"id,primary"`
	Name         string         `stored:"name"`
	Login        string         `stored:"login"`
	Score        int            `stored:"score"`
	FullName     string         `stored:"full_name"`
	Reactions    map[string]int `stored:"reactions"`
	Subscription bool           `stored:"subscription"`
	Sandbox      bool           `stored:"sandbox"`
}

type extra struct {
	ChatID int64  `stored:"chat_id"`
	Extra  string `stored:"extra"`
}

type extendedUser struct {
	ID    int64  `stored:"id,primary"`
	Name  string `stored:"name"`
	Extra *extra `stored:"extra"`
}

// AssertErrors list of errors
var AssertErrors = []string{}

// AssertMux mutex for errors
var AssertMux sync.Mutex

// return 1 if error
func assert(name string, err error) {
	if err == nil {
		fmt.Println("Success «" + name + "»")
		return
	}
	errStr := "Fail «" + name + "»: " + err.Error()
	fmt.Println(errStr)
	AssertMux.Lock()
	AssertErrors = append(AssertErrors, errStr)
	AssertMux.Unlock()
}

// TestsCheck will return list of tests result
func TestsCheck() []string {
	AssertMux.Lock()
	res := make([]string, len(AssertErrors))
	copy(res, AssertErrors)
	AssertMux.Unlock()
	return res
}

func testsSetGet(smUser *Object) error {
	err := smUser.Set(user{
		ID:    20,
		Login: "John23",
	}).Err()
	if err != nil {
		return err
	}

	newUser := user{ID: 20}
	err = smUser.Get(&newUser).Err()
	if err != nil {
		return err
	}
	if newUser.Login != "John23" {
		return errors.New("User not fetched")
	}

	return nil
}

func testsUpdate(smUser *Object) error {
	err := smUser.Set(user{
		ID:    80,
		Login: "John80",
	}).Err()
	if err != nil {
		return err
	}

	u := user{ID: 80}
	err = smUser.Update(&u, func() error {
		if u.Login == "John80" {
			u.Login = "John81"
		} else {
			return errors.New("update data is incorrect")
		}
		return nil
	}).Err()
	if err != nil {
		return err
	}

	newUser := user{ID: 80}
	err = smUser.Get(&newUser).Err()
	if err != nil {
		return err
	}
	if newUser.Login == "John80" {
		return errors.New("User update failed")
	}
	if newUser.Login != "John81" {
		return errors.New("User not fetched")
	}

	return nil
}

func testsSetGetPerformance(dir *Directory) error {
	smUser := dir.Object("small_user", user{}).Done()
	bgUser := dir.Object("big_user", bigUser{}).Done()

	for i := 0; i < 1; i++ {
		err := smUser.Set(user{ID: 2, Login: "some relevant amount of information for all the data should be passed with full object"}).Err()
		if err != nil {
			return err
		}

		err = bgUser.Set(bigUser{
			ID:       3,
			Name:     "hello",
			FullName: "Jared sull",
		}).Err()
		if err != nil {
			return err
		}

		newUser := user{ID: 2}
		err = smUser.Get(&newUser).Err()
		if err != nil {
			return err
		}

		newUser2 := bigUser{ID: 3}
		err = bgUser.Get(&newUser2).Err()
		if err != nil {
			return err
		}
	}
	return nil
}

func testsUnique(smUser *Object) error {
	err := smUser.Set(user{ID: 40, Login: "john25"}).Err() // user setted
	if err != nil {
		return err
	}

	gotUser := user{Login: "john25"}
	err = smUser.GetBy(&gotUser, "login").Err()
	if err != nil {
		return err
	}
	if gotUser.Login != "john25" {
		return errors.New("User not fetched")
	}
	return nil
}

func testsIndex(smUser *Object) error {
	err := smUser.Set(user{ID: 30, Login: "john24"}).Err() // user setted
	if err != nil {
		return err
	}
	gotUser := user{Login: "john24"}
	err = smUser.GetBy(&gotUser, "login").Err()
	if err != nil {
		return err
	}
	if gotUser.Login != "john24" {
		return errors.New("User not fetched")
	}
	return nil
}

func testsClear(dir *Directory) error {
	smUser := dir.Object("small_user", user{}).Done()
	err := smUser.Set(user{ID: 1, Login: "TmpJohn"}).Err()
	if err != nil {
		return err
	}

	err = dir.Clear()
	if err != nil {
		return err
	}

	newUser := user{ID: 1}
	err = smUser.Get(&newUser).Err()
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
	err := dbUser.Add(&user1).Err()
	if err != nil {
		return err
	}
	if user1.ID != 1 || user1.Login != "john" {
		return errors.New("new user1 incorrect")
	}
	user2 := userAutoInc{
		Login: "sam",
	}
	err = dbUser.Add(&user2).Err()
	if err != nil {
		return err
	}
	if user2.ID != 2 || user2.Login != "sam" {
		return errors.New("new user2 incorrect")
	}
	userGet := userAutoInc{ID: 1}
	err = dbUser.Get(&userGet).Err()
	if err != nil {
		return err
	}
	if userGet.ID != 1 || userGet.Login != "john" {
		return errors.New("get user1 incorrect")
	}
	userGet.ID = 2
	err = dbUser.Get(&userGet).Err()
	if err != nil {
		return err
	}
	if userGet.ID != 2 || userGet.Login != "sam" {
		return errors.New("get user2 incorrect")
	}
	return nil
}

func testsMultiGet(dbUser *Object) error {
	users := []*userAutoInc{}
	for i := 0; i < 10; i++ {
		toAdd := userAutoInc{
			Login: "sam" + strconv.Itoa(i),
		}
		err := dbUser.Add(&toAdd).Err()
		if err != nil {
			return err
		}
		users = append(users, &userAutoInc{ID: i + 1})
	}

	dbUser.MultiGet(users).Err()
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

func testsN2N(n2nUser *Object, n2nChat *Object, n2nUserChat *Relation) error {
	user1 := userN2N{
		Login: "John",
	}
	user2 := userN2N{
		Login: "Sam",
	}
	user3 := userN2N{
		Login: "Nick",
	}
	chat1 := chatN2N{
		Name: "Chat name 1",
	}
	chat2 := chatN2N{
		Name: "Chat name 2",
	}
	chat3 := chatN2N{
		Name: "Chat name 3",
	}
	chatToDelete := chatN2N{
		Name: "Chat to delete",
	}

	var err error
	err = n2nUser.Add(&user1).Err()
	if err != nil {
		return err
	}
	err = n2nUser.Add(&user2).Err()
	if err != nil {
		return err
	}
	err = n2nUser.Add(&user3).Err()
	if err != nil {
		return err
	}
	err = n2nChat.Add(&chat1).Err()
	if err != nil {
		return err
	}
	err = n2nChat.Add(&chat2).Err()
	if err != nil {
		return err
	}
	err = n2nChat.Add(&chat3).Err()
	if err != nil {
		return err
	}
	err = n2nChat.Add(&chatToDelete).Err()
	if err != nil {
		return err
	}

	var hasConnection bool
	hasConnection, err = n2nUserChat.Check(user1, chat1).Bool()
	if err != nil {
		return err
	}
	if hasConnection {
		return errors.New("Check n2n function is failed, true instead of false")
	}

	err = n2nUserChat.Set(user1, chat1).Err() // add using object
	if err != nil {
		return err
	}

	hasConnection, err = n2nUserChat.Check(user1, chat1).Bool()
	if err != nil {
		return err
	}
	if !hasConnection {
		return errors.New("Check n2n function is failed, false instead of true")
	}

	err = n2nUserChat.Set(user1, chatToDelete).Err()
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user1, chat2.ID).Err() // add using clients primary id
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user1, chat2).Err() // second update same relation should not increment counter
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user1.ID, chat3).Err() // add using hosts primary id
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user2, chat1).Err() // add using object
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user3, chat1).Err() // add using object
	if err != nil {
		return err
	}
	err = n2nUserChat.Delete(user1, chatToDelete).Err() // delete using object
	if err != nil {
		return err
	}

	chats := []chatN2N{}
	err = n2nUserChat.GetClients(user1, nil).Limit(20).ScanAll(&chats)
	if err != nil {
		return err
	}
	if len(chats) == 0 {
		return errors.New("no chats was fetched")
	}
	if len(chats) != 3 {
		return errors.New("incorrect chats amount was fetched")
	}
	if chats[0].Name != "Chat name 1" {
		return errors.New("chat 1 is invalid, name incorrect: " + chats[0].Name)
	}
	if chats[0].ID != 1 {
		return fmt.Errorf("chat 1 is invalid, id incorrect: %d", chats[0].ID)
	}
	if chats[1].Name != "Chat name 2" || chats[1].ID != 2 {
		return errors.New("chat 2 is invalid")
	}
	if chats[2].Name != "Chat name 3" || chats[2].ID != 3 {
		return errors.New("chat 3 is invalid")
	}

	// counter test
	var count int64
	count, err = n2nUserChat.GetClientsCount(user1).Int64()
	if err != nil {
		return err
	}
	if count != 3 {
		return errors.New("incorrect user counter count: " + strconv.Itoa(int(count)))
	}

	users := []userN2N{}
	err = n2nUserChat.GetHosts(chat1, nil).Limit(2).ScanAll(&users)
	if err != nil {
		return err
	}
	if len(users) != 2 {
		fmt.Println(len(users), "instead of 3", users)
		return errors.New("incorrect users amount was fetched")
	}
	if users[0].Login != "John" || users[0].ID != 1 {
		return errors.New("user 1 is invalid")
	}
	if users[1].Login != "Sam" || users[1].ID != 2 {
		return errors.New("user 2 is invalid")
	}
	users2 := []userN2N{}
	err = n2nUserChat.GetHosts(chat1, 3).Limit(10).ScanAll(&users2)
	if err != nil {
		return err
	}
	if users2[0].Login != "Nick" || users2[0].ID != 3 {
		return errors.New("user 3 is invalid with offset fetching")
	}

	count, err = n2nUserChat.GetHostsCount(chat1).Int64()
	if err != nil {
		return err
	}
	if count != 3 {
		return errors.New("incorrect chat counter count: " + strconv.Itoa(int(count)))
	}

	// Counters decrement test
	err = n2nUserChat.Delete(user3, chat1).Err()
	if err != nil {
		return err
	}
	count, err = n2nUserChat.GetHostsCount(chat1).Int64()
	if err != nil {
		return err
	}
	if count != 2 {
		return errors.New("incorrect chat counter count after delete one (should 2): " + strconv.Itoa(int(count)))
	}

	ids, err := n2nUserChat.GetClientIDs(user1, 0, 1000).Int64()
	if err != nil {
		return err
	}
	_, ok1 := ids[int64(1)]
	_, ok2 := ids[int64(2)]
	_, ok3 := ids[int64(3)]
	if !ok1 || !ok2 || !ok3 {
		return errors.New("incorrect get index IDs ")
	}

	return nil
}

func testsN2NData(n2nUser *Object, n2nChat *Object, n2nUserChat *Relation) error {
	user1 := userN2N{
		Login: "John",
		N2NData: struct {
			Mute bool
		}{
			Mute: true,
		},
	}
	user2 := userN2N{
		Login: "Sam",
		N2NData: struct {
			Mute bool
		}{
			Mute: false,
		},
	}
	chat1 := chatN2N{
		Name: "Chat name 1",
		N2NData: struct {
			Mute bool
		}{
			Mute: true,
		},
	}
	chat2 := chatN2N{
		Name: "Chat name 2",
		N2NData: struct {
			Mute bool
		}{
			Mute: false,
		},
	}
	var err error
	err = n2nUser.Add(&user1).Err()
	if err != nil {
		return err
	}
	err = n2nUser.Add(&user2).Err()
	if err != nil {
		return err
	}
	err = n2nChat.Add(&chat1).Err()
	if err != nil {
		return err
	}
	err = n2nChat.Add(&chat2).Err()
	if err != nil {
		return err
	}

	err = n2nUserChat.Set(user1, chat1).Err()
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user1, chat2).Err()
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user2, chat1).Err()
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user2, chat2).Err()
	if err != nil {
		return err
	}

	chats := []chatN2N{} // list of chats is here
	err = n2nUserChat.GetClients(user1, nil).Limit(20).ScanAll(&chats)
	if err != nil {
		return err
	}
	if len(chats) != 2 {
		return errors.New("fetch " + strconv.Itoa(len(chats)) + " chats, should be 2")
	}
	if chats[0].N2NData.Mute != true || chats[1].N2NData.Mute != false {
		return errors.New("Mute chats data is incorrect")
	}

	users := []userN2N{} // list of chats is here
	err = n2nUserChat.GetHosts(chat1, nil).Limit(20).ScanAll(&users)
	if err != nil {
		return err
	}
	if len(users) != 2 {
		return errors.New("fetch " + strconv.Itoa(len(users)) + " users, should be 2")
	}
	if users[0].N2NData.Mute != true || users[1].N2NData.Mute != false {
		fmt.Println(users[0], users[1])
		return errors.New("Mute users data is incorrect")
	}

	chatGet := chatN2N{}
	err = n2nUserChat.GetClientData(user1, chat1).Scan(&chatGet)
	if err != nil {
		return err
	}
	if chatGet.N2NData.Mute != true {
		fmt.Println("chatGet", chatGet)
		return errors.New("Mute chat data via GetClientData is incorrect")
	}

	userGet := chatN2N{}
	err = n2nUserChat.GetHostData(user2, chat2).Scan(&userGet)
	if err != nil {
		return err
	}
	if userGet.N2NData.Mute != false {
		fmt.Println("userGet", userGet)
		return errors.New("Mute user data via GetHostData is incorrect")
	}

	return nil
}

func testsTypes(dbUser *Object) error {
	u := bigUser{
		Login: "wow",
		Score: 1,
		Reactions: map[string]int{
			"hello": 1,
			"world": 2,
		},
		Subscription: true,
		Sandbox:      false,
	}
	dbUser.Add(&u).Err()

	fetchedUser := bigUser{ID: u.ID}
	err := dbUser.Get(&fetchedUser).Err()
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

func testsEditField(dbUser *Object) error {
	u := bigUser{
		Login: "wow",
		Score: 1,
	}
	err := dbUser.Add(&u).Err()
	if err != nil {
		return err
	}
	err = dbUser.IncFieldUnsafe(u, "score", 1).Err()
	if err != nil {
		return err
	}

	fetchedUser := bigUser{ID: u.ID}
	err = dbUser.Get(&fetchedUser).Err()
	if err != nil {
		return err
	}
	if fetchedUser.Score != 2 {
		return errors.New("score has incorrent value after Increment")
	}

	u.Score = 4
	dbUser.SetField(&u, "score").Err()

	fetchedUser.ID = u.ID
	err = dbUser.Get(&fetchedUser).Err()
	if err != nil {
		return err
	}
	if fetchedUser.Score != 4 {
		return errors.New("score has incorrent value after Update")
	}

	return nil
}

func testsMultiPrimary(dbMessage *Object, n2nMessageUser *Relation, n2nUser *Object) error {
	var err error
	msg := message{
		ChatID: 1,
		ID:     1,
		Text:   "first message",
	}
	err = dbMessage.Set(&msg).Err()
	if err != nil {
		return err
	}
	msg2 := message{
		ChatID: 1,
		ID:     2,
		Text:   "second message",
	}
	err = dbMessage.Set(&msg2).Err()
	if err != nil {
		return err
	}

	fetching := message{
		ChatID: 1,
		ID:     1,
	}
	err = dbMessage.Get(&fetching).Err()
	if err != nil {
		return err
	}
	if fetching.ID != 1 || fetching.ChatID != 1 {
		return errors.New("primary ids is incorrect while get")
	}
	if fetching.Text != "first message" {
		return errors.New("message.Text is incorrect while get")
	}

	// Tesing relations
	user1 := userN2N{
		Login: "Sender1",
	}
	user2 := userN2N{
		Login: "Sender2",
	}
	err = n2nUser.Add(&user1).Err()
	if err != nil {
		return err
	}
	err = n2nUser.Add(&user2).Err()
	if err != nil {
		return err
	}
	err = n2nMessageUser.Set(msg, user1).Err()
	if err != nil {
		return err
	}
	err = n2nMessageUser.Set(msg, user2).Err()
	if err != nil {
		return err
	}
	users := []userN2N{}
	err = n2nMessageUser.GetClients(msg, nil).Limit(100).ScanAll(&users)
	if err != nil {
		return err
	}
	if len(users) != 2 || users[0].Login != "Sender1" || users[1].Login != "Sender2" {
		return errors.New("relation users fetched error")
	}

	// Gest multi get
	messages := []message{}

	err = dbMessage.List(1).Limit(100).ScanAll(&messages)
	if err != nil {
		return err
	}
	if len(messages) != 2 {
		return errors.New("not enough messages")
	}
	if messages[0].ChatID != 1 {
		return fmt.Errorf("messages list corrupt msg 0, chat id should be 1, but %d", messages[0].ChatID)
	}
	if messages[0].Text != "first message" {
		return fmt.Errorf("messages list corrupt msg 0, text incorrect: %s", messages[0].Text)
	}
	if messages[1].ID != 2 || messages[1].Text != "second message" {
		return errors.New("messages list corrupt msg 1")
	}

	return nil
}

func testsN2NSelf(testUser *Object, userUser *Relation) error {
	user1 := user{
		Login: "Hello",
	}
	err := testUser.Add(&user1).Err()
	if err != nil {
		return err
	}
	user2 := user{
		Login: "World",
	}

	err = testUser.Add(&user2).Err()
	if err != nil {
		return err
	}
	err = userUser.Set(user1, user2).Err()
	if err != nil {
		return err
	}
	users := []*user{}
	err = userUser.GetClients(user1, nil).Limit(1000).ScanAll(&users)
	if err != nil {
		return err
	}
	if len(users) != 1 {
		return errors.New("Incorrect users found for 1: " + strconv.Itoa(len(users)) + ", should be 1")
	}
	if users[0].Login != "World" {
		return errors.New("Incorrect user found 1")
	}
	users = []*user{}
	err = userUser.GetClients(user2, nil).Limit(1000).ScanAll(&users)
	if err != nil {
		return err
	}
	if len(users) != 1 {
		return errors.New("Incorrect users found for 2: " + strconv.Itoa(len(users)) + ", should be 1")
	}
	if users[0].Login != "Hello" {
		return errors.New("Incorrect user found 2")
	}
	return nil
}

func testsCounter(dir *Directory) error {
	type usr struct {
		ID   int    `stored:"id"`
		Age  int    `stored:"age"`
		City string `stored:"city"`
	}
	user := dir.Object("counter_user", usr{})

	user.AutoIncrement("id")
	user.Primary("id")
	userCityAgeCount := user.Counter("city", "age")
	dbUser := user.Done()
	dbUser.Clear()
	dbUser.Add(&usr{
		Age:  18,
		City: "LA",
	}).Err()
	dbUser.Add(&usr{
		Age:  19,
		City: "LA",
	}).Err()
	dbUser.Add(&usr{
		Age:  18,
		City: "LA",
	}).Err()
	dbUser.Add(&usr{
		Age:  18,
		City: "SF",
	}).Err()
	count, err := userCityAgeCount.Get(usr{
		Age:  18,
		City: "LA",
	}).Int64()
	if err != nil {
		return err
	}
	if count != 2 {
		return fmt.Errorf("counter returned incorrect value %d instead of 2", count)
	}
	return nil
}

func testsN2NClientCounter(dir *Directory) error {
	type usr struct {
		ID   int    `stored:"id"`
		Age  int    `stored:"age"`
		City string `stored:"city"`
	}
	type cht struct {
		ID         int    `stored:"id"`
		Name       string `stored:"cht"`
		MembsCount int64  `stored:"membs_count"`
	}
	user := dir.Object("n2n_counter_user", usr{})
	user.AutoIncrement("id")
	user.Primary("id")
	dbUser := user.Done()
	chat := dir.Object("n2n_counter_chat", cht{})
	chat.AutoIncrement("id")
	chat.Primary("id")
	dbChat := chat.Done()

	n2n := user.N2N(chat, "")

	dbChat.Clear()
	dbUser.Clear()
	n2n.Counter(true)
	n2n.CounterClient(chat, "membs_count")

	cht1 := cht{
		Name: "My Chat",
	}
	err := dbChat.Add(&cht1).Err()
	if err != nil {
		fmt.Println("e1")
		return err
	}
	usr1 := usr{
		Age:  18,
		City: "LA",
	}
	err = dbUser.Add(&usr1).Err()
	if err != nil {
		fmt.Println("e2")
		return err
	}
	err = n2n.Add(&usr1, &cht1).Err()
	if err != nil {
		fmt.Println("e3")
		return err
	}
	usr2 := usr{
		Age:  19,
		City: "SF",
	}
	err = dbUser.Add(&usr2).Err()
	if err != nil {
		fmt.Println("e4")
		return err
	}
	err = n2n.Add(&usr2, &cht1).Err()
	if err != nil {
		fmt.Println("e5")
		return err
	}
	usr3 := usr{
		Age:  20,
		City: "NY",
	}
	err = dbUser.Add(&usr3).Err()
	if err != nil {
		fmt.Println("e6")
		return err
	}
	err = n2n.Add(&usr3, &cht1).Err()
	if err != nil {
		fmt.Println("e7")
		return err
	}

	// renew the object
	err = dbChat.Get(&cht1).Err()
	if err != nil {
		fmt.Println("e7")
		return err
	}

	if cht1.MembsCount != 3 {
		return fmt.Errorf("members count != 3, %d", cht1.MembsCount)
	}

	err = n2n.Delete(&usr2, &cht1).Err()
	if err != nil {
		fmt.Println("e7")
		return err
	}

	// renew the object
	err = dbChat.Get(&cht1).Err()
	if err != nil {
		fmt.Println("e7")
		return err
	}

	if cht1.MembsCount != 2 {
		return fmt.Errorf("members count != 2, %d", cht1.MembsCount)
	}

	return nil
}

func testsGeoIndex(dir *Directory) error {
	type geo struct {
		ID   int     `stored:"id"`
		Lat  float64 `stored:"lat"`
		Long float64 `stored:"long"`
	}
	g := dir.Object("geo_test", geo{})
	g.Primary("id")
	indexGeo := g.IndexGeo("lat", "long", 4)
	dbGeo := g.Done()
	dbGeo.Clear()

	row := geo{
		ID:   1,
		Lat:  30.1,
		Long: 50.101,
	}
	err := dbGeo.Set(&row).Err()
	if err != nil {
		return err
	}
	rows := []geo{}
	err = indexGeo.GetGeo(30.1, 50.10101).Limit(10).ScanAll(&rows)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return fmt.Errorf("incorrect rows count %d instead of 1", len(rows))
	}

	row.Lat = 50.2 // inverse the coordinates
	row.Long = 25.1
	err = dbGeo.Set(&row).Err()
	rows = []geo{}
	err = indexGeo.GetGeo(30.1, 50.10101).Limit(10).ScanAll(&rows)
	if err != nil {
		return err
	}
	if len(rows) != 0 {
		return fmt.Errorf("item found, should be empty, %d passed", len(rows))
	}

	return nil
}

func testsSingleField(dir *Directory) error {
	type row struct {
		ID int `stored:"id"`
	}
	r := dir.Object("single_field", row{})
	r.Primary("id")
	dbRow := r.Done()
	dbRow.Clear()

	item := row{
		ID: 25,
	}
	err := dbRow.Set(&item).Err()
	if err != nil {
		return err
	}

	items := []row{}
	err = dbRow.List().ScanAll(&items)
	if err != nil {
		return err
	}
	if len(items) != 1 {
		return fmt.Errorf("items len is incorrrect should be 1, but %d", len(items))
	}
	if items[0].ID != 25 {
		return fmt.Errorf("item id is incorrrect should be 25, but %d", items[0].ID)
	}
	return nil

}

// TestsRun runs tests for STORED FoundationdDB layer
func TestsRun(db *Cluster) {
	packed.Test()

	dir := db.Directory("tests")
	smUser := dir.Object("setget", user{})
	smUserIndex := dir.Object("index", user{})
	smUserIndex.Index("login")
	smUserUnique := dir.Object("unique", user{})
	smUserUnique.Unique("login")
	userAutoIncrement := dir.Object("increment", userAutoInc{})
	userMulti := dir.Object("multi", userAutoInc{})
	n2nUser := dir.Object("n2n_user", userN2N{})
	n2nUser.AutoIncrement("id")
	n2nChat := dir.Object("n2n_chat", chatN2N{})
	n2nChat.AutoIncrement("id")
	n2nUserChat := n2nUser.N2N(n2nChat, "")
	n2nUserChat.Counter(true)

	n2nUserChat2 := n2nUser.N2N(n2nChat, "")
	n2nUserChat2.HostData("n2n")
	n2nUserChat2.ClientData("n2n")

	dbBigUser := dir.Object("big_user", bigUser{})
	dbBigUser.AutoIncrement("id")

	dbMessage := dir.Object("message", message{})
	dbMessage.Primary("chat_id", "id")
	n2nMessageUser := dbMessage.N2N(n2nUser, "")

	// n2n_self
	n2nSelfUser := dir.Object("n2nselfuser", user{})
	n2nSelfUser.AutoIncrement("id")
	n2nSelfUserUser := n2nSelfUser.N2N(n2nSelfUser, "")

	//dbExtended := dir.Object("ex_user", extendedUser{})

	dir.Clear()
	assert("Clear", testsClear(dir))
	start := time.Now()
	userDB := smUser.Done()
	assert("SetGet", testsSetGet(userDB))
	assert("Update", testsUpdate(userDB))
	assert("Index", testsIndex(smUserIndex.Done()))
	assert("Unique", testsUnique(smUserUnique.Done()))
	assert("AutoIncrement", testsAutoIncrement(userAutoIncrement.Done()))
	assert("MultiGet", testsMultiGet(userMulti.Done()))
	assert("n2n", testsN2N(n2nUser.Done(), n2nChat.Done(), n2nUserChat))
	assert("n2n_data", testsN2NData(n2nUser.Done(), n2nChat.Done(), n2nUserChat2))
	assert("types", testsTypes(dbBigUser.Done()))
	//asset("incField", testsEditField(dbBigUser))
	assert("multiPrimary", testsMultiPrimary(dbMessage.Done(), n2nMessageUser, n2nUser.Done()))

	assert("n2n_self", testsN2NSelf(n2nSelfUser.Done(), n2nSelfUserUser))

	assert("counter", testsCounter(dir))
	assert("n2n_client_counter", testsN2NClientCounter(dir))

	assert("geo_index", testsGeoIndex(dir))

	assert("single_field", testsSingleField(dir))
	fmt.Println("elapsed", time.Since(start))
}
