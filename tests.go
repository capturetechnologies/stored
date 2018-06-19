package stored

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type user struct {
	ID    int    `stored:"id,primary"`
	Login string `stored:"login"`
}

type chat struct {
	ID   int    `stored:"id,primary"`
	Name string `stored:"name"`
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
	ID           int64  `stored:"id,primary"`
	Name         string `stored:"name"`
	Login        string `stored:"login"`
	Score        int    `stored:"score"`
	FullName     string `stored:"full_name"`
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
	err := smUser.Set(user{
		ID:    20,
		Login: "John23",
	})
	if err != nil {
		return err
	}

	newUser := user{}
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
	smUser := dir.Object("small_user", user{})
	bgUser := dir.Object("big_user", bigUser{})

	for i := 0; i < 1; i++ {
		err := smUser.Set(user{2, "some relevant amount of information for all the data should be passed with full object"})
		if err != nil {
			return err
		}

		err = bgUser.Set(bigUser{
			ID:       3,
			Name:     "hello",
			FullName: "Jared sull",
		})
		if err != nil {
			return err
		}

		newUser := user{}
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
	err := smUser.Set(user{40, "john25"}) // user setted
	if err != nil {
		return err
	}

	gotUser := user{}
	err = smUser.GetBy("login", "john25").Scan(&gotUser)
	if err != nil {
		return err
	}
	if gotUser.Login != "john25" {
		return errors.New("User not fetched")
	}
	return nil
}

func testsIndex(smUser *Object) error {
	err := smUser.Set(user{30, "john24"}) // user setted
	if err != nil {
		return err
	}
	gotUser := user{}
	err = smUser.GetBy("login", "john24").Scan(&gotUser)
	if err != nil {
		return err
	}
	if gotUser.Login != "john24" {
		return errors.New("User not fetched")
	}
	return nil
}

func testsClear(dir *Directory) error {
	smUser := dir.Object("small_user", user{})
	err := smUser.Set(user{1, "TmpJohn"})
	if err != nil {
		return err
	}

	err = dir.Clear()
	if err != nil {
		return err
	}

	newUser := user{}
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

func testsN2N(n2nUser *Object, n2nChat *Object, n2nUserChat *Relation) error {
	user1 := user{
		Login: "John",
	}
	user2 := user{
		Login: "Sam",
	}
	user3 := user{
		Login: "Nick",
	}
	chat1 := chat{
		Name: "Chat name 1",
	}
	chat2 := chat{
		Name: "Chat name 2",
	}
	chat3 := chat{
		Name: "Chat name 3",
	}
	chatToDelete := chat{
		Name: "Chat to delete",
	}
	var err error
	err = n2nUser.Add(&user1)
	if err != nil {
		return err
	}
	err = n2nUser.Add(&user2)
	if err != nil {
		return err
	}
	err = n2nUser.Add(&user3)
	if err != nil {
		return err
	}
	err = n2nChat.Add(&chat1)
	if err != nil {
		return err
	}
	err = n2nChat.Add(&chat2)
	if err != nil {
		return err
	}
	err = n2nChat.Add(&chat3)
	if err != nil {
		return err
	}
	err = n2nChat.Add(&chatToDelete)
	if err != nil {
		return err
	}

	err = n2nUserChat.Set(user1, chat1) // add using object
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user1, chatToDelete)
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user1, chat2.ID) // add using clients primary id
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user1.ID, chat3) // add using hosts primary id
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user2, chat1) // add using object
	if err != nil {
		return err
	}
	err = n2nUserChat.Set(user3, chat1) // add using object
	if err != nil {
		return err
	}
	err = n2nUserChat.Delete(user1, chatToDelete) // delete using object
	if err != nil {
		return err
	}

	chats := []chat{}
	err = n2nUserChat.GetClients(user1, nil, 20).ScanAll(&chats)
	if err != nil {
		return err
	}
	if len(chats) == 0 {
		return errors.New("no chats was fetched")
	}
	if len(chats) != 3 {
		return errors.New("incorrect chats amount was fetched")
	}
	if chats[0].Name != "Chat name 1" || chats[0].ID != 1 {
		return errors.New("chat 1 is invalid")
	}
	if chats[1].Name != "Chat name 2" || chats[1].ID != 2 {
		return errors.New("chat 2 is invalid")
	}
	if chats[2].Name != "Chat name 3" || chats[2].ID != 3 {
		return errors.New("chat 3 is invalid")
	}
	var count int64
	count, err = n2nUserChat.GetClientsCount(user1)
	if err != nil {
		return err
	}
	if count != 3 {
		return errors.New("incorrect user counter count: " + strconv.Itoa(int(count)))
	}

	users := []user{}
	err = n2nUserChat.GetHosts(chat1, nil, 2).ScanAll(&users)
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
	users2 := []user{}
	err = n2nUserChat.GetHosts(chat1, 3, 10).ScanAll(&users2)
	if users2[0].Login != "Nick" || users2[0].ID != 3 {
		return errors.New("user 3 is invalid with offset fetching")
	}

	count, err = n2nUserChat.GetHostsCount(chat1)
	if err != nil {
		return err
	}
	if count != 3 {
		return errors.New("incorrect chat counter count: " + strconv.Itoa(int(count)))
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

func testsEditField(dbUser *Object) error {
	u := bigUser{
		Login: "wow",
		Score: 1,
	}
	dbUser.Add(&u)
	dbUser.IncField(u, "score", 1)

	fetchedUser := bigUser{}
	err := dbUser.Get(u.ID).Scan(&fetchedUser)
	if err != nil {
		return err
	}
	if fetchedUser.Score != 2 {
		return errors.New("score has incorrent value after Increment")
	}

	dbUser.UpdateField(u, "score", 4)
	err = dbUser.Get(u.ID).Scan(&fetchedUser)
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
	err = dbMessage.Set(&msg)
	if err != nil {
		return err
	}
	msg2 := message{
		ChatID: 1,
		ID:     2,
		Text:   "second message",
	}
	err = dbMessage.Set(&msg2)
	if err != nil {
		return err
	}

	toFetch := message{
		ChatID: 1,
		ID:     1,
	}

	fetching := message{}
	err = dbMessage.Get(toFetch).Scan(&fetching)
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
	user1 := user{
		Login: "Sender1",
	}
	user2 := user{
		Login: "Sender2",
	}
	err = n2nUser.Add(&user1)
	if err != nil {
		return err
	}
	err = n2nUser.Add(&user2)
	if err != nil {
		return err
	}
	err = n2nMessageUser.Set(msg, user1)
	if err != nil {
		return err
	}
	err = n2nMessageUser.Set(msg, user2)
	if err != nil {
		return err
	}
	users := []user{}
	err = n2nMessageUser.GetClients(msg, nil, 100).ScanAll(&users)
	if err != nil {
		return err
	}
	if len(users) != 2 || users[0].Login != "Sender1" || users[1].Login != "Sender2" {
		return errors.New("relation users fetched error")
	}

	// Gest multi get
	messages := []message{}
	err = dbMessage.GetList(SelectOptions{
		Primary: tuple.Tuple{1},
		Limit:   1000,
	}).ScanAll(&messages)
	if err != nil {
		return err
	}
	if len(messages) != 2 {
		return errors.New("not enough messages")
	}
	if messages[0].ChatID != 1 || messages[1].ID != 2 || messages[0].Text != "first message" || messages[1].Text != "second message" {
		return errors.New("messages list corrupt")
	}

	return nil
}

// TestsRun runs tests for STORED FoundationdDB layer
func TestsRun(db *Connection) {
	dir := db.Directory("tests")
	smUser := dir.Object("setget", user{})
	smUserIndex := dir.Object("index", user{})
	smUserIndex.Index("login")
	smUserUnique := dir.Object("unique", user{})
	smUserUnique.Unique("login")
	userAutoIncrement := dir.Object("increment", userAutoInc{})
	userMulti := dir.Object("multi", userAutoInc{})
	n2nUser := dir.Object("n2n_user", user{})
	n2nUser.AutoIncrement("id")
	n2nChat := dir.Object("n2n_chat", chat{})
	n2nChat.AutoIncrement("id")
	n2nUserChat := n2nUser.N2N(n2nChat)
	n2nUserChat.Counter(true)

	dbBigUser := dir.Object("big_user", bigUser{})
	dbBigUser.AutoIncrement("id")

	dbMessage := dir.Object("message", message{})
	dbMessage.Primary("chat_id", "id")
	n2nMessageUser := dbMessage.N2N(n2nUser)

	dir.Clear()
	asset("Clear", testsClear(dir))
	start := time.Now()
	asset("SetGet", testsSetGet(smUser))
	asset("Index", testsIndex(smUserIndex))
	asset("Unique", testsUnique(smUserUnique))
	asset("AutoIncrement", testsAutoIncrement(userAutoIncrement))
	asset("MultiGet", testsMultiGet(userMulti))
	asset("n2n", testsN2N(n2nUser, n2nChat, n2nUserChat))
	asset("incField", testsEditField(dbBigUser))
	asset("multiPrimary", testsMultiPrimary(dbMessage, n2nMessageUser, n2nUser))
	fmt.Println("elapsed", time.Since(start))
	start = time.Now()
}
