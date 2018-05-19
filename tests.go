package stored

import "fmt"

type testUser struct {
	ID    int64  `stored:"i,primary"`
	Name  string `stored:"n"`
	Login string `stored:"l"`
}

func TestsRun(db *Connection) {
	fmt.Println("start testing")

	dbUser := db.Object("user", testUser{})

	user := testUser{1, "Derp", "derp"}

	err := dbUser.Set(user)
	fmt.Println("user set err", err)

	newUser := testUser{}
	err = dbUser.Get(int64(1)).Scan(&newUser)
	fmt.Println("user get err", err)
	fmt.Println("USER GOT", newUser)
}
