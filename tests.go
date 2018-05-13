package stored

type testUser struct {
	ID    int64  `stored:"i,primary"`
	Name  string `stored:"n"`
	Login string `stored:"l"`
}

func TestsRun(db *Connection) {
	dbUser := db.Object("user", testUser{}).Primary("i")

	user := testUser{1, "Derp", "derp"}

	dbUser.Set(user)
}
