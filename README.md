# FoundationDB layer
In development, not ready for use

## Connect to DB
```
db := stored.Connect("./fdb.cluster", "DB")
```

## Init object
```
dbUser = db.Object("user", User{}) // User could be any struct in your project
```

## Set primary key
```
dbUser.Primary("id")
```

## Write data to key
This way stored will write user object in set of keys each for each field with `stored:"some_key"` type annotation
```
user := User{1, "John"}
dbUser.Set(user)
```
example of *dbUser* declaration:
```
type dbUser struct {
	ID    int64  `stored:"i"`
	Name  string `stored:"n"`
}
```
keeping stored keys short helps to save memory

## Get data by primary ID
You could use method Get to fetch any object from stored by primary key
```
user := User{}
err := dbUser.Get(1).Scan(&user)
```
