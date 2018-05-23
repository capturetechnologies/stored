# FoundationDB layer
In development, not ready for use

## Connect to DB
```
db := stored.Connect("./fdb.cluster", "DB")
```

## Define a stored document
```
type dbUser struct {
	ID    int64  `stored:"i,primary"`
	Name  string `stored:"n"`
	Login string `stored:"l"`
}
```

## Init object
```
dbUser = db.Object("user", User{}) // User could be any struct in your project
```

## Set primary key
Alternative to setting primary in struct define annotation is setting it directly
```
dbUser.Primary("i")
```

## Add Index
```
dbUser.Index("l")
```

## Write data to key
This way stored will write user object in set of keys each for each field with `stored:"some_key"` type annotation
```
user := User{1, "John", "john"}
dbUser.Set(user)
```
keeping stored keys short helps to save memory

## Get data by primary ID
You could use method Get to fetch any object from stored by primary key
```
user := User{}
err := dbUser.Get(1).Scan(&user)
```

## Get data by index
```
user := User{}
err := dbUser.GetBy("l", "john").Scan(&user)
```
