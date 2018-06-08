# FoundationDB layer
In development, schema are about to change in future

## Init the stored schema
Before working with STORED document layer you should init the layer schema,
this way you will describe all the objects and indexes. You should locate this part of your application
before

### Connect to DB
```
db := stored.Connect("./fdb.cluster", "DB")
```

### Create directory
All objects should be stored in a directory. Using directories you could separate different logical parts of application.
```
testDir := db.Directory("test")
```

### Define a stored document
STORED could use any struct defined in your app as a schema, all you need is to add annotations, like ```stored:"id,primary"```
**primary** tag means this is the primary index, which will identify your object.
```
type dbUser struct {
  ID    int64  `stored:"id,primary"`
  Name  string `stored:"name"`
  Login string `stored:"login"`
}
```
List of options available:
- **primary** indicate primary row, *gets* and *sets* will use this row as index
- **autoincrement** indicates that row could be autoincremented, this makes **Add** method available

### Init object
Objects is a main workhorse of stored FoundationDB layer.
You should init objects for all the objects in your application at the initialization part of application.
```
dbUser = testDir.Object("user", User{}) // User could be any struct in your project
```

### Set primary key
Alternative to setting primary in struct define annotation is setting it directly.
```
dbUser.Primary("id")
```

### Add Index
**Unique** creates unique index. You could fetch document directly using this index.
*Add* and *Set* methods would fail if other item with same unique index presented.
```
dbUser.Unique("login")
```
**Index** creates regualar index. Could be many rows with this index. You are able to fetch first row or list of rows.
```
dbUser.Index("login")
```

### Write data to key
This way stored will write user object in set of keys each for each field with `stored:"some_key"` type annotation
```
user := User{1, "John", "john"}
dbUser.Set(user)
```
If you have **autoincrement** option at your primary field you are able to Add new rows
```
user := User{0, "John", "john"}
dbUser.Add(user) // after this user.ID will be 1
```

### Get data by primary ID
You could use method Get to fetch any object from stored by primary key
```
user := User{}
err := dbUser.Get(1).Scan(&user)
```
Also you could perform multiget. This way a lot of items will be requested simultaneously
```
users := []User{}
err := dbUser.MultiGet([]int64{1,2,3,4}).ScanAll(&users)
```

### Get data by index
```
user := User{}
err := dbUser.GetBy("login", "john").Scan(&user)
```

### TODO
- [x] Indexes
- [x] AutoIncrement
- [ ] Store schema inside FoundationDB
- [ ] Schema migration (delete each item with old schema and set with new one)
