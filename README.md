# FoundationDB layer
In development, schema are about to change in future

# Init the STORED FoundationDB layer
Before working with STORED document layer you should init the layer schema,
this way you will describe all the objects and indexes. You should locate this part of your application
before

#### Connect to DB
```
db := stored.Connect("./fdb.cluster", "DB")
```

#### Create directory
All objects should be stored in a directory. Using directories you could separate different logical parts of application.
```
testDir := db.Directory("test")
```

#### Define a stored document
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

#### Init object
Objects is a main workhorse of stored FoundationDB layer.
You should init objects for all the objects in your application at the initialization part of application.
```
dbUser = testDir.Object("user", User{}) // User could be any struct in your project
```

#### Set primary key
Alternative to setting primary in struct define annotation is setting it directly.
```
dbUser.Primary("id")
```

#### Add Index
**Unique** creates unique index. You could fetch document directly using this index.
*Add* and *Set* methods would fail if other item with same unique index presented.
```
dbUser.Unique("login")
```
**Index** creates regualar index. Could be many rows with this index. You are able to fetch first row or list of rows.
```
dbUser.Index("login")
```

#### Add Relation
**N2N** is the most usefull type of relations between database objects. N2N represents *many* to *many* type of connection.
```
dbUserChat := dbUser.N2N(dbChat)
```
In this example **dbUserChat** represents relation when any user has unlimited amount of connected chats and any chat has
unlimited amount of connected users. Also it is available to set any data value to each connection (user to chat and chat to user)

# Working with data
If database is successfully inited and schema is set up, you are ok to work with defined database objects.

#### Write data to key
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

#### Get data by primary ID
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

#### Get data by index
```
user := User{}
err := dbUser.GetBy("login", "john").Scan(&user)
```

#### Add new connection using relation
Before using the connection you should create new relation at #init section
```
dbUserChat.Set(user, chat)
```
*user* and *chat* objects should contain primary index values
There are cases when inside the relation you want to store some data, for example say:
* in user-chat connection you want to store last *message_id* user read
* in chat-user connection you want to store user join date in this chat
```
dbUserChat.SetData(user, chat, stored.Int64(lastMessageID), stored.Now())
```

# TODO
- [x] Indexes
- [x] AutoIncrement
- [ ] Store schema inside FoundationDB
- [ ] Schema migration (delete each item with old schema and set with new one)
