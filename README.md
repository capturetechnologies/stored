# FoundationDB layer
In development. Use with care, schema are about to change in future.

## Init the database
Before you start STORED document layer must be inited.
This way you will describe all the objects and indexes. Init part should be executed before
the actual part of your application. Example:
```Go
var dbUser, dbChat *stored.Object
var dbUserChat *stored.Relation
func init() { // init database
  db := stored.Connect("./fdb.cluster")
  dir := db.Directory("test")
  dbUser = dir.Object("user", User{})
  dbUser.Primary("id").AutoIncrement("id").Unique("login")
  dbChat = dir.Object("chat", Chat{})
  dbChat.Primary("id").AutoIncrement("id")
  dbUserChat = dbUser.N2N(dbChat)
}
```

All the steps will be described below:
#### Connect to DB
```Go
db := stored.Connect("./fdb.cluster")
```

#### Create directory
All objects should be stored in a directory. Using directories you could separate different logical parts of application.
```Go
dir := db.Directory("test")
```

#### Define a stored document
STORED could use any struct defined in your app as a schema, all you need is to add annotations, like ```stored:"online,mutable"```
**mutual** tag means this field will be changed often, so should not be packed.
```Go
type dbUser struct {
  ID    int64  `stored:"id"`
  Name  string `stored:"name"`
  Login string `stored:"login"`
  Online bool `stored:"online,mutable"`
}
```
List of options available:
- **mutable** indicates that field should kept separately if it going to be changed frequently *(not implemented yet)*

#### Objects initialization
Objects is a main workhorse of stored FoundationDB layer.
You should init objects for all the objects in your application at the initialization part of application.
```Go
dbUser = dir.Object("user", User{}) // User could be any struct in your project
```

#### Primary keys
Alternative to setting primary in struct define annotation is setting it directly.
```Go
dbUser.Primary("id")
```
Primary index could be multiple, for example:
```Go
dbUser.Primary("chat_id", "message_id")
```
In this case the combination of values will be the primary key. Fields order should not change.

#### AutoIncrement
Any key could be setup as autoincremented.
```Go
dbUser.AutoIncrement("id")
```
this way the value of this field will be set automaticly if **Add** `dbUser.Add(&ubser)` method triggered.

#### Indexes
**Unique** creates unique index. You could fetch document directly using this index.
*Add* and *Set* methods would fail if other item with same unique index presented.
```Go
dbUser.Unique("login")
```
**Index** creates regular index. Could be many rows with this index. You are able to fetch first row or list of rows.
```Go
dbUser.Index("login")
```

#### Relations
**N2N** is the most usefull type of relations between database objects. N2N represents *many* to *many* type of connection.
```Go
dbUserChat := dbUser.N2N(dbChat)
```
In this example **dbUserChat** represents relation when any user has unlimited amount of connected chats and any chat has
unlimited amount of connected users. Also it is available to set any data value to each connection (user to chat and chat to user)

## Working with data
If database is successfully inited and schema is set up, you are ok to work with defined database objects.
Make sure that init section is triggered once and before any work with database.

#### Write data to key
This way stored will write user object in set of keys each for each field with `stored:"some_key"` type annotation
```Go
user := User{1, "John", "john"}
dbUser.Set(user)
```
If you have **autoincrement** option at your primary field you are able to Add new rows
```Go
user := User{0, "John", "john"}
dbUser.Add(&user) // after this user.ID will be 1
```

#### Get data by primary ID
You could use method Get to fetch any object from stored by primary key
```Go
user := User{}
err := dbUser.Get(1).Scan(&user)
```
Also you could perform multiget. This way a lot of items will be requested simultaneously
```Go
users := []User{}
err := dbUser.MultiGet([]int64{1,2,3,4}).ScanAll(&users)
```

#### Get data by index
```Go
user := User{}
err := dbUser.GetBy("login", "john").Scan(&user)
```

#### Add new connection using relation
Before using the connection you should create new relation at #init section
```Go
dbUserChat.Set(user, chat)
```
*user* and *chat* objects should contain primary index values
There are cases when inside the relation you want to store some data, for example say:
* in user-chat connection you want to store last *message_id* user read
* in chat-user connection you want to store user join date in this chat
```Go
dbUserChat.SetData(user, chat, stored.Int64(lastMessageID), stored.Now())
```

#### Get list of objects using Relation
Say you have **N2N** relation between users and chats.
* **GetClients** allow you to fetch all objects using host of this relation
```Go
chats := []Chat{}
err = dbUserChat.GetClients(user, nil, 100).ScanAll(&chats)
```
* **GetHosts** allow you to fetch all objects using client of this relation
```Go
users := []User{}
err = dbUserChat.GetHosts(chat, nil, 100).ScanAll(&users)
```

## Testing
Stored has set of unit tests, you can easily run to check that everything set up properly.
Use this simple code snippet to run tests on your database.
```Go
dbDriver := stored.Connect("./fdb.cluster")
stored.TestsRun(dbDriver)
```

# TODO
- [x] Indexes
- [x] AutoIncrement
- [x] Multiple primary
- [ ] Store schema inside FoundationDB
- [ ] Schema migration (delete each item with old schema and set with new one)
