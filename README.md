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
  cluster := stored.Connect("./fdb.cluster")
  err := cluster.Err()
  if err != nil {
    fmt.Println("DB Problem", err)
    // no "return" needed, if db will wake up, client library will catch it up
  }
  db := cluster.Directory("test")
  user := db.Object("user", User{})
  chat := db.Object("chat", Chat{})
  dbUserChat = user.N2N(chat)
  dbUser = user.Done() // finish object
  dbChat = chat.Done()
}
```
It is required to create variable for each database object. This may seems like unnecessary
code but this approach allowes you to have much more control over your source code. For
example it makes easy to find all usage of specific object in your codebase.

All the steps will be described below:
#### Connect to DB
```Go
cluster := stored.Connect("./fdb.cluster")
```

#### Create directory
All objects should be stored in a directory. Using directories you are able to separate different logical parts of application.
```Go
db := cluster.Directory("test")
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
user := db.Object("user", User{}) // User could be any struct in your project
```

#### Primary keys
Alternative to setting primary in struct define annotation is setting it directly.
```Go
user.Primary("id")
```
Primary index could be multiple, for example:
```Go
user.Primary("chat_id", "message_id")
```
In this case the combination of values will be the primary key. Fields order should not change.

#### IDDate
This is the best way to generate new indexes for objects with fields like int64.
IDDate is most suitable way to generate unique identifiers for the most cases. IDDate will generate
int64 identifier based on current timestamp plus some random seed. This way ID could serve double purpose
storing ID plus storing timestamp of adding the object.
Since int64 is not precise enough to completely illuminate collisions, if field is in primary index at the
moment of Add STORED will check that no other key with such ID presented.

```Go
user.IDDate("id") // field id should be int64 or uint64
```

#### IDRandom
You should use IDRandom when you do not want your ID to unveil timestamp of object creation.
Since int64 is not precise enough to completely illuminate collisions, if field is in primary index at the
moment of Add STORED will check that no other key with such ID presented.

```Go
user.IDRandom("id") // field id should be int64 or uint64
```

#### AutoIncrement
Autoincrement is an easy way to provide automatically incremented values to an field.
At the moment of each Add new object will be written with incremented counter, 1,2,3 etc..

But autoincrement should be used with care, since incrementing of a counter creates collision
of transactions you should not use this options when there are more than 100 Adds per second

Any key could be setup as autoincremented.
```Go
user.AutoIncrement("id")
```
this way the value of this field will be set automaticly if **Add** `dbUser.Add(&user)` method triggered.

#### Indexes
**Unique** creates unique index. You could fetch document directly using this index.
*Add* and *Set* methods would fail if other item with same unique index presented.
```Go
user.Unique("login")
```
**Index** creates regular index. Could be many rows with this index. You are able to fetch first row or list of rows.
```Go
user.Index("login")
```

#### Relations
**N2N** is the most usefull type of relations between database objects. N2N represents *many* to *many* type of connection.
```Go
dbUserChat := user.N2N(chat)
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
err := dbUser.Set(user).Err()
```
If you have **autoincrement** option at your primary field you are able to Add new rows
```Go
user := User{0, "John", "john"}
err := dbUser.Add(&user).Err() // after this user.ID will be 1
```

#### Get data by primary ID
You could use method Get to fetch any object from stored by primary key
```Go
user := User{1}
err := dbUser.Get(&user).Err()
```
Also you could perform multiget. This way a lot of items will be requested simultaneously
```Go
users := []*User{&User{1},&User{2},&User{3},&User{4}}
err := dbUser.MultiGet(users).Err()
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
dbUserChat.ClientData("last_message") // last_message is the field at the client (Chat) object
dbUserChat.HostData("join_date") // last_message is the field at the client (Chat) object
```
Data object could be any type, even the struct.
But complicated struct object could got

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
